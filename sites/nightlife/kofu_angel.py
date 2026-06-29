"""
山梨ナイトナビ — 山梨県の夜のお店(キャバクラ・クラブ・ガールズバー等)スクレイパー

取得対象:
    - 山梨県内のナイト系店舗情報 (店舗名・カナ・業種・市町村・TEL・住所・
      営業時間・定休日・HP・LINE・各種SNS)

取得フロー:
    一覧ページ (/night/shop/) から店舗詳細URL (/night/shop/area-{N}/{shopId}/) を収集
        → 各詳細ページを取得して 1 件ずつ即 yield (Pattern B)
    ※ ページネーションは無し (一覧1ページに全店舗を掲載)

除外フィールド (著作権リスク回避):
    - 料金システム / 店舗PR (自由記述の長文プロースのため除外)

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/kofu_angel.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id kofu_angel
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

from bs4 import BeautifulSoup

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler


class KofuAngelCrawler(StaticCrawler):
    """山梨ナイトナビ クローラー — 山梨県内のナイト系店舗情報を取得"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["エリア"]  # 市町村 (例: 甲府市 / 富士吉田市)

    # 店舗詳細URL: /night/shop/area-{areaId}/{shopId}/
    _SHOP_PATH_RE = re.compile(r"^/night/shop/area-\d+/\d+/$")

    # th の img ファイル名 → フィールド種別
    _ICON_TIME = "time"
    _ICON_MAP = "map"
    _ICON_WEB = "web"
    _ICON_SNS = "sns"

    # data-gtm-action → Schema 定数
    _SNS_ACTION_MAP = {
        "twitter": Schema.X,
        "x": Schema.X,
        "instagram": Schema.INSTA,
        "facebook": Schema.FB,
        "tiktok": Schema.TIKTOK,
        "line@": Schema.LINE,
    }

    _PREF_RE = re.compile(
        r"^(北海道|東京都|大阪府|京都府|"
        r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|新潟|富山|"
        r"石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|和歌山|鳥取|島根|岡山|"
        r"広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県)"
    )

    # ------------------------------------------------------------------
    # メインフロー
    # ------------------------------------------------------------------

    def parse(self, url: str) -> Generator:
        # 引数 url を唯一のルートとし、一覧ページを派生させる
        # url = https://www.kofu-angel.net/night/  →  .../night/shop/
        list_url = urljoin(url, "shop/")

        shop_urls = self._collect_shop_urls(url, list_url)
        self.total_items = len(shop_urls)
        self.logger.info("店舗詳細URL候補数: %d", self.total_items)

        saved = 0
        failed = 0
        for index, shop_url in enumerate(shop_urls, start=1):
            try:
                record = self._scrape_detail(shop_url)
            except Exception as e:  # noqa: BLE001 個別エラーはスキップして継続
                failed += 1
                self.logger.warning(
                    "詳細取得失敗: %d/%d 取得済み%d件 失敗%d件 URL=%s (%s)",
                    index, self.total_items, saved, failed, shop_url, e,
                )
                continue

            if record and record.get(Schema.NAME):
                saved += 1
                self.logger.info(
                    "詳細取得OK: %d/%d 取得済み%d件 失敗%d件 店舗=%s",
                    index, self.total_items, saved, failed,
                    record.get(Schema.NAME),
                )
                yield record  # 1 件ごとに即 yield (途中中断でも無駄通信を防ぐ)
            else:
                failed += 1
                self.logger.warning(
                    "詳細取得スキップ(店舗名なし): %d/%d URL=%s",
                    index, self.total_items, shop_url,
                )

        self.logger.info(
            "詳細取得完了: 候補%d件 取得済み%d件 失敗/スキップ%d件",
            self.total_items, saved, failed,
        )

    # ------------------------------------------------------------------
    # 一覧 → 店舗URL収集
    # ------------------------------------------------------------------

    def _collect_shop_urls(self, root_url: str, list_url: str) -> list[str]:
        soup = self.get_soup(list_url)
        if soup is None:
            return []

        shop_urls: list[str] = []
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            path = href.split("?", 1)[0].split("#", 1)[0]
            if not self._SHOP_PATH_RE.match(path):
                continue
            full = urljoin(root_url, path)
            if full not in seen:
                seen.add(full)
                shop_urls.append(full)
        return shop_urls

    # ------------------------------------------------------------------
    # 詳細ページ解析
    # ------------------------------------------------------------------

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        record: dict = {Schema.URL: detail_url}

        wrapper = soup.select_one(".shop-title-wrapper")
        if wrapper is None:
            return None

        # 店舗名 (英字表記 / 正式名)
        strong = wrapper.select_one("strong")
        name = self._c(strong.get_text(" ", strip=True)) if strong else ""
        if not name:
            return None
        record[Schema.NAME] = name

        # 名称カナ
        kana = wrapper.select_one(".shop-kana")
        if kana:
            record[Schema.NAME_KANA] = self._c(kana.get_text(" ", strip=True))

        # 業種 / エリア(市町村) : (業種/市町村) の2リンク構成
        info = wrapper.select_one(".shop-info")
        if info:
            links = [self._c(a.get_text(" ", strip=True)) for a in info.find_all("a")]
            if len(links) >= 1 and links[0]:
                record[Schema.CAT_SITE] = links[0]
            if len(links) >= 2 and links[1]:
                record["エリア"] = links[1]

        # 電話番号
        tel = soup.select_one(".shop-tel")
        if tel:
            tel_text = re.sub(r"[^\d\-+]", "", tel.get_text(" ", strip=True))
            if tel_text:
                record[Schema.TEL] = tel_text

        # 営業時間 / 定休日 / 住所 / HP : table.wrap-shop-info の th アイコンで判別
        self._parse_info_table(soup, record)

        # SNS / LINE : data-gtm-action 付きリンクを横断的に収集
        for a in soup.select("a[data-gtm-action][href]"):
            action = (a.get("data-gtm-action") or "").strip().lower()
            schema_key = self._SNS_ACTION_MAP.get(action)
            href = (a.get("href") or "").strip()
            if schema_key and href and schema_key not in record:
                record[schema_key] = href

        return record

    def _parse_info_table(self, soup: BeautifulSoup, record: dict) -> None:
        table = soup.select_one("table.wrap-shop-info")
        if table is None:
            return

        for tr in table.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if th is None or td is None:
                continue

            # 定休日: th 内に <div class="shop_rest">休</div>
            if th.select_one(".shop_rest"):
                holiday = self._c(td.get_text(" ", strip=True))
                if holiday:
                    record[Schema.HOLIDAY] = holiday
                continue

            img = th.find("img")
            icon = ""
            if img and img.get("src"):
                m = re.search(r"/(\w+)\.png", img["src"])
                if m:
                    icon = m.group(1)

            if icon == self._ICON_TIME:
                val = self._c(td.get_text(" ", strip=True))
                if val:
                    record[Schema.TIME] = val
            elif icon == self._ICON_MAP:
                addr = self._c(td.get_text(" ", strip=True))
                if addr:
                    pref, body = self._split_pref(addr)
                    if pref:
                        record[Schema.PREF] = pref
                    record[Schema.ADDR] = body or addr
            elif icon == self._ICON_WEB:
                a = td.find("a", href=True)
                hp = a["href"].strip() if a else self._c(td.get_text(" ", strip=True))
                if hp:
                    record[Schema.HP] = hp
            # icon == sns / money 等はここでは扱わない (SNS は別ループ、money は除外)

    # ------------------------------------------------------------------
    # ユーティリティ
    # ------------------------------------------------------------------

    def _split_pref(self, address: str) -> tuple[str, str]:
        if not address:
            return "", ""
        m = self._PREF_RE.match(address)
        if not m:
            return "", address
        return m.group(1), address[m.end():].strip()

    def _c(self, value: str | None) -> str:
        if not value:
            return ""
        return re.sub(r"\s+", " ", str(value)).strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = KofuAngelCrawler()
    # 🔒 sites.yml に登録する url と完全一致させる (SSOT = sites.yml)
    scraper.execute("https://www.kofu-angel.net/night/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
