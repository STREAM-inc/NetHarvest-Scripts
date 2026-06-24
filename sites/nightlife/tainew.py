"""
体入ドットコム (tainew.com) — キャバクラ/ガールズバー等 体験入店求人スクレイパー

取得対象:
    - 全地域 (関東/関西/北海道/東北/東海/九州) の掲載店舗 (推定 約2,500件)
    - 店舗名 / 都道府県 / 住所 / TEL / 職種(サイト定義業種) / 営業時間 / 休日 / 店舗HP / SNS
    - サイト固有: 体入時給 / 本入時給 / 応募年齢層 / 設備 / 在籍キャスト / キャストの系統 /
      働く時の服装 / 料金システム / 最寄駅 / 近隣駅 / アクセス / 受付時間 / 担当

取得フロー:
    1. robots.txt の Sitemap 行から各地域の sitemap-shop.xml を収集 (引数 url を起点に派生)
    2. 各 sitemap-shop.xml の <loc> から店舗詳細URL (/shop/view/{id}/) を収集
    3. 店舗詳細ページの定義リスト(.p-simple-row-info)と求人テーブル(.custom/.simple-table)を
       1つのラベル辞書にマージして抽出
    4. 詳細1件を取得するごとに即 yield (重複URLは除外)

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/tainew.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id tainew
"""

from __future__ import annotations

import re
import sys
import warnings
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup, Tag
from bs4 import XMLParsedAsHTMLWarning

# sitemap.xml を html.parser で読む際の警告を抑制 (loc 抽出のみで十分なため)
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

from src.const.schema import Schema
from src.framework.static import StaticCrawler

_PREF_PATTERN = re.compile(
    r"(北海道|東京都|京都府|大阪府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_TEL_PATTERN = re.compile(r"0\d{1,4}-?\d{1,4}-?\d{3,4}")
_MAP_NOTE_RE = re.compile(r"\s*Map\s*(を開く|で表示)\s*$")
# 詳細URL: /shop/view/{id}/ (地域プレフィックス /kansai/ 等が付く場合もある)
_SHOP_PATH_RE = re.compile(r"/shop/view/[^/]+/?$")

# 抽出対象ラベル → EXTRA カラム名 (Schema に該当しないサイト固有の構造化情報)
_EXTRA_LABELS = {
    "体入時給": "体入時給",
    "本入時給": "本入時給",
    "応募年齢層": "応募年齢層",
    "設備": "設備",
    "在籍キャスト": "在籍キャスト",
    "キャストの系統": "キャストの系統",
    "働く時の服装": "働く時の服装",
    "料金システム": "料金システム",
    "最寄駅": "最寄駅",
    "近隣駅": "近隣駅",
    "アクセス": "アクセス",
    "受付時間": "受付時間",
    "担当": "担当",
}


class TainewScraper(StaticCrawler):
    """体入ドットコム スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS: list[str] = list(_EXTRA_LABELS.values())

    # ------------------------------------------------------------------ #
    # メインフロー (引数 url を唯一のルートとして使用)
    # ------------------------------------------------------------------ #

    def parse(self, url: str) -> Generator[dict, None, None]:
        shop_urls = self._collect_shop_urls(url)
        self.total_items = len(shop_urls)
        self.logger.info("対象店舗URL: %d件", len(shop_urls))

        for shop_url in shop_urls:
            try:
                record = self._scrape_detail(shop_url)
            except Exception as e:  # 個別店舗の失敗は握りつぶして続行
                self.logger.warning("詳細取得失敗: %s (%s)", shop_url, e)
                continue
            if record:
                self.logger.info(
                    "取得: %s (%s)",
                    record.get(Schema.NAME) or "?",
                    record.get(Schema.PREF) or "",
                )
                yield record

    # ------------------------------------------------------------------ #
    # sitemap から店舗詳細URLを収集
    # ------------------------------------------------------------------ #

    def _collect_shop_urls(self, root_url: str) -> list[str]:
        sitemaps = self._shop_sitemaps(root_url)
        seen: set[str] = set()
        urls: list[str] = []
        for sm in sitemaps:
            for loc in self._sitemap_locs(sm):
                if _SHOP_PATH_RE.search(loc) and loc not in seen:
                    seen.add(loc)
                    urls.append(loc)
        return urls

    def _shop_sitemaps(self, root_url: str) -> list[str]:
        """robots.txt の Sitemap 行から sitemap-shop.xml を収集。失敗時は既知地域にフォールバック。"""
        robots_url = urljoin(root_url, "/robots.txt")
        sitemaps: list[str] = []
        try:
            resp = self.session.get(robots_url, timeout=self.TIMEOUT)
            resp.raise_for_status()
            for line in resp.text.splitlines():
                line = line.strip()
                if line.lower().startswith("sitemap:"):
                    sm = line.split(":", 1)[1].strip()
                    if sm and "sitemap-shop.xml" in sm:
                        sitemaps.append(urljoin(root_url, sm))
        except Exception as e:
            self.logger.warning("robots.txt 取得失敗: %s (%s)", robots_url, e)

        if not sitemaps:
            sitemaps = [urljoin(root_url, "/sitemap-shop.xml")] + [
                urljoin(root_url, f"/{region}/sitemap-shop.xml")
                for region in ("kansai", "hokkaido", "tohoku", "tokai", "kyushu")
            ]
        return list(dict.fromkeys(sitemaps))

    def _sitemap_locs(self, sitemap_url: str) -> list[str]:
        try:
            resp = self.session.get(sitemap_url, timeout=self.TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            self.logger.warning("sitemap 取得失敗: %s (%s)", sitemap_url, e)
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        return [loc.get_text(strip=True) for loc in soup.find_all("loc") if loc.get_text(strip=True)]

    # ------------------------------------------------------------------ #
    # 詳細ページ
    # ------------------------------------------------------------------ #

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        name_el = soup.select_one(".shop-name") or soup.select_one("h1")
        name = self._clean(name_el.get_text(" ")) if name_el else ""
        if not name:
            return None

        info = self._info_map(soup)
        addr = _MAP_NOTE_RE.sub("", info.get("住所", "")).strip()
        pref = self._pref(addr)
        sns = self._detect_sns(soup)

        record = {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: self._first_tel(info.get("TEL", "")),
            Schema.CAT_SITE: self._clean(info.get("職種", "")),
            Schema.TIME: self._clean(info.get("時間", "")),
            Schema.HOLIDAY: self._clean(info.get("休日", "")),
            Schema.HP: self._homepage(soup, info),
            Schema.LINE: sns["line"],
            Schema.INSTA: sns["insta"],
            Schema.X: sns["x"],
            Schema.FB: sns["fb"],
            Schema.TIKTOK: sns["tiktok"],
        }
        for label, col in _EXTRA_LABELS.items():
            record[col] = self._clean(info.get(label, ""))
        return record

    def _info_map(self, soup: BeautifulSoup) -> dict[str, str]:
        """定義リスト(.p-simple-row-info) と求人テーブル(th/td) を1つのラベル辞書にマージ。

        申込フォーム(<form>)内のテーブル行は入力欄なので除外する。
        """
        info: dict[str, str] = {}

        # 1) 定義リスト形式
        for row in soup.select(".p-simple-row-info"):
            t = row.select_one(".simple-row-info-title")
            d = row.select_one(".simple-row-info-data")
            if not t or not d:
                continue
            label = self._clean(t.get_text())
            if label and label not in info:
                info[label] = self._clean(d.get_text(" "))

        # 2) テーブル形式 (custom-table / simple-table)。フォーム内は除外。
        for tr in soup.select("tr"):
            if tr.find_parent("form"):
                continue
            th = tr.select_one(".custom-table-th, .simple-table-th, th")
            td = tr.select_one(".custom-table-td, .simple-table-td, td")
            if not th or not td:
                continue
            label = self._clean(th.get_text())
            value = self._clean(td.get_text(" "))
            if label and value and label not in info:
                info[label] = value

        return info

    def _homepage(self, soup: BeautifulSoup, info: dict[str, str]) -> str:
        # 「店舗URL」行のアンカー優先、無ければ値テキストが http で始まれば採用
        for row in soup.select(".p-simple-row-info"):
            t = row.select_one(".simple-row-info-title")
            if t and self._clean(t.get_text()) == "店舗URL":
                a = row.select_one(".simple-row-info-data a[href^='http']")
                if a:
                    return a["href"].strip()
        val = info.get("店舗URL", "")
        return val if val.startswith("http") else ""

    def _detect_sns(self, soup: BeautifulSoup) -> dict[str, str]:
        """定義リスト(.simple-row-info-data)内のアンカーのみから SNS を判定 (フッター混入回避)。"""
        sns = {"insta": "", "x": "", "fb": "", "line": "", "tiktok": ""}
        for a in soup.select(".simple-row-info-data a[href]"):
            href = a.get("href", "").strip()
            if not href.startswith("http"):
                continue
            low = href.lower()
            if "instagram.com" in low and not sns["insta"]:
                sns["insta"] = href
            elif (
                ("x.com" in low or "twitter.com" in low)
                and "intent" not in low
                and "share" not in low
                and not sns["x"]
            ):
                sns["x"] = href
            elif "facebook.com" in low and not sns["fb"]:
                sns["fb"] = href
            elif ("line.me" in low or "lin.ee" in low) and not sns["line"]:
                sns["line"] = href
            elif "tiktok.com" in low and not sns["tiktok"]:
                sns["tiktok"] = href
        return sns

    # ------------------------------------------------------------------ #
    # ユーティリティ
    # ------------------------------------------------------------------ #

    @staticmethod
    def _pref(address: str) -> str:
        m = _PREF_PATTERN.search(address)
        return m.group(1) if m else ""

    @staticmethod
    def _first_tel(text: str) -> str:
        if not text:
            return ""
        m = _TEL_PATTERN.search(text)
        return m.group(0) if m else ""

    @staticmethod
    def _clean(value: str | None) -> str:
        if not value:
            return ""
        return re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = TainewScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.tainew.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
