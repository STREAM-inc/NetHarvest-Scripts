"""
キャバナビ (cabanavi.info) — 東北エリアのキャバクラ・ガールズバー店舗スクレイパー

取得対象:
    - 店名 / 都道府県 / 住所 / TEL / 業種 / 営業時間 / 定休日 / HP
    - エリア / 最寄り駅 / 客席数 / 在籍数 / 平均年齢 / 衣装 / カラオケ
    - 最低料金 / TAXサービス料

取得フロー:
    1. 一覧ページ (/shops/tohoku/?page=N) をページ送り (10件/ページ, 最大15ページ)
    2. 各 div.shopInfo からエリア・最寄り駅・詳細URL を取得
    3. 詳細ページ /shop/{id}/ の dl.detailRow__dl と table.detailRow__table から店舗情報を抽出
    4. 1件取得するごとに即 yield (Pattern B / 早期 yield)

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/cabanavi.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id cabanavi
"""

from __future__ import annotations

import re
import sys
import time
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup

from src.const.schema import Schema
from src.framework.static import StaticCrawler

_PREF_PATTERN = re.compile(
    r"^(北海道|東京都|京都府|大阪府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_TEL_PATTERN = re.compile(r"\d{2,4}-\d{2,4}-\d{4}")
_TOTAL_PATTERN = re.compile(r"(\d+)件")


_REQUEST_DELAY = 1.5  # 全 HTTP リクエスト間の待機秒数 (手動管理)


class CabanaviScraper(StaticCrawler):
    """キャバナビ スクレイパー"""

    DELAY = 0  # フレームワーク yield 後 sleep は使わず parse() 内で手動管理
    EXTRA_COLUMNS = [
        "エリア", "最寄り駅",
        "客席数", "在籍数", "平均年齢", "衣装", "カラオケ",
        "最低料金", "TAXサービス料",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        first_soup = self.get_soup(url)
        if first_soup is None:
            return

        total_el = first_soup.select_one("p.mb-10.pc")
        if total_el:
            m = _TOTAL_PATTERN.search(total_el.get_text())
            self.total_items = int(m.group(1)) if m else None

        page = 1
        while True:
            if page == 1:
                soup = first_soup
            else:
                time.sleep(_REQUEST_DELAY)  # 一覧ページ取得前の明示的待機
                soup = self.get_soup(f"{url}?page={page}")
            if soup is None:
                break

            items = soup.select("div.shopInfo")
            if not items:
                break

            for item in items:
                name_a = item.select_one(".shopInfo__name a")
                if not name_a or not name_a.get("href"):
                    continue

                detail_url = name_a["href"]
                ps = item.select(".shopInfo__name p.typography-body-1")
                area_genre = self._text(ps[0]) if ps else ""
                area, _ = self._split_area_genre(area_genre)
                station = self._text(ps[1]) if len(ps) >= 2 else ""

                time.sleep(_REQUEST_DELAY)  # 詳細ページ取得前の明示的待機
                try:
                    record = self._scrape_detail(detail_url, area=area, station=station)
                except Exception as e:
                    self.logger.warning("詳細取得失敗: url=%s (%s)", detail_url, e)
                    continue

                if record:
                    yield record

            next_link = soup.select_one(f'a[href*="?page={page + 1}"]')
            if not next_link:
                break
            page += 1

    def _scrape_detail(self, detail_url: str, area: str = "", station: str = "") -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        dl_data = self._extract_dl(soup)
        tbl_data = self._extract_table(soup)

        name = self._clean(dl_data.get("店名", ""))
        if not name:
            self.logger.warning("店名が空: %s", detail_url)
            return None

        address = self._clean(dl_data.get("住所", ""))
        pref, addr_body = self._split_pref(address)

        tel_raw = self._clean(dl_data.get("電話番号", ""))
        tel = self._extract_tel(tel_raw)

        hp = self._extract_hp(soup, dl_data)

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr_body or address,
            Schema.TEL: tel,
            Schema.CAT_SITE: self._clean(dl_data.get("業種", "")),
            Schema.TIME: self._clean(dl_data.get("営業時間", "")),
            Schema.HOLIDAY: self._clean(dl_data.get("定休日", "")),
            Schema.HP: hp,
            "エリア": area,
            "最寄り駅": station,
            "客席数": tbl_data.get("客席数", ""),
            "在籍数": tbl_data.get("在籍数", ""),
            "平均年齢": tbl_data.get("平均年齢", ""),
            "衣装": tbl_data.get("衣装", ""),
            "カラオケ": tbl_data.get("カラオケ", ""),
            "最低料金": tbl_data.get("最低料金", ""),
            "TAXサービス料": tbl_data.get("TAX・サービス料", ""),
        }

    def _extract_dl(self, soup: BeautifulSoup) -> dict[str, str]:
        data: dict[str, str] = {}
        for dl in soup.select("dl.detailRow__dl"):
            dt = dl.find("dt")
            dd = dl.find("dd")
            if dt and dd:
                label = dt.get_text(strip=True)
                if label and label not in data:
                    data[label] = dd.get_text(" ", strip=True)
        return data

    def _extract_table(self, soup: BeautifulSoup) -> dict[str, str]:
        data: dict[str, str] = {}
        for table in soup.select("table.detailRow__table"):
            for tr in table.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if th and td:
                    label = self._norm_label(th.get_text(strip=True))
                    if label and label not in data:
                        data[label] = td.get_text(" ", strip=True)
        return data

    def _norm_label(self, text: str) -> str:
        """◆・▼ などの装飾プレフィックスを除去し、スペースを詰める。"""
        return re.sub(r"^[◆▼■●◇▽□○★☆※\s]+", "", text or "").strip()

    def _extract_hp(self, soup: BeautifulSoup, dl_data: dict[str, str]) -> str:
        """ホームページ欄の <a href> を優先し、なければテキスト値を返す。"""
        for dl in soup.select("dl.detailRow__dl"):
            dt = dl.find("dt")
            if dt and "ホームページ" in dt.get_text(strip=True):
                dd = dl.find("dd")
                if dd:
                    a = dd.find("a", href=True)
                    if a:
                        return a["href"].strip()
                    return self._clean(dd.get_text(" ", strip=True))
        return ""

    def _extract_tel(self, raw: str) -> str:
        m = _TEL_PATTERN.search(raw or "")
        return m.group(0) if m else self._clean(raw)

    def _split_area_genre(self, text: str) -> tuple[str, str]:
        """'仙台・国分町/ガールズバー' → ('仙台・国分町', 'ガールズバー')"""
        text = self._clean(text)
        if "/" in text:
            parts = text.split("/", 1)
            return parts[0].strip(), parts[1].strip()
        return text, ""

    def _split_pref(self, address: str) -> tuple[str, str]:
        if not address:
            return "", ""
        m = _PREF_PATTERN.match(address)
        if not m:
            return "", address
        return m.group(1), address[m.end():].strip()

    def _text(self, node) -> str:
        if node is None:
            return ""
        return self._clean(node.get_text(" ", strip=True))

    def _clean(self, value: str | None) -> str:
        if not value:
            return ""
        return re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = CabanaviScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えると挙動がズレる。
    scraper.execute("https://cabanavi.info/shops/tohoku/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
