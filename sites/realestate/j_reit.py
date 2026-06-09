"""
Jリート（不動産投資信託）総合情報サイト j-reit.jp のスクレイパー

取得対象:
    - 国内全58銘柄のJリート情報

取得フロー:
    1. /brand/ から銘柄基本情報（証券コード・名称・上場日・決算期・資産運用会社）を取得
    2. /brand/index02.html から運用タイプ・運用対象・保有物件一覧URLを取得
    3. 証券コードをキーとして両ページのデータをマージして出力

実行方法:
    # ローカルテスト
    python scripts/sites/realestate/j_reit.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id j_reit
"""

import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://j-reit.jp"
BRAND_URL = f"{BASE_URL}/brand/"
BRAND2_URL = f"{BASE_URL}/brand/index02.html"

TYPE_LABELS = {
    "type1": "特化型",
    "type2": "複合・総合型",
}

TARGET_LABELS = {
    "target1": "オフィス",
    "target2": "住宅",
    "target3": "商業施設",
    "target4": "物流施設",
    "target5": "ホテル",
    "target6": "ヘルスケア",
    "target7": "その他",
}


class JReitScraper(StaticCrawler):
    """Jリート総合情報サイト (j-reit.jp) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["証券コード", "決算期", "資産運用会社", "資産運用会社HP", "運用タイプ", "運用対象", "保有物件一覧URL"]

    def parse(self, url: str):
        basic = self._parse_basic()
        self.logger.info("基本情報取得: %d 件", len(basic))

        soup2 = self.get_soup(BRAND2_URL)
        if soup2 is None:
            return

        rows = soup2.select("table.tableList tbody tr")
        self.total_items = len(rows)

        for tr in rows:
            try:
                item = self._parse_row(tr, basic)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("行パース失敗: %s", e)

    def _parse_basic(self) -> dict:
        """brand/ ページから証券コードをキーとした基本情報dictを返す"""
        soup = self.get_soup(BRAND_URL)
        if soup is None:
            return {}

        result = {}
        for tr in soup.select("table.tableList tbody tr"):
            code_el = tr.select_one("td.code")
            if not code_el:
                continue
            code = code_el.get_text(strip=True)
            if not code:
                continue

            entry = {}

            corp_a = tr.select_one("td.corporate a")
            if corp_a:
                entry[Schema.NAME] = corp_a.get_text(strip=True)
                href = corp_a.get("href", "")
                if href:
                    entry[Schema.URL] = BASE_URL + href if href.startswith("/") else href

            listing_el = tr.select_one("td.listing")
            if listing_el:
                entry[Schema.OPEN_DATE] = listing_el.get_text(strip=True)

            period_el = tr.select_one("td.period")
            if period_el:
                parts = [s.strip() for s in period_el.stripped_strings]
                entry["決算期"] = "、".join(parts) if parts else ""

            mgmt_a = tr.select_one("td.management a")
            if mgmt_a:
                entry["資産運用会社"] = mgmt_a.get_text(strip=True)
                mgmt_href = mgmt_a.get("href", "")
                if mgmt_href:
                    entry["資産運用会社HP"] = BASE_URL + mgmt_href if mgmt_href.startswith("/") else mgmt_href

            result[code] = entry
        return result

    def _parse_row(self, tr, basic: dict) -> dict | None:
        code_el = tr.select_one("td.code")
        if not code_el:
            return None
        code = code_el.get_text(strip=True)
        if not code:
            return None

        data = {"証券コード": code}
        data.update(basic.get(code, {}))

        # 運用タイプ (type1=特化型, type2=複合・総合型)
        type_td = tr.select_one("td.type")
        if type_td:
            active = []
            for cell in type_td.select("[data-key]"):
                key = cell.get("data-key", "")
                if "●" in cell.get_text() and key in TYPE_LABELS:
                    active.append(TYPE_LABELS[key])
            data["運用タイプ"] = "、".join(active)

        # 運用対象 (target1〜target7)
        target_td = tr.select_one("td.target")
        if target_td:
            active = []
            for cell in target_td.select("[data-key]"):
                key = cell.get("data-key", "")
                if "●" in cell.get_text() and key in TARGET_LABELS:
                    active.append(TARGET_LABELS[key])
            data["運用対象"] = "、".join(active)

        # 保有物件一覧URL
        relinfo_a = tr.select_one("td.relatedinfo a[href]")
        if relinfo_a:
            href = relinfo_a.get("href", "")
            data["保有物件一覧URL"] = BASE_URL + href if href.startswith("/") else href

        # 公式HP
        hp_a = tr.select_one("td.hp a[href]")
        if hp_a:
            data[Schema.HP] = hp_a.get("href", "")

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = JReitScraper()
    scraper.execute(BRAND2_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
