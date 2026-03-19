# scripts/sites/portal/hakkosha.py
"""
白光舎 (hakkosha.com) — クリーニング店舗スクレイパー

取得対象:
    - 店舗名、住所、電話、営業時間、定休日
    - 市区町村 (パンくずリストから)
    - セール期間、目玉情報

取得フロー:
    一覧ページ (/shop/) から全店舗リンクを収集 → 各詳細ページのテーブルからデータ取得

実行方法:
    # ローカルテスト
    python scripts/sites/portal/hakkosha.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id hakkosha
"""

import sys
import time
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://www.hakkosha.com"


class HakkoshaScraper(StaticCrawler):
    """白光舎 クリーニング店舗スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["市区町村", "セール期間", "目玉情報"]

    def parse(self, url: str):
        soup = self.get_soup(url)

        # 一覧ページから全店舗リンクを収集
        shop_links = []
        for a in soup.select('a[href*="/shop/"]'):
            href = a.get("href", "")
            # ナビゲーションの /shop/ 自体を除外
            if href.rstrip("/").endswith("/shop"):
                continue
            name = a.get_text(strip=True)
            if not name:
                continue
            full_url = href if href.startswith("http") else BASE_URL + href
            shop_links.append({"name": name, "url": full_url})

        self.total_items = len(shop_links)
        self.logger.info("店舗数: %d", self.total_items)

        for link in shop_links:
            if self.DELAY > 0:
                time.sleep(self.DELAY)

            try:
                item = self._scrape_detail(link["url"])
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("店舗 %s の取得に失敗: %s", link["name"], e)
                continue

    def _scrape_detail(self, url: str) -> dict | None:
        """詳細ページから店舗情報を取得"""
        soup = self.get_soup(url)

        # 店舗名: h2
        h2 = soup.select_one("h2")
        name = h2.get_text(strip=True) if h2 else ""

        # パンくずリストから市区町村を取得
        # 構造: HOME » お近くの店舗 » {市区町村} » {店舗名}
        area = ""
        breadcrumb = soup.select_one(".panListInner ul")
        if breadcrumb:
            items = breadcrumb.select("li")
            if len(items) >= 3:
                # 3番目の li のテキストから「»」を除去
                area_text = items[2].get_text(strip=True)
                area = area_text.replace("»", "").strip()

        # テーブルからデータ取得 (2つのテーブル: セール情報テーブル + 店舗情報テーブル)
        tables = soup.select("table")
        sale_data = {}
        shop_data = {}

        for table in tables:
            rows = {}
            for tr in table.select("tr"):
                th = tr.select_one("th")
                td = tr.select_one("td")
                if th and td:
                    rows[th.get_text(strip=True)] = td.get_text(strip=True)

            # セール情報テーブル (セール期間 or 目玉情報 がある)
            if "セール期間" in rows or "目玉情報" in rows:
                sale_data = rows
            # 店舗情報テーブル (営業時間 or 電話 がある)
            elif "営業時間" in rows or "電話" in rows:
                shop_data = rows

        item = {
            Schema.NAME: name,
            Schema.ADDR: shop_data.get("住所", ""),
            Schema.TEL: shop_data.get("電話", ""),
            Schema.TIME: shop_data.get("営業時間", ""),
            Schema.HOLIDAY: shop_data.get("定休日", ""),
            Schema.URL: url,
            "市区町村": area,
            "セール期間": sale_data.get("セール期間", ""),
            "目玉情報": sale_data.get("目玉情報", ""),
        }

        return item


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = HakkoshaScraper()
    scraper.execute("https://www.hakkosha.com/shop/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
