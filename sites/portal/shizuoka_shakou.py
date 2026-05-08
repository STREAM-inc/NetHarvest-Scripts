# scripts/sites/portal/shizuoka_shakou.py
"""
静岡県社交飲食業生活衛生同業組合 (shizuoka-shakou.com) — 加入店舗スクレイパー

取得対象:
    - 一覧ページ (/shop) から詳細ページURLを収集
    - 詳細ページから店舗名、住所、TEL、営業時間、定休日、HP等を取得

取得フロー:
    一覧ページ (10件/ページ, 全16ページ) → 詳細ページ → table.tbl-r02 からデータ取得

実行方法:
    python scripts/sites/portal/shizuoka_shakou.py
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

root_path = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(root_path))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class ShizuokShakouScraper(StaticCrawler):
    """静岡県社交飲食業組合 加入店舗スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["エリア", "FAX", "メール"]

    def parse(self, url: str):
        # --- 1. 全詳細ページ URL を収集 ---
        detail_urls = self._collect_detail_urls(url)
        self.total_items = len(detail_urls)
        self.logger.info("詳細ページ URL 収集完了: %d 件", len(detail_urls))

        # --- 2. 各詳細ページからデータ取得 ---
        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("詳細ページ取得失敗: %s (%s)", detail_url, e)
                continue

    def _collect_detail_urls(self, base_url: str) -> list[str]:
        """一覧ページをページネーションしながら詳細ページ URL を収集する"""
        detail_urls = []
        current_url = base_url

        while current_url:
            self.logger.info("一覧ページ取得: %s", current_url)
            soup = self.get_soup(current_url)
            if soup is None:
                break

            # 各店舗の詳細リンクを収集
            for li in soup.select("ol#post_list > li.article"):
                a_tag = li.select_one("a[href]")
                if a_tag:
                    abs_url = urljoin(current_url, a_tag["href"])
                    if abs_url not in detail_urls:
                        detail_urls.append(abs_url)

            # 次のページ（a.next.page-numbers）
            next_link = soup.select_one("a.next.page-numbers")
            if next_link and next_link.get("href"):
                current_url = urljoin(current_url, next_link["href"])
            else:
                current_url = None

        return detail_urls

    def _scrape_detail(self, detail_url: str) -> dict | None:
        """詳細ページから店舗情報を取得する"""
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        item = {Schema.URL: detail_url}

        # --- 店舗名 ---
        title = soup.select_one("#post_title")
        if title:
            item[Schema.NAME] = title.get_text(strip=True)

        # --- 一覧ページのカテゴリ情報（エリア・ジャンル）を詳細ページからも取得 ---
        # 詳細ページのカテゴリはmeta内に含まれないため、パンくず等から取得を試みる
        # カテゴリ情報はURLパスから地域を推定
        path = detail_url.replace("https://shizuoka-shakou.com/", "")
        region_map = {"central": "中部", "west": "西部", "east": "東部"}
        region_key = path.split("/")[0] if "/" in path else ""
        if region_key in region_map:
            item["エリア"] = region_map[region_key]

        # 都道府県は固定
        item[Schema.PREF] = "静岡県"

        # --- table.tbl-r02 からデータ取得 ---
        table = soup.select_one("table.tbl-r02")
        if table:
            for tr in table.select("tr"):
                th = tr.select_one("th")
                td = tr.select_one("td")
                if not th or not td:
                    continue
                label = th.get_text(strip=True)
                value = td.get_text(strip=True)

                if label == "住所":
                    item[Schema.ADDR] = value
                elif label == "電話番号":
                    item[Schema.TEL] = value
                elif label == "営業時間":
                    item[Schema.TIME] = value
                elif label == "定休日":
                    item[Schema.HOLIDAY] = value
                elif label == "E-MAIL":
                    if value:
                        item["メール"] = value
                elif label == "WEBサイト":
                    a = td.select_one("a[href]")
                    if a and a["href"]:
                        item[Schema.HP] = a["href"]
                elif label == "FAX":
                    if value:
                        item["FAX"] = value

        # 店舗名が取れなかった場合はスキップ
        if Schema.NAME not in item:
            return None

        return item


# =============================================================================
# ローカル実行用エントリーポイント
# =============================================================================
if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = ShizuokShakouScraper()
    scraper.execute("https://shizuoka-shakou.com/shop")

    print("\n" + "=" * 60)
    print("実行結果サマリ")
    print("=" * 60)
    print(f"  出力ファイル:     {scraper.output_filepath}")
    print(f"  取得件数:         {scraper.item_count}")
    print(f"  観測カラム数:     {len(scraper.observed_columns)}")
    print(f"  観測カラム:       {scraper.observed_columns}")
    print("=" * 60)
