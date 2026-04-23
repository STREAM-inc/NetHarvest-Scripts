"""
au PAY — 使えるお店ブランド一覧スクレイパー

取得対象:
    - au PAY (コード決済) が使えるブランド/店舗一覧
    - カテゴリ別に分類されたブランド名、カテゴリ、ロゴ画像URL、タグ情報

取得フロー:
    https://aupay.wallet.auone.jp/store/list/ (Nuxt SPA) を Playwright で描画し、
    .logo-list ブロックごとにカテゴリ名と所属ブランドをまとめて抽出する。

実行方法:
    # ローカルテスト
    python scripts/sites/service/aupay.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id aupay
"""

import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup
from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

LIST_URL = "https://aupay.wallet.auone.jp/store/list/"


class AuPayScraper(DynamicCrawler):
    """au PAY 使えるお店ブランド一覧スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["ロゴURL", "タグ"]

    def get_soup(self, url: str) -> BeautifulSoup | None:
        try:
            self.page.goto(url, wait_until="domcontentloaded")
            self.page.wait_for_selector(".logo-list-contents", timeout=30000)
            return BeautifulSoup(self.page.content(), "html.parser")
        except Exception as e:
            if self.CONTINUE_ON_ERROR:
                self.error_count += 1
                self.logger.warning("ページ取得エラー (スキップして継続): %s — %s", url, e)
                return None
            raise

    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)
        if soup is None:
            return

        category_blocks = soup.select("div.logo-list")
        items_collected: list[dict] = []

        for block in category_blocks:
            cat_el = block.select_one("p.logo-list-category")
            category = cat_el.get_text(strip=True) if cat_el else ""

            for li in block.select("ul.logo-list-images > li.logo-list-contents"):
                name_el = li.select_one("div.logo-list-item > p")
                name = name_el.get_text(strip=True) if name_el else ""
                if not name:
                    continue

                img_el = li.select_one("div.logo-list-icon img")
                logo = img_el.get("src", "") if img_el else ""
                if logo and logo.startswith("/"):
                    logo = "https://aupay.wallet.auone.jp" + logo

                tags = [
                    t.get_text(strip=True)
                    for t in li.select("ul.StoreList-tag__list li")
                    if t.get_text(strip=True)
                ]

                items_collected.append({
                    Schema.NAME: name,
                    Schema.CAT_SITE: category,
                    Schema.URL: url,
                    "ロゴURL": logo,
                    "タグ": " / ".join(tags),
                })

        self.total_items = len(items_collected)
        self.logger.info("取得対象ブランド数: %d", self.total_items)

        for item in items_collected:
            yield item


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = AuPayScraper()
    scraper.execute(LIST_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
