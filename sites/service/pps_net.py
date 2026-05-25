"""
新電力代理店（PPS-NET） — 新電力の代理店情報ディレクトリ

取得対象:
    - 全国574社の新電力代理店・販売企業の情報
    - 企業名、Webサイト URL、サービス説明、連絡先（メール・電話）

取得フロー:
    1. 一覧ページ（/agency）を Playwright でレンダリング
    2. 574件の企業リンク (li.clearfix) から企業 ID を抽出
    3. 各企業の詳細ページ (/agency/{id}) から全情報を取得

実行方法:
    # ローカルテスト
    python scripts/sites/service/pps_net.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id pps_net
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup
from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema


class PpsNetScraper(DynamicCrawler):
    """PPS-NET 新電力代理店スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["agency_id"]

    def get_soup(self, url: str):
        self.page.goto(url, wait_until="domcontentloaded")
        return BeautifulSoup(self.page.content(), "html.parser")

    def parse(self, url: str) -> Generator[dict, None, None]:
        # Get list page and extract agency IDs
        soup = self.get_soup(url)
        agency_items = soup.select("li.clearfix")
        self.total_items = len(agency_items)

        self.logger.info(f"Found {len(agency_items)} agencies")

        for idx, item in enumerate(agency_items, 1):
            try:
                # Extract agency link
                agency_link = item.select_one("a[href*='/agency/']")
                if not agency_link:
                    continue

                detail_url = agency_link.get("href")
                if not detail_url:
                    continue

                # Ensure full URL
                if detail_url.startswith("/"):
                    detail_url = urljoin("https://pps-net.org", detail_url)

                # Extract agency ID from URL
                match = re.search(r"/agency/(\d+)", detail_url)
                if not match:
                    continue

                agency_id = match.group(1)

                # Scrape detail page
                detail_data = self._scrape_detail(detail_url, agency_id)
                if detail_data:
                    yield detail_data

                if idx % 50 == 0:
                    self.logger.info(f"Progress: {idx}/{len(agency_items)}")

            except Exception as e:
                self.logger.error(f"Error scraping item {idx}: {e}")
                continue

    def _scrape_detail(self, detail_url: str, agency_id: str) -> dict | None:
        """Scrape detail page and extract all fields"""
        try:
            soup = self.get_soup(detail_url)

            # Extract agency name
            name_elem = soup.select_one("h3.normal")
            name = name_elem.get_text(strip=True) if name_elem else ""

            # Extract data from table (URL, email, phone, etc.)
            table = soup.select_one("table")
            table_data = {}
            if table:
                rows = table.select("tr")
                for row in rows:
                    th = row.select_one("th")
                    td = row.select_one("td")
                    if th and td:
                        key = th.get_text(strip=True).lower()
                        value = td.get_text(strip=True)
                        table_data[key] = value

            # Extract website URL (from first table row or link)
            website = ""
            if "url" in table_data:
                website = table_data["url"]
            else:
                # Look for href in table
                url_link = soup.select_one("table a[href]")
                if url_link:
                    website = url_link.get("href")

            # Extract description from ordered list
            description = ""
            desc_list = soup.select_one("ol")
            if desc_list:
                list_items = desc_list.select("li")
                descriptions = [li.get_text(strip=True) for li in list_items]
                description = " ".join(descriptions)
            else:
                # Fallback: get text from first content section
                content_div = soup.select_one("main div")
                if content_div:
                    description = content_div.get_text(strip=True)[:500]

            # Try to extract email and phone from table
            email = ""
            phone = ""

            for key, value in table_data.items():
                # Email patterns
                if "email" in key or "mail" in key:
                    if "@" in value:
                        email = value
                # Phone patterns
                if "phone" in key or "tel" in key or "telephone" in key:
                    if any(c.isdigit() for c in value):
                        phone = value

            # Build result
            result = {
                Schema.NAME: name,
                Schema.WEBSITE: website,
                Schema.DESCRIPTION: description,
                Schema.EMAIL: email,
                Schema.PHONE: phone,
                "agency_id": agency_id,
            }

            return result

        except Exception as e:
            self.logger.error(f"Error scraping detail page {detail_url}: {e}")
            return None


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = PpsNetScraper()
    scraper.execute("https://pps-net.org/agency")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
