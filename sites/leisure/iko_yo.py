"""
いこーよ（全国レジャー施設）

取得対象:
    - 全国のレジャー施設（テーマパーク、動物園、遊園地など）
    - 基本情報：施設名、住所、電話、HP、評価、レビュー数
    - 施設カテゴリ（複数）

取得フロー:
    1. 一覧ページを全ページ巡回（page パラメータ）
    2. 各施設の詳細ページにアクセス
    3. 詳細ページから郵便番号、電話、HP を抽出

実行方法:
    # ローカルテスト
    python scripts/sites/leisure/iko_yo.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id iko_yo
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class IkoYoScraper(StaticCrawler):
    """いこーよ（全国レジャー施設）スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["rating", "review_count"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        """
        一覧ページから施設を取得し、詳細ページから追加情報を抽出
        """
        page = 1
        seen_urls = set()

        while True:
            # Add page parameter if not already in URL
            if "?" in url:
                list_url = f"{url}&page={page}"
            else:
                list_url = f"{url}?page={page}"
            self.logger.info("Fetching list page: %s", list_url)

            try:
                soup = self.get_soup(list_url)
            except Exception as e:
                self.logger.error("Failed to fetch list page %d: %s", page, e)
                break

            # Extract all facility items from list
            items = soup.select("li.p-index-list-item:not(.p-index-list-item--pr)")
            if not items:
                self.logger.info("No items found on page %d, stopping", page)
                break

            # Set total items on first page
            if page == 1:
                # Get total count from page (2,675件中1〜15件)
                count_text = soup.select_one("section div")
                if count_text:
                    match = re.search(r"(\d{1,5})件中", count_text.get_text())
                    if match:
                        self.total_items = int(match.group(1))

            page_item_count = 0
            for item in items:
                try:
                    # Extract list data
                    facility_data = self._extract_list_item(item)
                    if not facility_data:
                        continue

                    facility_url = facility_data.get(Schema.URL)
                    if facility_url in seen_urls:
                        continue
                    seen_urls.add(facility_url)

                    # Fetch detail page for additional info
                    if facility_url:
                        detail_data = self._scrape_detail(facility_url)
                        facility_data.update(detail_data)

                    yield facility_data
                    page_item_count += 1

                except Exception as e:
                    self.logger.warning("Error processing item: %s", e)
                    continue

            self.logger.info("Page %d: extracted %d items", page, page_item_count)
            if page_item_count == 0:
                break

            page += 1

    def _extract_list_item(self, item) -> dict:
        """Extract data from list page item"""
        data = {}

        # Name
        name_el = item.select_one("h3")
        if name_el:
            data[Schema.NAME] = name_el.get_text(strip=True)
        else:
            return None

        # URL (relative path)
        link_el = item.select_one("a[href*='/facilities/']")
        if link_el:
            href = link_el.get("href")
            data[Schema.URL] = f"https://iko-yo.net{href}" if href.startswith("/") else href
        else:
            return None

        # Location and Categories
        # Format: "都道府県市区町村 / カテゴリ1, カテゴリ2, ..."
        location_text = None
        for div in item.select("div"):
            text = div.get_text(strip=True)
            if "県" in text or "市" in text:
                location_text = text
                break

        if location_text:
            # Split by " / " to separate location from categories
            parts = location_text.split(" / ")
            if parts:
                location_part = parts[0].strip()
                # Extract prefecture
                pref_match = re.match(r"^(北海道|(?:東京|大阪|京都|兵庫|福岡)都?道?府?県?|[^/]+?(?:都|道|府|県))", location_part)
                if pref_match:
                    data[Schema.PREF] = pref_match.group(1)
                    # Rest is address
                    data[Schema.ADDR] = location_part[len(pref_match.group(1)):].strip()
                else:
                    data[Schema.ADDR] = location_part

                # Categories (from second part)
                if len(parts) > 1:
                    data[Schema.CAT_SITE] = parts[1].strip()

        # Rating (format: "4.7261件" or similar)
        rating_text = None
        for div in item.select("div"):
            text = div.get_text(strip=True)
            if re.search(r"\d\.\d", text):  # Has decimal number
                rating_text = text
                break

        if rating_text:
            # Extract rating number
            rating_match = re.search(r"(\d\.\d+)", rating_text)
            if rating_match:
                data["rating"] = float(rating_match.group(1))

        # Review/Visit count (usually appears as "X,XXX件" or "X件")
        review_count = None
        for div in item.select("div"):
            text = div.get_text(strip=True)
            if re.search(r"\d{2,}件", text) and "件" in text:
                match = re.search(r"(\d{1,3}(?:,\d{3})*|\d+)件", text)
                if match:
                    review_count = match.group(1)
                    break

        if review_count:
            data["review_count"] = review_count

        return data

    def _scrape_detail(self, detail_url: str) -> dict:
        """Extract additional data from detail page"""
        detail_data = {}

        try:
            soup = self.get_soup(detail_url)
            time.sleep(self.DELAY)  # Rate limiting

            # Postal code - look for pattern XXX-XXXX
            page_text = soup.get_text()
            postal_match = re.search(r"(\d{3}-\d{4})", page_text)
            if postal_match:
                postal = postal_match.group(1)
                if postal and len(postal) > 0:
                    detail_data[Schema.POST_CODE] = postal

            # Telephone - look for phone pattern
            phone_match = re.search(r"(\d{2,4}[-\s]?\d{3,4}[-\s]?\d{4})", page_text)
            if phone_match:
                phone = phone_match.group(1)
                # Normalize: remove spaces and full-width dashes
                phone_clean = re.sub(r"[\s－―\-]", "", phone)
                if phone_clean and len(phone_clean) >= 10:
                    detail_data[Schema.TEL] = phone_clean

            # HP - look for external links (not iko-yo.net)
            hp_found = False
            for link in soup.select("a[href]"):
                href = link.get("href", "")
                text = link.get_text(strip=True)
                if href and "iko-yo.net" not in href and "http" in href and not hp_found:
                    # Skip obvious non-HP links
                    if any(x in href.lower() for x in ["line.me", "google", "otenki"]):
                        continue
                    # Prefer links with "site", "hp", "official" in text
                    if any(x in text.lower() for x in ["site", "hp", "official", "オフィシャル"]):
                        detail_data[Schema.HP] = href
                        hp_found = True
                        break

            # If no HP found with preferred text, take first external http link
            if not hp_found:
                for link in soup.select("a[href*='http']"):
                    href = link.get("href")
                    if href and "iko-yo.net" not in href and not any(x in href.lower() for x in ["google", "otenki", "line.me"]):
                        detail_data[Schema.HP] = href
                        break

        except Exception as e:
            self.logger.warning("Error scraping detail page %s: %s", detail_url, e)

        return detail_data


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = IkoYoScraper()
    scraper.execute("https://iko-yo.net/facilities?genre_cluster=1")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
