import sys
from pathlib import Path
import re
import time
from typing import Generator

import requests
from bs4 import BeautifulSoup

# sys.path を調整（4階層上へ）
base_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(base_dir))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class OnsenkykaiCrawler(StaticCrawler):
    SITE_ID = "onsen_kyokai"
    BASE_URL = "https://www.spa.or.jp"
    START_URL = "https://www.spa.or.jp/search_p/"
    DELAY = 1.5

    EXTRA_COLUMNS = [
        "address_2",
        "tel_2",
        "location",
        "spa_quality",
        "access_train",
        "access_car",
        "spa_indications",
        "spa_contraindications",
        "organization_name_primary",
        "organization_name_secondary",
        "spa_quality_detailed",
        "facility_id",
    ]

    def prepare(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        self.timeout = 10

    def parse(self, url: str) -> Generator[dict, None, None]:
        item_count = 0

        try:
            regions = self._get_regions()
            self.logger.info(f"Found {len(regions)} regions")

            for region_name, region_param in regions:
                self.logger.info(f"Scraping region: {region_name}")
                for item in self._scrape_region(region_name, region_param):
                    yield item
                    item_count += 1

            self.total_items = item_count
            self.logger.info(f"Total items scraped: {item_count}")
        except Exception as e:
            self.logger.error(f"Error in parse: {e}")

    def _get_regions(self):
        """温泉地の地域一覧を取得"""
        resp = requests.get(self.START_URL, headers=self.headers, timeout=self.timeout)
        resp.encoding = "utf-8"
        soup = BeautifulSoup(resp.text, "html.parser")

        regions = []
        select = soup.select_one('select[name="F_PREFS"]')
        if select:
            for option in select.find_all("option"):
                value = option.get("value")
                text = option.get_text(strip=True)
                if value and value != "":
                    regions.append((text, value))

        return regions

    def _scrape_region(self, region_name, region_param) -> Generator[dict, None, None]:
        """特定地域の温泉施設一覧をクロール"""
        page = 0
        max_pages = 1000  # 無限ループ防止

        while page < max_pages:
            try:
                url = f"{self.START_URL}?F_PREFS={region_param}&pg={page}"
                self.logger.debug(f"Fetching page {page}: {url}")

                resp = requests.get(url, headers=self.headers, timeout=self.timeout)
                resp.encoding = "utf-8"
                soup = BeautifulSoup(resp.text, "html.parser")

                articles = soup.select(".article")
                if not articles:
                    self.logger.debug(f"No articles found on page {page}, stopping pagination")
                    break

                for article in articles:
                    try:
                        item = self._scrape_article(article, region_param, page)
                        if item:
                            yield item
                    except Exception as e:
                        self.logger.warning(f"Error scraping article: {e}")
                        continue

                page += 1
                time.sleep(self.DELAY)

            except Exception as e:
                self.logger.warning(f"Error on page {page}: {e}")
                break

    def _scrape_article(self, article, region_param, current_page):
        """一覧ページから施設情報を抽出"""
        try:
            name_elem = article.select_one(".name a")
            if not name_elem:
                return None

            name = name_elem.get_text(strip=True)
            detail_url = name_elem.get("href")

            location = article.select_one(".location")
            location_text = location.get_text(strip=True) if location else ""

            description = article.select_one("[style*='sales_point']")
            description_text = description.get_text(strip=True) if description else ""

            facility_id = self._extract_facility_id(detail_url)

            # 詳細ページをスクレイプ
            detail_data = self._scrape_detail(detail_url, region_param, current_page)

            # 一覧と詳細データを統合
            item = {
                Schema.NAME: name,
                Schema.ADDR: detail_data.get(Schema.ADDR, ""),
                Schema.TEL: detail_data.get(Schema.TEL, ""),
                Schema.HP: detail_data.get(Schema.HP, ""),
                "address_2": detail_data.get("address_2", ""),
                "tel_2": detail_data.get("tel_2", ""),
                "location": location_text,
                "spa_quality": detail_data.get("spa_quality", ""),
                Schema.DESCRIPTION: description_text,
                "access_train": detail_data.get("access_train", ""),
                "access_car": detail_data.get("access_car", ""),
                "spa_indications": detail_data.get("spa_indications", ""),
                "spa_contraindications": detail_data.get("spa_contraindications", ""),
                "organization_name_primary": detail_data.get("organization_name_primary", ""),
                "organization_name_secondary": detail_data.get("organization_name_secondary", ""),
                "spa_quality_detailed": detail_data.get("spa_quality_detailed", ""),
                "facility_id": facility_id,
            }

            return item

        except Exception as e:
            self.logger.warning(f"Error scraping article: {e}")
            return None

    def _extract_facility_id(self, detail_url):
        """詳細URLからfacility_idを抽出"""
        match = re.search(r"F_ID=(\d+)", detail_url)
        return match.group(1) if match else ""

    def _scrape_detail(self, detail_url, region_param, current_page):
        """詳細ページから情報を抽出"""
        data = {
            Schema.ADDR: "",
            Schema.TEL: "",
            Schema.HP: "",
            "address_2": "",
            "tel_2": "",
            "spa_quality": "",
            "access_train": "",
            "access_car": "",
            "spa_indications": "",
            "spa_contraindications": "",
            "organization_name_primary": "",
            "organization_name_secondary": "",
            "spa_quality_detailed": "",
        }

        try:
            if not detail_url.startswith("http"):
                detail_url = f"https://www.spa.or.jp/search_p/{detail_url}"

            resp = requests.get(detail_url, headers=self.headers, timeout=self.timeout)
            resp.encoding = "utf-8"
            soup = BeautifulSoup(resp.text, "html.parser")

            # テーブルから情報を抽出
            tables = soup.select("table.tbl_info")

            for table in tables:
                rows = table.select("tr")
                for row in rows:
                    th = row.select_one("th")
                    td = row.select_one("td")

                    if not th or not td:
                        continue

                    label = th.get_text(strip=True)
                    value = td.get_text(strip=True)

                    if not value:
                        continue

                    if label in ["所在地", "所在地１"]:
                        if not data[Schema.ADDR]:
                            data[Schema.ADDR] = value
                    elif label == "所在地２":
                        data["address_2"] = value
                    elif label in ["TEL", "TEL１", "電話番号"]:
                        if not data[Schema.TEL]:
                            data[Schema.TEL] = value
                    elif label == "TEL２":
                        data["tel_2"] = value
                    elif label in ["ホームページ", "ホームページURL"]:
                        if "http" in value:
                            data[Schema.HP] = value.split()[0]
                    elif label == "泉質":
                        data["spa_quality"] = value
                    elif label in ["泉質（詳細）", "泉質詳細"]:
                        data["spa_quality_detailed"] = value
                    elif label == "適応症":
                        data["spa_indications"] = value
                    elif label == "禁忌症":
                        data["spa_contraindications"] = value
                    elif label in ["交通(電車)", "交通（電車）", "電車でのアクセス"]:
                        data["access_train"] = value
                    elif label in ["交通(クルマ)", "交通（クルマ）", "車でのアクセス"]:
                        data["access_car"] = value
                    elif "組織名" in label or "問い合わせ先" in label:
                        if "１" in label or "1" in label:
                            if not data["organization_name_primary"]:
                                data["organization_name_primary"] = value
                        elif "２" in label or "2" in label:
                            data["organization_name_secondary"] = value

            time.sleep(self.DELAY)

        except Exception as e:
            self.logger.warning(f"Error scraping detail page {detail_url}: {e}")

        return data


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    crawler = OnsenkykaiCrawler()
    crawler.execute(OnsenkykaiCrawler.START_URL)
