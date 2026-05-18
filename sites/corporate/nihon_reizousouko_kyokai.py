import sys
from pathlib import Path
import time
from typing import Generator

# sys.path を調整（4階層上へ）
base_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(base_dir))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class NihonReizousoukokKyokaiCrawler(StaticCrawler):
    """
    日本冷蔵倉庫協会 会員企業検索 - https://www.jarw.or.jp/find/memberlist/
    全国の冷蔵倉庫企業約1,500社以上の情報を取得
    """

    SITE_ID = "nihon_reizousouko_kyokai"
    BASE_URL = "https://www.jarw.or.jp"
    START_URL = "https://www.jarw.or.jp/find/memberlist/"
    DELAY = 0.6
    EXTRA_COLUMNS = [
        "district",
        "website_url",
        "fax",
    ]

    def prepare(self):
        """地域グループリストを初期化"""
        self.regions = [
            "hokkaido",
            "touhoku",
            "shutoken",
            "kantoukoushinetsu",
            "hokuriku",
            "chubu",
            "kinki",
            "chugoku",
            "shikoku",
            "kyushuokinawa",
        ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        """
        全地域グループの会員企業情報を取得
        各地域内に複数のテーブル（県別）がある場合も対応
        """
        item_count = 0

        for region_idx, region in enumerate(self.regions):
            try:
                region_url = f"{self.START_URL}{region}"
                self.logger.info(f"Scraping region {region_idx + 1}/{len(self.regions)}: {region}")

                soup = self.get_soup(region_url)
                if not soup:
                    self.logger.warning(f"Failed to get soup from {region}")
                    continue

                # 地域内のすべてのテーブルを取得（複数県の場合対応）
                tables = soup.find_all('table')
                if not tables:
                    self.logger.warning(f"No tables found in {region}")
                    continue

                self.logger.info(f"Found {len(tables)} tables in {region}")

                # 各テーブルをパース
                for table_idx, table in enumerate(tables):
                    rows = table.find_all('tr')

                    # ヘッダ行と説明行をスキップ（最初の3行）
                    # Row 0: 県名と協会名
                    # Row 1: グループ（エリア）
                    # Row 2: カラムヘッダ（地区、会社名、住所、電話、HP、FAX、冷凍）
                    data_rows = rows[3:] if len(rows) > 3 else []

                    if data_rows:
                        self.logger.debug(f"Table {table_idx}: {len(data_rows)} potential data rows")

                        # 各行をパース
                        for row in data_rows:
                            try:
                                item = self._parse_row(row, region)
                                if item:
                                    yield item
                                    item_count += 1
                            except Exception as e:
                                self.logger.warning(f"Error parsing row in {region}: {e}")
                                continue

                time.sleep(self.DELAY)

            except Exception as e:
                self.logger.error(f"Error processing region {region}: {e}")
                continue

        self.total_items = item_count
        self.logger.info(f"Total items scraped: {item_count}")

    def _parse_row(self, row, region: str) -> dict | None:
        """
        テーブル行から1件分の企業情報をパース
        """
        try:
            cells = row.find_all('td')
            if len(cells) < 3:
                return None

            # グループ分け行（セルが1-2個）をスキップ
            if len(cells) < 4:
                return None

            # 各セルからテキストを抽出
            district = cells[0].get_text(strip=True)
            company_name = cells[1].get_text(strip=True)
            address = cells[2].get_text(strip=True)
            phone = cells[3].get_text(strip=True) if len(cells) > 3 else ""
            website = cells[4].get_text(strip=True) if len(cells) > 4 else ""
            fax = cells[5].get_text(strip=True) if len(cells) > 5 else ""

            # 企業名が空の場合はスキップ（グループ分けや説明行）
            if not company_name or len(company_name) < 2:
                return None

            # 数値だけの行（冷凍能力のみ）はスキップ
            if company_name.isdigit():
                return None

            # 正規化：地区が空の場合は直前の地区を使用できるように
            # ここでは空の場合は空文字列のままとする

            # Schema マッピング
            item = {
                Schema.NAME: company_name,
                Schema.ADDR: address,
                Schema.TEL: phone,
                Schema.PREF: region,
                # EXTRA_COLUMNS
                "district": district,
                "website_url": website,
                "fax": fax,
            }

            return item

        except Exception as e:
            self.logger.warning(f"Error parsing row: {e}")
            return None


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    crawler = NihonReizousoukokKyokaiCrawler()
    crawler.execute(NihonReizousoukokKyokaiCrawler.START_URL)
