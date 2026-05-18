import sys
from pathlib import Path
import time
from typing import Generator

# sys.path を調整（4階層上へ）
base_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(base_dir))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class NittenkroCrawler(StaticCrawler):
    """
    JEXA Member Search Database - https://jexadb.nittenkyo.ne.jp/db2/
    日本展示会協会（JEXA）会員データベース
    展示会主催者、出展企業、支援企業などの会員情報を取得
    """

    SITE_ID = "nittenkyo"
    BASE_URL = "https://jexadb.nittenkyo.ne.jp"
    START_URL = "https://jexadb.nittenkyo.ne.jp/json/jexa_memb.tsv"
    DELAY = 0.5
    EXTRA_COLUMNS = [
        "member_id",
        "member_category",
        "member_subcategory",
        "member_type_en",
        "service_regions",
        "building_name",
        "department",
        "contact_person",
        "email",
        "service_categories_jp",
        "company_name_en",
        "address_en",
    ]

    def prepare(self):
        pass

    def parse(self, url: str) -> Generator[dict, None, None]:
        """
        TSVファイルから全会員データを取得
        """
        try:
            response = self._fetch_url(self.START_URL)
            if not response:
                self.logger.error("Failed to fetch TSV file")
                return

            lines = response.split("\n")
            item_count = 0

            # TSVファイルをパース（コメント行「%」をスキップ）
            for line_idx, line in enumerate(lines):
                if not line.strip() or line.startswith("%"):
                    continue

                try:
                    cols = line.split("\t")
                    if len(cols) < 27:
                        continue

                    item = self._parse_row(cols)
                    if item:
                        yield item
                        item_count += 1
                except Exception as e:
                    self.logger.warning(f"Error parsing row {line_idx}: {e}")
                    continue

            self.total_items = item_count
            self.logger.info(f"Total items scraped: {item_count}")

        except Exception as e:
            self.logger.error(f"Error in parse: {e}")

    def _fetch_url(self, url: str) -> str:
        """
        URLからテキストコンテンツを取得
        """
        try:
            import requests

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            response = requests.get(url, headers=headers, timeout=10)
            response.encoding = "utf-8"
            return response.text
        except Exception as e:
            self.logger.error(f"Failed to fetch {url}: {e}")
            return None

    def _parse_row(self, cols: list) -> dict | None:
        """
        TSVの1行をパース
        cols[0]: Member ID (A1309など)
        cols[2]: Company Name
        cols[3]: Company Name (Kana)
        cols[4]: Company Name (English)
        cols[5]: Member Category (会員区分)
        cols[6]: Member Subcategory (会員サブカテゴリー)
        cols[7]: Member Type (English)
        cols[9]: Service Regions (日本語)
        cols[12]: Postal Code
        cols[13]: Address (都道府県+市区町村)
        cols[14]: Building Name
        cols[17]: Phone
        cols[19]: Homepage URL
        cols[22]: Department
        cols[23]: Contact Person
        cols[26]: Email
        """
        try:
            if not cols[0]:
                return None

            # Schema マッピング
            item = {
                Schema.NAME: self._safe_get(cols, 2, "").strip(),
                Schema.NAME_KANA: self._safe_get(cols, 3, "").strip(),
                Schema.POST_CODE: self._safe_get(cols, 12, "").strip(),
                Schema.ADDR: self._safe_get(cols, 13, "").strip(),
                Schema.TEL: self._safe_get(cols, 17, "").strip(),
                Schema.HP: self._safe_get(cols, 19, "").strip(),
                # EXTRA_COLUMNS
                "member_id": self._safe_get(cols, 0, "").strip(),
                "member_category": self._safe_get(cols, 5, "").strip(),
                "member_subcategory": self._safe_get(cols, 6, "").strip(),
                "member_type_en": self._safe_get(cols, 7, "").strip(),
                "service_regions": self._safe_get(cols, 9, "").strip(),
                "building_name": self._safe_get(cols, 14, "").strip(),
                "department": self._safe_get(cols, 22, "").strip(),
                "contact_person": self._safe_get(cols, 23, "").strip(),
                "email": self._safe_get(cols, 26, "").strip(),
                "service_categories_jp": self._safe_get(cols, 35, "").strip(),
                "company_name_en": self._safe_get(cols, 4, "").strip(),
                "address_en": self._safe_get(cols, 15, "").strip(),
            }

            return item

        except Exception as e:
            self.logger.warning(f"Error parsing row: {e}")
            return None

    def _safe_get(self, cols: list, idx: int, default: str = "") -> str:
        """
        リストから安全にインデックスアクセス
        """
        try:
            return cols[idx] if idx < len(cols) else default
        except:
            return default


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    crawler = NittenkroCrawler()
    crawler.execute(NittenkroCrawler.START_URL)
