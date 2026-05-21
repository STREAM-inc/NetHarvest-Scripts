import sys
from pathlib import Path
import time
from typing import Generator

# sys.path を調整（4階層上へ）
base_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(base_dir))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class BijipuriCrawler(StaticCrawler):
    """
    ビジプリ - Exhibition Hall Information
    https://visipri.com/area_detail/index_exhibition_hall.php
    全国の展示会場情報を取得（印刷・パネル製作サービス関連）
    """

    SITE_ID = "bijipuri"
    BASE_URL = "https://visipri.com"
    START_URL = "https://visipri.com/area_detail/index_exhibition_hall.php"
    DELAY = 0.5
    EXTRA_COLUMNS = ["official_name", "region", "location_detail"]

    def prepare(self):
        pass

    def parse(self, url: str) -> Generator[dict, None, None]:
        """
        ビジプリ展示会場一覧ページをパース
        主要会場とその他会場の2つのテーブルから会場情報を抽出
        """
        try:
            soup = self.get_soup(url)
            if not soup:
                self.logger.error("Failed to fetch page")
                return

            item_count = 0

            # 2つのテーブルを処理（主要会場 + その他会場）
            tables = soup.select("table.pta")
            self.logger.info(f"Found {len(tables)} tables to process")

            for table_idx, table in enumerate(tables):
                table_name = "主要会場" if table_idx == 0 else "その他会場"
                self.logger.info(f"Processing table: {table_name}")

                rows = table.select("tbody > tr")
                self.logger.info(f"Found {len(rows)} rows in {table_name}")

                for row in rows:
                    try:
                        item = self._parse_row(row)
                        if item:
                            yield item
                            item_count += 1
                    except Exception as e:
                        self.logger.warning(f"Error parsing row: {e}")
                        continue

            self.total_items = item_count
            self.logger.info(f"Total exhibition halls scraped: {item_count}")

        except Exception as e:
            self.logger.error(f"Error in parse: {e}")

    def _parse_row(self, row) -> dict | None:
        """
        テーブルの1行をパース
        構造:
          <tr>
            <th class="pta_th">地域</th>
            <td class="pta_td">
              <div class="pta_td_01 pta_td_inn">所在地</div>
              <span class="pta_txt">所在地テキスト</span>
            </td>
            <td class="pta_td">
              <div class="pta_td_02 pta_td_inn">ホール名</div>
              <span class="pta_txt"><a href="...">ホール名</a></span>
            </td>
            <td class="pta_td">
              <div class="pta_td_03 pta_td_inn">正式名称</div>
              <span class="pta_txt">正式名称テキスト</span>
            </td>
          </tr>
        """
        try:
            # Extract region from first <th>
            region_th = row.select_one("th.pta_th")
            region = region_th.get_text(strip=True) if region_th else ""

            # Extract all <td> cells
            tds = row.select("td.pta_td")
            if len(tds) < 3:
                return None

            # Location (所在地)
            location_span = tds[0].select_one("span.pta_txt")
            location = location_span.get_text(strip=True) if location_span else ""

            # Hall Name (ホール名) and URL
            hall_link = tds[1].select_one("span.pta_txt > a")
            hall_name = ""
            detail_url = ""
            if hall_link:
                hall_name = hall_link.get_text(strip=True)
                href = hall_link.get("href", "")
                detail_url = href if href.startswith("http") else self.BASE_URL + href

            # Official Name (正式名称)
            official_span = tds[2].select_one("span.pta_txt")
            official_name = official_span.get_text(strip=True) if official_span else ""

            if not hall_name:
                return None

            item = {
                Schema.NAME: hall_name,
                Schema.ADDR: location,
                Schema.HP: detail_url,
                # EXTRA_COLUMNS
                "official_name": official_name,
                "region": region,
                "location_detail": location,
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

    crawler = BijipuriCrawler()
    crawler.execute(BijipuriCrawler.START_URL)
