import sys
from pathlib import Path
import time
from typing import Generator
import re

# sys.path を調整（4階層上へ）
base_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(base_dir))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema


class NihonRyokoKyokaiCrawler(DynamicCrawler):
    """
    日本旅館協会 宿泊施設検索 - https://www.tour.ne.jp/ext/yadonihon/j_hotel/list/
    全国の旅館・ホテル情報を地域別に取得
    約1,744件の施設データを収集
    """

    SITE_ID = "nihon_ryoko_kyokai"
    BASE_URL = "https://www.tour.ne.jp"
    START_URL = "https://www.tour.ne.jp/ext/yadonihon/j_hotel/list/?refpage=form"
    DELAY = 2.0

    EXTRA_COLUMNS = [
        "room_type",
        "meal_type",
        "price_from",
    ]

    def prepare(self):
        """初期化処理"""
        self.regions = [
            "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
            "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
            "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
            "岐阜県", "愛知県", "三重県",
            "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
            "鳥取県", "島根県", "岡山県", "広島県", "山口県",
            "徳島県", "香川県", "愛媛県", "高知県",
            "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県",
            "沖縄県",
        ]
        self.total_items = 0

    def parse(self, url: str) -> Generator[dict, None, None]:
        """複数地域から施設情報を取得"""
        from bs4 import BeautifulSoup

        item_count = 0

        for region_idx, region in enumerate(self.regions):
            try:
                self.logger.info(f"Scraping region {region_idx + 1}/{len(self.regions)}: {region}")

                # ページをロード
                self.page.goto(self.START_URL, wait_until="domcontentloaded", timeout=30000)
                self.page.wait_for_timeout(1000)

                # 地域を選択
                try:
                    # 地域選択フォームを見つけてクリック
                    self.page.click('[class*="destination"]')
                    self.page.wait_for_timeout(500)

                    # 地域を検索/選択
                    self.page.fill('input[placeholder*="市"]', region)
                    self.page.wait_for_timeout(800)

                    # リストから該当地域をクリック
                    self.page.click(f'text="{region}"')
                    self.page.wait_for_timeout(1000)
                except Exception as e:
                    self.logger.warning(f"Could not select region {region}: {e}")
                    continue

                # 検索を実行
                try:
                    self.page.click('button:has-text("検索")')
                    self.page.wait_for_timeout(3000)
                except Exception as e:
                    self.logger.warning(f"Could not trigger search for {region}: {e}")
                    continue

                # 結果をスクレイピング
                content = self.page.content()
                soup = BeautifulSoup(content, "html.parser")

                # テーブルからデータを抽出
                all_trs = soup.find_all("tr")
                data_trs = [tr for tr in all_trs if tr.get_text(strip=True) and "円" in tr.get_text()]

                self.logger.info(f"Found {len(data_trs)} items in {region}")

                # 各行をパース
                for row in data_trs:
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
        """テーブル行から1件分の施設情報をパース"""
        try:
            cells = row.find_all('td')
            if len(cells) < 4:
                return None

            # セルからテキストを抽出
            cell0_text = cells[0].get_text(strip=True)
            cell1_text = cells[1].get_text(strip=True)
            cell2_text = cells[2].get_text(strip=True)
            cell3_text = cells[3].get_text(strip=True)

            # 施設名の抽出（cell0から）
            facility_name = self._extract_facility_name(cell0_text)

            if not facility_name:
                facility_name = cell1_text[:100] if cell1_text else ""

            if not facility_name:
                return None

            # 価格の抽出
            price_match = self._extract_price(cell3_text)

            # Schema マッピング
            item = {
                Schema.NAME: facility_name,
                Schema.ADDR: region,
                # EXTRA_COLUMNS
                "room_type": cell2_text,
                "meal_type": cell1_text,
                "price_from": price_match,
            }

            return item

        except Exception as e:
            self.logger.warning(f"Error parsing row: {e}")
            return None

    def _extract_facility_name(self, text: str) -> str:
        """テキストから施設名を抽出"""
        if not text:
            return ""

        # 括弧内の情報を除去
        name = re.sub(r'（.*|【.*|\[.*|「.*', '', text).strip()

        # 不要な接尾辞を除去
        name = name.split('/')[0].strip()

        return name[:100]

    def _extract_price(self, text: str) -> str:
        """テキストから価格を抽出"""
        if not text:
            return ""

        # 最初の価格を抽出
        match = re.search(r'(\d+,?\d*円)', text)
        if match:
            return match.group(1)

        return text.split('～')[0].strip()[:50]


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    crawler = NihonRyokoKyokaiCrawler()
    crawler.execute(NihonRyokoKyokaiCrawler.START_URL)
