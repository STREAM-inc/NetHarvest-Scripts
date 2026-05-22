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
    全国の旅館・ホテル情報を取得
    約2,600件の施設データを収集（30件/ページ × 全ページネーション）
    """

    SITE_ID = "nihon_ryoko_kyokai"
    BASE_URL = "https://www.ryokan.or.jp"
    START_URL = "https://www.ryokan.or.jp/search/result/"
    DELAY = 1.0

    EXTRA_COLUMNS = [
        "room_type",
        "meal_type",
        "price_from",
    ]

    def prepare(self):
        """初期化処理"""
        self.total_items = 0

    def parse(self, url: str) -> Generator[dict, None, None]:
        """全ページをクロール - ページネーション対応"""
        from bs4 import BeautifulSoup

        item_count = 0
        page_num = 1
        max_pages = 100  # 最大100ページを想定（2600 ÷ 30 ≈ 87ページ）

        while page_num <= max_pages:
            self.logger.info(f"Processing page {page_num}...")

            # ページをロード
            self.page.goto(self.START_URL, wait_until="domcontentloaded", timeout=30000)
            self.page.wait_for_timeout(1500)

            # ページコンテンツを取得
            content = self.page.content()
            soup = BeautifulSoup(content, "html.parser")

            # テーブルからデータを抽出
            all_trs = soup.find_all("tr")
            data_trs = [tr for tr in all_trs if tr.get_text(strip=True) and "円" in tr.get_text()]

            if not data_trs:
                self.logger.info(f"No more data found on page {page_num}. Stopping.")
                break

            self.logger.info(f"Found {len(data_trs)} items on page {page_num}")

            # 各行をパース
            for row in data_trs:
                try:
                    item = self._parse_row(row)
                    if item:
                        yield item
                        item_count += 1
                except Exception as e:
                    self.logger.warning(f"Error parsing row on page {page_num}: {e}")
                    continue

            # 次ページボタンを探して押す
            next_button = self._find_next_button()
            if not next_button:
                self.logger.info(f"No next button found on page {page_num}. Stopping.")
                break

            # 次ページをクリック
            try:
                next_button.click()
                self.page.wait_for_timeout(1500)
                page_num += 1
            except Exception as e:
                self.logger.warning(f"Error clicking next button on page {page_num}: {e}")
                break

            time.sleep(self.DELAY)

        self.total_items = item_count
        self.logger.info(f"Total items scraped: {item_count}")

    def _parse_row(self, row) -> dict | None:
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

            # 施設名の抽出
            facility_name = self._extract_facility_name(cell0_text)
            if not facility_name:
                facility_name = cell1_text[:100] if cell1_text else ""
            if not facility_name:
                return None

            # 価格の抽出
            price_match = self._extract_price(cell3_text)

            item = {
                Schema.NAME: facility_name,
                Schema.ADDR: cell0_text[:100] if cell0_text else "",
                "room_type": cell2_text,
                "meal_type": cell1_text,
                "price_from": price_match,
            }

            return item

        except Exception as e:
            self.logger.warning(f"Error parsing row: {e}")
            return None

    def _find_next_button(self):
        """次ページボタン（または次へリンク）を探す"""
        try:
            # 複数のセレクタを試す
            selectors = [
                'a:has-text("次へ")',
                'button:has-text("次へ")',
                '[aria-label*="次"]',
                'a.next',
                '.pagination a[rel="next"]',
            ]

            for selector in selectors:
                try:
                    element = self.page.query_selector(selector)
                    if element:
                        return element
                except:
                    continue

            return None

        except Exception as e:
            self.logger.warning(f"Error finding next button: {e}")
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

        import re
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
