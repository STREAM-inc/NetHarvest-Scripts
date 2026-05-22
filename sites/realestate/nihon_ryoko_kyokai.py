import sys
from pathlib import Path
import time
from typing import Generator
import re

# sys.path を調整（src/ を含むディレクトリを探す）
base_dir = Path(__file__).resolve().parent.parent.parent.parent
if not (base_dir / "src").exists():
    base_dir = base_dir / "NetHarvest"
sys.path.insert(0, str(base_dir))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema


class NihonRyokoKyokaiCrawler(DynamicCrawler):
    """
    日本旅館協会 宿泊施設検索 - https://www.ryokan.or.jp/search/result/
    全国の旅館・ホテル情報を取得（約1,990件、10件/ページ）
    """

    SITE_ID = "nihon_ryoko_kyokai"
    BASE_URL = "https://www.ryokan.or.jp"
    START_URL = "https://www.ryokan.or.jp/search/result/"
    DELAY = 2.0

    def prepare(self):
        self.total_items = 0

    def parse(self, url: str) -> Generator[dict, None, None]:
        from bs4 import BeautifulSoup

        item_count = 0
        page_num = 1

        while True:
            page_url = self.START_URL if page_num == 1 else f"{self.BASE_URL}/search/result/result?page={page_num}"
            self.logger.info(f"Processing page {page_num}: {page_url}")

            self.page.goto(page_url, wait_until="domcontentloaded", timeout=30000)
            self.page.wait_for_timeout(1000)

            content = self.page.content()
            soup = BeautifulSoup(content, "html.parser")

            # 施設詳細リンクを持つ要素を施設カードとして扱う
            facility_links = soup.find_all("a", href=re.compile(r"^/inn/redirect_detail/\d+"))
            if not facility_links:
                self.logger.info(f"No items found on page {page_num}. Stopping.")
                break

            self.logger.info(f"Found {len(facility_links)} items on page {page_num}")

            for link in facility_links:
                item = self._parse_card(link)
                if item:
                    yield item
                    item_count += 1

            # 「次へ」リンクがあれば次ページへ
            next_link = soup.find("a", string=re.compile(r"次へ"))
            if not next_link:
                self.logger.info(f"No next page after page {page_num}. Stopping.")
                break

            page_num += 1
            time.sleep(self.DELAY)

        self.total_items = item_count
        self.logger.info(f"Total items scraped: {item_count}")

    def _parse_card(self, link) -> dict | None:
        try:
            detail_url = self.BASE_URL + link.get("href", "")

            # 施設名とフリガナ（<br> で区切られている）
            name_parts = [t.strip() for t in link.stripped_strings]
            name = name_parts[0] if name_parts else ""
            name_kana = name_parts[1] if len(name_parts) > 1 else ""

            if not name:
                return None

            # カード全体のコンテナ（施設リンクの親 div）を特定
            card = link.parent
            # h3 等にネストされている場合はさらに上へ
            if card and card.name != "div":
                card = card.parent

            pref = ""
            post_code = ""
            addr = ""
            description = ""

            if card:
                # リンクより後の <p> 要素を順番に処理
                # 構造: <p>都道府県+エリア</p> <p>説明文</p> <p>住所</p> <p>〒... 地図を見る</p> ...
                _LABELS = {"住所", "最寄り駅", "最寄りIC", "その他", "アクセス"}
                after_link = False
                after_link_p = []
                for child in card.children:
                    if child == link:
                        after_link = True
                        continue
                    if after_link and getattr(child, "name", None) == "p":
                        after_link_p.append(child)

                for i, p in enumerate(after_link_p):
                    p_text = p.get_text(" ", strip=True)
                    # 「地図を見る」等リンクテキストを除去
                    p_text_clean = re.sub(r"地図を見る", "", p_text).strip()

                    if "〒" in p_text_clean:
                        pc_match = re.search(r"〒(\d{3}-\d{4})", p_text_clean)
                        if pc_match:
                            post_code = pc_match.group(1)
                        addr = re.sub(r"〒\d{3}-\d{4}\s*", "", p_text_clean).strip()
                    elif i == 0 and p_text_clean not in _LABELS:
                        # リンク直後の最初の <p> = 都道府県+エリア
                        pref = re.split(r"[\s　]", p_text_clean)[0]
                    elif i == 1 and p_text_clean not in _LABELS and "〒" not in p_text_clean:
                        # 2番目の <p> = 説明文
                        description = p_text_clean

            return {
                Schema.NAME: name,
                Schema.NAME_KANA: name_kana,
                Schema.PREF: pref,
                Schema.POST_CODE: post_code,
                Schema.ADDR: addr,
                Schema.DESCRIPTION: description,
                Schema.HP: detail_url,
            }

        except Exception as e:
            self.logger.warning(f"Error parsing card: {e}")
            return None


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    crawler = NihonRyokoKyokaiCrawler()
    crawler.execute(NihonRyokoKyokaiCrawler.START_URL)
