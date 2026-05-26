"""
対象サイト: https://www.golf-ngk.or.jp/course/
日本ゴルフ場経営者協会（NGK）のゴルフ場検索ページから全国のゴルフ場情報を取得
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Generator

from urllib.parse import urljoin

root_path = Path(__file__).resolve().parent.parent.parent.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from src.const.schema import Schema
from src.framework.static import StaticCrawler


class JapanGolfNgkCrawler(StaticCrawler):
    """日本ゴルフ場経営者協会（NGK）からゴルフ場情報を取得する静的スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["holes"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 全地域ページのURL一覧
        regions = [
            {
                "name": "北海道",
                "url": "https://www.golf-ngk.or.jp/course/hokkaido.html",
            },
            {
                "name": "東北/関東",
                "url": "https://www.golf-ngk.or.jp/course/touhoku_kantou.html",
            },
            {
                "name": "東海",
                "url": "https://www.golf-ngk.or.jp/course/toukai.html",
            },
            {
                "name": "関西",
                "url": "https://www.golf-ngk.or.jp/course/kansai.html",
            },
            {
                "name": "九州",
                "url": "https://www.golf-ngk.or.jp/course/kyusyu.html",
            },
        ]

        all_items = []

        # 各地域ページを巡回
        for region in regions:
            self.logger.info("地域: %s (%s)", region["name"], region["url"])
            soup = self.get_soup(region["url"])
            if soup is None:
                self.logger.warning("地域ページ取得失敗: %s", region["name"])
                continue

            # テーブル内の各行をパース
            table = soup.select_one("table.table-05")
            if not table:
                self.logger.warning("テーブルが見つかりません: %s", region["name"])
                continue

            rows = table.select("tr")
            self.logger.info("地域 %s: %d件のコース", region["name"], len(rows))

            for row in rows:
                try:
                    item = self._extract_golf_course(row, region["url"])
                    if item:
                        all_items.append(item)
                except Exception as e:
                    self.logger.error("行のパース失敗: %s", str(e))
                    continue

        # 総件数を設定（進捗表示用）
        self.total_items = len(all_items)
        self.logger.info("全地域から %d件のコースを取得", self.total_items)

        # アイテムを1件ずつ出力
        for i, item in enumerate(all_items, 1):
            self.logger.info("処理中 [%d/%d]", i, len(all_items))
            yield item

    def _extract_golf_course(self, row, base_url: str) -> dict:
        """テーブル行からゴルフ場情報を抽出"""
        tds = row.select("td")
        if len(tds) < 4:
            return None

        # カラム1: コース名（リンク付き）
        name_cell = tds[0]
        name = ""
        course_url = ""
        a_tag = name_cell.select_one("a[href]")
        if a_tag:
            name = a_tag.get_text(strip=True)
            course_url = (a_tag.get("href") or "").strip()
        if not name:
            name = name_cell.get_text(strip=True)

        # カラム2: 所在地（住所）
        address = tds[1].get_text(strip=True) if len(tds) > 1 else ""

        # カラム3: 電話番号
        phone = tds[2].get_text(strip=True) if len(tds) > 2 else ""

        # カラム4: ホール数
        holes_text = tds[3].get_text(strip=True) if len(tds) > 3 else ""
        holes = holes_text if holes_text else ""

        return {
            Schema.NAME: name,
            Schema.ADDR: address,
            Schema.TEL: phone,
            Schema.HP: course_url,
            "holes": holes,
        }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="日本ゴルフ場経営者協会からゴルフ場情報を取得して CSV 保存"
    )
    parser.add_argument(
        "--url",
        default="https://www.golf-ngk.or.jp/course/hokkaido.html",
        help="取得対象の URL（実装では全地域を自動巡回）",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)

    scraper = JapanGolfNgkCrawler()
    scraper.site_name = "japan_golf_ngk"
    scraper.site_id = ""
    scraper.execute(args.url)

    print(f"CSV保存先: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")


if __name__ == "__main__":
    main()
