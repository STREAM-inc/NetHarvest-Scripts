"""
派遣キャバ夜遊びマップ (haken-kyaba.com/yoasobi-map/tokyo/) — 店舗情報スクレイパー

取得フロー:
    一覧ページ（WordPress /page/N/ 形式、全5ページ程度）から詳細ページURLを収集し、
    各詳細ページの <div class="basic-div"><table> をパースして店舗情報を取得する。
"""

import re
import sys
from pathlib import Path
from typing import Generator

root_path = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(root_path))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

LIST_URL = "https://haken-kyaba.com/yoasobi-map/tokyo/"

PREF_RE = re.compile(r"^(東京都|北海道|(?:京都|大阪)府|.+?県)")
DETAIL_URL_RE = re.compile(r"https://haken-kyaba\.com/yoasobi-map/tokyo/[^/]+/$")


class HakenKyabaScraper(StaticCrawler):
    """派遣キャバ夜遊びマップスクレイパー"""

    DELAY = 1.5

    def parse(self, url: str) -> Generator[dict, None, None]:
        visited: set[str] = set()
        current_url = LIST_URL

        while current_url:
            soup = self.get_soup(current_url)
            if soup is None:
                break

            for article in soup.find_all("article", class_="post"):
                detail_url = self._extract_detail_url(article)
                if detail_url and detail_url not in visited:
                    visited.add(detail_url)
                    item = self._parse_detail(detail_url)
                    if item:
                        yield item

            next_a = soup.find("a", class_="next page-numbers")
            current_url = next_a["href"] if next_a else None

    def _extract_detail_url(self, article) -> str | None:
        for a in article.find_all("a", href=DETAIL_URL_RE):
            return a["href"]
        return None

    def _parse_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        basic_div = soup.find("div", class_="basic-div")
        if not basic_div:
            return None

        table = basic_div.find("table")
        if not table:
            return None

        item: dict[str, str] = {Schema.URL: url}
        for tr in table.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not (th and td):
                continue
            label = th.get_text(strip=True)
            self._map_field(item, label, td)

        if Schema.NAME not in item:
            return None

        return item

    def _map_field(self, item: dict, label: str, td) -> None:
        value = td.get_text(strip=True)

        if label == "店名":
            if value:
                item[Schema.NAME] = value

        elif label == "業種":
            if value:
                item[Schema.CAT_SITE] = value

        elif label == "所在地":
            m = PREF_RE.match(value)
            if m:
                item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = value[len(m.group(1)):].strip()
            else:
                item[Schema.ADDR] = value

        elif label == "電話番号":
            if value:
                item[Schema.TEL] = value

        elif label in ("HP", "ホームページ", "公式サイト", "HP・SNS"):
            a = td.find("a", href=re.compile(r"^https?://"))
            if a:
                item[Schema.HP] = a["href"]
            elif value.startswith(("http://", "https://")):
                item[Schema.HP] = value

        elif label == "営業時間":
            if value:
                item["営業時間"] = value

        elif label == "定休日":
            if value:
                item["定休日"] = value


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = HakenKyabaScraper()
    scraper.execute(LIST_URL)

    print("\n" + "=" * 60)
    print("実行結果サマリ")
    print("=" * 60)
    print(f"  出力ファイル:     {scraper.output_filepath}")
    print(f"  取得件数:         {scraper.item_count}")
    print(f"  観測カラム数:     {len(scraper.observed_columns)}")
    print(f"  観測カラム:       {scraper.observed_columns}")
    print("=" * 60)
