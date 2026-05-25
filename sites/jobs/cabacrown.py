"""
キャバクラウン（cabacrown.net）— ナイトワーク求人/店舗情報スクレイパー

取得フロー:
    https://cabacrown.net/kw/all/all/?page=N を順にクロールし、
    各ページの詳細リンク（/{region}/jobdetail/?id={id}）を収集。
    詳細ページから NAME / TEL / PREF / ADDR / CAT_SITE / HP を取得する。
"""

import re
import sys
from pathlib import Path
from typing import Generator

root_path = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(root_path))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

BASE_URL = "https://cabacrown.net"
LIST_URL = f"{BASE_URL}/kw/all/all/"

PREF_RE = re.compile(r"^(東京都|北海道|(?:京都|大阪)府|.+?県)")
NAME_SUFFIX_RE = re.compile(
    r"\s*の(?:ナイトワーク|キャバクラ|ガールズバー|スナック|クラブ|ラウンジ|セクキャバ|コンカフェ|メイドカフェ|ホスト)求人.*$"
)
DETAIL_HREF_RE = re.compile(r"^/[^/]+/jobdetail/\?id=\d+$")


class CabaCrownScraper(StaticCrawler):
    """キャバクラウン（cabacrown.net）スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["エリア"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_ids: set[str] = set()
        page = 1

        while True:
            list_url = f"{LIST_URL}?page={page}" if page > 1 else LIST_URL
            soup = self.get_soup(list_url)
            if soup is None:
                break

            new_links = []
            for a in soup.find_all("a", href=DETAIL_HREF_RE):
                href = a["href"]
                if href not in seen_ids:
                    seen_ids.add(href)
                    new_links.append(href)

            if not new_links:
                break

            for href in new_links:
                item = self._scrape_detail(BASE_URL + href)
                if item:
                    yield item

            if not soup.find("a", string=re.compile(r"次へ")):
                break
            page += 1

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        item: dict[str, str] = {Schema.URL: url}

        # 店舗名: h1テキストから求人サフィックスを除去
        h1 = soup.find("h1")
        if h1:
            name = NAME_SUFFIX_RE.sub("", h1.get_text(strip=True))
            if name:
                item[Schema.NAME] = name

        # TEL: tel:リンクから取得
        tel_a = soup.find("a", href=re.compile(r"^tel:"))
        if tel_a:
            item[Schema.TEL] = tel_a["href"][4:].strip()

        # dl/dt/dd 構造から各項目を取得
        for dl in soup.find_all("dl"):
            for dt in dl.find_all("dt"):
                dd = dt.find_next_sibling("dd")
                if not dd:
                    continue
                label = dt.get_text(strip=True)
                self._map_field(item, label, dd)

        if Schema.NAME not in item:
            self.logger.warning("店舗名取得失敗のためスキップ: %s", url)
            return None

        return item

    @staticmethod
    def _dd_text(dd) -> str:
        """aタグを除いたdd内テキスト（「地図を印刷する」等を除去）"""
        parts = []
        for node in dd.children:
            if getattr(node, "name", None) == "a":
                continue
            t = str(node).strip().rstrip("／").strip()
            if t:
                parts.append(t)
        return " ".join(parts).strip()

    def _map_field(self, item: dict, label: str, dd) -> None:
        if label == "面接地住所":
            value = self._dd_text(dd)
            m = PREF_RE.match(value)
            if m:
                item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = value[len(m.group(1)):].strip()
            else:
                item[Schema.ADDR] = value

        elif label == "業種":
            value = dd.get_text(strip=True)
            if value:
                item[Schema.CAT_SITE] = value

        elif label == "お店HP":
            a = dd.find("a", href=re.compile(r"^https?://"))
            if a:
                item[Schema.HP] = a["href"]
            else:
                value = dd.get_text(strip=True)
                if value.startswith(("http://", "https://")):
                    item[Schema.HP] = value

        elif label in ("エリア", "地域", "店舗エリア"):
            value = dd.get_text(strip=True)
            if value:
                item["エリア"] = value


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = CabaCrownScraper()
    scraper.execute(LIST_URL)

    print("\n" + "=" * 60)
    print("実行結果サマリ")
    print("=" * 60)
    print(f"  出力ファイル:     {scraper.output_filepath}")
    print(f"  取得件数:         {scraper.item_count}")
    print(f"  観測カラム数:     {len(scraper.observed_columns)}")
    print(f"  観測カラム:       {scraper.observed_columns}")
    print("=" * 60)
