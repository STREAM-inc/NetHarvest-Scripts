"""
ナイスタ（naisuta.com）— ナイトワーク求人/店舗情報スクレイパー

取得フロー:
    https://naisuta.com/search/honnyu/ から始まり
    /search/honnyu/N/ をページ順にクロール。
    詳細ページ /shop/{slug}/ から NAME/TEL/PREF/ADDR/POST_CODE/CAT_SITE/HP/LINE を取得。
"""

import re
import sys
from pathlib import Path
from typing import Generator

root_path = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(root_path))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

BASE_URL = "https://naisuta.com"
LIST_URL = f"{BASE_URL}/search/honnyu/"

PREF_RE = re.compile(r"^(東京都|北海道|(?:京都|大阪)府|.+?県)")
POST_CODE_RE = re.compile(r"〒?(\d{3}-\d{4})")
NAME_SUFFIX_RE = re.compile(r"\s*の最新求人情報.*$")
DETAIL_HREF_RE = re.compile(r"^/shop/[^/]+/$")
NEXT_PAGE_RE = re.compile(r"/search/honnyu/(\d+)/")


class NaisutaScraper(StaticCrawler):
    """ナイスタ（naisuta.com）スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["エリア"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_slugs: set[str] = set()
        page = 1

        while True:
            list_url = f"{LIST_URL}{page}/" if page > 1 else LIST_URL
            soup = self.get_soup(list_url)
            if soup is None:
                break

            new_links = []
            for a in soup.find_all("a", href=DETAIL_HREF_RE):
                href = a["href"]
                if href not in seen_slugs:
                    seen_slugs.add(href)
                    new_links.append(href)

            if not new_links:
                break

            for href in new_links:
                item = self._scrape_detail(BASE_URL + href)
                if item:
                    yield item

            # 次ページリンク検出（相対・絶対URL両対応）
            next_page = page + 1
            if not soup.find("a", href=re.compile(rf"/search/honnyu/{next_page}/")):
                break
            page = next_page

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        item: dict[str, str] = {Schema.URL: url}

        # 店舗名: h1からサフィックス除去
        h1 = soup.find("h1")
        if h1:
            name = NAME_SUFFIX_RE.sub("", h1.get_text(strip=True))
            if name:
                item[Schema.NAME] = name

        # job-table (th/td形式) から各項目を取得
        table = soup.find("table", class_="job-table")
        if table:
            for tr in table.find_all("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if th and td:
                    label = th.get_text(strip=True).replace("　", "").replace(" ", "")
                    self._map_field(item, label, td)

        # HP: nightstyle.jpへのリンク（dt/ddで取得できなかった場合）
        if Schema.HP not in item:
            for a in soup.find_all("a", href=re.compile(r"^https://nightstyle\.jp/shop/")):
                item[Schema.HP] = a["href"]
                break

        # LINE: line.meリンク（dt/ddで取得できなかった場合）
        if Schema.LINE not in item:
            for a in soup.find_all("a", href=re.compile(r"^https://line\.me/")):
                item[Schema.LINE] = a["href"]
                break

        if Schema.NAME not in item:
            self.logger.warning("店舗名取得失敗のためスキップ: %s", url)
            return None

        return item

    def _map_field(self, item: dict, label: str, td) -> None:
        value = td.get_text(strip=True)

        if label in ("住所", "面接地住所", "店舗住所"):
            # <br>区切りで行分割し、郵便番号行と住所行のみ結合（最寄り駅情報を除去）
            lines = [
                ln.strip()
                for ln in td.get_text(separator="\n").splitlines()
                if ln.strip()
            ]
            addr_lines = []
            for ln in lines:
                # 地図リンクや駅情報が出現したら終了
                if re.search(r"地図|→|駅|徒歩|バス停", ln):
                    break
                addr_lines.append(ln)
            addr_text = " ".join(addr_lines).strip()

            m_post = POST_CODE_RE.search(addr_text)
            if m_post:
                item[Schema.POST_CODE] = m_post.group(1)
                addr = POST_CODE_RE.sub("", addr_text).strip().lstrip("〒").strip()
            else:
                addr = addr_text

            m_pref = PREF_RE.match(addr)
            if m_pref:
                item[Schema.PREF] = m_pref.group(1)
                item[Schema.ADDR] = addr[len(m_pref.group(1)):].strip()
            else:
                item[Schema.ADDR] = addr

        elif label == "業種":
            # aタグの中にあることがあるのでget_textで取得
            cat = td.get_text(strip=True)
            if cat:
                item[Schema.CAT_SITE] = cat

        elif label in ("TEL", "電話番号"):
            if Schema.TEL not in item:
                # 複数番号が<br>で区切られている場合は最初の1件のみ取得
                first_line = td.get_text(separator="\n").strip().splitlines()[0].strip()
                if first_line:
                    item[Schema.TEL] = first_line

        elif label == "LINE":
            a_tag = td.find("a", href=re.compile(r"^https://line\.me/"))
            if a_tag and Schema.LINE not in item:
                item[Schema.LINE] = a_tag["href"]
            elif value.startswith("https://line.me/") and Schema.LINE not in item:
                item[Schema.LINE] = value

        elif label in ("おみせHP", "お店HP", "公式HP", "HP", "ホームページ"):
            a_tag = td.find("a", href=re.compile(r"^https?://"))
            if a_tag and Schema.HP not in item:
                item[Schema.HP] = a_tag["href"]

        elif label in ("エリア", "地域", "店舗エリア"):
            if value:
                item["エリア"] = value


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = NaisutaScraper()
    scraper.execute(LIST_URL)

    print("\n" + "=" * 60)
    print("実行結果サマリ")
    print("=" * 60)
    print(f"  出力ファイル:     {scraper.output_filepath}")
    print(f"  取得件数:         {scraper.item_count}")
    print(f"  観測カラム数:     {len(scraper.observed_columns)}")
    print(f"  観測カラム:       {scraper.observed_columns}")
    print("=" * 60)
