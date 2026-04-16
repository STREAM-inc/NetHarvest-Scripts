import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://397.cafe"
START_URL = "https://397.cafe/search.php?mode=&page=1"

MAX_PAGES = 30


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("\u3000", " ")).strip()


class Cafe397Scraper(StaticCrawler):
    """397.cafe 求人店舗情報スクレイパー（397.cafe）"""

    DELAY = 1.5
    EXTRA_COLUMNS: list[str] = []

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_links: set[str] = set()
        seen_names: set[str] = set()

        for page in range(1, MAX_PAGES + 1):
            list_url = f"{BASE_URL}/search.php?mode=&page={page}"
            soup = self.get_soup(list_url)
            if soup is None:
                break

            links = [
                a["href"].strip()
                for a in soup.find_all("a", href=True)
                if "shop.php" in a["href"]
            ]
            if not links:
                break

            for href in links:
                full = href if href.startswith("http") else urljoin(BASE_URL, href)
                if full in seen_links:
                    continue
                seen_links.add(full)
                item = self._scrape_detail(full)
                if item and item.get(Schema.NAME):
                    key = item[Schema.NAME]
                    if key not in seen_names:
                        seen_names.add(key)
                        yield item

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        name_tag = soup.find(class_="shop-name")
        if name_tag:
            data[Schema.NAME] = _clean(name_tag.get_text())

        # 住所: p.pink-p "住所" の次の兄弟
        label = soup.find("p", class_="pink-p", string="住所")
        if label:
            sib = label.find_next_sibling("p")
            if sib:
                data[Schema.ADDR] = _clean(sib.get_text())

        # TEL: テキスト全体から正規表現で抽出
        tel_match = re.search(r"0\d{1,4}[-\u2010\u2011\u2012\u2013\u2014\uFF0D]?\d{2,4}[-\u2010\u2011\u2012\u2013\u2014\uFF0D]?\d{4}", soup.get_text())
        if tel_match:
            data[Schema.TEL] = tel_match.group(0)

        # HP: dt "公式サイト" → dd a[href]
        dt = soup.find("dt", string="公式サイト")
        if dt:
            dd = dt.find_next_sibling("dd")
            if dd and dd.find("a"):
                data[Schema.HP] = dd.find("a")["href"]

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    Cafe397Scraper().execute(START_URL)
