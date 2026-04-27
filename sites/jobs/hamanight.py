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

BASE_URL = "http://www.hamanight.com"

AREA_URLS = [
    "http://www.hamanight.com/location/kannai",
    "http://www.hamanight.com/location/fukutomicho",
    "http://www.hamanight.com/location/sakuragicho",
    "http://www.hamanight.com/location/yokohama",
    "http://www.hamanight.com/location/shin-yokohama",
]

EXCLUDE_KEYWORDS = ["/genre/", "/location/job"]


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class HamanightScraper(StaticCrawler):
    """ハマんナイト 横浜ナイト求人スクレイパー（hamanight.com）"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["業種", "公式サイト"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls: list[str] = []
        seen: set[str] = set()
        for area_url in AREA_URLS:
            self.logger.info("エリア取得: %s", area_url)
            urls = self._collect_area_urls(area_url)
            for u in urls:
                if u not in seen:
                    seen.add(u)
                    detail_urls.append(u)
        self.total_items = len(detail_urls)
        self.logger.info("詳細URL収集完了: %d 件", len(detail_urls))
        for detail_url in detail_urls:
            item = self._scrape_detail(detail_url)
            if item:
                yield item

    def _collect_area_urls(self, area_url: str) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        page = 1
        current = area_url
        while current:
            soup = self.get_soup(current)
            if soup is None:
                break
            for a in soup.select("a[href]"):
                href = a.get("href", "").strip()
                if not href:
                    continue
                if any(kw in href for kw in EXCLUDE_KEYWORDS):
                    continue
                full = urljoin(BASE_URL, href)
                if full.startswith(BASE_URL) and "/location/" in full and full not in seen and full != current:
                    seen.add(full)
                    urls.append(full)

            next_a = soup.select_one("a.page-next, a[rel='next']")
            if next_a and next_a.get("href"):
                page += 1
                next_url = f"{area_url}/page/{page}"
                current = next_url
            else:
                current = None

        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        name_h1 = soup.select_one("#location > div.wrap > div > div.main > h1")
        name_h2 = soup.select_one("#pickup div.view h2")
        name = (
            _clean(name_h1.get_text()) if name_h1
            else _clean(name_h2.get_text()) if name_h2
            else ""
        )
        if name:
            data[Schema.NAME] = name

        genre_a = soup.find("a", href=lambda h: h and "genre" in h)
        if genre_a:
            data["業種"] = genre_a.get_text(strip=True)

        # store info from ul
        ul_main = soup.select_one("#pickup > div.wrap > div > div.main > div.view > dl > dd > ul")
        if ul_main:
            self._extract_list_items(ul_main, data)

        # official site
        official = soup.select_one("div.official a[href]")
        if official:
            data["公式サイト"] = official["href"]

        if not data.get(Schema.NAME):
            return None
        return data

    def _extract_list_items(self, ul, data: dict):
        for li in ul.find_all("li"):
            text = _clean(li.get_text(" "))
            if "TEL" in text or "電話" in text:
                m = re.search(r"[\d\-]{8,}", text)
                if m:
                    data[Schema.TEL] = m.group(0)
            elif "住所" in text or "所在地" in text:
                val = re.sub(r"^[住所所在地]+[：:]\s*", "", text).strip()
                if val:
                    data[Schema.ADDR] = val
            elif "営業時間" in text:
                val = re.sub(r"^営業時間[：:]\s*", "", text).strip()
                if val:
                    data[Schema.TIME] = val
            elif "定休日" in text:
                val = re.sub(r"^定休日[：:]\s*", "", text).strip()
                if val:
                    data[Schema.HOLIDAY] = val


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    HamanightScraper().execute("http://www.hamanight.com")
