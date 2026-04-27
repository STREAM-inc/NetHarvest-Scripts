import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

BASE_URL = "https://catr.jp"
START_URL = "https://catr.jp/cities/0"

CITIES_NUM = 1898
PAGE_SIZE = 20
MAX_PAGES = 50  # 1都市あたり最大ページ数


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class KessanKokokuScraper(DynamicCrawler):
    """決算公告 企業情報スクレイパー（catr.jp）"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["業種", "発表日", "純利益", "利益剰余金", "純資産", "総資産"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        for city_id in range(CITIES_NUM):
            city_url = f"{BASE_URL}/cities/{city_id}"
            self.logger.info("都市ID: %d", city_id)
            yield from self._scrape_city(city_url, seen)

    def _scrape_city(self, city_url: str, seen: set) -> Generator[dict, None, None]:
        # 1ページ目
        soup = self.get_soup(city_url, wait_until="networkidle")
        if soup is None:
            return

        detail_urls = self._extract_detail_urls(soup)
        for detail_url in detail_urls:
            if detail_url not in seen:
                seen.add(detail_url)
                item = self._scrape_detail(detail_url)
                if item and item.get(Schema.NAME):
                    yield item

        # 追加ページ
        for page_num in range(2, MAX_PAGES + 1):
            offset = (page_num - 1) * PAGE_SIZE
            page_url = f"{city_url}?limit={PAGE_SIZE}&offset={offset}&order=desc&sort=total_assets"
            soup = self.get_soup(page_url, wait_until="networkidle")
            if soup is None:
                break

            page_urls = self._extract_detail_urls(soup)
            if not page_urls:
                break

            for detail_url in page_urls:
                if detail_url not in seen:
                    seen.add(detail_url)
                    item = self._scrape_detail(detail_url)
                    if item and item.get(Schema.NAME):
                        yield item

    def _extract_detail_urls(self, soup) -> list[str]:
        urls = []
        try:
            tbl = soup.find("table", class_="table")
            if tbl:
                for a in tbl.find_all("a", href=True):
                    href = a["href"]
                    full = href if href.startswith("http") else BASE_URL + "/" + href.lstrip("/")
                    urls.append(full)
        except Exception:
            pass
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url, wait_until="networkidle")
        if soup is None:
            return None

        data = {Schema.URL: url}

        # 「決算公告」h4 → 次の table
        h4 = soup.find("h4", string=lambda t: t and "決算公告" in t)
        if h4 is None:
            return None

        table = h4.find_next("table")
        if table is None:
            return None

        for tr in table.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            key = th.get_text(strip=True)
            val = _clean(td.get_text(" ").replace("\n", "").replace("\t", ""))

            if key == "会社名":
                data[Schema.NAME] = val
            elif key == "住所":
                data[Schema.ADDR] = val
            elif key == "代表":
                data[Schema.REP_NM] = val
            elif key == "業種":
                data["業種"] = val
            elif key == "発表日":
                data["発表日"] = val
            elif key == "純利益":
                data["純利益"] = val
            elif key == "利益剰余金":
                data["利益剰余金"] = val
            elif key == "純資産":
                data["純資産"] = val
            elif key == "総資産":
                data["総資産"] = val

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    KessanKokokuScraper().execute(START_URL)
