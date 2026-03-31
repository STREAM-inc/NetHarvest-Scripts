import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://www.yameshi-shokokai.jp"


class YameshiShokokaiScraper(StaticCrawler):
    """八女市商工会 会員情報スクレイパー（yameshi-shokokai.jp/member/）"""

    DELAY = 0.5
    EXTRA_COLUMNS = ["業種"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls = self._collect_detail_urls(url)
        self.total_items = len(detail_urls)
        self.logger.info("詳細URL収集完了: %d 件", len(detail_urls))
        for detail_url in detail_urls:
            item = self._scrape_detail(detail_url)
            if item:
                yield item

    def _collect_detail_urls(self, list_url: str) -> list[str]:
        soup = self.get_soup(list_url)
        if soup is None:
            return []
        urls: list[str] = []
        seen: set[str] = set()
        for a in soup.select("a[href]"):
            href = a.get("href", "").strip()
            if not href:
                continue
            full = urljoin(BASE_URL, href)
            if "/member/" in full and full != list_url and full not in seen:
                seen.add(full)
                urls.append(full)
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        h3 = soup.select_one("div.member_detail_ttl_wrap h3.ttl")
        if h3:
            data[Schema.NAME] = h3.get_text(" ", strip=True)

        dl = soup.select_one("dl.member_info")
        if dl:
            for dt_tag in dl.find_all("dt"):
                key = dt_tag.get_text(" ", strip=True)
                dd_tag = dt_tag.find_next_sibling("dd")
                val = dd_tag.get_text(" ", strip=True) if dd_tag else ""
                if "住所" in key:
                    data[Schema.ADDR] = val
                elif key == "TEL":
                    data[Schema.TEL] = val
                elif "業種" in key:
                    data["業種"] = val

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    YameshiShokokaiScraper().execute("https://www.yameshi-shokokai.jp/member/")
