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

BASE_URL = "https://ogori-shoukoukai.com"

CATEGORY_URLS = [
    "https://ogori-shoukoukai.com/companycat/sweets/",
    "https://ogori-shoukoukai.com/companycat/gourmet/",
    "https://ogori-shoukoukai.com/companycat/school/",
    "https://ogori-shoukoukai.com/companycat/shop/",
    "https://ogori-shoukoukai.com/companycat/others/",
]

KEY_MAP = {
    "電話番号": Schema.TEL,
    "TEL": Schema.TEL,
    "ホームページ": Schema.HP,
    "HP": Schema.HP,
    "住所": Schema.ADDR,
    "営業時間": Schema.TIME,
    "定休日": Schema.HOLIDAY,
}


def _clean(s) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class OgoriShokokaiScraper(StaticCrawler):
    """小郡市商工会 会員情報スクレイパー（ogori-shoukoukai.com）"""

    DELAY = 0.5
    EXTRA_COLUMNS = ["PR文", "FAX番号", "MAIL"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        for cat_url in CATEGORY_URLS:
            self.logger.info("カテゴリ: %s", cat_url)
            yield from self._scrape_category(cat_url, seen)

    def _scrape_category(self, start_url: str, seen: set) -> Generator[dict, None, None]:
        current = start_url
        while current:
            soup = self.get_soup(current)
            if soup is None:
                break

            topics = soup.select_one("div#topics")
            if topics:
                for dl in topics.select("dl.list-company"):
                    item = self._extract_from_dl(dl, current)
                    if item and item.get(Schema.NAME):
                        key = item[Schema.NAME]
                        if key not in seen:
                            seen.add(key)
                            yield item

            # next page
            next_a = (
                soup.find("link", rel=lambda x: x and "next" in x) or
                soup.find("a", rel=lambda x: x and "next" in x) or
                soup.select_one("a.next.page-numbers, a.page-numbers.next")
            )
            if next_a:
                href = next_a.get("href", "")
                next_url = urljoin(current, href)
                current = next_url if next_url != current else None
            else:
                current = None

    def _extract_from_dl(self, dl, page_url: str) -> dict:
        data = {Schema.URL: page_url}

        dt = dl.find("dt")
        if dt:
            data[Schema.NAME] = _clean(dt.get_text(" ", strip=True))

        pr = dl.select_one("dd.company-desc p.pr-text")
        if pr:
            data["PR文"] = _clean(pr.get_text(" ", strip=True))

        table = dl.select_one("table.company")
        if table:
            for tr in table.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                key_raw = _clean(th.get_text(" ", strip=True))
                if "ホームページ" in key_raw or key_raw == "HP":
                    a = td.find("a")
                    val = _clean(a["href"]) if a and a.get("href") else _clean(td.get_text(" ", strip=True))
                else:
                    val = _clean(td.get_text(" ", strip=True))
                schema_key = KEY_MAP.get(key_raw)
                if schema_key:
                    data[schema_key] = val
                elif key_raw in ("FAX番号", "FAX"):
                    data["FAX番号"] = val
                elif key_raw in ("MAIL", "メール", "E-mail", "Email"):
                    data["MAIL"] = val

        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    OgoriShokokaiScraper().execute("https://ogori-shoukoukai.com/companycat/")
