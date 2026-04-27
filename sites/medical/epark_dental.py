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

BASE_URL = "https://haisha-yoyaku.jp"
LIST_URL = "https://haisha-yoyaku.jp/bun2sdental/list/"


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class EparkDentalScraper(DynamicCrawler):
    """EPARK歯科 医院情報スクレイパー（haisha-yoyaku.jp）"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["診療項目", "アクセス"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls = self._collect_detail_urls(url)
        self.total_items = len(detail_urls)
        self.logger.info("詳細URL収集完了: %d 件", len(detail_urls))
        for detail_url in detail_urls:
            item = self._scrape_detail(detail_url)
            if item:
                yield item

    def _collect_detail_urls(self, start_url: str) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        current = start_url
        while current:
            soup = self.get_soup(current, wait_until="networkidle")
            if soup is None:
                break

            for a in soup.select("a[href]"):
                href = a.get("href", "").strip()
                if "/bun2sdental/detail/" in href or "/dental/detail/" in href:
                    full = href if href.startswith("http") else urljoin(BASE_URL, href)
                    if full not in seen:
                        seen.add(full)
                        urls.append(full)

            next_a = soup.select_one("a[rel='next'], a.pagination-next, a.next")
            if next_a and next_a.get("href"):
                next_url = urljoin(BASE_URL, next_a["href"])
                current = next_url if next_url != current else None
            else:
                current = None

        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url, wait_until="networkidle")
        if soup is None:
            return None

        data = {Schema.URL: url}

        # clinic name
        section = soup.select_one("div.area_section-detail02#clinic_basic-information")
        if section is None:
            section = soup

        name_p = section.select_one("p.main")
        if name_p:
            data[Schema.NAME] = _clean(name_p.get_text())
        else:
            h1 = soup.select_one("h1")
            if h1:
                data[Schema.NAME] = _clean(h1.get_text())

        # table rows: th/td pairs
        for tr in section.select("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            key = _clean(th.get_text(" "))
            if "住所" in key:
                # first p tag in td
                p = td.find("p")
                data[Schema.ADDR] = _clean(p.get_text()) if p else _clean(td.get_text(" "))
            elif "診療項目" in key or "診療科目" in key:
                items = [_clean(sp.get_text()) for sp in td.select("span.content")]
                if not items:
                    items = [_clean(td.get_text(" "))]
                data["診療項目"] = "|".join(filter(None, items))
            elif "アクセス" in key:
                data["アクセス"] = _clean(td.get_text(" "))
            elif key in ("TEL", "電話番号", "連絡先"):
                data[Schema.TEL] = _clean(td.get_text(" "))
            elif "営業時間" in key or "診療時間" in key:
                data[Schema.TIME] = _clean(td.get_text(" "))
            elif "定休日" in key or "休診日" in key:
                data[Schema.HOLIDAY] = _clean(td.get_text(" "))

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    EparkDentalScraper().execute(LIST_URL)
