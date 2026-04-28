import re
import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

BASE_URL = "https://doraever.jp"
LIST_URL = "https://doraever.jp/job-lists/{}"


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class DoraeverScraper(DynamicCrawler):
    """ドラEVER ドライバー求人スクレイパー（doraever.jp）"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["業種", "事業内容"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls = self._collect_detail_urls()
        self.total_items = len(detail_urls)
        self.logger.info("詳細URL収集完了: %d 件", len(detail_urls))
        seen_companies: set[str] = set()
        for detail_url in detail_urls:
            item = self._scrape_detail(detail_url)
            if item and item.get(Schema.NAME):
                key = item[Schema.NAME]
                if key not in seen_companies:
                    seen_companies.add(key)
                    yield item

    def _collect_detail_urls(self) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        consecutive_empty = 0
        page = 1

        while consecutive_empty < 5 and page < 10000:
            soup = self.get_soup(LIST_URL.format(page), wait_until="networkidle")
            if soup is None:
                consecutive_empty += 1
                page += 1
                continue

            found = 0
            for a in soup.select("a[href]"):
                href = a.get("href", "")
                # detail URLs match /job-NNNNNN pattern
                if re.match(r"^https://doraever\.jp/job-\d+", href) or \
                   re.match(r"^/job-\d+", href):
                    full = href if href.startswith("http") else BASE_URL + href
                    if full not in seen:
                        seen.add(full)
                        urls.append(full)
                        found += 1

            if found == 0:
                consecutive_empty += 1
            else:
                consecutive_empty = 0

            page += 1

        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        # navigate to company tab
        company_url = url.rstrip("/") + "#tab-company"
        soup = self.get_soup(company_url, wait_until="networkidle")
        if soup is None:
            soup = self.get_soup(url, wait_until="networkidle")
        if soup is None:
            return None

        data = {Schema.URL: url}

        # company overview section (h2.c_ttl + content blocks)
        company_section = soup.select_one("div#tab-company, section.company-overview")
        if company_section is None:
            company_section = soup

        # collect h2 label → next block content pairs
        info: dict[str, str] = {}
        for h2 in company_section.select("h2.c_ttl"):
            label = _clean(h2.get_text())
            content_block = h2.find_next_sibling()
            if content_block:
                info[label] = _clean(content_block.get_text(" "))

        # also try table
        for tr in company_section.select("table tr"):
            th = tr.find("th")
            td = tr.find("td")
            if th and td:
                info[_clean(th.get_text())] = _clean(td.get_text(" "))

        for key, val in info.items():
            if "会社名" in key or "企業名" in key:
                data.setdefault(Schema.NAME, val)
            elif "住所" in key or "所在地" in key:
                data.setdefault(Schema.ADDR, val)
            elif key in ("TEL", "電話番号"):
                data.setdefault(Schema.TEL, val)
            elif "業種" in key:
                data.setdefault("業種", val)
            elif "事業内容" in key:
                data.setdefault("事業内容", val)
            elif "代表" in key:
                data.setdefault(Schema.REP_NM, val)

        # fallback name from h1
        if not data.get(Schema.NAME):
            h1 = soup.select_one("h1")
            if h1:
                data[Schema.NAME] = _clean(h1.get_text())

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    DoraeverScraper().execute("https://doraever.jp/job-lists/1")
