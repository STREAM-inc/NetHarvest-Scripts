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

BASE_URL = "https://www.job-j.net"


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class KyujinJournalNetScraper(DynamicCrawler):
    """求人ジャーナルネット スクレイパー（job-j.net）"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["業種", "募集職種"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls = self._collect_detail_urls(url)
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
                if "/shousai/" in href or "/detail/" in href:
                    full = href if href.startswith("http") else urljoin(BASE_URL, href)
                    if full not in seen:
                        seen.add(full)
                        urls.append(full)

            next_a = soup.select_one("a[rel='next'], a.next, li.next a")
            if next_a and next_a.get("href"):
                next_url = urljoin(current, next_a["href"])
                current = next_url if next_url != current else None
            else:
                current = None

        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url, wait_until="networkidle")
        if soup is None:
            return None

        data = {Schema.URL: url}

        # Company info from tables
        for tr in soup.select("table tr, dl dt"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            key = th.get_text(strip=True)
            val = _clean(td.get_text(" "))
            if "会社名" in key or "企業名" in key:
                data[Schema.NAME] = val
            elif "住所" in key or "所在地" in key:
                data[Schema.ADDR] = val
            elif key in ("TEL", "電話番号"):
                data[Schema.TEL] = val
            elif "業種" in key:
                data["業種"] = val
            elif "募集職種" in key:
                data["募集職種"] = val
            elif "HP" in key or "ホームページ" in key:
                a = td.find("a", href=True)
                data[Schema.HP] = a["href"] if a else val

        # fallback name from h1/h2
        if not data.get(Schema.NAME):
            for tag in ("h1", "h2", "h3"):
                el = soup.select_one(tag)
                if el:
                    data[Schema.NAME] = _clean(el.get_text())
                    break

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    KyujinJournalNetScraper().execute("https://www.job-j.net")
