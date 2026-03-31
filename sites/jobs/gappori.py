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

BASE_URL = "https://gappori.jp"


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class GapporiScraper(StaticCrawler):
    """ガッポリ 求人情報スクレイパー（gappori.jp）"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["業種", "募集職種", "勤務地"]

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
            soup = self.get_soup(current)
            if soup is None:
                break

            for a in soup.select("a[href]"):
                href = a.get("href", "").strip()
                if "/a_detail/" in href or "/detail/" in href:
                    full = href if href.startswith("http") else urljoin(BASE_URL, href)
                    if full not in seen:
                        seen.add(full)
                        urls.append(full)

            # next page (image-based navigation)
            next_img = soup.find("img", src=lambda s: s and "page_navi_next" in s and "off" not in s)
            if next_img:
                next_a = next_img.find_parent("a")
                if next_a and next_a.get("href"):
                    next_url = urljoin(BASE_URL, next_a["href"])
                    current = next_url if next_url != current else None
                else:
                    current = None
            else:
                current = None

        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        # fix double-URL bug
        if url.startswith(BASE_URL + BASE_URL):
            url = url.replace(BASE_URL + BASE_URL, BASE_URL)

        data = {Schema.URL: url}

        company = soup.select_one("#shop_company")
        if company:
            for tr in company.select("tr"):
                th = tr.select_one("th")
                td = tr.select_one("td")
                if not th or not td:
                    continue
                key = th.get_text(strip=True)
                val = _clean(td.get_text(" ", strip=True))
                if "会社名" in key:
                    data[Schema.NAME] = val
                elif "業種" in key:
                    data["業種"] = val
                elif "会社HP" in key:
                    a = td.select_one("a[href]")
                    data[Schema.HP] = a["href"] if a else val
                elif "連絡先" in key:
                    tel_a = td.select_one("a[href^='tel:']")
                    if tel_a:
                        data[Schema.TEL] = tel_a.get_text(strip=True)
                    addr = " ".join(td.stripped_strings)
                    addr = re.sub(r"TEL[：:]?\s*[\d\-]+", "", addr).strip()
                    if addr:
                        data[Schema.ADDR] = addr

        # job type
        job_type_area = soup.select_one("#shop_info")
        if job_type_area:
            for tr in job_type_area.select("tr"):
                th = tr.select_one("th")
                td = tr.select_one("td")
                if not th or not td:
                    continue
                key = th.get_text(strip=True)
                val = _clean(td.get_text(" ", strip=True))
                if "募集職種" in key:
                    data["募集職種"] = val
                elif "勤務地" in key:
                    data["勤務地"] = val

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    GapporiScraper().execute("https://gappori.jp/a_search/w_/")
