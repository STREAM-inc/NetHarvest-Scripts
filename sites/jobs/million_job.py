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

BASE_URL = "https://www.million-job.com"
AREA_HREF_RE = re.compile(r"^/[a-z_]+/joblist/la-\d+/?$")
TEL_CLEAN_RE = re.compile(r"[^\d\-]+")


class MillionJobScraper(StaticCrawler):
    """ミリオンジョブ 風俗系求人サイト スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["業種", "受付時間"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        area_urls = self._get_area_urls(url)
        self.logger.info("エリア収集完了: %d 件", len(area_urls))
        detail_urls = self._collect_detail_urls(area_urls)
        self.total_items = len(detail_urls)
        self.logger.info("詳細URL収集完了: %d 件", len(detail_urls))
        for detail_url in detail_urls:
            item = self._scrape_detail(detail_url)
            if item:
                yield item

    def _get_area_urls(self, top_url: str) -> list[str]:
        soup = self.get_soup(top_url)
        if soup is None:
            return []
        seen: set[str] = set()
        urls: list[str] = []
        for a in soup.find_all("a", href=AREA_HREF_RE):
            full = urljoin(BASE_URL, a["href"].strip())
            if full not in seen:
                seen.add(full)
                urls.append(full)
        return urls

    def _collect_detail_urls(self, area_urls: list[str]) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        for area_url in area_urls:
            current = area_url
            while current:
                soup = self.get_soup(current)
                if soup is None:
                    break
                # 詳細ページリンク収集
                added = False
                for a in soup.find_all("a", href=True):
                    href = a["href"].strip()
                    full = href if href.startswith("http") else urljoin(BASE_URL, href)
                    if "/detail/" in full and full not in seen:
                        seen.add(full)
                        urls.append(full)
                        added = True
                # 次ページ
                next_a = soup.select_one("a.next, li.next a, a[rel='next']")
                if next_a:
                    href = next_a.get("href", "").strip()
                    next_url = href if href.startswith("http") else urljoin(BASE_URL, href)
                    current = next_url if next_url != current else None
                else:
                    current = None
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        # th/td テーブルから各フィールド
        def get_by_th(labels: list[str]) -> str:
            for th in soup.find_all("th"):
                if any(lbl in th.get_text(strip=True) for lbl in labels):
                    td = th.find_next_sibling("td")
                    if td:
                        a = td.find("a", href=True)
                        if a and any(lbl in ["HP", "ホームページ"] for lbl in labels):
                            return a["href"].strip()
                        return td.get_text(" ", strip=True)
            return ""

        # 名称
        name = get_by_th(["店名", "名称"])
        if not name:
            dt = soup.select_one(".entryBoxInner dl dt")
            if dt:
                name = dt.get_text(strip=True)
        data[Schema.NAME] = name

        data["業種"] = get_by_th(["業種"])
        data[Schema.ADDR] = get_by_th(["住所"])
        data[Schema.HP] = get_by_th(["HP", "ホームページ"])
        data["受付時間"] = get_by_th(["受付時間", "営業時間"])

        # TEL
        tel_dd = soup.select_one("dd.telNumber")
        if tel_dd:
            from bs4 import BeautifulSoup as _BS
            clone = _BS(str(tel_dd), "html.parser")
            for sp in clone.find_all("span"):
                sp.decompose()
            raw = clone.get_text(strip=True)
            tel = TEL_CLEAN_RE.sub("", raw)
            if tel:
                data[Schema.TEL] = tel

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    MillionJobScraper().execute("https://www.million-job.com")
