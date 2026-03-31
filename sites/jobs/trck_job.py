import gzip
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://job.trck.jp"
START_URL = "https://job.trck.jp/sitemap.xml"


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("\u3000", " ")).strip()


class TrckJobScraper(StaticCrawler):
    """トラッカーズジョブ 求人企業情報スクレイパー（job.trck.jp）"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["業種", "資本金", "従業員数", "設立日"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        company_urls = self._collect_from_sitemaps(url)
        self.total_items = len(company_urls)
        self.logger.info("企業URL収集完了: %d 件", len(company_urls))
        for company_url in company_urls:
            item = self._scrape_detail(company_url)
            if item and item.get(Schema.NAME):
                yield item

    def _collect_from_sitemaps(self, start_url: str) -> list[str]:
        queue = [start_url]
        visited: set[str] = set()
        company_urls: list[str] = []

        while queue:
            sm_url = queue.pop(0)
            if sm_url in visited:
                continue
            visited.add(sm_url)

            resp = self.session.get(sm_url, timeout=20)
            if resp.status_code != 200:
                continue
            raw = resp.content
            if raw[:2] == b"\x1f\x8b":
                try:
                    raw = gzip.decompress(raw)
                except Exception:
                    pass

            try:
                root = ET.fromstring(raw)
            except ET.ParseError:
                continue

            ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            for loc_el in root.findall(".//sm:sitemap/sm:loc", ns):
                loc = loc_el.text and loc_el.text.strip()
                if loc and loc not in visited:
                    queue.append(loc)
            for loc_el in root.findall(".//sm:url/sm:loc", ns):
                loc = loc_el.text and loc_el.text.strip()
                if loc and "companies/" in loc:
                    company_urls.append(loc)

        return list(dict.fromkeys(company_urls))

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        tbl = soup.find("table", class_="c-table")
        if tbl:
            for tr in tbl.find_all("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                label = th.get_text(strip=True)
                val = _clean(td.get_text(" "))
                if label in ("事業所名", "企業名", "社名") and not data.get(Schema.NAME):
                    data[Schema.NAME] = val
                elif label == "所在地" and not data.get(Schema.ADDR):
                    data[Schema.ADDR] = val
                elif label == "資本金" and not data.get("資本金"):
                    data["資本金"] = val
                elif label == "従業員数" and not data.get("従業員数"):
                    data["従業員数"] = val
                elif "代表" in label and not data.get(Schema.REP_NM):
                    data[Schema.REP_NM] = val
                elif label == "設立" and not data.get("設立日"):
                    data["設立日"] = val
                elif label in ("業種", "事業内容") and not data.get("業種"):
                    data["業種"] = val

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    TrckJobScraper().execute(START_URL)
