import gzip
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree as ET

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://barberin.jp"
START_URL = "https://barberin.jp/"


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("\u3000", " ")).strip()


class BarberinScraper(StaticCrawler):
    """バーバリン 美容室情報スクレイパー（barberin.jp）"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["スタッフ数"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        tenpo_urls = self._collect_from_sitemaps()
        self.total_items = len(tenpo_urls)
        self.logger.info("店舗URL収集完了: %d 件", len(tenpo_urls))
        for tenpo_url in tenpo_urls:
            item = self._scrape_detail(tenpo_url)
            if item and item.get(Schema.NAME):
                yield item

    def _collect_from_sitemaps(self) -> list[str]:
        # robots.txt からサイトマップ URL を取得
        entry_sitemaps = []
        try:
            resp = self.session.get(urljoin(BASE_URL, "/robots.txt"), timeout=15)
            if resp.status_code == 200:
                for line in resp.text.splitlines():
                    line = line.strip()
                    if line.lower().startswith("sitemap:"):
                        sm_url = line.split(":", 1)[1].strip()
                        entry_sitemaps.append(urljoin(BASE_URL, sm_url))
        except Exception:
            pass

        if not entry_sitemaps:
            entry_sitemaps = [
                urljoin(BASE_URL, "/sitemap.xml"),
                urljoin(BASE_URL, "/sitemap_index.xml"),
            ]

        queue = list(entry_sitemaps)
        visited: set[str] = set()
        tenpo_urls: list[str] = []

        while queue:
            sm_url = queue.pop(0)
            if sm_url in visited:
                continue
            visited.add(sm_url)

            try:
                resp = self.session.get(sm_url, timeout=15)
                if resp.status_code != 200:
                    continue
                raw = resp.content
                if raw[:2] == b"\x1f\x8b":
                    raw = gzip.decompress(raw)
            except Exception:
                continue

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
                if loc and "/tenpo/" in urlparse(loc).path:
                    tenpo_urls.append(loc)

        return list(dict.fromkeys(tenpo_urls))

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        # 「店舗情報」h3 → 親ブロック → table
        h3 = None
        for tag in soup.find_all("h3"):
            if "店舗情報" in tag.get_text():
                h3 = tag
                break

        if h3:
            section = h3.find_parent(["section", "div"]) or soup
            table = section.find("table") or h3.find_next("table")
            if table:
                for tr in table.find_all("tr"):
                    th = tr.find("th")
                    td = tr.find("td")
                    if not th or not td:
                        continue
                    key = _clean(th.get_text())
                    if key == "店舗名":
                        data[Schema.NAME] = _clean(td.get_text())
                    elif key == "電話番号":
                        tel = re.sub(r"[^\d\-()+]", "", td.get_text())
                        data[Schema.TEL] = tel
                    elif key == "住所":
                        addr = re.sub(r"〒\d{3}-\d{4}\s*", "", _clean(td.get_text()))
                        data[Schema.ADDR] = addr.strip()
                    elif key == "スタッフ数":
                        data["スタッフ数"] = _clean(td.get_text())
                    elif key == "URL":
                        a = td.find("a", href=True)
                        data[Schema.HP] = a["href"] if a else _clean(td.get_text())
                    elif "代表者" in key or "責任者" in key:
                        data[Schema.REP_NM] = _clean(td.get_text())

        # 名称フォールバック
        if not data.get(Schema.NAME):
            top = soup.find("section", id="tenpo-single-top")
            if top:
                h2 = top.find("h2")
                if h2:
                    data[Schema.NAME] = _clean(h2.get_text())
            if not data.get(Schema.NAME):
                h3_hl = soup.select_one("#tenpo-single-headline h3")
                if h3_hl:
                    data[Schema.NAME] = _clean(h3_hl.get_text())

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    BarberinScraper().execute(START_URL)
