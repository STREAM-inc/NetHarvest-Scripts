import gzip
import io
import json
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

BASE_URL = "https://gaten-job.com"
START_URL = "https://gaten-job.com/sitemap.xml"

DETAIL_PATTERNS = [
    re.compile(r"/job/"),
    re.compile(r"/recruit/"),
    re.compile(r"/detail"),
    re.compile(r"/kyujin/"),
    re.compile(r"/works?/\d+"),
    re.compile(r"/[a-z0-9_-]+/\d{3,}"),
]

EXCLUDE_PATTERNS = [
    re.compile(r"^/wp-admin/"),
    re.compile(r"^/category/"),
    re.compile(r"^/tag/"),
    re.compile(r"^/page/"),
    re.compile(r"^/search"),
    re.compile(r"^/sitemap"),
    re.compile(r"^/privacy|/policy|/terms|/contact|/about|/company|/guide"),
]


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("\u3000", " ")).strip()


def _is_detail_url(url: str) -> bool:
    path = urlparse(url).path or "/"
    if any(p.search(path) for p in EXCLUDE_PATTERNS):
        return False
    return any(p.search(path) for p in DETAIL_PATTERNS)


class GatenJobScraper(StaticCrawler):
    """ガテン系仕事ナビ 求人企業情報スクレイパー（gaten-job.com）"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["業種", "従業員数"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls = self._collect_from_sitemaps(url)
        self.total_items = len(detail_urls)
        self.logger.info("詳細URL収集完了: %d 件", len(detail_urls))
        seen: set[str] = set()
        for detail_url in detail_urls:
            if detail_url in seen:
                continue
            seen.add(detail_url)
            item = self._scrape_detail(detail_url)
            if item and item.get(Schema.NAME):
                yield item

    def _collect_from_sitemaps(self, start_url: str) -> list[str]:
        queue = [start_url]
        visited: set[str] = set()
        detail_urls: list[str] = []

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
            # sitemap index
            for loc_el in root.findall(".//sm:sitemap/sm:loc", ns):
                loc = loc_el.text and loc_el.text.strip()
                if loc and loc not in visited:
                    queue.append(loc)
            # urlset
            for loc_el in root.findall(".//sm:url/sm:loc", ns):
                loc = loc_el.text and loc_el.text.strip()
                if loc and urlparse(loc).netloc == urlparse(BASE_URL).netloc:
                    if _is_detail_url(loc):
                        detail_urls.append(loc)

        return list(dict.fromkeys(detail_urls))

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        # JSON-LD から NAME, HP 抽出
        for tag in soup.find_all("script", type="application/ld+json"):
            txt = tag.string or tag.get_text() or ""
            if not txt:
                continue
            try:
                obj = json.loads(txt)
            except Exception:
                continue
            items = obj if isinstance(obj, list) else [obj]
            for o in items:
                if not isinstance(o, dict):
                    continue
                typ = o.get("@type")
                if typ == "JobPosting":
                    org = o.get("hiringOrganization", {})
                    if isinstance(org, dict):
                        name = _clean(org.get("name", ""))
                        hp = _clean(org.get("sameAs") or org.get("url") or "")
                        if name and not data.get(Schema.NAME):
                            data[Schema.NAME] = name
                        if hp and not data.get(Schema.HP):
                            data[Schema.HP] = hp
                elif typ == "Organization":
                    name = _clean(o.get("name", ""))
                    hp = _clean(o.get("sameAs") or o.get("url") or "")
                    if name and not data.get(Schema.NAME):
                        data[Schema.NAME] = name
                    if hp and not data.get(Schema.HP):
                        data[Schema.HP] = hp

        # HTML テーブルから補完
        label_map = {
            "ホームページ": Schema.HP,
            "電話番号": Schema.TEL,
            "TEL": Schema.TEL,
            "住所": Schema.ADDR,
            "所在地": Schema.ADDR,
            "代表者": Schema.REP_NM,
            "業種": "業種",
            "事業内容": "業種",
            "従業員数": "従業員数",
            "社員数": "従業員数",
        }
        for table in soup.find_all("table"):
            for tr in table.find_all("tr"):
                th = tr.find(["th", "dt"])
                td = tr.find(["td", "dd"])
                if not th or not td:
                    continue
                label = _clean(th.get_text())
                field = label_map.get(label)
                if field is None:
                    for k, v in label_map.items():
                        if label.startswith(k):
                            field = v
                            break
                if field is None:
                    continue
                a = td.find("a", href=True)
                if a and a["href"].startswith("http"):
                    val = a["href"]
                else:
                    val = _clean(td.get_text(" "))
                if val and not data.get(field):
                    data[field] = val

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    GatenJobScraper().execute(START_URL)
