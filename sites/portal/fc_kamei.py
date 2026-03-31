import gzip
import re
import sys
from pathlib import Path
from typing import Generator
from xml.etree import ElementTree as ET

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

SITEMAP_URL = "https://fc-kamei.net/brand/brand_sitemap.xml.gz"
START_URL = "https://fc-kamei.net"


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class FcKameiScraper(StaticCrawler):
    """フランチャイズ加盟募集 ブランド情報スクレイパー（fc-kamei.net）"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["業種", "事業内容"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        brand_urls = self._collect_from_sitemap()
        self.total_items = len(brand_urls)
        self.logger.info("ブランドURL収集完了: %d 件", len(brand_urls))
        for brand_url in brand_urls:
            item = self._scrape_detail(brand_url)
            if item and item.get(Schema.NAME):
                yield item

    def _collect_from_sitemap(self) -> list[str]:
        try:
            resp = self.session.get(SITEMAP_URL, timeout=20)
            resp.raise_for_status()
            raw = resp.content
            if raw[:2] == b"\x1f\x8b" or SITEMAP_URL.endswith(".gz"):
                try:
                    raw = gzip.decompress(raw)
                except OSError:
                    pass
            root = ET.fromstring(raw)
            urls = []
            for loc in root.iter("{http://www.sitemaps.org/schemas/sitemap/0.9}loc"):
                if loc.text and loc.text.strip():
                    urls.append(loc.text.strip())
            return urls
        except Exception as e:
            self.logger.error("サイトマップ取得失敗: %s", e)
            return []

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        # h1 → 名称
        h1 = soup.find("h1")
        if h1:
            data[Schema.NAME] = _clean(h1.get_text())

        # dt.bold → dd pairs
        for dt in soup.find_all("dt", class_="bold"):
            key = dt.get_text(strip=True)
            dd = dt.find_next_sibling("dd")
            if not dd:
                continue
            val = _clean(re.sub(r"\s+", " ", dd.get_text(separator=" ")))

            if key == "会社名":
                data[Schema.NAME] = val
            elif key == "住所":
                data[Schema.ADDR] = val
            elif key == "電話番号":
                data[Schema.TEL] = val
            elif key == "事業内容":
                data["事業内容"] = val
            elif key == "HP":
                a = dd.find("a", href=True)
                data[Schema.HP] = a["href"] if a else val
            elif key == "業種" and not data.get("業種"):
                items = [li.get_text(strip=True) for li in dd.find_all("li")]
                data["業種"] = "、".join(items) if items else val

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    FcKameiScraper().execute(START_URL)
