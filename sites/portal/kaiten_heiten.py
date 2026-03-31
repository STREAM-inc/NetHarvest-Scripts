import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://kaiten-heiten-24.com"
_SHOP_PATTERN = re.compile(r"^https://kaiten-heiten-24\.com/.+")

_SITEMAP_CANDIDATES = [
    "sitemap.xml",
    "sitemap_index.xml",
    "sitemap-index.xml",
    "wp-sitemap.xml",
]


class KaitenHeitenScraper(StaticCrawler):
    """開店閉店ドットコム・新（kaiten-heiten-24.com）店舗情報スクレイパー"""

    DELAY = 3.0
    EXTRA_COLUMNS = ["法人名", "担当者名", "加盟団体"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        shop_urls = self._collect_shop_urls()
        self.total_items = len(shop_urls)
        self.logger.info("店舗URL収集完了: %d 件", len(shop_urls))
        for shop_url in shop_urls:
            item = self._scrape_detail(shop_url)
            if item:
                yield item

    def _collect_shop_urls(self) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        queue: list[str] = []

        for candidate in _SITEMAP_CANDIDATES:
            sm_url = urljoin(BASE_URL, candidate)
            try:
                r = self.session.get(sm_url, timeout=self.TIMEOUT)
                if r.status_code == 200:
                    queue.append(sm_url)
            except Exception:
                continue

        if not queue:
            return []

        visited: set[str] = set()
        while queue:
            sm_url = queue.pop(0)
            if sm_url in visited:
                continue
            visited.add(sm_url)
            try:
                r = self.session.get(sm_url, timeout=self.TIMEOUT)
                if r.status_code != 200:
                    continue
                root = ET.fromstring(r.content)
                locs = [el.text.strip() for el in root.iter() if el.tag.endswith("loc") and el.text]
                if root.tag.lower().endswith("sitemapindex"):
                    queue.extend(locs)
                else:
                    for u in locs:
                        if _SHOP_PATTERN.match(u) and u not in seen:
                            seen.add(u)
                            urls.append(u)
            except Exception as e:
                self.logger.debug("サイトマップスキップ %s: %s", sm_url, e)
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        # dl.top_info から 住所・営業時間・定休日
        for dl in soup.select("dl.top_info"):
            dt = dl.find("dt")
            dd = dl.find("dd")
            if not dt or not dd:
                continue
            key = dt.get_text(strip=True)
            val = re.sub(r"\s+", " ", dd.get_text(separator=" ", strip=True)).strip()
            if key == "住所":
                data[Schema.ADDR] = val
            elif key == "営業時間":
                data[Schema.TIME] = val
            elif key == "定休日":
                data[Schema.HOLIDAY] = val

        # h1 から名称
        h1 = soup.select_one("h1")
        if h1:
            data[Schema.NAME] = h1.get_text(strip=True)

        # div.info_bottom から追加情報（key：value形式）
        info = soup.select_one("div.info_bottom")
        if info:
            for td in info.select("table tbody tr td"):
                text = re.sub(r"\s+", " ", td.get_text(separator=" ", strip=True)).strip()
                if "：" not in text:
                    continue
                key, val = text.split("：", 1)
                key = key.strip()
                val = val.strip()
                a = td.find("a", href=True)
                if a and a["href"].strip():
                    href = a["href"].strip()
                    if (not val) or (val == a.get_text(strip=True)):
                        val = href
                if key == "法人名":
                    data["法人名"] = val
                elif key == "担当者名":
                    data["担当者名"] = val
                elif key == "TEL" or key == "電話":
                    data[Schema.TEL] = val
                elif key == "HP" or "ホームページ" in key:
                    data[Schema.HP] = val
                elif key == "加盟団体":
                    data["加盟団体"] = val

        if not data.get(Schema.NAME) and not data.get(Schema.ADDR):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    KaitenHeitenScraper().execute("https://kaiten-heiten-24.com")
