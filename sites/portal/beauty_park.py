import json
import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin
from xml.etree import ElementTree as ET

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

BASE_URL = "https://www.beauty-park.jp"
PHONE_RE = re.compile(r"(0\d{1,4}-\d{1,4}-\d{3,4})")


class BeautyParkScraper(StaticCrawler):
    """Beauty Parkの店舗情報を取得する静的クローラー。"""

    DELAY = 0.5

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_detail_urls: set[str] = set()

        for area_url in self._resolve_area_urls(url):
            yield from self._scrape_area_pages(area_url, seen_detail_urls)

    def _resolve_area_urls(self, url: str) -> list[str]:
        if ".xml" not in url:
            return [url]

        loc_urls = self._fetch_xml_locs(url)
        area_sitemap_urls = [loc for loc in loc_urls if "/sitemap/area" in loc]

        if area_sitemap_urls:
            area_urls: list[str] = []
            for sitemap_url in area_sitemap_urls:
                area_urls.extend(self._fetch_xml_locs(sitemap_url))
            return self._dedupe(area_urls)

        return self._dedupe(
            [
                loc for loc in loc_urls
                if loc.startswith(BASE_URL) and "/shop/" not in loc
            ]
        )

    def _fetch_xml_locs(self, url: str) -> list[str]:
        self.logger.info("XML取得: %s", url)
        response = self.session.get(url, timeout=self.TIMEOUT)
        response.raise_for_status()

        root = ET.fromstring(response.text)
        urls: list[str] = []

        for elem in root.iter():
            if elem.tag.rsplit("}", 1)[-1] == "loc" and elem.text:
                urls.append(elem.text.strip())

        return urls

    def _scrape_area_pages(
        self,
        area_url: str,
        seen_detail_urls: set[str],
    ) -> Generator[dict, None, None]:
        current_url = area_url

        while current_url:
            self.logger.info("一覧ページ取得: %s", current_url)
            soup = self.get_soup(current_url)

            detail_urls: list[str] = []
            for link in soup.select('h4.summaryshop-text a[href^="/shop/"]'):
                href = link.get("href")
                if not href:
                    continue

                detail_url = urljoin(BASE_URL, href)
                if detail_url in seen_detail_urls:
                    continue

                seen_detail_urls.add(detail_url)
                detail_urls.append(detail_url)

            for detail_url in detail_urls:
                if self.DELAY > 0:
                    time.sleep(self.DELAY)

                item = self._scrape_detail(detail_url)
                if item:
                    yield item

            next_link = soup.select_one("a.page-next[href]")
            if next_link and next_link.get("href"):
                current_url = urljoin(current_url, next_link["href"])
            else:
                current_url = None

    def _scrape_detail(self, url: str) -> dict | None:
        self.logger.info("詳細ページ取得: %s", url)
        soup = self.get_soup(url)

        item: dict[str, str] = {
            Schema.URL: url,
        }

        name = self._extract_text(soup.select_one("span.p-modal-share__shopname-main"))
        if name:
            item[Schema.NAME] = name

        category = self._extract_text(
            soup.select_one("p.p-shopinfo-pc__meta-item-main.c-tag-1--border")
        )
        if category:
            item[Schema.CAT_SITE] = category

        for text in self._extract_info_texts(soup):
            normalized = text.replace("−", "-").replace("ー", "-")

            phone_match = PHONE_RE.search(normalized)
            if phone_match and Schema.TEL not in item:
                item[Schema.TEL] = phone_match.group(1)

            if Schema.ADDR not in item and not phone_match:
                item[Schema.ADDR] = text

        self._merge_json_ld(soup, item)

        if Schema.NAME not in item:
            return None

        return item

    def _extract_info_texts(self, soup) -> list[str]:
        values: list[str] = []

        for elem in soup.select("p.p-modal-share__text.j-copy-target"):
            text = self._extract_text(elem)
            if text and text not in values:
                values.append(text)

        return values

    def _merge_json_ld(self, soup, item: dict[str, str]) -> None:
        for script in soup.select('script[type="application/ld+json"]'):
            raw = script.string or script.get_text(strip=True)
            if not raw:
                continue

            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue

            candidates = payload if isinstance(payload, list) else [payload]
            for candidate in candidates:
                if not isinstance(candidate, dict):
                    continue
                if candidate.get("@type") != "LocalBusiness":
                    continue

                if Schema.NAME not in item and candidate.get("name"):
                    item[Schema.NAME] = self._normalize_text(candidate["name"])
                if Schema.TEL not in item and candidate.get("telephone"):
                    item[Schema.TEL] = self._normalize_text(candidate["telephone"])

                address = candidate.get("address", {})
                if isinstance(address, dict):
                    postal_code = self._normalize_text(address.get("postalCode", ""))
                    pref = self._normalize_text(address.get("addressRegion", ""))
                    locality = self._normalize_text(address.get("addressLocality", ""))
                    street = self._normalize_text(address.get("streetAddress", ""))

                    if postal_code and Schema.POST_CODE not in item:
                        item[Schema.POST_CODE] = postal_code
                    if pref and Schema.PREF not in item:
                        item[Schema.PREF] = pref
                    if Schema.ADDR not in item:
                        full_address = "".join(part for part in [pref, locality, street] if part)
                        if full_address:
                            item[Schema.ADDR] = full_address
                return

    @staticmethod
    def _extract_text(elem) -> str:
        if elem is None:
            return ""
        return BeautyParkScraper._normalize_text(elem.get_text(" ", strip=True))

    @staticmethod
    def _normalize_text(text: str) -> str:
        return " ".join(str(text).split())

    @staticmethod
    def _dedupe(urls: list[str]) -> list[str]:
        seen: set[str] = set()
        deduped: list[str] = []

        for url in urls:
            if not url or url in seen:
                continue
            seen.add(url)
            deduped.append(url)

        return deduped


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    BeautyParkScraper().execute("https://www.beauty-park.jp/sitemap.xml")
