import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.dynamic import DynamicCrawler

SALON_RE = re.compile(r"^https://minimodel\.jp/salon/[A-Za-z0-9_-]+/?$")


class MinimoScraper(DynamicCrawler):
    """minimoのサロン詳細ページから店舗情報を取得するクローラー。"""

    DELAY = 0.5

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls = self._resolve_detail_urls(url)
        self.total_items = len(detail_urls)

        for detail_url in detail_urls:
            if self.DELAY > 0:
                time.sleep(self.DELAY)

            item = self._scrape_detail(detail_url)
            if item:
                yield item

    def _resolve_detail_urls(self, url: str) -> list[str]:
        if SALON_RE.match(url):
            return [url.rstrip("/")]

        sitemap_urls = self._fetch_xml_locs(url)
        detail_urls: list[str] = []

        for sitemap_url in sitemap_urls:
            if not sitemap_url.endswith(".xml"):
                continue
            for loc in self._fetch_xml_locs(sitemap_url):
                normalized = loc.rstrip("/")
                if SALON_RE.match(normalized):
                    detail_urls.append(normalized)

        return self._dedupe(detail_urls)

    def _fetch_xml_locs(self, url: str) -> list[str]:
        self.logger.info("XML取得: %s", url)
        response = requests.get(
            url,
            headers={"User-Agent": self.USER_AGENT},
            timeout=30,
        )
        response.raise_for_status()

        root = ET.fromstring(response.text)
        urls: list[str] = []
        for elem in root.iter():
            if elem.tag.rsplit("}", 1)[-1] == "loc" and elem.text:
                urls.append(elem.text.strip())
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        self.logger.info("詳細ページ取得: %s", url)
        soup, body_text = self._get_page_state(url)

        item: dict[str, str] = {
            Schema.URL: url,
        }

        name = self._extract_text(soup.select_one("span.SalonHeaderSection_salonName__Wo4tr"))
        kana = self._extract_text(soup.select_one("span.SalonHeaderSection_salonNameKana__9QO2I"))
        address = self._extract_text(soup.select_one("p.SalonHeaderSection_address__x9xOi"))
        station = self._extract_text(soup.select_one("span.Location_locationText__ab6nU"))

        if not name and soup.select_one("h1"):
            raw_name = self._normalize_text(soup.select_one("h1").get_text(" ", strip=True))
            name, kana = self._split_name_and_kana(raw_name)

        if not station:
            station = self._extract_station_from_description(soup)

        if name:
            item[Schema.NAME] = name
        if kana:
            item[Schema.NAME_KANA] = kana
        if address:
            item[Schema.ADDR] = address
        if station:
            item["最寄駅"] = station

        category = self._infer_category(soup, body_text)
        if category:
            item[Schema.CAT_SITE] = category

        for dt in soup.select("dt"):
            dd = dt.find_next_sibling("dd")
            if not dd:
                continue

            label = self._normalize_text(dt.get_text(" ", strip=True))
            value = self._normalize_text(dd.get_text(" ", strip=True))
            if not value:
                continue

            if label == "営業時間":
                item[Schema.TIME] = value
            elif label == "支払い方法":
                item[Schema.PAYMENTS] = value
            elif label == "席数":
                item["席数"] = value
            elif label == "駐車場":
                item["駐車場"] = value
            elif label == "禁煙・喫煙":
                item["禁煙・喫煙"] = value
            elif label == "キッズスペース":
                item["キッズスペース"] = value
            elif label == "サロン電話番号":
                phone = self._normalize_phone(value)
                if phone:
                    item[Schema.TEL] = phone

        if Schema.NAME not in item:
            return None

        return item

    def _get_page_state(self, url: str) -> tuple[BeautifulSoup, str]:
        self.page.goto(url, wait_until="networkidle", timeout=120000)
        html = self.page.content()
        body_text = self.page.text_content("body") or ""
        return BeautifulSoup(html, "html.parser"), self._normalize_text(body_text)

    @staticmethod
    def _extract_text(elem) -> str:
        if elem is None:
            return ""
        return MinimoScraper._normalize_text(elem.get_text(" ", strip=True))

    @staticmethod
    def _split_name_and_kana(value: str) -> tuple[str, str]:
        match = re.match(r"^(.+?)([ァ-ヶー・\s]+)$", value)
        if match:
            return match.group(1).strip(), match.group(2).strip()
        return value.strip(), ""

    @staticmethod
    def _extract_station_from_description(soup) -> str:
        desc = soup.select_one('meta[name="description"]')
        content = desc.get("content", "") if desc else ""
        match = re.search(r"[(（]([^()（）]+駅)[)）]", content)
        return match.group(1).strip() if match else ""

    @staticmethod
    def _infer_category(soup, body_text: str) -> str:
        desc = soup.select_one('meta[name="description"]')
        content = desc.get("content", "") if desc else ""
        source = f"{content} {body_text}"
        for keyword in [
            "ヘアサロン",
            "ネイルサロン",
            "マツエクサロン",
            "眉毛サロン",
            "エステサロン",
            "リラクサロン",
            "美容室",
            "美容院",
        ]:
            if keyword in source:
                return keyword
        return ""

    @staticmethod
    def _normalize_phone(value: str) -> str:
        digits = re.sub(r"\D", "", value or "")
        if digits.startswith("0") and 10 <= len(digits) <= 11:
            return digits
        return ""

    @staticmethod
    def _normalize_text(text: str) -> str:
        return " ".join(str(text).split())

    @staticmethod
    def _dedupe(urls: list[str]) -> list[str]:
        seen: set[str] = set()
        deduped: list[str] = []
        for url in urls:
            normalized = url.rstrip("/")
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(normalized)
        return deduped


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    MinimoScraper().execute("https://minimodel.jp/sitemap.xml")
