import gzip
import json
import re
import sys
import time
from pathlib import Path
from typing import Generator
from xml.etree import ElementTree as ET

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

SITEMAP_URL = "https://byoinnavi.jp/sitemaps/sitemap.xml.gz"
CLINIC_RE = re.compile(r"^https://byoinnavi\.jp/clinic/\d+$")


class ByoinnaviScraper(StaticCrawler):
    """病院なびの医療機関情報を取得する静的クローラー。"""

    DELAY = 0.3

    EXTRA_COLUMNS = ["特徴", "備考", "情報更新日時"]

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
        if CLINIC_RE.match(url):
            return [url]

        sitemap_urls = self._fetch_gzip_xml_locs(url)
        detail_urls: list[str] = []

        for sitemap_url in sitemap_urls:
            if not sitemap_url.endswith(".xml.gz"):
                continue
            for loc in self._fetch_gzip_xml_locs(sitemap_url):
                if CLINIC_RE.match(loc):
                    detail_urls.append(loc)

        return self._dedupe(detail_urls)

    def _fetch_gzip_xml_locs(self, url: str) -> list[str]:
        self.logger.info("XML取得: %s", url)
        response = self.session.get(url, timeout=self.TIMEOUT)
        response.raise_for_status()

        content = gzip.decompress(response.content)
        root = ET.fromstring(content)
        urls: list[str] = []
        for elem in root.iter():
            if elem.tag.rsplit("}", 1)[-1] == "loc" and elem.text:
                urls.append(elem.text.strip())
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        self.logger.info("詳細ページ取得: %s", url)
        soup = self.get_soup(url)
        body_text = self._normalize_text(soup.get_text(" ", strip=True))

        item: dict[str, str] = {
            Schema.URL: url,
        }

        h1 = soup.select_one("h1")
        if h1:
            item[Schema.NAME] = self._normalize_text(h1.get_text(" ", strip=True))

        tel_link = soup.select_one("a[href^='tel:']")
        if tel_link:
            item[Schema.TEL] = tel_link.get("href", "").replace("tel:", "").strip()

        http_links = [a.get("href", "").strip() for a in soup.select("a[href^='http']")]
        for href in http_links:
            if "business.site" in href or href.startswith("http://") or href.startswith("https://"):
                if "google.com/maps" in href or "form-mailer" in href or "job-medley" in href:
                    continue
                if href != url:
                    item[Schema.HP] = href
                    break

        self._merge_json_ld(soup, item)
        self._merge_text_patterns(body_text, item)

        if Schema.NAME not in item:
            return None

        return item

    def _merge_json_ld(self, soup, item: dict[str, str]) -> None:
        for script in soup.select('script[type="application/ld+json"]'):
            raw = script.get_text(strip=True)
            if not raw:
                continue

            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue

            if not isinstance(payload, dict):
                continue
            if payload.get("@type") != "MedicalOrganization":
                continue

            if Schema.NAME not in item and payload.get("name"):
                item[Schema.NAME] = self._normalize_text(payload["name"])

            address = payload.get("address", {})
            if isinstance(address, dict):
                pref = self._normalize_text(address.get("addressRegion", ""))
                locality = self._normalize_text(address.get("addressLocality", ""))
                street = self._normalize_text(address.get("streetAddress", ""))
                if pref:
                    item[Schema.PREF] = pref
                full_address = "".join(part for part in [pref, locality, street] if part)
                if full_address:
                    item[Schema.ADDR] = full_address

            specialties = payload.get("medicalSpecialty", [])
            if isinstance(specialties, list) and specialties:
                item[Schema.CAT_SITE] = " , ".join(self._normalize_text(x) for x in specialties if x)

            return

    def _merge_text_patterns(self, body_text: str, item: dict[str, str]) -> None:
        address_match = re.search(r"所在地\s+(.+?)\s+電話", body_text)
        if address_match and Schema.ADDR not in item:
            item[Schema.ADDR] = address_match.group(1).replace("[アクセス]", "").strip()

        if Schema.PREF not in item and Schema.ADDR in item:
            pref_match = re.match(r"^(北海道|東京都|京都府|大阪府|..県)", item[Schema.ADDR])
            if pref_match:
                item[Schema.PREF] = pref_match.group(1)

        subjects_match = re.search(r"診療科目\s+(.+?)\s+これらの診療科目で他の医療機関を探す", body_text)
        if subjects_match and Schema.CAT_SITE not in item:
            item[Schema.CAT_SITE] = subjects_match.group(1).replace(" , ", ", ").strip()

        holiday_match = re.search(r"休診日:\s*(.+?)(?:\s+備考:|\s+医師・施設情報)", body_text)
        if holiday_match:
            item[Schema.HOLIDAY] = holiday_match.group(1).strip()

        note_match = re.search(r"備考:\s*(.+?)\s+医師・施設情報", body_text)
        if note_match:
            item["備考"] = note_match.group(1).strip()

        feature_match = re.search(r"機能・特徴\s+(.+?)\s+【掲載情報に関するご注意】", body_text)
        if feature_match:
            item["特徴"] = feature_match.group(1).strip()

        updated_match = re.search(r"情報更新日時:\s*(.+?)\s*\(医療機関ID", body_text)
        if updated_match:
            item["情報更新日時"] = updated_match.group(1).strip()

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
    ByoinnaviScraper().execute(SITEMAP_URL)
