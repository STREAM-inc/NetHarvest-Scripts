"""
Cabago (lulinego.jp) shop list scraper.

Target:
    https://lulinego.jp/
"""

from __future__ import annotations

import math
import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

from bs4 import BeautifulSoup

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler


BASE_URL = "https://lulinego.jp/"
AREAMAP_URL = urljoin(BASE_URL, "areamap/")
MEDIA_NAME = "\u30ad\u30e3\u30d0\u30b4\u30fc"

LABEL_ADDR = "\u4f4f\u6240"
LABEL_HOURS = "\u55b6\u696d\u6642\u9593"
LABEL_HOLIDAY = "\u5b9a\u4f11\u65e5"

COL_AREA = "\u30a8\u30ea\u30a2"
COL_MEDIA = "\u63b2\u8f09\u5a92\u4f53\u540d"
COL_PHONE_KIND = "\u96fb\u8a71\u756a\u53f7\u7a2e\u5225"
COL_STORE_PHONE = "\u5e97\u8217\u756a\u53f7"
COL_MOBILE_PHONE = "\u643a\u5e2f\u756a\u53f7"

KIND_STORE = "\u5e97\u8217\u756a\u53f7"
KIND_MOBILE = "\u643a\u5e2f\u756a\u53f7"

_AREA_TEXT_RE = re.compile(r"(.+?)\((\d+)\)$")
_MOBILE_RE = re.compile(r"^0[789]0")
_PREF_RE = re.compile(
    r"^(北海道|東京都|京都府|大阪府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_TEL_RE = re.compile(
    r"(?<!\d)(?:0\d{1,4}[-ー−]?\d{1,4}[-ー−]?\d{3,4}|"
    r"0[789]0[-ー−]?\d{4}[-ー−]?\d{4}|0120[-ー−]?\d{3}[-ー−]?\d{3})(?!\d)"
)


def _clean(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value.replace("\xa0", " ")).strip()


def _phone_digits(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def _split_pref(address: str, fallback_pref: str = "") -> tuple[str, str]:
    address = _clean(address)
    match = _PREF_RE.match(address)
    if not match:
        return fallback_pref, address
    pref = match.group(1)
    return pref, address[match.end() :].strip()


def _normalize_pref_heading(value: str) -> str:
    value = _clean(value)
    if not value:
        return ""
    if value in {"\u5317\u6d77\u9053", "\u6771\u4eac\u90fd", "\u4eac\u90fd\u5e9c", "\u5927\u962a\u5e9c"} or value.endswith("\u770c"):
        return value
    if value == "\u6771\u4eac":
        return "\u6771\u4eac\u90fd"
    if value == "\u4eac\u90fd":
        return "\u4eac\u90fd\u5e9c"
    if value == "\u5927\u962a":
        return "\u5927\u962a\u5e9c"
    return f"{value}\u770c"


def _industry_levels(name: str) -> dict[str, str]:
    text = name.lower()
    lv1 = "\u98f2\u98df\u5e97"
    lv2 = "\u30ca\u30a4\u30c8\u30ec\u30b8\u30e3\u30fc"
    lv3 = "\u30ad\u30e3\u30d0\u30af\u30e9\u30fb\u30af\u30e9\u30d6\u30fb\u30e9\u30a6\u30f3\u30b8"

    if (
        "\u30ac\u30fc\u30eb\u30ba\u30d0\u30fc" in text
        or "\u30ac\u30fc\u30eb\u30ba" in text
        or ("girl" in text and "bar" in text)
    ):
        lv3 = "\u30ac\u30fc\u30eb\u30ba\u30d0\u30fc"
    elif "\u30b3\u30f3\u30ab\u30d5\u30a7" in text or "concept" in text:
        lv3 = "\u30b3\u30f3\u30ab\u30d5\u30a7"
    elif "\u30b9\u30ca\u30c3\u30af" in text:
        lv3 = "\u30b9\u30ca\u30c3\u30af"
    elif "\u30e9\u30a6\u30f3\u30b8" in text or "lounge" in text:
        lv3 = "\u30e9\u30a6\u30f3\u30b8"
    elif "\u30af\u30e9\u30d6" in text or "club" in text:
        lv3 = "\u30af\u30e9\u30d6"
    elif "\u30ad\u30e3\u30d0" in text or "caba" in text:
        lv3 = "\u30ad\u30e3\u30d0\u30af\u30e9"
    elif "\u30d0\u30fc" in text or "bar" in text:
        lv3 = "\u30d0\u30fc"

    return {
        Schema.CAT_LV1: lv1,
        Schema.CAT_LV2: lv2,
        Schema.CAT_LV3: lv3,
        Schema.CAT_NM: lv3,
        Schema.CAT_SITE: lv3,
    }


class CabagoScraper(StaticCrawler):
    """Cabago shop list scraper."""

    USER_AGENT = "NetHarvestBot/1.0 (+https://github.com/STREAM-inc/NetHarvest-Scripts)"
    DELAY = 0.0
    REQUEST_DELAY = 0.4
    ITEMS_PER_PAGE = 30

    EXTRA_COLUMNS = [
        COL_AREA,
        COL_MEDIA,
        COL_PHONE_KIND,
        COL_STORE_PHONE,
        COL_MOBILE_PHONE,
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        area_infos = self._collect_area_infos(AREAMAP_URL)
        self.total_items = sum(info["count"] for info in area_infos)
        self.logger.info("Area links collected: %d, expected shops: %d", len(area_infos), self.total_items)

        seen: set[tuple[str, str, str]] = set()
        yielded = 0

        for info in area_infos:
            page_count = max(1, math.ceil(info["count"] / self.ITEMS_PER_PAGE))
            for page in range(1, page_count + 1):
                list_url = info["url"] if page == 1 else f"{info['url']}&paging={page}"
                time.sleep(self.REQUEST_DELAY)

                soup = self.get_soup(list_url)
                if soup is None:
                    continue

                blocks = [h2.parent for h2 in soup.select(".l-mainContent__inner .post_content h2")]
                if not blocks:
                    self.logger.warning("No shop blocks found: %s", list_url)
                    continue

                for block in blocks:
                    item = self._parse_shop_block(block, info, list_url)
                    if not item:
                        continue

                    key = (
                        item.get(Schema.NAME, ""),
                        item.get(Schema.ADDR, ""),
                        item.get(Schema.TEL, ""),
                    )
                    if key in seen:
                        continue
                    seen.add(key)
                    yielded += 1
                    yield item

        self.logger.info("Cabago scrape completed: yielded=%d, expected=%d", yielded, self.total_items)

    def _collect_area_infos(self, areamap_url: str) -> list[dict[str, object]]:
        soup = self.get_soup(areamap_url)
        if soup is None:
            return []

        root = soup.select_one(".areamap-list")
        if root is None:
            self.logger.error("Areamap list not found: %s", areamap_url)
            return []

        top_ul = root.find("ul", recursive=False)
        if top_ul is None:
            self.logger.error("Areamap top ul not found: %s", areamap_url)
            return []

        infos: list[dict[str, object]] = []
        for pref_li in top_ul.find_all("li", recursive=False):
            nested = pref_li.find("ul", recursive=False)
            if nested is None:
                continue

            pref = self._extract_pref_name(pref_li)
            for anchor in nested.select('a[href*="/list/"]'):
                text = _clean(anchor.get_text(" ", strip=True))
                match = _AREA_TEXT_RE.match(text)
                if not match:
                    continue
                area = match.group(1)
                count = int(match.group(2))
                infos.append(
                    {
                        "pref": pref,
                        "area": area,
                        "count": count,
                        "url": urljoin(BASE_URL, anchor.get("href", "")),
                    }
                )
        return infos

    def _extract_pref_name(self, pref_li) -> str:
        for child in pref_li.children:
            if getattr(child, "name", None) == "ul":
                break
            if getattr(child, "get_text", None):
                text = _clean(child.get_text(" ", strip=True))
            else:
                text = _clean(str(child))
            if text:
                return _normalize_pref_heading(text)
        first = pref_li.find(string=True, recursive=False)
        return _normalize_pref_heading(str(first)) if first else ""

    def _parse_shop_block(self, block, info: dict[str, object], list_url: str) -> dict | None:
        h2 = block.find("h2")
        name = _clean(h2.get_text(" ", strip=True) if h2 else "")
        if not name:
            return None

        rows = self._extract_table_rows(block)
        pref, addr = _split_pref(rows.get(LABEL_ADDR, ""), str(info.get("pref", "")))
        hours = rows.get(LABEL_HOURS, "")
        holiday = rows.get(LABEL_HOLIDAY, "")
        phone = self._extract_phone(block)
        digits = _phone_digits(phone)
        is_mobile = bool(_MOBILE_RE.match(digits))

        item = {
            Schema.URL: list_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: phone,
            Schema.TIME: hours,
            Schema.HOLIDAY: holiday,
            COL_AREA: str(info.get("area", "")),
            COL_MEDIA: MEDIA_NAME,
            COL_PHONE_KIND: KIND_MOBILE if is_mobile else (KIND_STORE if phone else ""),
            COL_STORE_PHONE: "" if is_mobile else phone,
            COL_MOBILE_PHONE: phone if is_mobile else "",
        }
        item.update(_industry_levels(name))
        return item

    def _extract_table_rows(self, block) -> dict[str, str]:
        rows: dict[str, str] = {}
        for tr in block.select("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            key = _clean(th.get_text(" ", strip=True))
            value = _clean(td.get_text(" ", strip=True))
            if key and value:
                rows[key] = value
        return rows

    def _extract_phone(self, block) -> str:
        for anchor in block.select('a[href^="tel:"]'):
            text = _clean(anchor.get_text(" ", strip=True))
            if text:
                return text

        text = _clean(block.get_text(" ", strip=True))
        match = _TEL_RE.search(text)
        return match.group(0) if match else ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = CabagoScraper()
    scraper.execute(BASE_URL)

    print(f"\nOutput file: {scraper.output_filepath}")
    print(f"Items: {scraper.item_count}")
