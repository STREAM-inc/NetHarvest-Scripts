# https://www.caba2.net/ 用
"""
キャバキャバ (www.caba2.net) — キャバクラ・ガールズバー店舗スクレイパー

取得対象:
    - 店舗名 / 名称_カナ / TEL / 営業時間 / 定休日 / 住所 / 都道府県
    - サイト定義業種 / HP / 支払い方法 / SNS

取得フロー:
    1. bar_sitemap.xml から店舗トップURLを収集（約5768件）
    2. 詳細ページ div.bar-info から店舗情報を抽出

実行方法:
    python scripts/sites/nightlife/caba2.py
    python bin/run_flow.py --site-id caba2
"""

from __future__ import annotations

import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup

from src.const.schema import Schema
from src.framework.static import StaticCrawler

_PREF_PATTERN = re.compile(
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
_TEL_PATTERN = re.compile(r"\d{2,4}-\d{2,4}-\d{4}")
_TEL_NOTE_RE = re.compile(r"「キャバキャバ見た」.*$")
_CARD_BRANDS = (
    "VISA",
    "AMERICAN EXPRESS",
    "Diners Club",
    "JCB",
    "Master Card",
    "NICOS",
    "UC Card",
)


class Caba2Scraper(StaticCrawler):
    """キャバキャバ スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS: list[str] = []

    BASE_URL = "https://www.caba2.net"
    SITEMAP_URL = "https://www.caba2.net/bar_sitemap.xml"
    AREA_SITEMAP_URL = "https://www.caba2.net/area_display_sitemap.xml"

    _SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    _SHOP_URL_RE = re.compile(
        r"^https://www\.caba2\.net/[^/]+/[^/]+/[^/]+/[^/]+/?$"
    )
    _LISTING_URL_RE = re.compile(r"/_list/?$")

    def parse(self, url: str) -> Generator[dict, None, None]:
        shop_urls = self._collect_shop_urls(url)
        self.total_items = len(shop_urls)
        self.logger.info("対象店舗URL数: %d", self.total_items)

        saved_count = 0
        failed_count = 0
        for index, shop_url in enumerate(shop_urls, start=1):
            remaining = self.total_items - index
            try:
                soup = self.get_soup(shop_url)
                if soup is None:
                    failed_count += 1
                    continue
                record = self._parse_shop_page(shop_url, soup)
            except Exception as e:
                failed_count += 1
                self.logger.warning(
                    "詳細取得失敗: %d/%d URL=%s (%s)",
                    index,
                    self.total_items,
                    shop_url,
                    e,
                )
                continue

            if record:
                saved_count += 1
                self.logger.info(
                    "詳細取得OK: %d/%d 残り%d件 店舗=%s",
                    index,
                    self.total_items,
                    remaining,
                    record.get(Schema.NAME) or shop_url,
                )
                yield record
            else:
                failed_count += 1
                self.logger.warning(
                    "詳細取得スキップ: %d/%d URL=%s",
                    index,
                    self.total_items,
                    shop_url,
                )

        self.logger.info(
            "詳細取得完了: 候補%d件 取得%d件 失敗/スキップ%d件",
            self.total_items,
            saved_count,
            failed_count,
        )

    def _collect_shop_urls(self, seed_url: str) -> list[str]:
        normalized = seed_url.rstrip("/")
        if self._SHOP_URL_RE.match(normalized + "/"):
            return [normalized + "/"]

        sitemap_url = seed_url if seed_url.endswith(".xml") else self.SITEMAP_URL
        shop_urls = self._collect_sitemap_urls(sitemap_url)
        if shop_urls:
            return shop_urls

        self.logger.warning(
            "bar_sitemap から店舗URLを取得できませんでした。一覧ページ巡回にフォールバックします"
        )
        return self._collect_shop_urls_from_listings()

    def _collect_sitemap_urls(self, sitemap_url: str) -> list[str]:
        try:
            response = self.session.get(sitemap_url, timeout=self.TIMEOUT)
            response.raise_for_status()
            root = ET.fromstring(response.content)
        except Exception as e:
            self.logger.warning("サイトマップ取得失敗: %s (%s)", sitemap_url, e)
            return []

        loc_nodes = root.findall(".//sm:loc", self._SITEMAP_NS)
        urls = [node.text.strip() for node in loc_nodes if node.text]
        shop_urls = [
            u
            for u in urls
            if self._SHOP_URL_RE.match(u.rstrip("/") + "/")
            and urlparse(u).netloc == "www.caba2.net"
        ]
        return list(dict.fromkeys(shop_urls))

    def _collect_shop_urls_from_listings(self) -> list[str]:
        listing_urls = self._collect_listing_urls(self.AREA_SITEMAP_URL)
        shop_urls: list[str] = []
        seen: set[str] = set()

        for listing_url in listing_urls:
            for shop_url in self._collect_shop_urls_from_listing(listing_url):
                if shop_url not in seen:
                    seen.add(shop_url)
                    shop_urls.append(shop_url)

        return shop_urls

    def _collect_listing_urls(self, sitemap_url: str) -> list[str]:
        try:
            response = self.session.get(sitemap_url, timeout=self.TIMEOUT)
            response.raise_for_status()
            root = ET.fromstring(response.content)
        except Exception as e:
            self.logger.warning("エリアサイトマップ取得失敗: %s (%s)", sitemap_url, e)
            return []

        loc_nodes = root.findall(".//sm:loc", self._SITEMAP_NS)
        urls = [node.text.strip() for node in loc_nodes if node.text]
        listing_urls = [
            u.rstrip("/")
            for u in urls
            if self._LISTING_URL_RE.search(urlparse(u).path.rstrip("/"))
        ]
        return list(dict.fromkeys(listing_urls))

    def _collect_shop_urls_from_listing(self, listing_url: str) -> list[str]:
        shop_urls: list[str] = []
        seen: set[str] = set()
        page = 1

        while True:
            page_url = listing_url if page == 1 else f"{listing_url}?page={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                break

            found_on_page = 0
            for anchor in soup.find_all("a", href=True):
                href = urljoin(self.BASE_URL, anchor["href"])
                parsed = urlparse(href)
                if parsed.netloc != "www.caba2.net":
                    continue
                normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}/"
                if not self._SHOP_URL_RE.match(normalized):
                    continue
                if normalized not in seen:
                    seen.add(normalized)
                    shop_urls.append(normalized)
                    found_on_page += 1

            if found_on_page == 0:
                break
            page += 1

        return shop_urls

    def _parse_shop_page(self, shop_url: str, soup: BeautifulSoup) -> dict | None:
        labels = self._extract_bar_info(soup)
        name = labels.get("店名", "")
        kana = labels.get("名称_カナ", "")
        address = self._clean(labels.get("住所", ""))
        tel = self._extract_tel(soup, labels)
        sns = self._extract_sns(soup)

        if not name:
            self.logger.warning("店舗名が空です: %s", shop_url)
            return None
        if not address:
            self.logger.warning("住所が空です: %s", shop_url)
            return None
        if not tel:
            self.logger.warning("TELが空です: %s", shop_url)
            return None

        pref, addr_body = self._split_pref(address)
        payments = self._extract_payments(soup)

        return {
            Schema.URL: shop_url,
            Schema.NAME: name,
            Schema.NAME_KANA: kana,
            Schema.PREF: pref,
            Schema.ADDR: addr_body or address,
            Schema.TEL: tel,
            Schema.TIME: self._clean(labels.get("営業時間", "")),
            Schema.HOLIDAY: self._clean(labels.get("店休日", "")),
            Schema.CAT_SITE: self._clean(labels.get("業種", "")),
            Schema.HP: self._clean(labels.get("HP", "")),
            Schema.PAYMENTS: payments,
            Schema.INSTA: sns["insta"],
            Schema.X: sns["x"],
            Schema.FB: sns["fb"],
            Schema.LINE: sns["line"],
            Schema.TIKTOK: sns["tiktok"],
        }

    def _extract_bar_info(self, soup: BeautifulSoup) -> dict[str, str]:
        data: dict[str, str] = {}
        for li in soup.select("div.bar-info ul.info li.item"):
            h4 = li.find("h4")
            if not h4:
                continue
            label = self._clean(h4.get_text(strip=True))
            if not label:
                continue

            if label == "店名":
                name_el = li.select_one("p.bar-name")
                kana_el = li.select_one("p.bar-kana")
                if name_el and "店名" not in data:
                    data["店名"] = self._clean(name_el.get_text(strip=True))
                if kana_el and "名称_カナ" not in data:
                    data["名称_カナ"] = self._clean(kana_el.get_text(strip=True))
                continue

            if label in ("SNS", "系列店", "スマホ版QR", "クーポンページ"):
                continue

            if label == "HP":
                if "HP" not in data:
                    anchor = li.find("a", href=True)
                    if anchor and anchor["href"].startswith("http"):
                        data["HP"] = anchor["href"].strip()
                continue

            if label in data:
                continue

            text_wrapper = li.select_one(".text-wrapper")
            if text_wrapper:
                value = self._clean(text_wrapper.get_text(" ", strip=True))
            else:
                parts: list[str] = []
                for paragraph in li.find_all("p"):
                    classes = paragraph.get("class") or []
                    if "bar-name" in classes or "bar-kana" in classes:
                        continue
                    text = self._clean(paragraph.get_text(" ", strip=True))
                    if text:
                        parts.append(text)
                value = self._clean(" ".join(parts))

            if value:
                data[label] = value

        return data

    def _extract_tel(self, soup: BeautifulSoup, labels: dict[str, str]) -> str:
        tel_link = soup.select_one("a[href^='tel:']")
        if tel_link:
            raw = tel_link.get("href", "").replace("tel:", "")
            match = _TEL_PATTERN.search(raw)
            if match:
                return match.group(0)

        raw = self._clean(labels.get("電話番号", ""))
        raw = _TEL_NOTE_RE.sub("", raw).strip()
        match = _TEL_PATTERN.search(raw)
        if match:
            return match.group(0)
        return raw

    def _extract_sns(self, soup: BeautifulSoup) -> dict[str, str]:
        sns = {"insta": "", "x": "", "fb": "", "line": "", "tiktok": ""}
        anchors = []

        for li in soup.select("div.bar-info ul.info li.item"):
            h4 = li.find("h4")
            if h4 and self._clean(h4.get_text(strip=True)) == "SNS":
                anchors.extend(li.find_all("a", href=True))
                break

        if not anchors:
            anchors = soup.select("div.bar-info a[href^='http']")

        for anchor in anchors:
            href = anchor.get("href", "").strip()
            if not href or href.startswith("javascript"):
                continue
            low = href.lower()
            if "instagram.com" in low and not sns["insta"]:
                sns["insta"] = href
            elif (
                ("x.com" in low or "twitter.com" in low)
                and "intent/tweet" not in low
                and not sns["x"]
            ):
                sns["x"] = href
            elif "facebook.com" in low and not sns["fb"]:
                sns["fb"] = href
            elif "line.me" in low and not sns["line"]:
                sns["line"] = href
            elif "tiktok.com" in low and not sns["tiktok"]:
                sns["tiktok"] = href

        return sns

    def _extract_payments(self, soup: BeautifulSoup) -> str:
        brands: list[str] = []
        seen: set[str] = set()

        for item in soup.select("ul.credit-card li"):
            brand = self._clean(item.get_text(" ", strip=True))
            if brand and brand not in seen:
                seen.add(brand)
                brands.append(brand)

        if brands:
            return " / ".join(brands)

        containers: list[BeautifulSoup] = []
        for heading in soup.find_all(["h3", "h4"]):
            if "ご利用可能" not in heading.get_text(strip=True):
                continue
            parent = heading.find_parent("div")
            if parent is not None:
                containers.append(parent)

        if not containers:
            containers = soup.select("[class*='fee']")

        for container in containers:
            text = container.get_text(" ", strip=True)
            for brand in _CARD_BRANDS:
                if brand in text and brand not in seen:
                    seen.add(brand)
                    brands.append(brand)

        return " / ".join(brands)

    def _split_pref(self, address: str) -> tuple[str, str]:
        if not address:
            return "", ""
        match = _PREF_PATTERN.match(address)
        if not match:
            return "", address
        pref = match.group(1)
        return pref, address[match.end() :].strip()

    def _clean(self, value: str | None) -> str:
        if not value:
            return ""
        return re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Caba2Scraper()
    scraper.execute(Caba2Scraper.SITEMAP_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
