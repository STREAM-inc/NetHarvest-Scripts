"""
キャバノリ (cabanori.com) — 西東京/多摩地区のキャバクラ・ガールズバー店舗スクレイパー

取得対象:
    - 店舗名 / 都道府県 / 住所 / TEL / 業種 / 営業時間 / 定休日 / LINE

取得フロー:
    1. /shops からエリアスラッグ (/shops/{slug}) を列挙
    2. 各エリアページから /shops/{id} を収集して dedupe
    3. 詳細ページ `table.p-content--shop-information__table` から店舗情報を抽出

実行方法:
    python scripts/sites/nightlife/cabanori.py
    python bin/run_flow.py --site-id cabanori
"""

from __future__ import annotations

import re
import sys
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
_SHOP_ID_RE = re.compile(r"^/shops/(\d+)(?:[/?#]|$)")
_AREA_SLUG_RE = re.compile(r"^/shops/([a-z_]+)$")


class CabanoriScraper(StaticCrawler):
    """キャバノリ スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS: list[str] = []

    BASE_URL = "https://cabanori.com"
    SHOPS_INDEX_URL = "https://cabanori.com/shops"

    def parse(self, url: str) -> Generator[dict, None, None]:
        shop_urls = self._collect_shop_urls()
        self.total_items = len(shop_urls)
        self.logger.info("対象店舗URL数: %d", self.total_items)

        saved = 0
        failed = 0
        for index, shop_url in enumerate(shop_urls, start=1):
            remaining = self.total_items - index
            try:
                record = self._scrape_detail(shop_url)
            except Exception as e:
                failed += 1
                self.logger.warning(
                    "詳細取得失敗: %d/%d URL=%s (%s)",
                    index, self.total_items, shop_url, e,
                )
                continue

            if record:
                saved += 1
                self.logger.info(
                    "詳細取得OK: %d/%d 残り%d件 店舗=%s",
                    index, self.total_items, remaining,
                    record.get(Schema.NAME) or shop_url,
                )
                yield record
            else:
                failed += 1

        self.logger.info(
            "詳細取得完了: 候補%d件 取得%d件 失敗/スキップ%d件",
            self.total_items, saved, failed,
        )

    def _collect_shop_urls(self) -> list[str]:
        area_slugs = self._collect_area_slugs()
        self.logger.info("エリアスラッグ数: %d", len(area_slugs))

        seen: set[str] = set()
        ordered: list[str] = []

        # /shops トップとエリア別ページを順番に巡回
        targets = [self.SHOPS_INDEX_URL] + [
            f"{self.BASE_URL}/shops/{slug}" for slug in area_slugs
        ]

        for listing_url in targets:
            for shop_url in self._collect_shop_urls_from_listing(listing_url):
                if shop_url not in seen:
                    seen.add(shop_url)
                    ordered.append(shop_url)

        return ordered

    def _collect_area_slugs(self) -> list[str]:
        soup = self.get_soup(self.SHOPS_INDEX_URL)
        if soup is None:
            return []
        slugs: list[str] = []
        seen: set[str] = set()
        for anchor in soup.find_all("a", href=True):
            href = anchor["href"]
            parsed = urlparse(urljoin(self.BASE_URL, href))
            if parsed.netloc and parsed.netloc != "cabanori.com":
                continue
            m = _AREA_SLUG_RE.match(parsed.path)
            if not m:
                continue
            slug = m.group(1)
            if slug not in seen:
                seen.add(slug)
                slugs.append(slug)
        return slugs

    def _collect_shop_urls_from_listing(self, listing_url: str) -> list[str]:
        soup = self.get_soup(listing_url)
        if soup is None:
            return []
        urls: list[str] = []
        seen: set[str] = set()
        for anchor in soup.find_all("a", href=True):
            href = anchor["href"]
            parsed = urlparse(urljoin(self.BASE_URL, href))
            if parsed.netloc and parsed.netloc != "cabanori.com":
                continue
            m = _SHOP_ID_RE.match(parsed.path)
            if not m:
                continue
            shop_url = f"{self.BASE_URL}/shops/{m.group(1)}"
            if shop_url not in seen:
                seen.add(shop_url)
                urls.append(shop_url)
        return urls

    def _scrape_detail(self, shop_url: str) -> dict | None:
        soup = self.get_soup(shop_url)
        if soup is None:
            return None

        labels = self._extract_info_table(soup)
        name = self._clean(labels.get("店名", ""))
        if not name:
            self.logger.warning("店舗名が空です: %s", shop_url)
            return None

        address = self._clean(labels.get("所在地", ""))
        pref, addr_body = self._split_pref(address)
        tel = self._extract_tel(soup)
        line = self._extract_line(soup)

        return {
            Schema.URL: shop_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr_body or address,
            Schema.TEL: tel,
            Schema.CAT_SITE: self._clean(labels.get("業種", "")),
            Schema.TIME: self._clean(labels.get("営業時間", "")),
            Schema.HOLIDAY: self._clean(labels.get("定休日", "")),
            Schema.LINE: line,
        }

    def _extract_info_table(self, soup: BeautifulSoup) -> dict[str, str]:
        data: dict[str, str] = {}
        table = soup.select_one("table.p-content--shop-information__table")
        if not table:
            return data
        for tr in table.select("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not (th and td):
                continue
            label = re.sub(r"\s+", "", th.get_text(strip=True).replace("　", ""))
            value = self._clean(td.get_text(" ", strip=True))
            if label and label not in data:
                data[label] = value
        return data

    def _extract_tel(self, soup: BeautifulSoup) -> str:
        tel_link = soup.select_one("a[href^='tel:']")
        if not tel_link:
            return ""
        raw = tel_link.get("href", "").replace("tel:", "")
        m = _TEL_PATTERN.search(raw)
        return m.group(0) if m else raw.strip()

    def _extract_line(self, soup: BeautifulSoup) -> str:
        body = soup.select_one("div.p-shop__body-icons")
        if not body:
            return ""
        for anchor in body.find_all("a", href=True):
            href = anchor["href"].strip()
            if "line.me" in href.lower() or "lin.ee" in href.lower():
                return href
        return ""

    def _split_pref(self, address: str) -> tuple[str, str]:
        if not address:
            return "", ""
        m = _PREF_PATTERN.match(address)
        if not m:
            return "", address
        return m.group(1), address[m.end():].strip()

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

    scraper = CabanoriScraper()
    scraper.execute(CabanoriScraper.SHOPS_INDEX_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
