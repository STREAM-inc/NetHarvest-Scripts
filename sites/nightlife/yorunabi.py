# ヨルナビ用https://yorumachi.jp/
"""
ヨルナビ (yorumachi.jp) — スナック・ラウンジ求人スクレイパー

取得対象:
    - 店舗名 / 名称_カナ / TEL / 営業時間 / 定休日 / 住所 / 都道府県 / サイト定義業種

取得フロー:
    1. sitemap.xml から求人詳細URLを収集（約295件）
    2. 求人詳細の motion.shop_detail 内 dl/dt/dd から店舗情報を抽出
    3. 店舗名 + 住所 + TEL で重複排除（1店舗1行）

実行方法:
    python scripts/sites/nightlife/yorunabi.py          # サイトマップ全件
    python scripts/sites/nightlife/yorunabi.py --sample # サンプル2件のみ
    python bin/run_flow.py --site-id yorunabi
"""

from __future__ import annotations

import re
import sys
import xml.etree.ElementTree as ET
from copy import copy
from pathlib import Path
from typing import Generator
from urllib.parse import urlparse

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
_MAP_LINK_RE = re.compile(r"勤務地の地図を見る\s*[>＞]?")
_TRAILING_ARROW_RE = re.compile(r"\s*[>＞〉]\s*$")
_NAME_KANA_RE = re.compile(r"^(.+?)[(（]([^)）]+)[)）]\s*$")


class YorunabiScraper(StaticCrawler):
    """ヨルナビ スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS: list[str] = []

    BASE_URL = "https://yorumachi.jp"
    SITEMAP_URL = "https://yorumachi.jp/sitemap.xml"
    SAMPLE_SEED = "sample"
    SAMPLE_TEST_URLS: tuple[str, ...] = (
        "https://yorumachi.jp/tokyo/job/tokyo/adachi/202605182027.html",
        "https://yorumachi.jp/tokyo/job/tokyo/adachi/201609071534.html",
    )

    _SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    _JOB_URL_RE = re.compile(r"^https://yorumachi\.jp/[a-z]+/job/.+\.html$")

    def parse(self, url: str) -> Generator[dict, None, None]:
        job_urls = self._collect_job_urls(url)
        self.total_items = len(job_urls)
        self.logger.info("対象求人URL数: %d", self.total_items)

        seen_shops: set[tuple[str, str, str]] = set()
        saved_count = 0
        duplicate_count = 0
        failed_count = 0

        for index, job_url in enumerate(job_urls, start=1):
            remaining = self.total_items - index
            try:
                soup = self.get_soup(job_url)
                if soup is None:
                    failed_count += 1
                    continue
                record = self._parse_job_page(job_url, soup)
            except Exception as e:
                failed_count += 1
                self.logger.warning(
                    "詳細取得失敗: %d/%d URL=%s (%s)",
                    index,
                    self.total_items,
                    job_url,
                    e,
                )
                continue

            if not record:
                failed_count += 1
                self.logger.warning(
                    "詳細取得スキップ: %d/%d URL=%s",
                    index,
                    self.total_items,
                    job_url,
                )
                continue

            shop_key = self._shop_key(record)
            if shop_key in seen_shops:
                duplicate_count += 1
                self.logger.info(
                    "店舗重複スキップ: %d/%d 店舗=%s",
                    index,
                    self.total_items,
                    record.get(Schema.NAME) or job_url,
                )
                continue

            seen_shops.add(shop_key)
            saved_count += 1
            self.logger.info(
                "詳細取得OK: %d/%d 残り%d件 店舗=%s",
                index,
                self.total_items,
                remaining,
                record.get(Schema.NAME) or job_url,
            )
            yield record

        self.logger.info(
            "詳細取得完了: 候補%d件 取得%d件 店舗重複スキップ%d件 失敗/スキップ%d件",
            self.total_items,
            saved_count,
            duplicate_count,
            failed_count,
        )

    def _collect_job_urls(self, seed_url: str) -> list[str]:
        if seed_url == self.SAMPLE_SEED:
            return list(self.SAMPLE_TEST_URLS)

        normalized = seed_url.rstrip("/")
        if self._JOB_URL_RE.match(normalized):
            return [normalized]

        sitemap_url = seed_url if seed_url.endswith(".xml") else self.SITEMAP_URL
        return self._collect_sitemap_job_urls(sitemap_url)

    def _collect_sitemap_job_urls(self, sitemap_url: str) -> list[str]:
        try:
            response = self.session.get(sitemap_url, timeout=self.TIMEOUT)
            response.raise_for_status()
            root = ET.fromstring(response.content)
        except Exception as e:
            self.logger.warning("サイトマップ取得失敗: %s (%s)", sitemap_url, e)
            return []

        loc_nodes = root.findall(".//sm:loc", self._SITEMAP_NS)
        urls = [node.text.strip() for node in loc_nodes if node.text]
        job_urls = [
            u
            for u in urls
            if self._JOB_URL_RE.match(u) and urlparse(u).netloc == "yorumachi.jp"
        ]
        return list(dict.fromkeys(job_urls))

    def _parse_job_page(self, job_url: str, soup: BeautifulSoup) -> dict | None:
        labels = self._extract_shop_detail_labels(soup)
        name_raw = self._clean(labels.get("店舗名", "")) or self._extract_name_from_h1(soup)
        name, kana = self._split_name_kana(name_raw)
        address = labels.get("住所", "")
        tel = self._extract_tel(soup)

        if not name:
            self.logger.warning("店舗名が空です: %s", job_url)
            return None
        if not address:
            self.logger.warning("住所が空です: %s", job_url)
            return None
        if not tel:
            self.logger.warning("TELが空です: %s", job_url)
            return None

        pref, addr_body = self._split_pref(address)

        return {
            Schema.URL: job_url,
            Schema.NAME: name,
            Schema.NAME_KANA: kana,
            Schema.PREF: pref,
            Schema.ADDR: addr_body or address,
            Schema.TEL: tel,
            Schema.TIME: self._clean(labels.get("営業時間", "")),
            Schema.HOLIDAY: self._clean(labels.get("定休日", "")),
            Schema.CAT_SITE: self._clean(labels.get("業種", "")),
        }

    def _extract_shop_detail_labels(self, soup: BeautifulSoup) -> dict[str, str]:
        data: dict[str, str] = {}
        container = soup.select_one("motion.shop_detail") or soup.select_one(".shop_detail")
        if container is None:
            return data

        for dl in container.find_all("dl"):
            for dt in dl.find_all("dt"):
                label = self._clean(dt.get_text(strip=True))
                if not label or label in data:
                    continue
                dd = dt.find_next_sibling("dd")
                if dd is None:
                    continue
                if label == "住所":
                    value = self._extract_address_from_dd(dd)
                else:
                    value = self._clean(dd.get_text(" ", strip=True))
                if value:
                    data[label] = value

        return data

    def _extract_name_from_h1(self, soup: BeautifulSoup) -> str:
        h1 = soup.find("h1")
        if not h1:
            return ""
        text = self._clean(h1.get_text(strip=True))
        if text.endswith("の求人情報"):
            text = text[: -len("の求人情報")].strip()
        return text

    def _split_name_kana(self, raw: str) -> tuple[str, str]:
        text = self._clean(raw)
        if not text:
            return "", ""
        match = _NAME_KANA_RE.match(text)
        if match:
            return self._clean(match.group(1)), self._clean(match.group(2))
        return text, ""

    def _extract_address_from_dd(self, dd) -> str:
        dd_copy = copy(dd)
        for anchor in dd_copy.find_all("a"):
            anchor.decompose()
        return self._clean_address(dd_copy.get_text(" ", strip=True))

    def _extract_tel(self, soup: BeautifulSoup) -> str:
        for anchor in soup.find_all("a", href=True):
            href = anchor.get("href", "")
            if not href.startswith("tel:"):
                continue
            raw = href.replace("tel:", "").strip()
            match = _TEL_PATTERN.search(raw)
            if match:
                return match.group(0)

        page_text = soup.get_text(" ", strip=True)
        match = _TEL_PATTERN.search(page_text)
        if match:
            return match.group(0)
        return ""

    def _shop_key(self, record: dict) -> tuple[str, str, str]:
        pref = record.get(Schema.PREF, "")
        addr = record.get(Schema.ADDR, "")
        full_address = f"{pref}{addr}" if pref else addr
        return (
            record.get(Schema.NAME, ""),
            full_address,
            record.get(Schema.TEL, ""),
        )

    def _clean_address(self, value: str) -> str:
        value = self._clean(value)
        value = _MAP_LINK_RE.sub("", value)
        value = _TRAILING_ARROW_RE.sub("", value)
        return value.strip()

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
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    use_sample = "--sample" in sys.argv
    seed = (
        YorunabiScraper.SAMPLE_SEED
        if use_sample
        else YorunabiScraper.SITEMAP_URL
    )

    scraper = YorunabiScraper()
    if use_sample:
        scraper.logger.info(
            "サンプルモード: %d件（全件は引数なしで実行）",
            len(YorunabiScraper.SAMPLE_TEST_URLS),
        )
    scraper.execute(seed)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
