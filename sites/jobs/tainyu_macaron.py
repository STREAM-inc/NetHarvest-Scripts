"""
対象サイト: https://picsastock.com/sitemap.xml
"""

import csv
import gzip
import re
import sys
import time
import xml.etree.ElementTree as ET
from html import unescape
from pathlib import Path
from typing import Generator
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler


BASE_URL = "https://picsastock.com"
NAME_FURI_RE = re.compile(r"^(?P<name>.*?)[（(](?P<furi>[^）)]+)[）)]\s*$")
PHONE_RE = re.compile(r"(?:0\d{1,4}-\d{1,4}-\d{3,4}|0\d{9,10})")


def normalize_ws(value: str | None) -> str:
    if value is None:
        return ""
    value = unescape(value).replace("\u3000", " ").replace("\xa0", " ")
    return re.sub(r"\s+", " ", value).strip()


def split_name_furi(raw_name: str) -> tuple[str, str]:
    raw_name = normalize_ws(raw_name)
    match = NAME_FURI_RE.match(raw_name)
    if match:
        return normalize_ws(match.group("name")), normalize_ws(match.group("furi"))
    return raw_name, ""


def is_shop_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return False
    if parsed.netloc not in ("picsastock.com", "www.picsastock.com"):
        return False
    if parsed.path.rstrip("/") != "/kw":
        return False
    query = parse_qs(parsed.query)
    return "q" in query and len(query["q"]) == 1 and re.fullmatch(r"\d+", query["q"][0]) is not None


class TainyuMacaronScraper(StaticCrawler):
    """体入マカロン 店舗情報スクレイパー"""

    DELAY = 3.0
    SITEMAP_DELAY = 0.5
    EXTRA_COLUMNS = [
        "名称_フリガナ",
        "業種",
        "エリア",
        "最寄り駅",
        "掲載開始日",
        "掲載終了日",
        "掲載終了時刻",
        "応募方法",
        "応募電話",
    ]

    def prepare(self) -> None:
        """初期化処理"""
        self.processed_urls: set[str] = set()
        resume_from_csv = r"C:\NetHarvest\output\20260514_TainyuMacaronScraper_17881件.csv"
        if Path(resume_from_csv).exists():
            self._load_processed_urls(resume_from_csv)

    def _load_processed_urls(self, csv_path: str) -> None:
        """既に処理されたURLを読み込む"""
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get('取得URL'):
                        self.processed_urls.add(row['取得URL'])
            self.logger.info("既に処理済みのURL: %d 件", len(self.processed_urls))
        except Exception as exc:
            self.logger.warning("既存CSVの読み込み失敗: %s", exc)

    def parse(self, url: str) -> Generator[dict, None, None]:
        shop_urls = self._collect_shop_urls(url)
        self.total_items = len(shop_urls)
        self.logger.info("店舗URL収集完了: %d 件", len(shop_urls))

        # 未処理のURLのみをフィルター
        unprocessed_urls = [u for u in shop_urls if u not in self.processed_urls]
        self.logger.info("未処理URL: %d 件", len(unprocessed_urls))

        for shop_url in unprocessed_urls:
            item = self._scrape_detail(shop_url)
            if item:
                yield item

    def _collect_shop_urls(self, sitemap_url: str) -> list[str]:
        sitemap_pages = self._collect_sitemap_pages(sitemap_url)
        self.logger.info("店舗サイトマップページ収集完了: %d 件", len(sitemap_pages))

        urls: list[str] = []
        seen: set[str] = set()
        for page_url in sitemap_pages:
            try:
                soup = self.get_soup(page_url)
                if soup is None:
                    continue
                for link in soup.select("a[href]"):
                    href = normalize_ws(link.get("href"))
                    shop_url = urljoin(BASE_URL, href)
                    if is_shop_url(shop_url) and shop_url not in seen:
                        seen.add(shop_url)
                        urls.append(shop_url)
            except Exception as exc:
                self.logger.warning("店舗サイトマップ解析をスキップ: %s (%s)", page_url, exc)
            finally:
                time.sleep(self.SITEMAP_DELAY)

        return urls

    def _collect_sitemap_pages(self, sitemap_url: str) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()

        try:
            response = self.session.get(sitemap_url, timeout=self.TIMEOUT)
            response.raise_for_status()
            content = self._decode_response_content(response.content)
            root = ET.fromstring(content)
        except Exception as exc:
            self.logger.warning("サイトマップ取得失敗: %s (%s)", sitemap_url, exc)
            return []

        locs = [
            normalize_ws(elem.text)
            for elem in root.iter()
            if elem.tag.endswith("loc") and elem.text
        ]
        for loc in locs:
            if loc.endswith("/shop") and loc not in seen:
                seen.add(loc)
                urls.append(loc)

        return urls

    def _decode_response_content(self, content: bytes) -> bytes:
        if content.startswith(b"\x1f\x8b"):
            return gzip.decompress(content)
        return content

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        block = soup.select_one("div.shop_detail") or soup
        table = block.select_one("table.shop_detail__table") or block

        name, furi = split_name_furi(self._extract_by_label(table, "店名"))
        if not name:
            self.logger.warning("店名が取得できないためスキップ: %s", url)
            return None

        term = self._extract_by_label(table, "掲載期間")
        start_date, end_date, end_time = self._split_term(term)
        apply_method = self._extract_by_label(table, "応募方法")

        return {
            Schema.URL: url,
            Schema.NAME: name,
            "名称_フリガナ": furi,
            "業種": self._extract_by_label(table, "業種"),
            "エリア": self._extract_by_label(table, "エリア"),
            Schema.ADDR: self._extract_by_label(table, "住所"),
            "最寄り駅": self._extract_by_label(table, "最寄り駅"),
            Schema.TIME: self._extract_by_label(table, "営業時間"),
            Schema.HOLIDAY: self._extract_by_label(table, "定休日"),
            "掲載開始日": start_date,
            "掲載終了日": end_date,
            "掲載終了時刻": end_time,
            "応募方法": apply_method,
            "応募電話": self._extract_phone(apply_method),
        }

    def _extract_by_label(self, table: BeautifulSoup, label: str) -> str:
        for th in table.select("th.shop_detail__label"):
            if normalize_ws(th.get_text()) != label:
                continue
            td = th.find_next("td")
            if td:
                return normalize_ws(" ".join(part.strip() for part in td.stripped_strings))
        return ""

    def _split_term(self, term: str) -> tuple[str, str, str]:
        if not term:
            return "", "", ""

        parts = re.split(r"\s*[~〜]\s*", term)
        start_date = normalize_ws(parts[0]) if parts else ""
        end_date = ""
        end_time = ""

        if len(parts) >= 2:
            right = normalize_ws(parts[1])
            match = re.search(r"(.+?)\s+(\d{1,2}:\d{2})$", right)
            if match:
                end_date = normalize_ws(match.group(1))
                end_time = match.group(2)
            else:
                end_date = right

        return start_date, end_date, end_time

    def _extract_phone(self, text: str) -> str:
        match = PHONE_RE.search(text or "")
        return match.group(0) if match else ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    TainyuMacaronScraper().execute("https://picsastock.com/sitemap.xml")
