"""
対象サイト: https://philippine-pub.com/
"""

from __future__ import annotations

import html
import re
import sys
from pathlib import Path
import xml.etree.ElementTree as ET
from typing import Generator
from urllib.parse import urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler


class PhilippinePubScraper(StaticCrawler):
    """フィリピンパブスクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["エリア", "FAX", "メール"]

    _SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    def parse(self, url: str) -> Generator[dict, None, None]:
        sitemap_index_url = self._resolve_sitemap_index_url(url)
        shop_urls = self._collect_shop_urls(sitemap_index_url)
        start_index = getattr(self, "_start_index", 0)
        if start_index > 0:
            self.logger.info("再開モード: 先頭 %d 件をスキップします", start_index)
        shop_urls = shop_urls[start_index:]
        self.total_items = len(shop_urls)
        self.logger.info("対象店舗ページ数: %d", self.total_items)

        for shop_url in shop_urls:
            item = self._parse_shop_page(shop_url)
            if item is not None:
                yield item

    def _resolve_sitemap_index_url(self, seed_url: str) -> str:
        parsed = urlparse(seed_url)
        if parsed.path.endswith(".xml"):
            return seed_url
        return f"{parsed.scheme}://{parsed.netloc}/sitemap_index.xml"

    def _collect_shop_urls(self, sitemap_index_url: str) -> list[str]:
        all_page_urls: list[str] = []
        for child_sitemap_url in self._extract_locs_from_sitemap(sitemap_index_url):
            all_page_urls.extend(self._extract_locs_from_sitemap(child_sitemap_url))

        deduped = list(dict.fromkeys(all_page_urls))
        shop_urls: list[str] = []
        for page_url in deduped:
            if self._is_possible_shop_url(page_url):
                shop_urls.append(page_url)
        return shop_urls

    def _extract_locs_from_sitemap(self, sitemap_url: str) -> list[str]:
        try:
            response = self.session.get(sitemap_url, timeout=self.TIMEOUT)
            response.raise_for_status()
            root = ET.fromstring(response.text)
        except Exception as e:
            self.logger.warning("サイトマップ取得失敗: %s (%s)", sitemap_url, e)
            return []

        loc_nodes = root.findall(".//sm:loc", self._SITEMAP_NS)
        locs = [node.text.strip() for node in loc_nodes if node.text]
        self.logger.info("URL抽出: %s -> %d件", sitemap_url, len(locs))
        return locs

    def _is_possible_shop_url(self, page_url: str) -> bool:
        parsed = urlparse(page_url)
        if parsed.netloc != "philippine-pub.com":
            return False
        path = parsed.path.rstrip("/")
        if not path or path.count("/") < 2:
            return False
        if path.endswith(".xml"):
            return False
        if any(token in path for token in ("/category/", "/tag/", "/author/", "/feed/")):
            return False
        return True

    def _parse_shop_page(self, page_url: str) -> dict | None:
        page_html = self._fetch_html_text(page_url)
        if not page_html:
            return None

        shop_info = self._extract_labeled_table_from_html(page_html, "Shop Information")
        if not shop_info:
            return None

        price_info = self._extract_labeled_table_from_html(page_html, "Price System")

        name = self._first_non_empty(
            shop_info.get("店名（英）"),
            shop_info.get("店名"),
            self._extract_h2_name(page_html),
        )
        address = self._clean(shop_info.get("住所"))
        post_code = self._extract_post_code(address)
        pref = self._extract_prefecture(address)
        tel = self._clean(shop_info.get("電話番号"))
        business = self._clean(shop_info.get("業態"))
        hp = self._normalize_placeholder(shop_info.get("ウェブサイト"))
        sns = self._parse_sns(shop_info.get("SNS"))
        business_hours = self._clean(shop_info.get("営業時間"))
        plain_text = self._html_to_text(page_html)
        status = "閉業" if "閉業" in plain_text else ""
        area = self._extract_area(page_html)
        fax = self._extract_from_text(r"(?:FAX|ＦＡＸ)\s*[:：]?\s*([0-9\-]{8,15})", plain_text)
        mail = self._extract_from_text(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", plain_text)
        payment_text = self._build_payments(price_info, page_html)

        return {
            Schema.NAME: name,
            Schema.TEL: tel,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: address,
            Schema.CAT_SITE: business,
            Schema.HP: hp,
            Schema.INSTA: sns.get("insta", ""),
            Schema.FB: sns.get("fb", ""),
            Schema.X: sns.get("x", ""),
            Schema.LINE: sns.get("line", ""),
            Schema.TIME: business_hours,
            Schema.STS_NM: status,
            Schema.PAYMENTS: payment_text,
            "エリア": area,
            "FAX": fax,
            "メール": mail,
        }

    def _fetch_html_text(self, url: str) -> str:
        try:
            response = self.session.get(url, timeout=self.TIMEOUT)
            response.raise_for_status()
            return self._decode_best_effort(response.content)
        except Exception as e:
            self.logger.warning("ページ取得失敗: %s (%s)", url, e)
            return ""

    def _decode_best_effort(self, raw: bytes) -> str:
        candidates: list[str] = []
        for enc in ("utf-8", "cp932", "euc-jp"):
            try:
                candidates.append(raw.decode(enc))
            except Exception:
                continue
        if not candidates:
            return raw.decode("utf-8", errors="ignore")

        def score(text: str) -> tuple[int, int]:
            mojibake_count = text.count("�")
            jp_char_count = len(re.findall(r"[ぁ-んァ-ン一-龥]", text))
            return (mojibake_count, -jp_char_count)

        return sorted(candidates, key=score)[0]

    def _extract_labeled_table_from_html(self, page_html: str, section_keyword: str) -> dict[str, str]:
        heading_match = re.search(
            rf"<h[23][^>]*>[^<]*{re.escape(section_keyword)}.*?</h[23]>",
            page_html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not heading_match:
            return {}

        after_heading = page_html[heading_match.end() :]
        table_match = re.search(r"<table[^>]*>(.*?)</table>", after_heading, flags=re.IGNORECASE | re.DOTALL)
        if not table_match:
            return {}

        rows: dict[str, str] = {}
        for tr_html in re.findall(r"<tr[^>]*>(.*?)</tr>", table_match.group(1), flags=re.IGNORECASE | re.DOTALL):
            cells = re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", tr_html, flags=re.IGNORECASE | re.DOTALL)
            if len(cells) < 2:
                continue
            key = self._clean(self._html_to_text(cells[0]))
            value = self._clean(self._html_to_text(cells[1]))
            if key:
                rows[key] = value
        return rows

    def _extract_h2_name(self, page_html: str) -> str:
        m = re.search(r"<h2[^>]*>(.*?)</h2>", page_html, flags=re.IGNORECASE | re.DOTALL)
        if not m:
            return ""
        return self._clean(self._html_to_text(m.group(1)))

    def _extract_post_code(self, address: str) -> str:
        if not address:
            return ""
        m = re.search(r"\d{3}-\d{4}", address)
        return m.group(0) if m else ""

    def _extract_prefecture(self, address: str) -> str:
        if not address:
            return ""
        pref_re = r"(北海道|東京都|(?:京都|大阪)府|..県)"
        m = re.search(pref_re, address)
        return m.group(1) if m else ""

    def _parse_sns(self, sns_text: str | None) -> dict[str, str]:
        if not sns_text:
            return {"insta": "", "fb": "", "x": "", "line": ""}
        normalized = self._normalize_placeholder(sns_text)
        if not normalized:
            return {"insta": "", "fb": "", "x": "", "line": ""}
        links = [part.strip() for part in normalized.split() if part.strip()]
        sns = {"insta": "", "fb": "", "x": "", "line": ""}
        for link in links:
            low = link.lower()
            if "instagram.com" in low:
                sns["insta"] = link
            elif "facebook.com" in low:
                sns["fb"] = link
            elif "x.com" in low or "twitter.com" in low:
                sns["x"] = link
            elif "line.me" in low:
                sns["line"] = link
        return sns

    def _extract_area(self, page_html: str) -> str:
        breadcrumb_match = re.search(
            r"<ul[^>]*class=[\"'][^\"']*breadcrumb[^\"']*[\"'][^>]*>(.*?)</ul>",
            page_html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not breadcrumb_match:
            return ""
        li_values = re.findall(r"<li[^>]*>(.*?)</li>", breadcrumb_match.group(1), flags=re.IGNORECASE | re.DOTALL)
        if len(li_values) < 2:
            return ""
        area_text = self._html_to_text(li_values[-2])
        return self._clean(area_text.replace(",", " ").replace("，", " "))

    def _build_payments(self, price_info: dict[str, str], page_html: str) -> str:
        payment_keywords = (
            "クレジットカード",
            "カード",
            "現金",
            "電子マネー",
            "QR",
            "PayPay",
            "d払い",
            "楽天ペイ",
            "au PAY",
            "Apple Pay",
            "Google Pay",
            "交通系",
        )
        parts: list[str] = []
        for key, value in price_info.items():
            row_text = f"{key}:{value}".strip(":")
            if any(keyword in row_text for keyword in payment_keywords):
                parts.append(row_text)

        if "card_all.gif" in page_html and not any("クレジットカード" in p for p in parts):
            parts.append("クレジットカード:利用可")
        return " / ".join(parts)

    def _normalize_placeholder(self, value: str | None) -> str:
        text = self._clean(value)
        if text in ("-", "－", "―", "–", "ー", "なし", "無し"):
            return ""
        return text

    def _clean(self, value: str | None) -> str:
        if not value:
            return ""
        return re.sub(r"\s+", " ", value).strip()

    def _first_non_empty(self, *values: str) -> str:
        for value in values:
            cleaned = self._clean(value)
            if cleaned:
                return cleaned
        return ""

    def _extract_from_text(self, pattern: str, text: str) -> str:
        m = re.search(pattern, text)
        return self._clean(m.group(1) if m and m.groups() else m.group(0) if m else "")

    def _html_to_text(self, html_fragment: str) -> str:
        text = re.sub(r"<[^>]+>", " ", html_fragment)
        return html.unescape(re.sub(r"\s+", " ", text)).strip()


if __name__ == "__main__":
    import argparse
    import logging

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Philippine Pub scraper")
    parser.add_argument("--url", default="https://philippine-pub.com/", help="seed URL or sitemap URL")
    parser.add_argument("--start-index", type=int, default=0, help="skip first N shop URLs")
    args = parser.parse_args()

    crawler = PhilippinePubScraper()
    crawler._start_index = max(args.start_index, 0)
    target = args.url
    crawler.execute(target)
