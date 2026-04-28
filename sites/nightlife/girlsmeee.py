"""
対象サイト: https://girlsmeee.com/sitemap.xml
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urlparse
import xml.etree.ElementTree as ET

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# h1 例: 「店名 兵庫県神戸市…のガールズバー(勤務)の求人の魅力」→ 店名は都道府県より前の部分に置くことが多い
_PREF_HEADING_RE = re.compile(
    r"(?:北海道|東京都|大阪府|京都府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|和歌山|"
    r"鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県)"
)


class GirlsmeeeScraper(StaticCrawler):
    """Girlsmeee 求人ページ スクレイパー"""

    DELAY = 1.0
    _SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    _SHOP_URL_RE = re.compile(r"^https://girlsmeee\.com/(?:kanto|kansai)/[^/]+/\d+$")

    def parse(self, url: str) -> Generator[dict, None, None]:
        sitemap_url = self._resolve_sitemap_url(url)
        shop_urls = self._collect_shop_urls(sitemap_url)
        self.total_items = len(shop_urls)
        self.logger.info("対象詳細URL数: %d", self.total_items)

        for shop_url in shop_urls:
            soup = self.get_soup(shop_url)
            if soup is None:
                continue

            item = self._parse_shop_page(shop_url, soup.get_text(" ", strip=True), soup)
            if item[Schema.NAME]:
                yield item

    def _resolve_sitemap_url(self, seed_url: str) -> str:
        parsed = urlparse(seed_url)
        if parsed.path.endswith(".xml"):
            return seed_url
        return f"{parsed.scheme}://{parsed.netloc}/sitemap.xml"

    def _collect_shop_urls(self, sitemap_url: str) -> list[str]:
        try:
            response = self.session.get(sitemap_url, timeout=self.TIMEOUT)
            response.raise_for_status()
            root = ET.fromstring(response.content)
        except Exception as e:
            self.logger.warning("サイトマップ取得失敗: %s (%s)", sitemap_url, e)
            return []

        loc_nodes = root.findall(".//sm:loc", self._SITEMAP_NS)
        urls = [node.text.strip() for node in loc_nodes if node.text]
        shop_urls = [u for u in urls if self._is_shop_url(u)]
        return list(dict.fromkeys(shop_urls))

    def _is_shop_url(self, page_url: str) -> bool:
        if not self._SHOP_URL_RE.match(page_url):
            return False
        parsed = urlparse(page_url)
        if parsed.netloc != "girlsmeee.com":
            return False
        return True

    def _parse_shop_page(self, page_url: str, plain_text: str, soup) -> dict:
        labels = self._extract_labeled_values(soup)

        title = self._clean(soup.title.get_text(" ", strip=True) if soup.title else "")
        h1 = self._clean(soup.select_one("h1").get_text(" ", strip=True) if soup.select_one("h1") else "")
        ld_org_name = self._extract_organization_name_from_ldjson(soup)
        compact_h1 = self._compact_heading_to_shop_name(h1)
        compact_title = self._compact_heading_to_shop_name(title)
        name_from_heading = self._take_shop_name_from_heading(compact_h1)
        name = self._first_non_empty(
            name_from_heading,
            labels.get("店舗名"),
            labels.get("店名"),
            ld_org_name,
            compact_h1,
            compact_title,
        )

        tel = self._extract_tel(soup, labels, plain_text)
        address = self._first_non_empty(labels.get("住所"), self._extract_address_from_ldjson(soup))
        business_hours = self._extract_store_hours(labels)
        hp = self._extract_hp_url(soup, labels)
        sns = self._extract_sns_urls(soup)

        return {
            Schema.URL: page_url,
            Schema.NAME: name,
            Schema.ADDR: self._clean(address),
            Schema.TEL: tel,
            Schema.TIME: self._clean(business_hours),
            Schema.HP: hp,
            Schema.INSTA: sns["insta"],
            Schema.X: sns["x"],
            Schema.FB: sns["fb"],
            Schema.LINE: sns["line"],
        }

    def _extract_labeled_values(self, soup) -> dict[str, str]:
        data: dict[str, str] = {}

        for li in soup.select("li.list-group-item"):
            label_node = li.select_one("div.text-xs.text-primary")
            if label_node:
                key = self._clean(label_node.get_text(" ", strip=True))
                value = self._extract_value_from_li(li)
                if key and value and key not in data:
                    data[key] = value

            key_span = li.select_one("span.text-primary")
            val_anchor = li.select_one("a[href]")
            if key_span and val_anchor:
                key = self._clean(key_span.get_text(" ", strip=True))
                value = self._clean(val_anchor.get_text(" ", strip=True))
                if key and value and key not in data:
                    data[key] = value

        return data

    def _extract_value_from_li(self, li) -> str:
        # ラベル付きLIは値のDOM構造が複数パターンあるため、ラベルdiv以外の直下divから最初の非空値を採用する
        for div in li.find_all("div", recursive=False):
            classes = div.get("class") or []
            if "text-xs" in classes and "text-primary" in classes:
                continue
            text = self._clean(div.get_text(" ", strip=True))
            if text:
                return text
        value_node = li.select_one("div.break-word.break-spaces")
        return self._clean(value_node.get_text(" ", strip=True)) if value_node else ""

    def _extract_store_hours(self, labels: dict[str, str]) -> str:
        if labels.get("営業時間"):
            return self._clean(labels.get("営業時間"))
        for key, value in labels.items():
            norm_key = key.replace(" ", "")
            if "営業" in norm_key and "時間" in norm_key:
                cleaned = self._clean(value)
                if cleaned:
                    return cleaned
        return ""

    def _extract_tel(self, soup, labels: dict[str, str], plain_text: str) -> str:
        tel_link = soup.select_one("a[href^='tel:']")
        if tel_link:
            normalized = re.sub(r"[^\d\-]", "", tel_link.get("href", "").replace("tel:", ""))
            if normalized:
                return normalized

        for key, value in labels.items():
            if "TEL" in key.upper() or "電話" in key:
                m = re.search(r"0\d{1,4}-?\d{1,4}-?\d{3,4}", value)
                if m:
                    return m.group(0)

        m = re.search(r"0\d{1,4}-?\d{1,4}-?\d{3,4}", plain_text)
        return m.group(0) if m else ""

    def _extract_organization_name_from_ldjson(self, soup) -> str:
        """JobPosting の hiringOrganization.name は店舗名のみ（h1 は求人全文になりがち）。"""
        script = soup.select_one("script[type='application/ld+json']")
        if not script:
            return ""
        text = script.get_text(strip=True)
        if not text:
            return ""
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return ""
        org = data.get("hiringOrganization") or {}
        name = org.get("name") if isinstance(org, dict) else ""
        return self._clean(name) if isinstance(name, str) else ""

    def _compact_heading_to_shop_name(self, raw: str) -> str:
        """h1 / title から求人・採用まわりの_suffixを落として店名に近い文字列を返す（JSON-LD欠落時のフォールバック）。"""
        text = self._clean(raw)
        if not text:
            return ""
        # 「の求人の魅力」「の求人」「の採用情報」など以降を除去
        for marker in ("の求人の魅力", "の求人について", "の求人", "の採用について", "の採用", "｜"):
            idx = text.find(marker)
            if idx > 0:
                text = text[:idx].strip()
        # 末尾のサイト名など
        text = re.sub(r"\s*[\|｜]\s*.*$", "", text).strip()
        return self._clean(text)

    def _take_shop_name_from_heading(self, compact_heading: str) -> str:
        """都道府県より前を店名とみなす。見出しに県名が無い場合は空（フォールバック側に任せる）。"""
        text = self._clean(compact_heading)
        if not text:
            return ""
        m = _PREF_HEADING_RE.search(text)
        if not m:
            return ""
        head = text[: m.start()].strip()
        head = re.sub(
            r"\s*[（(](?:勤務|アルバイト|パート|正社員|業務委託)[）)]\s*$",
            "",
            head,
        ).strip()
        return self._clean(head)

    def _extract_address_from_ldjson(self, soup) -> str:
        script = soup.select_one("script[type='application/ld+json']")
        if not script:
            return ""

        text = script.get_text(strip=True)
        if not text:
            return ""

        street_m = re.search(r'"streetAddress"\s*:\s*"([^"]+)"', text)
        locality_m = re.search(r'"addressLocality"\s*:\s*"([^"]+)"', text)
        region_m = re.search(r'"addressRegion"\s*:\s*"([^"]+)"', text)
        postcode_m = re.search(r'"postalCode"\s*:\s*"([^"]+)"', text)

        parts = [postcode_m.group(1) if postcode_m else "", region_m.group(1) if region_m else ""]
        mid = locality_m.group(1) if locality_m else ""
        tail = street_m.group(1) if street_m else ""
        return self._clean(" ".join([p for p in [parts[0], parts[1], mid, tail] if p]))

    def _extract_hp_url(self, soup, labels: dict[str, str]) -> str:
        for key, value in labels.items():
            if any(token in key for token in ("公式", "HP", "ホームページ", "WEB")) and value.startswith("http"):
                return value

        for a in soup.select("a[href^='http']"):
            href = a.get("href", "").strip()
            if not href:
                continue
            low = href.lower()
            if "girlsmeee.com" in low:
                continue
            if any(
                x in low
                for x in (
                    "instagram.com",
                    "x.com",
                    "twitter.com",
                    "facebook.com",
                    "line.me",
                    "tiktok.com",
                    "youtube.com",
                )
            ):
                continue
            anchor_text = self._clean(a.get_text(" ", strip=True))
            if any(k in anchor_text for k in ("公式", "オフィシャル", "HP", "ホームページ", "WEB")):
                return href
        return ""

    def _extract_sns_urls(self, soup) -> dict[str, str]:
        sns = {"insta": "", "x": "", "fb": "", "line": ""}
        ignore_tokens = ("girlsmeeekansai", "tainew_girlsmeee")
        for a in soup.select("a[href^='http']"):
            href = a.get("href", "").strip()
            low = href.lower()
            if any(token in low for token in ignore_tokens):
                continue
            if "instagram.com" in low:
                if not sns["insta"]:
                    sns["insta"] = href
            elif "x.com" in low or "twitter.com" in low:
                if not sns["x"]:
                    sns["x"] = href
            elif "facebook.com" in low:
                if not sns["fb"]:
                    sns["fb"] = href
            elif "line.me" in low:
                if not sns["line"]:
                    sns["line"] = href
        return sns

    def _first_non_empty(self, *values: str) -> str:
        for value in values:
            cleaned = self._clean(value)
            if cleaned:
                return cleaned
        return ""

    def _clean(self, value: str | None) -> str:
        if not value:
            return ""
        return re.sub(r"\s+", " ", value).strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    scraper = GirlsmeeeScraper()
    scraper.execute("https://girlsmeee.com")
