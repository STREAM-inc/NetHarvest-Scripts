"""
体入ガールズミー — ガールズバー・コンカフェ 体入求人スクレイパー（関西地域）

取得対象:
    - girlsmeee.com の関西地域（kansai）掲載店舗詳細ページ

取得フロー:
    1. sitemap.xml → 関西店舗URL収集
    2. 各詳細ページを解析してフィールド取得

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/girlsmeee.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id girlsmeee
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

_PREF_RE = re.compile(
    r"^(北海道|東京都|大阪府|京都府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|和歌山|"
    r"鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県)"
)


class GirlsmeeeScraper(StaticCrawler):
    """体入ガールズミー スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["エリア", "体入時給", "最低保証時給", "平均時給", "謝礼金", "キャッチフレーズ"]

    _SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    _SHOP_URL_RE = re.compile(
        r"^https://girlsmeee\.com/kansai/[^/]+/\d+(?:/.*)?$"
    )
    _SNS_IGNORE = ("girlsmeeekansai", "tainew_girlsmeee")

    def parse(self, url: str) -> Generator[dict, None, None]:
        sitemap_url = self._resolve_sitemap_url(url)
        shop_urls = self._collect_shop_urls(sitemap_url)
        self.total_items = len(shop_urls)
        self.logger.info("対象詳細URL数: %d", self.total_items)

        for shop_url in shop_urls:
            try:
                soup = self.get_soup(shop_url)
                if soup is None:
                    continue
                item = self._parse_shop_page(shop_url, soup)
                if item[Schema.NAME]:
                    yield item
            except Exception as e:
                self.logger.warning("スキップ %s: %s", shop_url, e)
                continue

    # ------------------------------------------------------------------
    # Sitemap collection
    # ------------------------------------------------------------------

    def _resolve_sitemap_url(self, seed_url: str) -> str:
        parsed = urlparse(seed_url)
        if parsed.path.endswith(".xml"):
            return seed_url
        return f"{parsed.scheme}://{parsed.netloc}/sitemap.xml"

    def _collect_shop_urls(self, sitemap_url: str) -> list[str]:
        shop_urls = self._collect_sitemap_urls(sitemap_url)
        self.logger.info("サイトマップ収集: %d 件", len(shop_urls))
        return shop_urls

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
        shop_urls = [u for u in urls if self._SHOP_URL_RE.match(u) and urlparse(u).netloc == "girlsmeee.com"]
        return list(dict.fromkeys(shop_urls))

    # ------------------------------------------------------------------
    # Detail page parsing
    # ------------------------------------------------------------------

    def _parse_shop_page(self, page_url: str, soup) -> dict:
        labels = self._extract_labeled_values(soup)
        plain_text = soup.get_text(" ", strip=True)

        name = self._extract_name(soup, labels)
        address = self._c(labels.get("住所") or self._extract_address_from_ldjson(soup))
        pref, addr_body = self._split_pref(address)
        tel = self._extract_tel(soup, labels, plain_text)
        sns = self._extract_sns_urls(soup)

        return {
            Schema.URL: page_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr_body or address,
            Schema.TEL: tel,
            Schema.TIME: self._c(labels.get("営業時間", "")),
            Schema.HOLIDAY: self._c(labels.get("定休日", "")),
            Schema.CAT_SITE: self._c(labels.get("業種", "")),
            Schema.HP: self._extract_hp_url(soup, labels),
            Schema.INSTA: sns["insta"],
            Schema.X: sns["x"],
            Schema.FB: sns["fb"],
            Schema.LINE: sns["line"],
            Schema.TIKTOK: sns["tiktok"],
            "エリア": self._c(labels.get("エリア", "")),
            "体入時給": self._extract_salary_field(soup, "体入時給"),
            "最低保証時給": self._extract_salary_field(soup, "最低保証時給"),
            "平均時給": self._extract_salary_field(soup, "平均時給"),
            "謝礼金": self._extract_congratulatory_money(soup),
            "キャッチフレーズ": self._extract_catchphrase(soup),
        }

    def _extract_labeled_values(self, soup) -> dict[str, str]:
        data: dict[str, str] = {}
        for li in soup.select("#detail li.list-group-item, li.list-group-item"):
            label_el = li.select_one("div.text-xs.text-primary")
            if not label_el:
                continue
            key = self._c(label_el.get_text(" ", strip=True))
            # TEL系ラベルは "TEL 090-xxxx" 形式で key に番号が混入するので正規化
            key = re.sub(r"\s*\d[\d\-]+$", "", key).strip()
            if not key or key in data:
                continue
            value = self._extract_li_value(li)
            if value:
                data[key] = value
        return data

    def _extract_li_value(self, li) -> str:
        for div in li.find_all("div", recursive=False):
            if "text-xs" in (div.get("class") or []) and "text-primary" in (div.get("class") or []):
                continue
            text = self._c(div.get_text(" ", strip=True))
            if text:
                return text
        node = li.select_one("div.break-word") or li.select_one("a[href^='tel:']")
        if node:
            if node.name == "a":
                return node.get("href", "").replace("tel:", "").strip()
            return self._c(node.get_text(" ", strip=True))
        return ""

    def _extract_name(self, soup, labels: dict[str, str]) -> str:
        # JSON-LD hiringOrganization.name が最も確実
        ld_name = self._extract_ld_org_name(soup)
        if ld_name:
            return ld_name
        # h1 内の .text-lg.text-primary (詳細ページ店名div)
        name_div = soup.select_one("h1 .text-lg.text-primary")
        if name_div:
            return self._c(name_div.get_text(" ", strip=True))
        return self._c(labels.get("店舗名") or labels.get("店名") or "")

    def _extract_ld_org_name(self, soup) -> str:
        script = soup.select_one("script[type='application/ld+json']")
        if not script:
            return ""
        try:
            data = json.loads(script.get_text(strip=True))
        except (json.JSONDecodeError, ValueError):
            return ""
        org = data.get("hiringOrganization") if isinstance(data, dict) else None
        if isinstance(org, dict):
            return self._c(str(org.get("name", "")))
        return ""

    def _extract_address_from_ldjson(self, soup) -> str:
        script = soup.select_one("script[type='application/ld+json']")
        if not script:
            return ""
        text = script.get_text(strip=True)
        parts = []
        for key in ("addressRegion", "addressLocality", "streetAddress"):
            m = re.search(rf'"{key}"\s*:\s*"([^"]+)"', text)
            if m:
                parts.append(m.group(1))
        return self._c(" ".join(parts))

    def _split_pref(self, address: str) -> tuple[str, str]:
        if not address:
            return "", ""
        m = _PREF_RE.match(address)
        if m:
            return m.group(1), address[m.end():].strip()
        return "", address

    def _extract_tel(self, soup, labels: dict[str, str], plain_text: str) -> str:
        tel_link = soup.select_one("a[href^='tel:']")
        if tel_link:
            raw = tel_link.get("href", "").replace("tel:", "")
            normalized = re.sub(r"[^\d\-]", "", raw)
            if normalized:
                return normalized
        for key, value in labels.items():
            if "TEL" in key.upper() or "電話" in key:
                m = re.search(r"0\d{1,4}-?\d{1,4}-?\d{3,4}", value)
                if m:
                    return m.group(0)
        m = re.search(r"0\d{1,4}-?\d{1,4}-?\d{3,4}", plain_text)
        return m.group(0) if m else ""

    def _extract_salary_field(self, soup, field_name: str) -> str:
        salary_sec = soup.select_one("#salary")
        if not salary_sec:
            return ""
        for li in salary_sec.select("li.list-group-item"):
            label_el = li.select_one("[style*='6rem']")
            if label_el and field_name in label_el.get_text():
                val_el = li.select_one(".text-sm:not([style])")
                if val_el:
                    return self._c(val_el.get_text(" ", strip=True))
        return ""

    def _extract_congratulatory_money(self, soup) -> str:
        el = soup.select_one(".rounded-pill.bg-primary .text-lg")
        return self._c(el.get_text(" ", strip=True)) if el else ""

    def _extract_catchphrase(self, soup) -> str:
        # 詳細ページのキャッチフレーズ div
        el = soup.select_one(".text-lg.text-primary.break-word")
        return self._c(el.get_text(" ", strip=True)) if el else ""

    def _extract_hp_url(self, soup, labels: dict[str, str]) -> str:
        for key, value in labels.items():
            if any(t in key for t in ("公式", "HP", "ホームページ", "WEB", "ウェブサイト")) and value.startswith("http"):
                return value
        for a in soup.select("a[href^='http']"):
            href = a.get("href", "").strip()
            low = href.lower()
            if "girlsmeee.com" in low:
                continue
            if any(x in low for x in ("instagram", "x.com", "twitter", "facebook", "line.me", "tiktok", "youtube")):
                continue
            anchor_text = self._c(a.get_text(" ", strip=True))
            if any(k in anchor_text for k in ("公式", "オフィシャル", "HP", "ホームページ", "WEB")):
                return href
        return ""

    def _extract_sns_urls(self, soup) -> dict[str, str]:
        sns = {"insta": "", "x": "", "fb": "", "line": "", "tiktok": ""}
        for a in soup.select("a[href^='http']"):
            href = a.get("href", "").strip()
            low = href.lower()
            if any(t in low for t in self._SNS_IGNORE):
                continue
            if "instagram.com" in low and not sns["insta"]:
                sns["insta"] = href
            elif ("x.com" in low or "twitter.com" in low) and not sns["x"]:
                sns["x"] = href
            elif "facebook.com" in low and not sns["fb"]:
                sns["fb"] = href
            elif "line.me" in low and not sns["line"]:
                sns["line"] = href
            elif "tiktok.com" in low and not sns["tiktok"]:
                sns["tiktok"] = href
        return sns

    def _c(self, value: str | None) -> str:
        if not value:
            return ""
        return re.sub(r"\s+", " ", str(value)).strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    scraper = GirlsmeeeScraper()
    scraper.execute("https://girlsmeee.com")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
