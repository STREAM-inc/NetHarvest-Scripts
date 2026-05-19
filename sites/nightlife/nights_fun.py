# ナイツネット用
"""
ナイツネット (www.nights.fun) — キャバクラ・ガールズバー・スナック店舗スクレイパー

取得対象:
    - 店舗名 / TEL / 営業時間 / 定休日 / 住所 / 都道府県
    - サイト定義業種 / HP（オフィシャルサイト） / 支払い方法（クレジットカードブランド）

取得フロー:
    1. 04-sitemap-shop-list.xml から店舗トップURLを収集（約2062件）
    2. PC版: div.info_text2 / スマホ版: 店舗情報テーブル（電話番号ラベル等）から抽出

実行方法:
    python scripts/sites/nightlife/nights_fun.py
    python bin/run_flow.py --site-id nights_fun
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

from bs4 import BeautifulSoup, Tag

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
_MAP_LINK_TEXT_RE = re.compile(r"\s*\[?\s*地図はこちら\s*\]?\s*")
_EMPTY_BRACKETS_RE = re.compile(r"\s*\[\s*\]\s*")
_SHOP_INFO_HEADING = "\u5e97\u8217\u60c5\u5831"  # 店舗情報

# th ラベル → 内部キー
_LABEL_ALIASES: dict[str, tuple[str, ...]] = {
    "TEL": ("TEL", "電話番号", "電話"),
    "営業時間": ("営業時間",),
    "定休日": ("定休日",),
    "住所": ("住所",),
    "HP": (
        "オフィシャルサイト（PC）",
        "オフィシャルサイト",
        "HP",
        "ホームページ",
        "オフィシャルサイト(PC)",
    ),
}
_ALIAS_TO_KEY: dict[str, str] = {
    alias: key for key, aliases in _LABEL_ALIASES.items() for alias in aliases
}
_FEE_TABLE_TH_MARKERS = ("1set", "MENU", "延長", "システム料金", "TAX", "メンバー", "ビジター")
_PAREN_CONTENT_RE = re.compile(r"[（(][^）)]*[）)]")


class NightsFunScraper(StaticCrawler):
    """ナイツネット スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS: list[str] = []

    BASE_URL = "https://www.nights.fun"
    SITEMAP_URL = "https://www.nights.fun/04-sitemap-shop-list.xml"

    _SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    _SHOP_URL_RE = re.compile(
        r"^https://www\.nights\.fun/[a-z]+/A\d+/A\d+/[^/]+/?$"
    )

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
        if self._SHOP_URL_RE.match(seed_url.rstrip("/") + "/"):
            normalized = seed_url if seed_url.endswith("/") else seed_url + "/"
            return [normalized]

        sitemap_url = seed_url if seed_url.endswith(".xml") else self.SITEMAP_URL
        return self._collect_sitemap_urls(sitemap_url)

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
            if self._SHOP_URL_RE.match(u)
            and urlparse(u).netloc == "www.nights.fun"
            and "/map/" not in u
        ]
        return list(dict.fromkeys(shop_urls))

    def _parse_shop_page(self, shop_url: str, soup: BeautifulSoup) -> dict | None:
        info = soup.select_one("div.info_text2") or soup.select_one("#info div.info_text2")

        if info is not None:
            labels = self._extract_labeled_values(info)
            name = self._extract_name_from_info(info, soup)
            cat_site = self._extract_cat_site_from_info(info)
        else:
            labels = self._extract_labels_from_shop_section(soup)
            if not labels:
                labels = self._extract_labels_from_page_tables(soup)
            name = self._extract_name_from_title(soup)
            cat_site = ""

        if not cat_site:
            cat_site = self._extract_cat_site_from_title(soup)

        if not name:
            self.logger.warning("店舗名が空です: %s", shop_url)
            return None

        if not any(labels.get(k) for k in ("TEL", "営業時間", "定休日", "住所")):
            self.logger.warning("店舗情報テーブルが見つかりません: %s", shop_url)
            return None

        address = self._clean(labels.get("住所", ""))
        pref, addr_body = self._split_pref(address)
        payments = self._extract_credit_cards(soup)

        return {
            Schema.URL: shop_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr_body or address,
            Schema.TEL: self._extract_tel(labels.get("TEL", "")),
            Schema.TIME: self._normalize_hours(labels.get("営業時間", "")),
            Schema.HOLIDAY: self._clean(labels.get("定休日", "")),
            Schema.CAT_SITE: cat_site,
            Schema.HP: self._clean(labels.get("HP", "")),
            Schema.PAYMENTS: payments,
        }

    def _extract_labels_from_shop_section(self, soup: BeautifulSoup) -> dict[str, str]:
        """スマホ版: h2「店舗情報」直下のテーブルを優先"""
        for heading in soup.find_all(["h2", "h3"]):
            if _SHOP_INFO_HEADING not in heading.get_text(strip=True):
                continue
            container = heading.find_parent("section") or heading.parent
            if container is None:
                continue
            labels = self._extract_labeled_values_from_root(container)
            if labels:
                return labels
        return {}

    def _extract_labels_from_page_tables(self, soup: BeautifulSoup) -> dict[str, str]:
        """スマホ版フォールバック: ページ内の店舗属性テーブル行を収集"""
        return self._extract_labeled_values_from_root(soup)

    def _extract_labeled_values_from_root(self, root: Tag) -> dict[str, str]:
        data: dict[str, str] = {}
        for tr in root.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            label = self._clean(th.get_text(strip=True))
            if not label or self._is_fee_table_label(label):
                continue
            key = _ALIAS_TO_KEY.get(label)
            if not key or key in data:
                continue
            data[key] = self._extract_td_value(td, key)
        return data

    def _is_fee_table_label(self, label: str) -> bool:
        if any(marker in label for marker in _FEE_TABLE_TH_MARKERS):
            return True
        if re.search(r"\d+分", label):
            return True
        return False

    def _extract_labeled_values(self, info: Tag) -> dict[str, str]:
        data: dict[str, str] = {}
        for tr in info.select("table tbody tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            label = self._clean(th.get_text(strip=True))
            key = _ALIAS_TO_KEY.get(label, label)
            if not key or key in data:
                continue
            data[key] = self._extract_td_value(td, key)
        return data

    def _extract_td_value(self, td: Tag, key: str) -> str:
        td_copy = copy(td)
        if key == "住所":
            for a in td_copy.find_all("a", href=True):
                href = a.get("href", "")
                if "/map/" in href or "地図" in a.get_text(strip=True):
                    a.decompose()
            value = td_copy.get_text(" ", strip=True)
            value = _MAP_LINK_TEXT_RE.sub("", value)
            value = _EMPTY_BRACKETS_RE.sub("", value)
            return self._clean(value)
        if key == "HP":
            return self._extract_hp_from_td(td_copy)
        value = td_copy.get_text(" ", strip=True)
        return self._clean(value)

    def _extract_hp_from_td(self, td: Tag) -> str:
        for a in td.find_all("a", href=True):
            href = a.get("href", "").strip()
            if href.startswith("http"):
                return href
        text = self._clean(td.get_text(strip=True))
        if text.startswith("http"):
            return text
        return ""

    def _extract_name_from_info(self, info: Tag, soup: BeautifulSoup) -> str:
        name_el = info.select_one("span.top_shop, span[itemprop='name']")
        if name_el:
            name = self._clean(name_el.get_text(strip=True))
            if name:
                return name
        return self._extract_name_from_title(soup)

    def _extract_name_from_title(self, soup: BeautifulSoup) -> str:
        title_tag = soup.find("title")
        if not title_tag:
            return ""
        title = title_tag.get_text(strip=True)
        if " - " in title:
            return self._clean(title.split(" - ", 1)[0])
        if "｜" in title:
            return self._clean(title.split("｜", 1)[0])
        return self._clean(title)

    def _extract_cat_site_from_info(self, info: Tag) -> str:
        """info_text2 内の店舗名直下テキスト行（例: ガールズバー(スタンダード/…)）"""
        block = copy(info)
        for el in block.select("table, #kyuujin"):
            el.decompose()
        for el in block.select("span.top_shop, span[itemprop='name']"):
            el.decompose()
        lines = [
            line.strip()
            for line in block.get_text("\n", strip=True).split("\n")
            if line.strip()
        ]
        if lines:
            return self._normalize_cat_site(lines[0])
        return ""

    def _extract_cat_site_from_title(self, soup: BeautifulSoup) -> str:
        """title の「 - 」以降・「｜」より前（例: すすきの/ニュークラブ・キャバクラ）"""
        title_tag = soup.find("title")
        if not title_tag:
            return ""
        title = title_tag.get_text(strip=True)
        if " - " not in title:
            return ""
        tail = title.split(" - ", 1)[1]
        for sep in ("｜", "|"):
            if sep in tail:
                tail = tail.split(sep, 1)[0]
        return self._normalize_cat_site(tail)

    def _normalize_cat_site(self, raw: str) -> str:
        """括弧内を除去し、エリア/業種形式はスラッシュ以降（業種）のみ残す"""
        value = self._clean(raw)
        if not value:
            return ""
        value = _PAREN_CONTENT_RE.sub("", value)
        value = self._clean(value)
        if "/" in value:
            value = value.rsplit("/", 1)[-1].strip()
        return self._clean(value)

    def _extract_credit_cards(self, soup: BeautifulSoup) -> str:
        credit = soup.select_one(".credit-info")
        if credit is None:
            return ""

        brands: list[str] = []
        seen: set[str] = set()
        for img in credit.select("img[alt]"):
            alt = self._clean(img.get("alt", ""))
            if not alt or alt in seen:
                continue
            if "決済" in alt or "サービス" in alt:
                continue
            seen.add(alt)
            brands.append(alt)

        if brands:
            return " / ".join(brands)

        text = self._clean(credit.get_text(" ", strip=True))
        text = text.replace("■各種決済サービスをご利用頂けます", "").strip()
        text = text.replace("クレジットカード", "").strip()
        return text

    def _extract_tel(self, raw: str) -> str:
        if not raw:
            return ""
        match = _TEL_PATTERN.search(raw)
        return match.group(0) if match else self._clean(raw)

    def _normalize_hours(self, raw: str) -> str:
        value = self._clean(raw)
        if value in ("", "."):
            return ""
        return value

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

    scraper = NightsFunScraper()
    scraper.execute(NightsFunScraper.SITEMAP_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
