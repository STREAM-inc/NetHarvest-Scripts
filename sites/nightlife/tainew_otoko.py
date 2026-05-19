# https://tainew-otoko.com/用
"""
メンズ体入 (tainew-otoko.com) — キャバクラボーイ・黒服求人スクレイパー

取得対象:
    - 店舗名 / 名称_カナ / TEL / 住所 / 都道府県
    - サイト定義業種 / 定休日(休日) / HP / SNS

取得フロー:
    1. sitemap.xml から店舗詳細URL（/shop/view/）を収集（約390件）
    2. shopFirstView・応募情報テーブルから抽出
    3. shopViewWrap 内の SNS / HP 候補も補完
    4. 店舗名 + 住所 + TEL で重複排除（1店舗1行）

実行方法:
    python scripts/sites/nightlife/tainew_otoko.py
    python scripts/sites/nightlife/tainew_otoko.py --sample
    python bin/run_flow.py --site-id tainew_otoko
"""

from __future__ import annotations

import re
import sys
import xml.etree.ElementTree as ET
from copy import copy
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

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
_TEL_NOTE_RE = re.compile(r"\(受付時間[^)]*\)|（受付時間[^）]*）")
_KANA_TEXT_RE = re.compile(r"^[\u3041-\u309f\u30a0-\u30ff\u30fc\s・･ー－\-]+$")
_HP_SKIP_HOSTS = (
    "instagram.com",
    "line.me",
    "lin.ee",
    "twitter.com",
    "x.com",
    "facebook.com",
    "tiktok.com",
    "maps.google.com",
    "google.com",
    "maps.app.goo.gl",
    "tainew-otoko.com",
    "tainew.com",
    "storage.googleapis.com",
    "luline.jp",
)
class TainewOtokoScraper(StaticCrawler):
    """メンズ体入 スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS: list[str] = []

    BASE_URL = "https://tainew-otoko.com"
    SITEMAP_URL = "https://tainew-otoko.com/sitemap.xml"
    SAMPLE_SEED = "sample"
    SAMPLE_TEST_URLS: tuple[str, ...] = (
        "https://tainew-otoko.com/shop/view/v300004/",
        "https://tainew-otoko.com/shop/view/v300342/",
        "https://tainew-otoko.com/shop/view/v303589/",
        "https://tainew-otoko.com/shop/view/v305559/",
    )

    _SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    _SHOP_URL_RE = re.compile(
        r"^https://tainew-otoko\.com/shop/view/[^/]+/?$"
    )

    def parse(self, url: str) -> Generator[dict, None, None]:
        shop_urls = self._collect_shop_urls(url)
        self.total_items = len(shop_urls)
        self.logger.info("対象店舗URL数: %d", self.total_items)

        seen_shops: set[tuple[str, str, str]] = set()
        saved_count = 0
        duplicate_count = 0
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

            if not record:
                failed_count += 1
                self.logger.warning(
                    "詳細取得スキップ: %d/%d URL=%s",
                    index,
                    self.total_items,
                    shop_url,
                )
                continue

            shop_key = self._shop_key(record)
            if shop_key in seen_shops:
                duplicate_count += 1
                self.logger.info(
                    "店舗重複スキップ: %d/%d 店舗=%s",
                    index,
                    self.total_items,
                    record.get(Schema.NAME) or shop_url,
                )
                continue

            seen_shops.add(shop_key)
            saved_count += 1
            self.logger.info(
                "詳細取得OK: %d/%d 残り%d件 店舗=%s",
                index,
                self.total_items,
                remaining,
                record.get(Schema.NAME) or shop_url,
            )
            yield record

        self.logger.info(
            "詳細取得完了: 候補%d件 取得%d件 店舗重複スキップ%d件 失敗/スキップ%d件",
            self.total_items,
            saved_count,
            duplicate_count,
            failed_count,
        )

    def _collect_shop_urls(self, seed_url: str) -> list[str]:
        if seed_url == self.SAMPLE_SEED:
            return list(self.SAMPLE_TEST_URLS)

        normalized = seed_url.rstrip("/")
        if self._SHOP_URL_RE.match(normalized + "/"):
            return [normalized + "/"]

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
            u if u.endswith("/") else u + "/"
            for u in urls
            if self._SHOP_URL_RE.match(u.rstrip("/") + "/")
            and urlparse(u).netloc == "tainew-otoko.com"
        ]
        return list(dict.fromkeys(shop_urls))

    def _parse_shop_page(self, shop_url: str, soup: BeautifulSoup) -> dict | None:
        labels = self._extract_shop_labels(soup)
        name, kana = self._extract_name_kana(soup)
        if not kana:
            kana = self._extract_kana_from_meta(soup, name)
        if not kana:
            kana = self._extract_kana_from_first_view(soup, name)

        address = self._extract_address(labels.get("住所"))
        tel = self._extract_tel(soup, labels)
        holiday = self._extract_holiday(soup)
        sns = self._extract_sns(soup, labels.get("SNS"))
        hp = self._extract_hp(soup, labels.get("店舗URL"))

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

        return {
            Schema.URL: shop_url,
            Schema.NAME: name,
            Schema.NAME_KANA: kana,
            Schema.PREF: pref,
            Schema.ADDR: addr_body or address,
            Schema.TEL: tel,
            Schema.CAT_SITE: self._extract_cat_site(soup),
            Schema.HOLIDAY: holiday,
            Schema.HP: hp,
            Schema.INSTA: sns["insta"],
            Schema.X: sns["x"],
            Schema.FB: sns["fb"],
            Schema.LINE: sns["line"],
            Schema.TIKTOK: sns["tiktok"],
        }

    def _extract_shop_labels(self, soup: BeautifulSoup) -> dict[str, Tag]:
        for table in soup.find_all("table"):
            row_map: dict[str, Tag] = {}
            for tr in table.find_all("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if th is None or td is None:
                    continue
                label = self._clean(th.get_text(strip=True))
                if label:
                    row_map[label] = td
            if "住所" in row_map and "TEL" in row_map:
                return row_map
        return {}

    def _extract_kana_from_first_view(self, soup: BeautifulSoup, name: str) -> str:
        section = soup.select_one("section.shopFirstView")
        if section is None:
            return ""

        cat_site = self._extract_cat_site(soup)
        area_links = {
            self._clean(a.get_text(strip=True))
            for a in section.find_all("a", href=True)
            if "shoplist/area" in a.get("href", "")
        }

        for span in section.find_all("span"):
            text = self._clean(span.get_text(strip=True))
            if not text or text == name or text == cat_site or text in area_links:
                continue
            if text.endswith("店") and len(text) <= 6:
                continue
            if _KANA_TEXT_RE.match(text):
                return text
        return ""

    def _extract_holiday(self, soup: BeautifulSoup) -> str:
        values: list[str] = []
        for table in soup.find_all("table"):
            for tr in table.find_all("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if th is None or td is None:
                    continue
                if self._clean(th.get_text(strip=True)) != "休日":
                    continue
                value = self._clean(td.get_text(" ", strip=True))
                if value and value not in values:
                    values.append(value)
        return " | ".join(values)

    def _extract_name_kana(self, soup: BeautifulSoup) -> tuple[str, str]:
        h1 = soup.select_one("h1.shopName")
        if h1 is None:
            return "", ""

        kana_el = h1.find("span")
        kana = self._clean(kana_el.get_text(strip=True)) if kana_el else ""

        name_parts: list[str] = []
        for child in h1.children:
            if isinstance(child, str):
                text = child.strip()
                if text:
                    name_parts.append(text)
            elif getattr(child, "name", None) != "span":
                text = self._clean(child.get_text(strip=True))
                if text:
                    name_parts.append(text)

        name = self._clean("".join(name_parts))
        if not name:
            h1_copy = copy(h1)
            if kana_el:
                kana_el.decompose()
            name = self._clean(h1_copy.get_text(strip=True))
        return name, kana

    def _extract_kana_from_meta(self, soup: BeautifulSoup, name: str) -> str:
        meta = soup.find("meta", attrs={"name": "keywords"})
        if meta is None or not meta.get("content"):
            return ""

        parts = [self._clean(part) for part in meta["content"].split(",") if part.strip()]
        if not parts:
            return ""

        if name and parts[0] == name and len(parts) > 1:
            candidate = parts[1]
            if _KANA_TEXT_RE.match(candidate):
                return candidate

        for part in parts[1:4]:
            if _KANA_TEXT_RE.match(part) and part != name:
                return part
        return ""

    def _extract_cat_site(self, soup: BeautifulSoup) -> str:
        section = soup.select_one("section.shopFirstView")
        if section is None:
            return ""

        for anchor in section.find_all("a", href=True):
            href = anchor["href"]
            if "shoplist/type" not in href:
                continue
            parsed = urlparse(urljoin(self.BASE_URL, href))
            if parsed.netloc and parsed.netloc != "tainew-otoko.com":
                continue
            text = self._clean(anchor.get_text(strip=True))
            if text:
                return text
        return ""

    def _extract_address(self, td: Tag | None) -> str:
        if td is None:
            return ""

        map_link = td.select_one("a.mapLink, a.map_link")
        if map_link is not None:
            address = self._clean(map_link.get_text(" ", strip=True))
            if address:
                return address

        return self._clean(td.get_text(" ", strip=True))

    def _extract_hp(self, soup: BeautifulSoup, td: Tag | None) -> str:
        if td is not None:
            anchor = td.find("a", href=True)
            if anchor:
                href = anchor["href"].strip()
                if href.startswith("http") and not self._is_excluded_hp(href):
                    return href
            text = self._clean(td.get_text(" ", strip=True))
            if text.startswith("http") and not self._is_excluded_hp(text):
                return text

        wrap = soup.select_one("motion.shopViewWrap, div.shopViewWrap")
        if wrap is None:
            return ""

        for anchor in wrap.find_all("a", href=True):
            href = anchor["href"].strip()
            if not href.startswith("http"):
                continue
            if self._is_excluded_hp(href):
                continue
            return href
        return ""

    def _is_excluded_hp(self, href: str) -> bool:
        host = urlparse(href).netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        return any(skip in host for skip in _HP_SKIP_HOSTS)

    def _extract_tel(self, soup: BeautifulSoup, labels: dict[str, Tag]) -> str:
        wrap = soup.select_one("motion.shopViewWrap, div.shopViewWrap") or soup
        for anchor in wrap.find_all("a", href=True):
            href = anchor.get("href", "")
            if not href.startswith("tel:"):
                continue
            raw = href.replace("tel:", "").strip()
            match = _TEL_PATTERN.search(raw)
            if match:
                return match.group(0)

        tel_td = labels.get("TEL")
        if tel_td is not None:
            raw = self._clean(tel_td.get_text(" ", strip=True))
            raw = _TEL_NOTE_RE.sub("", raw).strip()
            match = _TEL_PATTERN.search(raw)
            if match:
                return match.group(0)

        return ""

    def _extract_sns(self, soup: BeautifulSoup, sns_td: Tag | None) -> dict[str, str]:
        sns = {"insta": "", "x": "", "fb": "", "line": "", "tiktok": ""}

        anchors: list[Tag] = []
        if sns_td is not None:
            anchors.extend(sns_td.find_all("a", href=True))

        wrap = soup.select_one("motion.shopViewWrap, div.shopViewWrap")
        if wrap is not None:
            anchors.extend(wrap.find_all("a", href=True))

        seen: set[str] = set()
        for anchor in anchors:
            href = anchor.get("href", "").strip()
            if not href or href in seen:
                continue
            seen.add(href)
            self._assign_sns_url(sns, href)

        return sns

    def _assign_sns_url(self, sns: dict[str, str], href: str) -> None:
        if not href.startswith("http"):
            return
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
        elif ("line.me" in low or "lin.ee" in low) and not sns["line"]:
            sns["line"] = href
        elif "tiktok.com" in low and not sns["tiktok"]:
            sns["tiktok"] = href

    def _shop_key(self, record: dict) -> tuple[str, str, str]:
        pref = record.get(Schema.PREF, "")
        addr = record.get(Schema.ADDR, "")
        full_address = f"{pref}{addr}" if pref else addr
        return (
            record.get(Schema.NAME, ""),
            full_address,
            record.get(Schema.TEL, ""),
        )

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

    use_sample = "--sample" in sys.argv
    seed = (
        TainewOtokoScraper.SAMPLE_SEED
        if use_sample
        else TainewOtokoScraper.SITEMAP_URL
    )

    scraper = TainewOtokoScraper()
    if use_sample:
        scraper.logger.info(
            "サンプルモード: %d件（全件は引数なしで実行）",
            len(TainewOtokoScraper.SAMPLE_TEST_URLS),
        )
    scraper.execute(seed)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
