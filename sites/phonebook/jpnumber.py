"""
Target site: https://www.jpnumber.com/
STREAMREQ-6259: new-power sales/agency candidates from JPNumber.

Only factual directory fields are saved. Review bodies and free-form
commentary are intentionally ignored.
"""

from __future__ import annotations

import math
import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import quote, urljoin

for parent in Path(__file__).resolve().parents:
    if (parent / "src").exists():
        sys.path.insert(0, str(parent))
        break

from src.const.schema import Schema
from src.framework.static import StaticCrawler


BASE_URL = "https://www.jpnumber.com"
RESULTS_PER_PAGE = 20
MAX_PAGES_PER_KEYWORD = 50

SEARCH_KEYWORDS = [
    "新電力",
    "新電力 代理店",
    "電力代理店",
    "電気代理店",
    "電気販売 代理店",
    "電力営業",
    "電気料金",
    "電力プラン",
    "電力切替",
    "ハルエネ",
    "ハルエネ電気",
    "東京電力 代理店",
]

COL_SOURCE_SITE = "情報元サイト"
COL_SEARCH_KEYWORD = "検索キーワード"
COL_PHONE_TYPE = "電話番号種別"
COL_FIXED_TEL = "固定番号"
COL_MOBILE_TEL = "代表携帯番号"
COL_SEARCH_COUNT = "検索回数"
COL_ACCESS_COUNT = "アクセス数"
COL_REVIEW_COUNT = "口コミ数"
COL_STATION = "最寄り駅"
COL_DETAIL_URL = "取得元詳細URL"
COL_CANDIDATE = "候補判定"
COL_AREA = "エリア"

EXTRA_COLUMNS = [
    COL_SOURCE_SITE,
    COL_SEARCH_KEYWORD,
    COL_PHONE_TYPE,
    COL_FIXED_TEL,
    COL_MOBILE_TEL,
    COL_SEARCH_COUNT,
    COL_ACCESS_COUNT,
    COL_REVIEW_COUNT,
    COL_STATION,
    COL_DETAIL_URL,
    COL_CANDIDATE,
    COL_AREA,
]

PREFECTURES = (
    "北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    "埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    "岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    "鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    "佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)

DETAIL_HREF_RE = re.compile(
    r"(?:^|/)(?:mobile/|freedial/|ipphone/)?numberinfo_[^\"']+\.html"
)
RESULT_START_RE = re.compile(r"^(0\d{9,10})\s*\|\s*([0-9０-９][0-9０-９\-\sー−‐‑‒–—―]+)")
POST_CODE_RE = re.compile(r"〒?\s*(\d{3})[-‐‑‒–—―ー−]?\s*(\d{4})")
PREF_RE = re.compile(rf"^({PREFECTURES})")

POSITIVE_WORDS = (
    "代理店",
    "新電力",
    "電力営業",
    "電気料金",
    "電力切替",
    "電力プラン",
    "でんき",
    "電気営業",
    "ハルエネ",
    "エネルギー",
    "エナジー",
)
OFFICIAL_OR_LOW_PRIORITY_WORDS = (
    "パワーグリッド",
    "送配電",
    "営業所",
    "料金お問い合わせ",
    "カスタマーセンター",
    "市役所",
    "警察",
    "病院",
    "学校",
)
RISK_WORDS = ("詐欺", "ニセ", "偽", "自動音声", "アンケート", "迷惑")


def _clean(value: object) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip()


def _digits(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def _line_value(lines: list[str], label: str, stop_labels: tuple[str, ...] = ()) -> str:
    for idx, line in enumerate(lines):
        bare = line.rstrip("：:")
        if bare == label:
            if idx + 1 >= len(lines):
                return ""
            nxt = lines[idx + 1]
            if RESULT_START_RE.match(nxt):
                return ""
            if nxt in stop_labels or any(nxt.startswith(s + " ") for s in stop_labels):
                return ""
            return nxt
        for prefix in (f"{label}：", f"{label}:", f"{label} "):
            if not line.startswith(prefix):
                continue
            value = line[len(prefix):].strip()
            if value:
                return value
    return ""


def _split_post_code(address: str) -> tuple[str, str]:
    address = _clean(address)
    match = POST_CODE_RE.search(address)
    if not match:
        return "", address
    post_code = f"{match.group(1)}-{match.group(2)}"
    address = _clean(address[:match.start()] + address[match.end():])
    return post_code, address


def _extract_pref(address: str) -> str:
    match = PREF_RE.match(_clean(address))
    return match.group(1) if match else ""


def _classify_phone(phone: str, fallback: str = "") -> str:
    digits = _digits(phone)
    if fallback:
        return fallback
    if digits.startswith(("070", "080", "090")) and len(digits) == 11:
        return "携帯電話"
    if digits.startswith(("0120", "0800")):
        return "フリーダイヤル"
    if digits.startswith("050"):
        return "IP電話"
    if digits.startswith("0570"):
        return "ナビダイヤル"
    return "固定電話" if digits else ""


def _candidate_label(*texts: str) -> str:
    joined = " ".join(t for t in texts if t)
    if any(word in joined for word in RISK_WORDS):
        return "要確認: 迷惑/詐欺系表記あり"
    if any(word in joined for word in OFFICIAL_OR_LOW_PRIORITY_WORDS):
        return "除外候補: 電力会社本体/公共系の可能性"
    if "代理店" in joined:
        return "高: 代理店表記あり"
    if any(word in joined for word in POSITIVE_WORDS):
        return "中: 電力営業関連"
    return "低: 要確認"


class JpnumberScraper(StaticCrawler):
    """JPNumber scraper for new-power agency candidate facts."""

    DELAY = 1.5
    EXTRA_COLUMNS = EXTRA_COLUMNS

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_phones: set[str] = set()

        for keyword in SEARCH_KEYWORDS:
            self.logger.info("JPNumber keyword: %s", keyword)
            for page_url in self._search_urls(keyword):
                soup = self.get_soup(page_url)
                if soup is None or self._is_blocked(soup):
                    self.logger.warning("JPNumber page unavailable or blocked: %s", page_url)
                    break

                records = self._parse_search_page(soup, page_url, keyword)
                if not records:
                    break

                for record in records:
                    tel_key = _digits(record.get(Schema.TEL, ""))
                    if not tel_key or tel_key in seen_phones:
                        continue
                    seen_phones.add(tel_key)

                    detail_url = record.get(COL_DETAIL_URL, "")
                    if detail_url:
                        time.sleep(self.DELAY)
                        detail = self._scrape_detail(detail_url)
                        self._merge_detail(record, detail)

                    record[COL_CANDIDATE] = _candidate_label(
                        record.get(Schema.NAME, ""),
                        record.get(Schema.LOB, ""),
                        record.get(Schema.CAT_SITE, ""),
                    )
                    yield record

                time.sleep(self.DELAY)

    def _search_urls(self, keyword: str) -> Generator[str, None, None]:
        encoded = quote(keyword)
        first_url = f"{BASE_URL}/searchnumber.do?number={encoded}"
        soup = self.get_soup(first_url)
        if soup is None or self._is_blocked(soup):
            self.logger.warning("JPNumber first page unavailable or blocked: %s", first_url)
            return

        total = self._extract_total_count(soup)
        max_page = min(MAX_PAGES_PER_KEYWORD, max(1, math.ceil(total / RESULTS_PER_PAGE))) if total else 1

        yield first_url
        for page in range(2, max_page + 1):
            yield f"{first_url}&page={page}"

    def _parse_search_page(self, soup, page_url: str, keyword: str) -> list[dict]:
        detail_by_phone = self._detail_links_by_phone(soup)
        lines = [_clean(line) for line in soup.get_text("\n", strip=True).splitlines()]
        lines = [line for line in lines if line]

        records: list[dict] = []
        start_indexes = [
            idx
            for idx, line in enumerate(lines)
            if RESULT_START_RE.match(line) and (idx == 0 or lines[idx - 1] != "電話番号:")
        ]
        for pos, start in enumerate(start_indexes):
            end = start_indexes[pos + 1] if pos + 1 < len(start_indexes) else len(lines)
            block = lines[start:end]
            item = self._parse_result_block(block, page_url, keyword, detail_by_phone)
            if item:
                records.append(item)
        return records

    def _detail_links_by_phone(self, soup) -> dict[str, str]:
        links: dict[str, str] = {}
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            text = _clean(a.get_text(" ", strip=True))
            if not DETAIL_HREF_RE.search(href) or "|" not in text:
                continue
            phone = _digits(text.split("|", 1)[0])
            if phone and phone not in links:
                links[phone] = urljoin(BASE_URL, href)
        return links

    def _parse_result_block(
        self,
        block: list[str],
        page_url: str,
        keyword: str,
        detail_by_phone: dict[str, str],
    ) -> dict | None:
        start_match = RESULT_START_RE.match(block[0]) if block else None
        if not start_match:
            return None

        tel = _clean(start_match.group(2))
        tel_digits = _digits(tel)
        item: dict = {
            Schema.URL: page_url,
            Schema.TEL: tel,
            COL_SOURCE_SITE: "JPNumber",
            COL_SEARCH_KEYWORD: keyword,
            COL_DETAIL_URL: detail_by_phone.get(tel_digits, ""),
        }

        name = _line_value(block, "事業者名")
        if name:
            item[Schema.NAME] = name

        address_value = _line_value(block, "住所")
        if address_value:
            post_code, address = _split_post_code(address_value)
            if post_code:
                item[Schema.POST_CODE] = post_code
            if address:
                item[Schema.ADDR] = address
                pref = _extract_pref(address)
                if pref:
                    item[Schema.PREF] = pref
                    item[COL_AREA] = pref

        station = _line_value(block, "最寄り駅")
        if station:
            item[COL_STATION] = station

        self._set_stats(item, block)

        for line in block:
            if ">" in line and any(t in line for t in ("固定電話", "携帯電話", "フリーダイヤル", "IP電話")):
                item[COL_PHONE_TYPE] = _clean(line.split(">", 1)[0])

        phone_type = _classify_phone(item.get(Schema.TEL, ""), item.get(COL_PHONE_TYPE, ""))
        if phone_type:
            item[COL_PHONE_TYPE] = phone_type
        if phone_type == "携帯電話":
            item[COL_MOBILE_TEL] = item.get(Schema.TEL, "")
        else:
            item[COL_FIXED_TEL] = item.get(Schema.TEL, "")

        return item if item.get(Schema.NAME) and item.get(Schema.TEL) else None

    def _set_stats(self, item: dict, lines: list[str]) -> None:
        joined = " ".join(lines)
        for label, col in (
            ("検索回数", COL_SEARCH_COUNT),
            ("アクセス数", COL_ACCESS_COUNT),
            ("口コミ数", COL_REVIEW_COUNT),
        ):
            match = re.search(rf"{label}[:：]\s*([0-9,]+)", joined)
            if match:
                item[col] = match.group(1).replace(",", "")
                continue
            for idx, line in enumerate(lines):
                bare = line.lstrip("| ").rstrip("：:")
                if bare == label and idx + 1 < len(lines) and re.fullmatch(r"[0-9,]+", lines[idx + 1]):
                    item[col] = lines[idx + 1].replace(",", "")
                    break

    def _scrape_detail(self, detail_url: str) -> dict:
        soup = self.get_soup(detail_url)
        if soup is None or self._is_blocked(soup):
            return {}

        text = soup.get_text("\n", strip=True)
        text = re.split(r"口コミ掲示板|口コミを書く|アクセス急上昇電話番号一覧", text, maxsplit=1)[0]
        lines = [_clean(line) for line in text.splitlines()]
        lines = [line for line in lines if line]

        item: dict = {}
        name = _line_value(lines, "事業者名称")
        if name:
            item[Schema.NAME] = name

        lob = _line_value(lines, "業種")
        if lob:
            item[Schema.LOB] = lob
            item[Schema.CAT_SITE] = lob

        address = _line_value(lines, "住所")
        if address:
            post_code, address = _split_post_code(address)
            if post_code:
                item[Schema.POST_CODE] = post_code
            if address:
                item[Schema.ADDR] = address
                pref = _extract_pref(address)
                if pref:
                    item[Schema.PREF] = pref
                    item[COL_AREA] = pref

        tel = _line_value(lines, "問い合わせ先")
        if tel:
            item[Schema.TEL] = tel

        station = _line_value(lines, "最寄り駅")
        if station:
            item[COL_STATION] = station

        hp = _line_value(lines, "公式サイト")
        if not hp:
            hp = self._first_external_link(soup)
        if hp:
            item[Schema.HP] = hp

        return item

    def _merge_detail(self, record: dict, detail: dict) -> None:
        for key, value in detail.items():
            if value and not record.get(key):
                record[key] = value

        phone_type = _classify_phone(record.get(Schema.TEL, ""), record.get(COL_PHONE_TYPE, ""))
        if phone_type:
            record[COL_PHONE_TYPE] = phone_type
        if phone_type == "携帯電話":
            record[COL_MOBILE_TEL] = record.get(Schema.TEL, "")
        else:
            record[COL_FIXED_TEL] = record.get(Schema.TEL, "")

    def _first_external_link(self, soup) -> str:
        for a in soup.find_all("a", href=True):
            href = a.get("href", "").strip()
            if not href.startswith("http"):
                continue
            if any(host in href for host in ("jpnumber.com", "twitter.com", "b.hatena.ne.jp")):
                continue
            return href
        return ""

    def _extract_total_count(self, soup) -> int:
        text = soup.get_text(" ", strip=True)
        match = re.search(r"検索結果\s+([0-9,]+)\s+件", text)
        return int(match.group(1).replace(",", "")) if match else 0

    def _is_blocked(self, soup) -> bool:
        text = soup.get_text(" ", strip=True)
        return "Just a moment" in text or "Access denied" in text or "Enable JavaScript" in text


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    scraper = JpnumberScraper()
    scraper.execute(BASE_URL)
    print(f"output: {scraper.output_filepath}")
