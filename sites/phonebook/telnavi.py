"""
Target site: https://www.telnavi.jp/
STREAMREQ-6259: new-power sales/agency candidates from Telnavi.

Only factual directory fields are saved. Review bodies, generated article
paragraphs, and other free-form text are intentionally ignored.
"""

from __future__ import annotations

import math
import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import quote, urljoin

import bs4

for parent in Path(__file__).resolve().parents:
    if (parent / "src").exists():
        sys.path.insert(0, str(parent))
        break

from src.const.schema import Schema
from src.framework.dynamic import DynamicCrawler


BASE_URL = "https://www.telnavi.jp"
RESULTS_PER_PAGE = 20
MAX_PAGES_PER_KEYWORD = 30

SEARCH_KEYWORDS = [
    "新電力",
    "新電力 代理店",
    "電力代理店",
    "電気代理店",
    "電力営業",
    "電気料金",
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
COL_REVIEW_COUNT = "口コミ数"
COL_ACCESS_COUNT = "アクセス数"
COL_SEARCH_RESULT_COUNT = "検索結果表示回数"
COL_USER_RATING = "ユーザー評価"
COL_NUISANCE_SCORE = "迷惑電話度"
COL_FAX = "FAX"
COL_DETAIL_URL = "取得元詳細URL"
COL_CANDIDATE = "候補判定"
COL_AREA = "エリア"

EXTRA_COLUMNS = [
    COL_SOURCE_SITE,
    COL_SEARCH_KEYWORD,
    COL_PHONE_TYPE,
    COL_FIXED_TEL,
    COL_MOBILE_TEL,
    COL_REVIEW_COUNT,
    COL_ACCESS_COUNT,
    COL_SEARCH_RESULT_COUNT,
    COL_USER_RATING,
    COL_NUISANCE_SCORE,
    COL_FAX,
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

RESULT_START_RE = re.compile(r"^(0\d{9,10})\s*/\s*([0-9０-９][0-9０-９\-\sー−‐‑‒–—―]+)")
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
STOP_LABELS = (
    "フリガナ",
    "住所",
    "市外局番",
    "市内局番",
    "加入者番号",
    "電話番号",
    "回線種別",
    "FAX番号",
    "業種タグ",
    "PR文",
    "ユーザー評価",
    "アクセス数",
    "検索結果表示回数",
    "アクセス推移グラフ",
    "大量発信情報",
    "迷惑電話度",
    "登録情報更新",
)


def _clean(value: object) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip()


def _digits(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def _line_value(lines: list[str], label: str, stop_labels: tuple[str, ...] = STOP_LABELS) -> str:
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
        return "携帯電話/PHS"
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


class TelnaviScraper(DynamicCrawler):
    """Telnavi scraper for new-power agency candidate facts."""

    DELAY = 5.0
    EXTRA_COLUMNS = EXTRA_COLUMNS

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_phones: set[str] = set()

        for keyword in SEARCH_KEYWORDS:
            self.logger.info("Telnavi keyword: %s", keyword)
            first_soup = self._fetch_soup(self._search_url(keyword, 1))
            if first_soup is None:
                continue

            total = self._extract_total_count(first_soup)
            max_page = min(MAX_PAGES_PER_KEYWORD, max(1, math.ceil(total / RESULTS_PER_PAGE))) if total else 1

            for page in range(1, max_page + 1):
                soup = first_soup if page == 1 else self._fetch_soup(self._search_url(keyword, page))
                if soup is None:
                    break

                records = self._parse_search_page(soup, self._search_url(keyword, page), keyword)
                if not records:
                    break

                for record in records:
                    tel_key = _digits(record.get(Schema.TEL, ""))
                    if not tel_key or tel_key in seen_phones:
                        continue
                    seen_phones.add(tel_key)

                    detail_url = record.get(COL_DETAIL_URL, "")
                    if detail_url:
                        detail = self._scrape_detail(detail_url)
                        self._merge_detail(record, detail)

                    record[COL_CANDIDATE] = _candidate_label(
                        record.get(Schema.NAME, ""),
                        record.get(Schema.LOB, ""),
                        record.get(Schema.CAT_SITE, ""),
                        record.get(COL_NUISANCE_SCORE, ""),
                    )
                    yield record

    def _search_url(self, keyword: str, page: int) -> str:
        encoded = quote(keyword)
        if page <= 1:
            return f"{BASE_URL}/search?q={encoded}"
        return f"{BASE_URL}/search?p={page}&q={encoded}"

    def _fetch_soup(self, target_url: str) -> bs4.BeautifulSoup | None:
        time.sleep(self.DELAY)
        try:
            self.page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
            self.page.wait_for_timeout(1500)
            html = self.page.content()
            if self._is_challenge_html(html):
                self.logger.warning("Telnavi challenge page detected, waiting once: %s", target_url)
                self.page.wait_for_timeout(8000)
                html = self.page.content()
            if self._is_challenge_html(html):
                self.logger.warning("Telnavi challenge still active, skipping: %s", target_url)
                return None
            return bs4.BeautifulSoup(html, "html.parser")
        except Exception as e:
            self.logger.warning("Telnavi fetch failed: %s / %s", target_url, e)
            return None

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
            if not re.search(r"/phone/\d+", href):
                continue
            text = _clean(a.get_text(" ", strip=True))
            phone = _digits(text)
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
            COL_SOURCE_SITE: "電話帳ナビ",
            COL_SEARCH_KEYWORD: keyword,
            COL_DETAIL_URL: detail_by_phone.get(tel_digits, f"{BASE_URL}/phone/{tel_digits}"),
        }

        name = _line_value(block, "事業者名")
        if name:
            item[Schema.NAME] = name

        phone_value = _line_value(block, "電話番号")
        if phone_value and not item.get(Schema.TEL):
            item[Schema.TEL] = phone_value.split("/", 1)[0].strip()

        joined = " ".join(block)
        match = re.search(r"クチコミ数\s*([0-9,]+)件", joined)
        if match:
            item[COL_REVIEW_COUNT] = match.group(1).replace(",", "")

        phone_type = _classify_phone(item.get(Schema.TEL, ""))
        if phone_type:
            item[COL_PHONE_TYPE] = phone_type
        if phone_type.startswith("携帯"):
            item[COL_MOBILE_TEL] = item.get(Schema.TEL, "")
        else:
            item[COL_FIXED_TEL] = item.get(Schema.TEL, "")

        return item if item.get(Schema.NAME) and item.get(Schema.TEL) else None

    def _scrape_detail(self, detail_url: str) -> dict:
        soup = self._fetch_soup(detail_url)
        if soup is None:
            return {}

        text = soup.get_text("\n", strip=True)
        section = self._basic_info_section(text)
        if not section:
            return {}

        lines = [_clean(line) for line in section.splitlines()]
        lines = [line for line in lines if line]

        item: dict = {}
        name = _line_value(lines, "事業者名")
        if name:
            item[Schema.NAME] = name.strip('"')

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

        phone = _line_value(lines, "電話番号")
        if phone and _digits(phone):
            item[Schema.TEL] = phone

        phone_type = _line_value(lines, "回線種別")
        if phone_type:
            item[COL_PHONE_TYPE] = phone_type

        fax = _line_value(lines, "FAX番号")
        if fax:
            item[COL_FAX] = fax

        category = _line_value(lines, "業種タグ")
        if category:
            item[Schema.LOB] = category
            item[Schema.CAT_SITE] = category

        rating = _line_value(lines, "ユーザー評価")
        if rating:
            item[COL_USER_RATING] = rating

        access_count = _line_value(lines, "アクセス数")
        if access_count:
            item[COL_ACCESS_COUNT] = access_count

        search_count = _line_value(lines, "検索結果表示回数")
        if search_count:
            item[COL_SEARCH_RESULT_COUNT] = search_count

        nuisance = self._extract_nuisance(lines)
        if nuisance:
            item[COL_NUISANCE_SCORE] = nuisance

        return item

    def _basic_info_section(self, text: str) -> str:
        start = re.search(r"基本情報", text)
        if not start:
            return ""
        tail = text[start.end():]
        end = re.search(r"クチコミ|口コミ|登録情報更新|不適切な情報を通報", tail)
        return tail[:end.start()] if end else tail

    def _extract_nuisance(self, lines: list[str]) -> str:
        values: list[str] = []
        for idx, line in enumerate(lines):
            if not line.startswith("迷惑電話度"):
                continue
            values.append(line)
            for nxt in lines[idx + 1: idx + 4]:
                if any(nxt.startswith(label) for label in ("安全:", "普通:", "迷惑:")):
                    values.append(nxt)
            break
        return " / ".join(values)

    def _merge_detail(self, record: dict, detail: dict) -> None:
        for key, value in detail.items():
            if value and not record.get(key):
                record[key] = value

        phone_type = _classify_phone(record.get(Schema.TEL, ""), record.get(COL_PHONE_TYPE, ""))
        if phone_type:
            record[COL_PHONE_TYPE] = phone_type
        if phone_type.startswith("携帯"):
            record[COL_MOBILE_TEL] = record.get(Schema.TEL, "")
        else:
            record[COL_FIXED_TEL] = record.get(Schema.TEL, "")

    def _extract_total_count(self, soup) -> int:
        text = soup.get_text(" ", strip=True)
        match = re.search(r"(?:約)?([0-9,]+)件中", text)
        if not match:
            match = re.search(r"キーワード:\s*.+?\s*\((?:約)?([0-9,]+)件\)", text)
        return int(match.group(1).replace(",", "")) if match else 0

    def _is_challenge_html(self, html: str) -> bool:
        markers = ("Just a moment", "cf_chl", "cf-challenge", "Enable JavaScript and cookies")
        return any(marker in html for marker in markers)


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    scraper = TelnaviScraper()
    scraper.execute(BASE_URL)
    print(f"output: {scraper.output_filepath}")
