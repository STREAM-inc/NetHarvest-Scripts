"""
Target site: https://www.jpnumber.com/

Collects power retail / new-electricity sales-agent candidates from JPNumber.
Only factual fields are stored; review text and free-form descriptions are not
persisted.
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import quote, urljoin

import bs4

_current = Path(__file__).resolve()
_script_root = _current.parents[2]
_framework_candidates = [
    _current.parents[3],
    _script_root.parent / "NetHarvest",
]
for _path in _framework_candidates:
    if (_path / "src").exists() and str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema


BASE_URL = "https://www.jpnumber.com"

KEYWORDS = [
    "新電力",
    "ハルエネ",
    "電力代理店",
    "東京電力 代理店",
    "関西電力 代理店",
    "中部電力 代理店",
    "電力切替営業",
    "電力小売",
]

DETAIL_FETCH_LIMIT = 50
MAX_PAGES_PER_KEYWORD = 40
KEYWORD_PAGE_LIMITS = {
    "新電力": 40,
    "ハルエネ": 5,
    "電力代理店": 10,
    "東京電力 代理店": 5,
    "関西電力 代理店": 5,
    "中部電力 代理店": 5,
    "電力切替営業": 10,
    "電力小売": 10,
}

PREFS = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県", "滋賀県", "京都府",
    "大阪府", "兵庫県", "奈良県", "和歌山県", "鳥取県", "島根県",
    "岡山県", "広島県", "山口県", "徳島県", "香川県", "愛媛県",
    "高知県", "福岡県", "佐賀県", "長崎県", "熊本県", "大分県",
    "宮崎県", "鹿児島県", "沖縄県",
]

MAJOR_UTILITY_HINTS = [
    "東京電力パワーグリッド",
    "東京電力エナジーパートナー",
    "中部電力ミライズ",
    "関西電力",
    "九州電力",
    "東北電力",
    "北陸電力",
    "北海道電力",
    "中国電力",
    "四国電力",
    "沖縄電力",
]

AGENT_HINTS = [
    "代理店",
    "営業",
    "勧誘",
    "切替",
    "切り替え",
    "電気変更",
    "プラン",
    "ハルエネ",
    "エネパル",
    "でんき",
    "電力サービス",
]


def _clean(value: str | None) -> str:
    if not value:
        return ""
    value = re.sub(r"\s+", " ", value.replace("\xa0", " ")).strip()
    value = re.sub(r"(?<=[ぁ-んァ-ン一-龯])\s+(?=[ぁ-んァ-ン一-龯])", "", value)
    value = value.replace("〒 ", "〒")
    return value


def _digits(value: str | None) -> str:
    return re.sub(r"\D", "", value or "")


def _extract_int(text: str, label: str) -> str:
    match = re.search(rf"{re.escape(label)}\s*:\s*([0-9,]+)", text)
    return match.group(1).replace(",", "") if match else ""


def _split_post_code(address: str) -> tuple[str, str]:
    match = re.search(r"〒?\s*(\d{3})-?(\d{4})", address)
    if not match:
        return "", address.strip()
    post_code = f"{match.group(1)}-{match.group(2)}"
    rest = (address[:match.start()] + address[match.end():]).strip()
    return post_code, rest


def _extract_pref(address: str) -> str:
    return next((pref for pref in PREFS if pref in address), "")


def _candidate_type(name: str, keyword: str, category: str) -> str:
    joined = f"{name} {keyword} {category}"
    if any(hint in joined for hint in AGENT_HINTS):
        return "代理店・営業候補"
    if any(hint in joined for hint in MAJOR_UTILITY_HINTS):
        return "電力会社本体候補"
    return "新電力関連候補"


class JpnumberPowerAgentsScraper(DynamicCrawler):
    """JPNumber new-electricity sales-agent candidate scraper."""

    DELAY = 0.0
    REQUEST_DELAY = 1.2
    EXTRA_COLUMNS = [
        "データ元",
        "検索キーワード",
        "番号種別",
        "検索回数",
        "アクセス数",
        "口コミ数",
        "最寄り駅",
        "候補判定",
        "詳細URL",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_numbers: set[str] = set()
        detail_count = 0

        for keyword in KEYWORDS:
            page = 1
            max_pages = KEYWORD_PAGE_LIMITS.get(keyword, MAX_PAGES_PER_KEYWORD)
            while page <= max_pages:
                list_url = self._build_search_url(keyword, page)
                soup = self._get_soup(list_url)
                if soup is None:
                    self.logger.warning("search page skipped: %s", list_url)
                    break

                total = self._extract_total_count(soup)
                items = self._parse_search_items(soup, keyword, list_url)
                if not items:
                    break

                self.logger.info(
                    "keyword=%s page=%d items=%d total=%s",
                    keyword,
                    page,
                    len(items),
                    total or "unknown",
                )

                for item in items:
                    tel_key = _digits(item.get(Schema.TEL))
                    if not tel_key or tel_key in seen_numbers:
                        continue
                    seen_numbers.add(tel_key)

                    detail_url = item.get("詳細URL", "")
                    if detail_url and detail_count < DETAIL_FETCH_LIMIT:
                        detail = self._fetch_detail(detail_url)
                        detail_count += 1
                        item.update({k: v for k, v in detail.items() if v})

                    yield item

                if total and page * 20 >= total:
                    break
                page += 1

    def _build_search_url(self, keyword: str, page: int) -> str:
        base = f"{BASE_URL}/searchnumber.do?number={quote(keyword)}"
        return base if page <= 1 else f"{base}&page={page}"

    def _get_soup(self, url: str) -> bs4.BeautifulSoup | None:
        time.sleep(self.REQUEST_DELAY)
        self.page.goto(url, wait_until="domcontentloaded", timeout=30000)
        self.page.wait_for_timeout(900)

        title = self.page.title() or ""
        body = self.page.locator("body").inner_text(timeout=5000)
        if "Just a moment" in title or "Enable JavaScript and cookies" in body:
            self.page.wait_for_timeout(8000)
            title = self.page.title() or ""
            body = self.page.locator("body").inner_text(timeout=5000)
            if "Just a moment" in title or "Enable JavaScript and cookies" in body:
                self.error_count += 1
                return None

        return bs4.BeautifulSoup(self.page.content(), "html.parser")

    def _extract_total_count(self, soup: bs4.BeautifulSoup) -> int:
        text = soup.get_text(" ", strip=True)
        match = re.search(r"検索結果\s*([0-9,]+)\s*件", text)
        return int(match.group(1).replace(",", "")) if match else 0

    def _parse_search_items(
        self,
        soup: bs4.BeautifulSoup,
        keyword: str,
        list_url: str,
    ) -> list[dict]:
        items: list[dict] = []
        blocks = [
            div for div in soup.select("#result-main-right div")
            if "frame-728-orange-l" in div.get("class", [])
        ]

        for block in blocks:
            text = _clean(block.get_text(" ", strip=True))
            phone_match = re.search(r"電話番号:\s*([0-9]+)\s*\|\s*([0-9\-]+)", text)
            name_match = re.search(
                r"事業者名：\s*(.*?)\s*(固定電話|携帯電話|フリーダイヤル|IP電話)\s*>",
                text,
            )
            if not phone_match or not name_match:
                continue

            raw_addr = ""
            addr_match = re.search(r"住所：\s*(.*?)(?:\s*最寄り駅：|$)", text)
            if addr_match:
                raw_addr = _clean(addr_match.group(1))

            station = ""
            station_match = re.search(r"最寄り駅：\s*(.*)$", text)
            if station_match:
                station = _clean(station_match.group(1))

            post_code, address = _split_post_code(raw_addr)
            pref = _extract_pref(address)
            detail_url = self._detail_url(block)
            name = _clean(name_match.group(1))
            line_type = name_match.group(2)

            item = {
                Schema.URL: list_url,
                Schema.NAME: name,
                Schema.TEL: phone_match.group(2),
                Schema.POST_CODE: post_code,
                Schema.PREF: pref,
                Schema.ADDR: address,
                Schema.CAT_SITE: "電話番号検索",
                "データ元": "JPNumber",
                "検索キーワード": keyword,
                "番号種別": line_type,
                "検索回数": _extract_int(text, "検索回数"),
                "アクセス数": _extract_int(text, "アクセス数"),
                "口コミ数": _extract_int(text, "口コミ数"),
                "最寄り駅": station,
                "候補判定": _candidate_type(name, keyword, line_type),
                "詳細URL": detail_url,
            }
            items.append(item)

        return items

    def _detail_url(self, block: bs4.Tag) -> str:
        for link in block.select('a[href*="numberinfo"]'):
            href = link.get("href", "")
            if href and "#comment" not in href and "#writecomment" not in href:
                return urljoin(BASE_URL, href)
        return ""

    def _fetch_detail(self, detail_url: str) -> dict:
        soup = self._get_soup(detail_url)
        if soup is None:
            return {}

        text = soup.get_text("\n", strip=True)
        data: dict = {Schema.URL: detail_url}

        industry = self._detail_value(text, "業種", "住所")
        if industry:
            data[Schema.CAT_NM] = industry

        hp = self._detail_value(text, "公式サイト", "電話番号")
        if hp and hp.startswith("http"):
            data[Schema.HP] = hp

        detail_address = self._detail_value(text, "住所", "問い合わせ先")
        if detail_address:
            post_code, address = _split_post_code(_clean(detail_address))
            data[Schema.POST_CODE] = post_code
            data[Schema.ADDR] = address
            data[Schema.PREF] = _extract_pref(address)

        station = self._detail_value(text, "最寄り駅", "アクセス")
        if station:
            data["最寄り駅"] = station

        return data

    def _detail_value(self, text: str, start: str, end: str) -> str:
        pattern = rf"\n{re.escape(start)}\n(.*?)\n{re.escape(end)}\n"
        match = re.search(pattern, text, flags=re.S)
        if not match:
            return ""
        return _clean(match.group(1))


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    JpnumberPowerAgentsScraper().execute(BASE_URL)
