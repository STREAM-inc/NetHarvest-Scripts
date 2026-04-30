"""
ナイトスタイル (nightstyle.jp) — 風俗・ナイトワーク業種情報スクレイパー

取得対象:
    - 全国47都道府県の掲載店舗情報

取得フロー:
    1. 47都道府県の一覧ページを全ページ巡回 (/{pref}/ → a.next_page)
    2. lazy_render エンドポイントのJSを解析して店舗URLを収集
    3. 各店舗詳細ページ (/shop/{id}/) から店舗情報を抽出

実行方法:
    python scripts/sites/nightlife/nightstyle.py
    python bin/run_flow.py --site-id nightstyle
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup

from src.framework.static import StaticCrawler
from src.const.schema import Schema


BASE_URL = "https://nightstyle.jp"

PREFECTURES = [
    ("tokyo",     "東京都"),
    ("saitama",   "埼玉県"),
    ("chiba",     "千葉県"),
    ("kanagawa",  "神奈川県"),
    ("tochigi",   "栃木県"),
    ("gunma",     "群馬県"),
    ("ibaraki",   "茨城県"),
    ("hokkaido",  "北海道"),
    ("aomori",    "青森県"),
    ("iwate",     "岩手県"),
    ("miyagi",    "宮城県"),
    ("akita",     "秋田県"),
    ("yamagata",  "山形県"),
    ("fukushima", "福島県"),
    ("niigata",   "新潟県"),
    ("toyama",    "富山県"),
    ("ishikawa",  "石川県"),
    ("fukui",     "福井県"),
    ("yamanashi", "山梨県"),
    ("nagano",    "長野県"),
    ("gifu",      "岐阜県"),
    ("shizuoka",  "静岡県"),
    ("aichi",     "愛知県"),
    ("mie",       "三重県"),
    ("shiga",     "滋賀県"),
    ("kyoto",     "京都府"),
    ("osaka",     "大阪府"),
    ("hyogo",     "兵庫県"),
    ("nara",      "奈良県"),
    ("wakayama",  "和歌山県"),
    ("tottori",   "鳥取県"),
    ("shimane",   "島根県"),
    ("okayama",   "岡山県"),
    ("hiroshima", "広島県"),
    ("yamaguchi", "山口県"),
    ("tokushima", "徳島県"),
    ("kagawa",    "香川県"),
    ("ehime",     "愛媛県"),
    ("kochi",     "高知県"),
    ("fukuoka",   "福岡県"),
    ("saga",      "佐賀県"),
    ("nagasaki",  "長崎県"),
    ("kumamoto",  "熊本県"),
    ("oita",      "大分県"),
    ("miyazaki",  "宮崎県"),
    ("kagoshima", "鹿児島県"),
    ("okinawa",   "沖縄県"),
]

# JS の .before("...") / .append("...") / .html("...") 引数を抽出
_JS_HTML_RE = re.compile(r'\.(?:before|append|html)\("((?:\\.|[^"\\])*)"\)', re.DOTALL)
# 店舗トップページのみ許可: /shop/<shopid>/
_SHOP_TOP_RE = re.compile(r"^/shop/[^/]+/$")

_HEX2 = re.compile(r"^[0-9a-fA-F]{2}$")
_HEX4 = re.compile(r"^[0-9a-fA-F]{4}$")
_HEXU = re.compile(r"^[0-9a-fA-F]{1,6}$")


def _js_unescape(raw: str) -> str:
    """JSダブルクォート文字列内のエスケープを復元する。"""
    out = []
    i = 0
    n = len(raw)
    while i < n:
        c = raw[i]
        if c != "\\":
            out.append(c)
            i += 1
            continue
        i += 1
        if i >= n:
            out.append("\\")
            break
        c = raw[i]
        if c == "\n":
            i += 1
            continue
        if c == "\r":
            i += 2 if (i + 1 < n and raw[i + 1] == "\n") else 1
            continue
        if c == "n":
            out.append("\n"); i += 1; continue
        if c == "r":
            out.append("\r"); i += 1; continue
        if c == "t":
            out.append("\t"); i += 1; continue
        if c in ("\\", '"', "'", "/"):
            out.append(c); i += 1; continue
        if c == "x" and i + 2 < n:
            h = raw[i + 1: i + 3]
            if _HEX2.match(h):
                out.append(chr(int(h, 16))); i += 3; continue
        if c == "u":
            if i + 1 < n and raw[i + 1] == "{":
                j = raw.find("}", i + 2)
                if j != -1:
                    h = raw[i + 2: j]
                    if _HEXU.match(h):
                        out.append(chr(int(h, 16))); i = j + 1; continue
            if i + 4 < n:
                h = raw[i + 1: i + 5]
                if _HEX4.match(h):
                    out.append(chr(int(h, 16))); i += 5; continue
        out.append(c)
        i += 1
    return "".join(out)


def _extract_lazy_html(js_text: str) -> str:
    parts = _JS_HTML_RE.findall(js_text)
    if not parts:
        return ""
    return "".join(_js_unescape(p) for p in parts)


class NightstyleScraper(StaticCrawler):
    """ナイトスタイル (nightstyle.jp) の店舗情報スクレイパー"""

    DELAY = 0.3

    def parse(self, url: str):
        seen: set[str] = set()
        shop_entries: list[tuple[str, str]] = []

        for pref_code, pref_ja in PREFECTURES:
            for shop_url in self._collect_shops(pref_code):
                if shop_url in seen:
                    continue
                seen.add(shop_url)
                shop_entries.append((shop_url, pref_ja))

        self.total_items = len(shop_entries)
        self.logger.info("収集した店舗数: %d", self.total_items)

        for shop_url, pref_ja in shop_entries:
            item = self._scrape_detail(shop_url, pref_ja)
            if item:
                yield item

    def _collect_shops(self, pref_code: str) -> list[str]:
        """都道府県の全ページを巡回して店舗URLを収集する。"""
        current = f"{BASE_URL}/{pref_code}/"
        all_urls: list[str] = []

        while current:
            soup = self.get_soup(current)
            if soup is None:
                break

            lazy_a = soup.find("a", href=re.compile(r"^/lazy_render/shop_list"))
            if lazy_a and lazy_a.get("href"):
                lazy_url = urljoin(BASE_URL, lazy_a["href"])
                all_urls.extend(self._fetch_lazy_shops(lazy_url, current))

            next_tag = soup.find("a", class_="next_page")
            if next_tag and next_tag.get("href"):
                current = urljoin(current, next_tag["href"])
            else:
                break

        return all_urls

    def _fetch_lazy_shops(self, lazy_url: str, referer: str) -> list[str]:
        """lazy_render エンドポイントのJSから店舗URLを返す。"""
        try:
            resp = self.session.get(
                lazy_url,
                headers={"Referer": referer, "X-Requested-With": "XMLHttpRequest"},
                timeout=self.TIMEOUT,
            )
            resp.raise_for_status()
            resp.encoding = "utf-8"
        except Exception as e:
            self.logger.warning("lazy_render 取得失敗: %s — %s", lazy_url, e)
            return []

        lazy_html = _extract_lazy_html(resp.text)
        if not lazy_html:
            return []

        soup = BeautifulSoup(lazy_html, "html.parser")
        seen: set[str] = set()
        urls: list[str] = []
        for a in soup.select('a[href^="/shop/"]'):
            href = a.get("href", "")
            if not _SHOP_TOP_RE.match(href):
                continue
            full = urljoin(BASE_URL, href)
            if full not in seen:
                seen.add(full)
                urls.append(full)
        return urls

    def _scrape_detail(self, url: str, pref_ja: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        container = soup.find("div", class_="shop-address")
        if not container:
            return None

        name, kana = "", ""
        p_tag = container.find("p")
        if p_tag:
            name = "".join(t for t in p_tag.contents if isinstance(t, str)).strip()
            span = p_tag.find("span")
            kana = span.get_text(strip=True) if span else ""

        info: dict[str, str] = {}
        dl = container.find("dl")
        if dl:
            for dt, dd in zip(dl.find_all("dt"), dl.find_all("dd")):
                key = dt.get_text(strip=True)
                if key == "業種":
                    val = ", ".join(a.get_text(strip=True) for a in dd.find_all("a"))
                else:
                    val = " ".join(dd.stripped_strings)
                info[key] = val

        return {
            Schema.URL:      url,
            Schema.NAME:     name,
            Schema.NAME_KANA: kana,
            Schema.PREF:     pref_ja,
            Schema.ADDR:     info.get("住所", ""),
            Schema.TEL:      info.get("電話番号", ""),
            Schema.CAT_SITE: info.get("業種", ""),
            Schema.TIME:     info.get("営業時間", ""),
            Schema.HOLIDAY:  info.get("定休日", ""),
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = NightstyleScraper()
    scraper.execute(BASE_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
