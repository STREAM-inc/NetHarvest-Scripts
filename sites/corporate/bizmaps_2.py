"""
対象サイト: https://biz-maps.com/s/prefs/1
BIZMAPS（ビズマップ） — 企業情報スクレイパー（北海道のみ・北海道に絞った巡回）

取得対象:
    - 名称 / 都道府県 / 郵便番号 / 住所 / 代表者名 / HP(企業URL・詳細ページのみ)
    - 業種 大分類 / 小分類 (複数は " / " 連結)
    - 設立年度 / 資本金 / 従業員数(詳細ページのみ) / 企業詳細URL

取得フロー（2026-11-09 改修: 全国巡回 約104万件 → 北海道に絞った巡回 約5.6万リクエスト）:
    フェーズ1: 北海道の「市区町村×業種」一覧ページ
        robots.txt が公開しているサイトマップ /sitemap_xml/city_industry/1 から
        /s/cities/01xxxx/(b-inds|m-inds)/N の URL (約1.6万) を列挙し、各一覧の
        1 ページ目 (最大 20 社) を取得して一覧上の項目から出力する。
        ページ送り (/s/*?*page=) は robots で Disallow のため使わない。
    フェーズ2: 北海道の企業が固まっている内部企業 ID 帯の企業詳細ページ
        企業サイトマップ /sitemap_xml/item/{開始ID}/{終了ID} を _HOKKAIDO_ID_RANGES の
        範囲だけ取得し、フェーズ1で出力済みの企業を除いた詳細ページを取得する。
        ID 帯は 2026-11-09 に全サイトマップ (1,040,722件) を約1,100件抽出して特定した
        (北海道以外の詳細は住所判定で落とす)。

注意:
    - TEL は未ログインでは非表示のため取得しない。
    - robots.txt の Disallow (/s/*?*page=, /api/, /download/, /item_modal/ 等) は使わない。
    - ログイン・会員機能は使わない。
    - 実通信ごとに DELAY 秒空ける。
    - 利用規約にスクレイピング/クローリングの明示的禁止は無いが、第11条で
      私的利用を超える複製・第三者提供を禁じ、第13条で違約金を定めている。

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/bizmaps_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id bizmaps_2
"""

import re
import sys
import time
import warnings
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import XMLParsedAsHTMLWarning

from src.framework.static import StaticCrawler
from src.const.schema import Schema

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)


# フェーズ1: 北海道の市区町村×業種サイトマップ
_CITY_INDUSTRY_SITEMAP = "/sitemap_xml/city_industry/1"
_CITY_LIST_RE = re.compile(r"/s/cities/01\d{4}/(?:b-inds|m-inds)/\d+/?$")

# フェーズ2: 北海道の企業が固まっている内部企業 ID 帯 (start, end)
# 2026-11-09 のサンプリング結果 (前後に余裕を持たせている)
_HOKKAIDO_ID_RANGES = [
    (955000, 1027400),    # 北海道帯A (約2.9万件)
    (1788400, 1799700),   # 北海道帯B (約1万件)
    (2200001, 2250001),   # 小規模ウィンドウ (北海道を含む・約440件)
    (2400001, 2450001),   # 小規模ウィンドウ (北海道を含む・約1,000件)
    (2800001, 2850001),   # 小規模ウィンドウ (北海道を含む・数件)
]
_ITEM_SITEMAP = "/sitemap_xml/item/{start}/{end}"
_ITEM_SITEMAP_STEP = 10000  # 1 回のサイトマップ取得を小さく保つ

_PREF = (
    r"北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県"
    r"|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県"
    r"|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県"
    r"|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県"
    r"|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
_PREF_RE = re.compile(rf"^({_PREF})")
_TARGET_PREFS = {"北海道"}

_POST_RE = re.compile(r"〒?\s*(\d{3})\s*[-－ー]\s*(\d{4})")
_DATE_RE = re.compile(r"^(\d{4})(?:/(\d{1,2}))?(?:/(\d{1,2}))?$")
_ITEM_RE = re.compile(r"/item/([A-Za-z0-9]+)/?$")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def _blank_if_dash(s: str) -> str:
    return "" if s in ("-", "－", "ー", "―") else s


def _norm_date(s: str) -> str:
    m = _DATE_RE.match(s)
    if not m:
        return ""
    return "-".join(
        [m.group(1)] + [f"{int(g):02d}" for g in m.groups()[1:] if g]
    )


def _split_addr(td) -> tuple[str, str, str]:
    """住所セル → (郵便番号, 都道府県, 都道府県以降の住所)。"""
    if td is None:
        return "", "", ""
    text = _clean(td.get_text(" "))
    post_code = ""
    m = _POST_RE.search(text)
    if m:
        post_code = f"{m.group(1)}-{m.group(2)}"
        text = text[m.end():]
    full_addr = _clean(text.replace("〒", ""))
    pm = _PREF_RE.match(full_addr)
    pref = pm.group(1) if pm else ""
    return post_code, pref, full_addr[len(pref):].strip()


class BizmapsScraper(StaticCrawler):
    """BIZMAPS 企業スクレイパー (北海道)"""

    DELAY = 3.0  # 実通信ごとの最小間隔（秒）
    ITEM_DELAY = 0

    def parse(self, url: str) -> Generator[dict, None, None]:
        self._last_request_at = 0.0
        seen: set[str] = set()  # 出力済み (または判定済み) の企業キー

        # ---- フェーズ1: 市区町村×業種 一覧 (1 ページ目のみ) ----
        for list_url in self._city_list_urls(url):
            for key, item in self._parse_list(list_url):
                if key in seen:
                    continue
                seen.add(key)
                yield item

        # ---- フェーズ2: 北海道 ID 帯の詳細ページ ----
        for start, end in _HOKKAIDO_ID_RANGES:
            s = start
            while s < end:
                e = min(s + _ITEM_SITEMAP_STEP, end)
                sitemap_url = urljoin(url, _ITEM_SITEMAP.format(start=s, end=e))
                for detail_url in self._sitemap_locs(sitemap_url, _ITEM_RE):
                    key = _ITEM_RE.search(detail_url).group(1)
                    if key in seen:
                        continue
                    seen.add(key)
                    item = self._parse_detail(urljoin(url, detail_url))
                    if item:
                        yield item
                s = e

    # ------------------------------------------------------------------
    def _fetch(self, url: str):
        """直前の実通信から DELAY 秒以上空けて get_soup する (キャッシュヒットは待たない)。"""
        remain = self.DELAY - (time.monotonic() - self._last_request_at)
        if remain > 0:
            time.sleep(remain)
        soup = self.get_soup(url)
        if not self._last_fetch_from_cache:
            self._last_request_at = time.monotonic()
        return soup

    def _sitemap_locs(self, sitemap_url: str, pattern: re.Pattern) -> list[str]:
        soup = self._fetch(sitemap_url)
        urls = []
        if soup is not None:
            for loc in soup.find_all("loc"):
                u = _clean(loc.get_text())
                if pattern.search(u):
                    urls.append(u)
        return urls

    def _city_list_urls(self, url: str) -> list[str]:
        urls = self._sitemap_locs(urljoin(url, _CITY_INDUSTRY_SITEMAP), _CITY_LIST_RE)
        # http:// で載っている場合も https で取得する
        return [urljoin(url, re.sub(r"^https?://[^/]+", "", u)) for u in urls]

    def _parse_list(self, list_url: str) -> Generator[tuple[str, dict], None, None]:
        soup = self._fetch(list_url)
        if soup is None:
            return
        for li in soup.select("ul.results__list li.results__item"):
            a = li.find("a", href=_ITEM_RE)
            if a is None:
                continue
            key = _ITEM_RE.search(a["href"]).group(1)
            name_tag = li.select_one(".results__name")
            name = _clean(name_tag.get_text()) if name_tag else ""
            if not name:
                continue

            info: dict = {}
            for tr in li.select("table.results__table tr"):
                th, td = tr.find("th"), tr.find("td")
                if th and td:
                    info[_clean(th.get_text())] = td

            post_code, pref, addr = _split_addr(info.get("住所"))
            if pref not in _TARGET_PREFS:
                continue

            def val(label: str) -> str:
                td = info.get(label)
                return _blank_if_dash(_clean(td.get_text(" "))) if td is not None else ""

            inds = [x.strip() for x in val("業種").split(",") if x.strip()]
            yield key, {
                Schema.URL: urljoin(list_url, f"/item/{key}"),
                Schema.NAME: name,
                Schema.PREF: pref,
                Schema.POST_CODE: post_code,
                Schema.ADDR: addr,
                Schema.REP_NM: val("代表者名"),
                Schema.HP: "",
                Schema.CAT_LV1: inds[0] if inds else "",
                Schema.CAT_LV3: " / ".join(inds[1:]),
                Schema.OPEN_DATE: _norm_date(val("設立年度")),
                Schema.CAP: val("資本金等"),
                Schema.EMP_NUM: "",
            }

    def _parse_detail(self, url: str) -> dict | None:
        soup = self._fetch(url)
        if soup is None:
            return None

        info: dict = {}
        for tr in soup.select("table.company__info__table tr"):
            th, td = tr.find("th"), tr.find("td")
            if th and td:
                info[_clean(th.get_text())] = td

        post_code, pref, addr = _split_addr(info.get("住所"))
        if pref not in _TARGET_PREFS:
            return None

        name_tag = soup.select_one("h1.company__name")
        name = _clean(name_tag.get_text()) if name_tag else ""
        if not name:
            return None

        def val(label: str) -> str:
            td = info.get(label)
            return _blank_if_dash(_clean(td.get_text(" "))) if td is not None else ""

        hp = ""
        hp_td = info.get("URL")
        if hp_td is not None:
            a = hp_td.find("a", href=True)
            hp = _clean(a["href"]) if a else val("URL")
            if not hp.startswith("http"):
                hp = ""

        big, mid = [], []
        for a in soup.select("ul.company__category a[href]"):
            label = _clean(a.get_text())
            if "big_industry" in a["href"]:
                big.append(label)
            elif "mid_industry" in a["href"]:
                mid.append(label)

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.REP_NM: val("代表者"),
            Schema.HP: hp,
            Schema.CAT_LV1: " / ".join(big),
            Schema.CAT_LV3: " / ".join(mid),
            Schema.OPEN_DATE: _norm_date(val("設立年度")),
            Schema.CAP: val("資本金等"),
            Schema.EMP_NUM: val("従業員数"),
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BizmapsScraper()
    scraper.execute("https://biz-maps.com/s/prefs/1")
