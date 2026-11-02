"""
対象サイト: https://biz-maps.com/s/prefs/1
BIZMAPS（ビズマップ） — 企業詳細ページ 企業情報スクレイパー（北海道のみ）

取得対象 (企業詳細ページ /item/{key} で完結):
    - 名称 / 都道府県 / 郵便番号 / 住所 / 代表者名 / HP(企業URL)
    - 業種 大分類 (big_industry) / 小分類 (mid_industry, 複数は " / " 連結)
    - 設立年度 / 資本金 / 従業員数 / 企業詳細URL

取得フロー:
    robots.txt が公開している企業サイトマップ /sitemap_xml/item/{開始ID}/{終了ID}
    (内部企業IDの範囲指定。1/50001, 50001/100001, ... と 50,000 刻みで巡回) から
    詳細 URL を列挙し、詳細ページを 1 件取得するたびに判定して即 yield する。
    サイトマップは ID 上限 (2026-11 時点で約 275 万) を超えると空になるため、
    空ウィンドウが _MAX_EMPTY_WINDOWS 回連続したら上限とみなす (途中に疎な区間がある)。
    北海道の密度が高い _PRIORITY_START から巡回し、上限到達後に 1 へ戻って
    残りの区間を巡回する。

    一覧に都道府県情報は無いため、詳細ページの住所先頭の都道府県で
    北海道のみ出力する。
    対象外の詳細も取得は発生するため、実通信ごとに DELAY 秒空ける (ITEM_DELAY=0)。

注意:
    - TEL は未ログインでは非表示のため取得しない (JSON-LD の telephone も出力しない)。
    - robots.txt の Disallow (/s/*?*page=, /api/, /download/ 等) は使わない。
    - ログイン・会員機能は使わない。
    - 企業紹介文 (company__text) は自由記述プロースのため取得しない。
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

# サイトマップ (XML) を get_soup (html.parser) で読むための警告抑止
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)


# 企業サイトマップ (内部企業 ID の範囲指定)。ルート URL から urljoin で派生させる。
_SITEMAP_PATH = "/sitemap_xml/item/{start}/{end}"
_WINDOW = 50000
# 空ウィンドウがこの回数連続したら ID 上限に達したとみなす (= ID 100 万件分)
_MAX_EMPTY_WINDOWS = 20
# 巡回開始 ID。ID は取込バッチ単位で都道府県が固まっており (先頭帯はほぼ東京)、
# 2026-11 のサンプリングでは 100万〜105万 = 北海道。最初の出力を早めるため北海道帯から巡回し、
# 上限まで巡回した後に 1 へ戻って残りを巡回する (網羅性は変わらない)。
_PRIORITY_START = 1 + 20 * _WINDOW  # 1,000,001

_PREF = (
    r"北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県"
    r"|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県"
    r"|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県"
    r"|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県"
    r"|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
_PREF_RE = re.compile(rf"^({_PREF})")

# 出力対象 (北海道)
_TARGET_PREFS = {"北海道"}

_POST_RE = re.compile(r"〒?\s*(\d{3})\s*[-－ー]\s*(\d{4})")
_DATE_RE = re.compile(r"^(\d{4})(?:/(\d{1,2}))?(?:/(\d{1,2}))?$")
_ITEM_RE = re.compile(r"/item/[A-Za-z0-9]+/?$")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def _blank_if_dash(s: str) -> str:
    return "" if s in ("-", "－", "ー", "―") else s


def _norm_date(s: str) -> str:
    """'2006/1/1' → '2006-01-01', '1976/4' → '1976-04', '1980' → '1980'。

    年欠落 ('/9/19') 等の崩れた値は空にする。
    """
    m = _DATE_RE.match(s)
    if not m:
        return ""
    return "-".join(
        [m.group(1)] + [f"{int(g):02d}" for g in m.groups()[1:] if g]
    )


class BizmapsScraper(StaticCrawler):
    """BIZMAPS 企業詳細スクレイパー (北海道)"""

    DELAY = 3.0  # 実通信ごとの最小間隔（秒）
    # 対象外の詳細取得でも間隔を空けるため、待機は _fetch() 側で行う
    ITEM_DELAY = 0

    def parse(self, url: str) -> Generator[dict, None, None]:
        self._last_request_at = 0.0
        self._last_window_count = 0
        seen: set[str] = set()
        for start in self._window_starts(url):
            sitemap_url = urljoin(
                url, _SITEMAP_PATH.format(start=start, end=start + _WINDOW)
            )
            detail_urls = [u for u in self._sitemap_urls(sitemap_url) if u not in seen]
            # ウィンドウ境界 (end == 次の start) の重複除外用。前ウィンドウ分のみ保持
            seen = set(detail_urls)

            for detail_url in detail_urls:
                item = self._parse_detail(urljoin(url, detail_url))
                if item:
                    yield item

    def _window_starts(self, url: str) -> Generator[int, None, None]:
        """_PRIORITY_START → ID 上限 (空ウィンドウ連続で判定) → 1 → _PRIORITY_START 手前。"""
        empty_streak = 0
        start = _PRIORITY_START
        while empty_streak < _MAX_EMPTY_WINDOWS:
            yield start
            empty_streak = 0 if self._last_window_count else empty_streak + 1
            start += _WINDOW
        yield from range(1, _PRIORITY_START, _WINDOW)

    def _sitemap_urls(self, sitemap_url: str) -> list[str]:
        soup = self._fetch(sitemap_url)
        urls = []
        if soup is not None:
            for loc in soup.find_all("loc"):
                u = _clean(loc.get_text())
                if _ITEM_RE.search(u):
                    urls.append(u)
        self._last_window_count = len(urls)
        return urls

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

    def _parse_detail(self, url: str) -> dict | None:
        soup = self._fetch(url)
        if soup is None:
            return None

        info: dict[str, str] = {}
        for tr in soup.select("table.company__info__table tr"):
            th, td = tr.find("th"), tr.find("td")
            if th and td:
                info[_clean(th.get_text())] = td

        # 住所: 「〒 170-0013 <br> 東京都豊島区...」
        addr_td = info.get("住所")
        post_code, full_addr = "", ""
        if addr_td is not None:
            text = _clean(addr_td.get_text(" "))
            m = _POST_RE.search(text)
            if m:
                post_code = f"{m.group(1)}-{m.group(2)}"
                text = text[m.end():]
            full_addr = _clean(text.replace("〒", ""))

        pm = _PREF_RE.match(full_addr)
        pref = pm.group(1) if pm else ""
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
            Schema.ADDR: full_addr[len(pref):].strip(),
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
