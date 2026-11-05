"""
対象サイト: https://biz-maps.com/
BIZMAPS（ビズマップ） — 企業詳細ページ 企業情報スクレイパー（東北・新潟・北陸のみ）

取得対象 (企業詳細ページ /item/{key} で完結):
    - 名称 / 都道府県 / 郵便番号 / 住所 / 代表者名 / HP(企業URL)
    - 業種 大分類 (big_industry) / 小分類 (mid_industry, 複数は " / " 連結)
    - 設立年度 / 資本金 / 従業員数 / 企業詳細URL

取得フロー:
    robots.txt が公開している企業サイトマップ /sitemap_xml/item/{開始ID}/{終了ID}
    (内部企業IDの範囲指定。1/50001, 50001/100001, ... と 50,000 刻みで巡回) から
    詳細 URL を列挙する。サイトマップは ID 上限 (2026-11 時点で約 275 万) を超えると
    空になるため、空ウィンドウが _MAX_EMPTY_WINDOWS 回連続したら上限とみなす。
    東北・北陸の密度が高い _PRIORITY_START から巡回し、上限到達後に 1 へ戻って
    残りの区間を巡回する。

    適応サンプリング (STREAMREQ-28638):
        サイトマップ全体は約 110 万社あり、全詳細を DELAY=3 秒で取ると約 38 日かかる。
        サイトマップ内の URL は内部 ID 順で、ID は取込バッチ単位で同じ都道府県が
        連続している (2026-11-05 のサンプリングで確認)。そこで各ウィンドウの URL を
        _STEP 件おきの「アンカー」で区切り、アンカー詳細だけを先に取得して都道府県を
        判定する。同じ対象外県のアンカーが4つ連続する区間 (= 取込バッチの県ブロック
        の内部) だけ取得を省き、それ以外 (対象県・判定不能・県の切り替わり・県が1件ごとに
        入れ替わる新規登録帯) は区間内の詳細を全件取得する
        (同一県ブロックの内部に対象県が _STEP 件未満だけ埋もれた場合のみ取りこぼす)。

    詳細ページの住所先頭の都道府県で
    青森・岩手・宮城・秋田・山形・福島・新潟・富山・石川・福井 のみ出力する。
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
    python scripts/sites/corporate/bizmaps_3.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id bizmaps_3
"""

import logging
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

_log = logging.getLogger(__name__)

# 企業サイトマップ (内部企業 ID の範囲指定)。ルート URL から urljoin で派生させる。
_SITEMAP_PATH = "/sitemap_xml/item/{start}/{end}"
_WINDOW = 50000
# 空ウィンドウがこの回数連続したら ID 上限に達したとみなす (= ID 100 万件分)
_MAX_EMPTY_WINDOWS = 20
# 巡回開始 ID。ID は取込バッチ単位で都道府県が固まっており (先頭帯はほぼ東京)、
# 2026-11 のサンプリングでは 110万〜114万 = 岩手・宮城、106万〜108万 = 福島、
# 121万 = 青森、129万 = 石川 付近。最初の出力を早めるため岩手・宮城帯から巡回し、
# 上限まで巡回した後に 1 へ戻って残りを巡回する (網羅性は変わらない)。
_PRIORITY_START = 1 + 22 * _WINDOW  # 1,100,001
# 適応サンプリングのアンカー間隔 (サイトマップ内 URL の件数)
_STEP = 50

_PREF = (
    r"北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県"
    r"|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県"
    r"|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県"
    r"|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県"
    r"|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
_PREF_RE = re.compile(rf"^({_PREF})")

# 出力対象 (東北・新潟・北陸)
_TARGET_PREFS = {
    "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "新潟県", "富山県", "石川県", "福井県",
}

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
    """BIZMAPS 企業詳細スクレイパー (東北・新潟・北陸)"""

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
            yield from self._crawl_window(
                start, [urljoin(url, u) for u in detail_urls]
            )

    def _crawl_window(self, start: int, urls: list[str]) -> Generator[dict, None, None]:
        """アンカー (_STEP 件おき) の都道府県で、区間ごとに全件取得するかを決める。

        1) アンカー詳細を先に全部取得して都道府県列 p[0..m-1] を得る。
        2) 区間 (k, k+1) は「両端と前後のアンカーがすべて同じ対象外県」
           (= p[k-1]==p[k]==p[k+1]==p[k+2]。ウィンドウ端では存在する側のみ) のときだけ
           取得を省く。
           それ以外 (どちらかの端が対象県・判定不能・県の切り替わり) は区間内を全件取得する。
        取込バッチ単位で県がまとまっている帯では大半の区間を省けるが、新規登録帯の
        ように県が1件ごとに入れ替わる帯ではほぼ全件取得になる (取りこぼし防止を優先)。
        """
        n = len(urls)
        if n == 0:
            return
        anchors = list(range(0, n, _STEP))
        if anchors[-1] != n - 1:
            anchors.append(n - 1)
        fetched = hits = skipped = 0
        prefs: list[str | None] = []
        for a in anchors:
            pref, item = self._parse_detail(urls[a])
            fetched += 1
            prefs.append(pref)
            if item:
                hits += 1
                yield item
        m = len(anchors)

        def plain(k: int) -> bool:
            p = prefs[k]
            return p is not None and p != "" and p not in _TARGET_PREFS

        for k in range(m - 1):
            same = plain(k) and plain(k + 1) and prefs[k] == prefs[k + 1]
            run = (
                same
                and m >= 3
                and (k - 1 < 0 or prefs[k - 1] == prefs[k])
                and (k + 2 >= m or prefs[k + 2] == prefs[k + 1])
            )
            if run:
                skipped += anchors[k + 1] - anchors[k] - 1
                continue
            for i in range(anchors[k] + 1, anchors[k + 1]):
                _, inner = self._parse_detail(urls[i])
                fetched += 1
                if inner:
                    hits += 1
                    yield inner
        _log.info(
            "window %d: urls=%d fetched=%d skipped=%d hits=%d",
            start, n, fetched, skipped, hits,
        )

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

    def _parse_detail(self, url: str) -> tuple[str | None, dict | None]:
        """(都道府県, 出力 item) を返す。取得失敗は (None, None)、対象外は (県名, None)。"""
        soup = self._fetch(url)
        if soup is None:
            return None, None

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
            return pref, None

        name_tag = soup.select_one("h1.company__name")
        name = _clean(name_tag.get_text()) if name_tag else ""
        if not name:
            return pref, None

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

        return pref, {
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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BizmapsScraper()
    scraper.execute("https://biz-maps.com/")
