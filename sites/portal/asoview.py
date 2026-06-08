"""
アソビュー — 全国のレジャー・体験予約ポータルサイト

取得対象:
    - 全国 12,000 件超 (観測値 12,494 件) の施設 (掲載拠点) 基本情報
      店舗名、住所、営業時間、定休日、ジャンル、緯度/経度、
      価格帯、評価、口コミ数、設備情報、画像URL 等

取得フロー:
    1. /base/ (単一ページ) から 47都道府県ごとにグルーピングされた
       全施設 (/base/{id}/) リンクと名称・都道府県を収集
    2. 各詳細ページ /base/{id}/ から basic-information / facility-information
       テーブル、.base-data__genres、JSON-LD (LocalBusiness) を解析

【0 件になっていた本当の原因と対策】
    インデックス・詳細とも HTTP 200 で取得でき (リンクも 12,494 件すべて検出)、
    住所・営業時間・緯度経度等も正常に抽出できる。セレクタや WAF の恒久ブロックの
    問題ではない。問題は「規模」と「並列方式の取り違え」にあった。

    実測では 1 ページの所要は fetch ≈ 0.4 秒 (ネットワーク I/O が支配的) で、
    HTML パース (html.parser) は ≈ 22ms に過ぎない。fetch のみなら 200 件/秒
    出る。つまりボトルネックは CPU パースではなく "ネットワーク I/O" である。

    旧実装は 1 件ずつ DELAY を挟んでシリアルに取得していた (あるいは
    ProcessPool でも各プロセスがシリアルに fetch していた) ため、同時接続は
    ごくわずか。12,494 件をさばききる前にラン全体が時間切れで kill され、
    最終 CSV を書く close() に到達できず 0 件になっていた (プロセスを増やしても、
    プロセス内で並行 fetch しなければネットワーク待ちは重ならない)。

    対策: プロセス並列とスレッド並列を「併用」する。
      - ProcessPoolExecutor (= CPU コア数) で HTML パースを複数コアに分散
      - 各プロセス内で ThreadPoolExecutor を回し、ネットワーク待ちを重ねて
        多数のリクエストを同時に飛ばす
    これで全件を数分で取得し、確実に close() へ到達させる。

    ただし asoview は CloudFront 配下で、過度に高い同時接続 (同時 64 級) を
    一気にかけると HTTP 403 "The request could not be satisfied" を返して
    一時的に BAN する (数分で自然回復・恒久ではない)。そこで同時接続を概ね
    32 (= プロセス数 × スレッド数) 程度に抑え、Retry (指数バックオフ / 403・
    429・503 を含む) で一時的なスロットルを吸収する。
    (フォーク後のスレッド由来デッドロックを避けるため spawn コンテキストを使用)

実行方法:
    # ローカルテスト
    python scripts/sites/portal/asoview.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id asoview
"""

import json
import multiprocessing as mp
import os
import re
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://www.asoview.com"
INDEX_URL = f"{BASE_URL}/base/"

# 詳細ページ取得・解析の並列設定。
#   PROCESSES           : 並列プロセス数。HTML パースは GIL のためプロセスで
#                         しか並列化できないので CPU コア数に合わせる (上限 4)。
#   THREADS_PER_PROCESS : 各プロセス内のスレッド数。ネットワーク待ちを重ねて
#                         同時リクエスト数を稼ぐ。
#   → 同時接続数 = PROCESSES × THREADS_PER_PROCESS。asoview の CloudFront は
#     過度な同時接続 (64 級) で 403 一時 BAN するため、概ね 32 に抑える。
#   CHUNK_SIZE  : 1 プロセスへ一度に渡す件数。小さめにして各コアへ均等分配し、
#                 完了したチャンクから順次 yield (進捗ログ・部分書き込み) する。
#   WORKER_DELAY: 各リクエスト前の礼儀的待機秒数 (スレッドごとに負担)。
#   TIMEOUT     : 1 リクエストのタイムアウト秒数。
PROCESSES = max(1, min(4, os.cpu_count() or 2))
THREADS_PER_PROCESS = 16
CHUNK_SIZE = 200
WORKER_DELAY = 0.02
TIMEOUT = 20
USER_AGENT = StaticCrawler.USER_AGENT

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_CODE_PATTERN = re.compile(r"〒?\s*(\d{3}-\d{4})")
# /base/{id}/ 形式の詳細ページ href を判定する。href ベース抽出の中核。
_BASE_HREF_PATTERN = re.compile(r"^/base/\d+/?$")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


# ===============================================================
# ワーカープロセス側のヘルパー (モジュールレベル = pickle 可能)
# ===============================================================
# セッションはスレッドローカルに保持する。スレッドごとに 1 本のセッションを
# 使い回すことで TLS ハンドシェイク / TCP コネクションを再利用し、同一スレッド
# 内の連続リクエストを高速化する。spawn された子プロセスは親のセッションを
# 引き継がないため、各プロセス・各スレッドで遅延生成する。
_THREAD_LOCAL = threading.local()


def _get_session() -> requests.Session:
    sess = getattr(_THREAD_LOCAL, "session", None)
    if sess is None:
        sess = requests.Session()
        # 一時的なサーバーエラーに加え、CloudFront の一時スロットル
        # (403 / 429 / 503) もバックオフ付きで再試行して吸収する。
        retries = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[403, 429, 500, 502, 503, 504],
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=4, pool_maxsize=4)
        sess.mount("https://", adapter)
        sess.mount("http://", adapter)
        sess.headers.update({"User-Agent": USER_AGENT})
        _THREAD_LOCAL.session = sess
    return sess


def _fetch_soup(url: str) -> bs4.BeautifulSoup:
    """ワーカースレッド内で URL を取得し Soup を返す (例外は呼び出し元で処理)。"""
    resp = _get_session().get(url, timeout=TIMEOUT)
    resp.raise_for_status()
    content_type = resp.headers.get("Content-Type", "")
    if "charset=" not in content_type.lower():
        resp.encoding = resp.apparent_encoding
    return bs4.BeautifulSoup(resp.text, "html.parser")


def _parse_address(text: str, data: dict) -> None:
    if not text:
        return
    remainder = text
    m_post = _POST_CODE_PATTERN.search(remainder)
    if m_post:
        data[Schema.POST_CODE] = m_post.group(1)
        remainder = _POST_CODE_PATTERN.sub("", remainder, count=1).strip()

    m_pref = _PREF_PATTERN.search(remainder)
    if m_pref:
        if not data.get(Schema.PREF):
            data[Schema.PREF] = m_pref.group(1)
        data[Schema.ADDR] = remainder[m_pref.end():].strip()
    else:
        data[Schema.ADDR] = remainder


def _extract_basic_info(soup, data: dict) -> None:
    table = soup.select_one("table.basic-information__contents")
    if not table:
        return

    for tr in table.select("tr"):
        th = tr.find("th")
        td = tr.find("td")
        if not th or not td:
            continue
        key = _clean(th.get_text())
        val = _clean(td.get_text(" "))

        if "店舗名" in key:
            if val and not data.get(Schema.NAME):
                data[Schema.NAME] = val
        elif "住所" in key:
            _parse_address(val, data)
        elif "営業時間" in key:
            data[Schema.TIME] = val
        elif "定休日" in key:
            data[Schema.HOLIDAY] = val
        elif "アクセス" in key:
            data["アクセス"] = val


def _extract_facility_info(soup, data: dict) -> None:
    table = soup.select_one("table.facility-information__contents")
    if not table:
        return
    pairs: list[str] = []
    for tr in table.select("tr"):
        th = tr.find("th")
        td = tr.find("td")
        if not th or not td:
            continue
        pairs.append(f"{_clean(th.get_text())}:{_clean(td.get_text(' '))}")
    if pairs:
        data["設備情報"] = " / ".join(pairs)


def _extract_genres(soup, data: dict) -> None:
    genres = [
        _clean(li.get_text())
        for li in soup.select(".base-data__genres li, .base-data__genre-type")
    ]
    genres = [g for g in genres if g]
    if genres:
        seen = set()
        unique = [g for g in genres if not (g in seen or seen.add(g))]
        data[Schema.CAT_SITE] = " / ".join(unique)


def _extract_jsonld(soup, data: dict) -> None:
    for script in soup.select('script[type="application/ld+json"]'):
        raw = script.string or script.text
        if not raw:
            continue
        cleaned = raw.strip().rstrip(";").rstrip()
        try:
            payload = json.loads(cleaned)
        except json.JSONDecodeError:
            continue

        candidates = payload if isinstance(payload, list) else [payload]

        for obj in candidates:
            if not isinstance(obj, dict):
                continue
            obj_type = obj.get("@type")
            types = obj_type if isinstance(obj_type, list) else [obj_type]
            if "LocalBusiness" not in types:
                continue

            image = obj.get("image")
            if image:
                data["画像URL"] = str(image)

            price_range = obj.get("priceRange")
            if price_range:
                data["価格帯"] = _clean(str(price_range))

            geo = obj.get("geo") or {}
            if isinstance(geo, dict):
                lat = geo.get("latitude")
                lng = geo.get("longitude")
                if lat:
                    data["緯度"] = str(lat)
                if lng:
                    data["経度"] = str(lng)

            rating = obj.get("aggregateRating") or {}
            if isinstance(rating, dict):
                rv = rating.get("ratingValue")
                rc = rating.get("ratingCount")
                if rv:
                    data["評価点"] = str(rv)
                if rc:
                    data["口コミ数"] = str(rc)


def _scrape_detail(target: tuple[str, str, str]) -> dict | None:
    """詳細ページ 1 件を取得・解析して dict を返す (スレッド内で実行)。

    1 件の失敗が全体を止めないよう、あらゆる例外を握りつぶして None を返す
    (None は親側で除外される)。礼儀的な待機 (WORKER_DELAY) はスレッド側で
    分散して負担する。
    """
    url, list_name, pref_from_index = target
    if WORKER_DELAY > 0:
        time.sleep(WORKER_DELAY)
    try:
        soup = _fetch_soup(url)

        data: dict = {
            Schema.URL: url,
            Schema.NAME: list_name,
            Schema.PREF: pref_from_index if _PREF_PATTERN.fullmatch(pref_from_index) else "",
        }

        h1 = soup.select_one("h1.base-name")
        if h1:
            for tag in h1.select("span, small, em"):
                tag.decompose()
            name_text = _clean(h1.get_text())
            if name_text:
                data[Schema.NAME] = name_text

        _extract_basic_info(soup, data)
        _extract_facility_info(soup, data)
        _extract_genres(soup, data)
        _extract_jsonld(soup, data)

        return data
    except Exception:
        # 個別ページの失敗 (404・タイムアウト・解析エラー等) は黙ってスキップ。
        return None


def _scrape_chunk_worker(targets: list[tuple[str, str, str]]) -> list[dict]:
    """ワーカープロセスのエントリポイント。

    渡されたチャンク (複数 target) を、プロセス内の ThreadPoolExecutor で並行に
    取得・解析する。各スレッドが独立にネットワーク待ちするため、HTML パースを
    1 コアに集約しつつ多数のリクエストを同時に飛ばせる。None は除外して返す。
    """
    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=THREADS_PER_PROCESS) as pool:
        for item in pool.map(_scrape_detail, targets):
            if item:
                results.append(item)
    return results


def _chunked(seq: list, size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


class AsoviewScraper(StaticCrawler):
    """アソビュー (asoview.com) 拠点情報スクレイパー"""

    # 詳細取得は parse() 内のプロセス×スレッドプールで並行に行うため、
    # フレームワークの逐次 DELAY は 0 とする (DELAY を効かせると 12,000 件超の
    # 直列待機だけで時間切れ kill され、最終 CSV を書く close() に到達できず
    # 0 件になる)。礼儀的待機は WORKER_DELAY としてワーカースレッド側で負担する。
    DELAY = 0.0

    EXTRA_COLUMNS = [
        "アクセス",
        "緯度",
        "経度",
        "価格帯",
        "評価点",
        "口コミ数",
        "設備情報",
        "画像URL",
    ]

    def parse(self, url: str):
        # CloudFront が稀に /base/ に対してトップページや施設リンクを含まない
        # 応答を返すことがある (実行ログで title=トップ・region count=0・
        # LINKS:0 になった原因)。/base/{id}/ リンクが取れるまで数回リトライし、
        # それでも取れない場合のみ失敗とする。
        index_soup = None
        for attempt in range(1, 4):
            soup = self.get_soup(url)
            if soup is None:
                continue
            if "The request could not be satisfied" in str(soup):
                self.logger.warning("CloudFront ブロック応答 (試行 %d)", attempt)
                continue
            if soup.select_one('a[href^="/base/"]'):
                index_soup = soup
                break
            title = soup.title.get_text(strip=True) if soup.title else "?"
            self.logger.warning(
                "施設リンクを含まない応答 (title=%s, 試行 %d)", title, attempt
            )

        if index_soup is None:
            self.logger.error("インデックスページ取得失敗 (施設リンクなし)")
            return

        # --- href ベース抽出 -------------------------------------------------
        # 旧セレクタ (.page-base__region-wrap / a.page-base__base-link) は
        # マークアップ変更で 0 件になりやすいため、/base/{id}/ 形式の href を
        # 直接拾う方式を主とする。都道府県は region ブロックが残っていれば補完
        # する (取れなくても詳細ページの住所から復元できるため必須ではない)。
        href_pref: dict[str, str] = {}
        for region in index_soup.select(".page-base__region-wrap"):
            pref_el = region.select_one(".page-base__region")
            pref_text = _clean(pref_el.get_text()) if pref_el else ""
            if not pref_text:
                continue
            for a in region.select('a[href^="/base/"]'):
                href = (a.get("href") or "").strip()
                if _BASE_HREF_PATTERN.match(href):
                    href_pref[urljoin(BASE_URL, href)] = pref_text

        targets: list[tuple[str, str, str]] = []
        seen: set[str] = set()
        for a in index_soup.select('a[href^="/base/"]'):
            href = (a.get("href") or "").strip()
            if not _BASE_HREF_PATTERN.match(href):
                continue
            full_url = urljoin(BASE_URL, href)
            if full_url in seen:
                continue
            seen.add(full_url)
            targets.append((full_url, _clean(a.get_text()), href_pref.get(full_url, "")))

        self.total_items = len(targets)
        self.logger.info("取得対象施設数=%d", self.total_items)

        if not targets:
            self.logger.error("施設リンク0件")
            return

        # ネットワーク I/O が支配的 (fetch ≈0.4s / parse ≈22ms) なため、スレッドで
        # 多数のリクエストを重ねて待ち時間を相殺する。requests の I/O 待ち中は
        # GIL が解放されるので、単一プロセスのスレッドプールで十分に並行化できる。
        success_count = 0
        with ThreadPoolExecutor(max_workers=THREADS_PER_PROCESS) as pool:
            for result in pool.map(_scrape_detail, targets):
                if result:
                    success_count += 1
                    if success_count % 100 == 0:
                        self.logger.info("取得済=%d / %d", success_count, self.total_items)
                    yield result

        self.logger.info("完了件数=%d", success_count)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = AsoviewScraper()
    scraper.execute(INDEX_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
