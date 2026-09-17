# -*- coding: utf-8 -*-
"""
LIFULL HOME'S (ライフルホームズ) — 不動産会社[不動産屋]の検索

対象サイト: https://www.homes.co.jp/realtor/

取得対象:
    - 全国の不動産会社(店舗)の会社情報
      (会社名・カナ・所在地・TEL・FAX・営業時間・定休日・HP・免許番号・
       交通・所属団体名・保証協会・取引物件・特徴)

取得フロー (全件 = 都道府県ごとに探索):
    ルート(/realtor/) を起点に
      → 都道府県の店舗一覧 /realtor/{pref}/list/ を ?page=N でページ送り
        (1ページ30件。div.mod-realtorBuilding が1店舗ぶんのカード)
      → カードから 会社名・所在地・交通 を取り出して 1 レコードを組み立てる
        (この時点で 1 件として成立する = 1 通信で30件)
      → 各店舗詳細 /realtor/mid-XXXX/ を WORKERS 本で並行取得し、カナ・TEL・FAX・
        営業時間・免許番号などを上書きマージして完了ぶんから即 yield
        (mid-ID で全国グローバル重複排除)

★ AWS WAF (本サイト最大の制約) と、その突破方法:
    homes.co.jp は AWS WAF を使っており、aws-waf-token クッキーを持たない素の
    HTTP クライアントは **1 リクエスト目から** HTTP 202 + JS チャレンジ /
    CAPTCHA ページ (本文に awsWafCookieDomainList / gokuProps /
    Human Verification を含む) を返す。requests だけでは何度待って再試行しても
    永久に 202 のままで 1 件も取得できない (旧実装が 0 件だった真因)。

    そこで本クローラーは
      1. 最初の 1 回だけ Playwright(Chromium ヘッドレス) でルート URL を開き、
         JS チャレンジを実際に実行させて aws-waf-token を発行させる
         (自動化検知を避けるため --disable-blink-features=AutomationControlled と
          navigator.webdriver の除去が必須。無いと CAPTCHA 画面で止まる)
      2. 取得したクッキーを requests セッションへ移植する
      3. 以降の一覧・詳細ページは requests で高速・並行取得する
         (トークンが効いている間は 200 が返る。実測で移植後は全ページ 200)
      4. トークンが失効して再び 202 になったら 1. をやり直す
    という「ブラウザでトークンだけ取り、あとは静的取得」方式を採る。
    ブラウザは毎回その場で起動・クローズするのでリソースを抱え込まない。

実行方法:
    # ローカルテスト
    python scripts/sites/realestate/lifull_home_s.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id lifull_home_s
"""

from __future__ import annotations

import re
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from pathlib import Path
from typing import Generator, List, Optional, Sequence, Tuple
from urllib.parse import urljoin

import bs4
import requests

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 47 都道府県 (homes.co.jp の romaji スラッグ, 表示名)。
# ルートページにはこの他に地方ブロック (kanto, kinki 等) のリンクも含まれるため、
# 確実な 47 件のみを明示する (全件 = 都道府県ごとに探索)。
PREFS = [
    ("hokkaido", "北海道"), ("aomori", "青森県"), ("iwate", "岩手県"),
    ("miyagi", "宮城県"), ("akita", "秋田県"), ("yamagata", "山形県"),
    ("fukushima", "福島県"), ("ibaraki", "茨城県"), ("tochigi", "栃木県"),
    ("gunma", "群馬県"), ("saitama", "埼玉県"), ("chiba", "千葉県"),
    ("tokyo", "東京都"), ("kanagawa", "神奈川県"), ("niigata", "新潟県"),
    ("toyama", "富山県"), ("ishikawa", "石川県"), ("fukui", "福井県"),
    ("yamanashi", "山梨県"), ("nagano", "長野県"), ("gifu", "岐阜県"),
    ("shizuoka", "静岡県"), ("aichi", "愛知県"), ("mie", "三重県"),
    ("shiga", "滋賀県"), ("kyoto", "京都府"), ("osaka", "大阪府"),
    ("hyogo", "兵庫県"), ("nara", "奈良県"), ("wakayama", "和歌山県"),
    ("tottori", "鳥取県"), ("shimane", "島根県"), ("okayama", "岡山県"),
    ("hiroshima", "広島県"), ("yamaguchi", "山口県"), ("tokushima", "徳島県"),
    ("kagawa", "香川県"), ("ehime", "愛媛県"), ("kochi", "高知県"),
    ("fukuoka", "福岡県"), ("saga", "佐賀県"), ("nagasaki", "長崎県"),
    ("kumamoto", "熊本県"), ("oita", "大分県"), ("miyazaki", "宮崎県"),
    ("kagoshima", "鹿児島県"), ("okinawa", "沖縄県"),
]

# 店舗詳細(ルート)URL: /realtor/mid-XXXX/ のみ。/bukken/ /coupon/ /map/ 等の下層は除外。
_RE_DETAIL = re.compile(r"/realtor/(mid-[A-Za-z0-9_]+)/?$")
_RE_POST = re.compile(r"〒?\s*(\d{3}-?\d{4})")
# AWS WAF のチャレンジ / CAPTCHA ページの目印
_WAF_MARKERS = ("awsWafCookieDomainList", "gokuProps", "Human Verification")

# ヘッドレス Chromium の自動化検知を避けるための起動引数。
# --disable-blink-features=AutomationControlled が無いと WAF が CAPTCHA 画面
# ("Human Verification") を出してしまい、JS チャレンジが自動で解けない。
_BROWSER_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-dev-shm-usage",
]
# navigator.webdriver を隠す (同上)
_STEALTH_JS = "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"

# 詳細ページの table.table-def から拾うラベル → 出力カラム
_DETAIL_FIELDS = [
    ("TEL", Schema.TEL),
    ("営業時間", Schema.TIME),
    ("定休日", Schema.HOLIDAY),
    ("交通", "交通"),
    ("FAX", "FAX"),
    ("免許番号", "免許番号"),
    ("所属団体名", "所属団体名"),
    ("保証協会", "保証協会"),
    ("取引物件（賃貸）", "取引物件（賃貸）"),
    ("取引物件（売買）", "取引物件（売買）"),
    ("特徴", "特徴"),
]


class _WafChallenge(requests.exceptions.RequestException):
    """AWS WAF のチャレンジ/CAPTCHA に阻まれた (通常の通信エラーと区別する)。"""


class LifullHomeSScraper(StaticCrawler):
    """LIFULL HOME'S 不動産会社検索 スクレイパー"""

    # 流量調整は REQ_INTERVAL のスロットルで行うため、アイテム間ウェイトは持たせない。
    DELAY = 0.0
    TIMEOUT = 20
    # 詳細ページの並行取得数
    WORKERS = 3
    # リクエスト開始間隔の下限(秒)。全スレッド共通のスロットル。
    REQ_INTERVAL = 0.35
    # 一覧ページ(1通信=30件)は粘る。202 を踏んだときのクールダウン秒。
    LIST_RETRIES = 4
    LIST_COOLDOWN = (1.0, 2.0, 4.0, 8.0)
    # 詳細ページは 2 回まで (カード情報だけでもレコードは成立するため深追いしない)
    DETAIL_RETRIES = 2
    DETAIL_COOLDOWN = (2.0, 5.0)
    # 詳細が WAF に当たり続けたら、この秒数は詳細取得自体を止める
    DETAIL_BREAK = 30.0
    # ブラウザで JS チャレンジが解けるのを待つ上限(秒)
    WAF_SOLVE_TIMEOUT = 40.0
    # トークン取得直後に他スレッドが重ねて取り直すのを防ぐ猶予(秒)
    TOKEN_GRACE = 8.0
    # 1 都道府県あたりの一覧ページ上限 (無限ループ防止。1ページ30件)
    MAX_PAGES = 400

    # 現行 Chrome 相当の UA。ブラウザ側と完全に一致させないとトークンが弾かれる。
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )

    EXTRA_COLUMNS = [
        "交通",
        "FAX",
        "免許番号",
        "所属団体名",
        "保証協会",
        "取引物件（賃貸）",
        "取引物件（売買）",
        "特徴",
    ]

    # 並行取得まわりの共有状態 (基盤の __init__ はオーバーライド禁止のためクラス属性で持つ)
    _net_lock = threading.Lock()
    _waf_lock = threading.Lock()
    _next_request_at = 0.0    # 次にリクエストしてよい時刻 (monotonic)
    _detail_open_at = 0.0     # 詳細取得を再開してよい時刻 (monotonic)
    _token_at = 0.0           # 最後に aws-waf-token を取得した時刻 (0 = 未取得)
    _root_url = ""            # parse() が受け取ったルート URL (チャレンジ突破先)

    # ------------------------------------------------------------------ #
    # 通信 (WAF トークン取得 + スロットル)
    # ------------------------------------------------------------------ #
    def _setup(self):
        """標準セッションに加え、ブラウザ相当のヘッダを付与する。"""
        super()._setup()
        self.session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            "Upgrade-Insecure-Requests": "1",
        })
        # 並行取得ぶんのコネクションをプールしておく (毎回ハンドシェイクしない)
        for adapter in self.session.adapters.values():
            try:
                adapter.poolmanager.connection_pool_kw["maxsize"] = self.WORKERS + 2
            except AttributeError:
                pass

    def _throttle(self, extra_wait: float = 0.0):
        """全スレッド共通でリクエスト間隔を空ける。extra_wait は WAF クールダウン。"""
        with self._net_lock:
            start_at = max(time.monotonic(), self._next_request_at) + extra_wait
            self._next_request_at = start_at + self.REQ_INTERVAL
        delay = start_at - time.monotonic()
        if delay > 0:
            time.sleep(delay)

    def _ensure_token(self, url: str, force: bool = False) -> bool:
        """aws-waf-token を確保する。未取得 or 失効時のみブラウザを起動する。

        Args:
            url: チャレンジを踏ませる URL (ルート URL を優先して使う)
            force: 202 を踏んだ後の取り直し
        """
        with self._waf_lock:
            now = time.monotonic()
            if self._token_at:
                # 直近に他スレッドが取り直したばかりなら、そのトークンを使う
                if not force or now - self._token_at < self.TOKEN_GRACE:
                    return True
            return self._solve_challenge(self._root_url or url)

    def _solve_challenge(self, url: str) -> bool:
        """Playwright で JS チャレンジを実行させ、クッキーを requests に移植する。

        呼び出し元 (_ensure_token) が _waf_lock を保持している前提。
        """
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            self.logger.error("playwright が利用できないため AWS WAF を突破できません")
            return False

        self.logger.info("AWS WAF のチャレンジをブラウザで突破します: %s", url)
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True, args=_BROWSER_ARGS)
                try:
                    context = browser.new_context(
                        user_agent=self.USER_AGENT,
                        locale="ja-JP",
                        viewport={"width": 1440, "height": 900},
                        extra_http_headers={"Accept-Language": "ja,en-US;q=0.9,en;q=0.8"},
                    )
                    context.add_init_script(_STEALTH_JS)
                    page = context.new_page()
                    page.goto(url, wait_until="domcontentloaded", timeout=45000)
                    # チャレンジ JS が走り切る (= 目印が消える) まで待つ
                    deadline = time.monotonic() + self.WAF_SOLVE_TIMEOUT
                    while time.monotonic() < deadline:
                        html = page.content()
                        if not any(m in html for m in _WAF_MARKERS):
                            break
                        page.wait_for_timeout(1000)
                    else:
                        self.logger.warning("WAF チャレンジが時間内に解けませんでした")
                        return False
                    cookies = context.cookies()
                finally:
                    browser.close()
        except Exception as e:   # ブラウザ起動失敗等でクロール全体を落とさない
            self.logger.warning("WAF 突破に失敗しました: %s", e)
            return False

        got = False
        for ck in cookies:
            try:
                self.session.cookies.set(
                    ck["name"], ck["value"],
                    domain=ck.get("domain", ""), path=ck.get("path", "/"))
            except Exception:
                continue
            if ck["name"] == "aws-waf-token":
                got = True
        LifullHomeSScraper._token_at = time.monotonic()
        self.logger.info("WAF トークンを取得しました (aws-waf-token: %s)",
                         "あり" if got else "なし")
        return True

    def get_soup(self, url: str, retries: Optional[int] = None,
                 cooldowns: Sequence[float] = ()) -> Optional[bs4.BeautifulSoup]:
        """HTML を取得する。202 チャレンジを検知したらトークンを取り直して再試行する。

        StaticCrawler.get_soup は 202 を成功扱い(raise_for_status 非該当)にしてしまうため、
        ここで 202 / チャレンジ本文 / 空ボディを明示的に検知する。retries 回まで
        再試行し、駄目なら _WafChallenge を送出する。
        """
        attempts = self.LIST_RETRIES if retries is None else retries
        waits = tuple(cooldowns) or self.LIST_COOLDOWN

        def _fetch() -> str:
            cooldown = 0.0
            for attempt in range(attempts):
                # 未取得ならここでブラウザを起動してトークンを作る (初回のみ)
                self._ensure_token(url, force=False)
                self._throttle(cooldown)
                resp = self.session.get(url, timeout=self.TIMEOUT)
                body = resp.text
                if resp.status_code == 202 or not body.strip() \
                        or any(m in body[:4000] for m in _WAF_MARKERS):
                    cooldown = waits[min(attempt, len(waits) - 1)]
                    self.logger.warning(
                        "WAF 応答 (HTTP %s) — トークンを取り直して再試行 %d/%d: %s",
                        resp.status_code, attempt + 1, attempts, url)
                    # トークン失効。取り直してから次の試行へ
                    self._ensure_token(url, force=True)
                    continue
                resp.raise_for_status()
                if "charset=" not in resp.headers.get("Content-Type", "").lower():
                    resp.encoding = resp.apparent_encoding
                return resp.text
            raise _WafChallenge(f"WAF により取得不能: {url}")

        try:
            html = self._fetch_html_cached(url, variant="", fetcher=_fetch)
            return bs4.BeautifulSoup(html, "html.parser")
        except _WafChallenge as e:
            self.error_count += 1
            self.logger.warning("WAF で取得できず: %s", e)
            raise
        except requests.exceptions.RequestException as e:
            self.error_count += 1
            self.logger.warning("取得失敗 (スキップして継続): %s — %s", url, e)
            return None

    # ------------------------------------------------------------------ #
    # 一覧 → 詳細
    # ------------------------------------------------------------------ #
    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url を唯一のルート(SSOT)として使う。末尾スラッシュを保証。
        root = url if url.endswith("/") else url + "/"
        # WAF チャレンジもこのルート URL に対して踏ませる
        LifullHomeSScraper._root_url = root
        seen: set[str] = set()

        for slug, pref_ja in PREFS:
            # 都道府県の全店舗一覧 (市区町村をたどらなくても全件ページ送りできる)
            list_url = urljoin(root, f"{slug}/list/")
            yield from self._crawl_pref(list_url, pref_ja, seen)

    def _crawl_pref(self, list_url: str, pref_ja: str,
                    seen: set) -> Generator[dict, None, None]:
        """1 都道府県の店舗一覧を ?page=N で巡回し、カード + 詳細で yield。"""
        prev_ids: Optional[List[str]] = None
        for page in range(1, self.MAX_PAGES + 1):
            page_url = list_url if page == 1 else f"{list_url}?page={page}"
            try:
                soup = self.get_soup(page_url)
            except _WafChallenge:
                break   # この都道府県は諦めて次へ (次の県で通ることがある)
            if soup is None:
                break

            cards = self._extract_cards(soup, page_url)
            if not cards:
                break
            ids = [c["id"] for c in cards]
            # ページ番号がクランプされ同じページが返ってきたら終了。
            if ids == prev_ids:
                break
            prev_ids = ids

            targets = [c for c in cards if c["id"] not in seen]
            seen.update(ids)
            self.logger.info("%s %dページ目: 店舗 %d 件 (新規 %d 件)",
                             pref_ja, page, len(cards), len(targets))
            yield from self._fetch_details(targets, pref_ja)

    def _extract_cards(self, soup: bs4.BeautifulSoup, base_url: str) -> List[dict]:
        """一覧ページの店舗カード (div.mod-realtorBuilding) から基本情報を取り出す。

        カードが取れない(レイアウト変更等)場合も、詳細リンクだけは拾って
        詳細ページ側から情報を取れるようにフォールバックする。
        """
        cards: List[dict] = []
        found: set[str] = set()

        for box in soup.select("div.mod-realtorBuilding"):
            mid = url = ""
            for link in box.select("a[href]"):
                href = link["href"].split("#")[0].split("?")[0]
                m = _RE_DETAIL.search(href)
                if m:
                    mid, url = m.group(1), urljoin(base_url, href)
                    break
            if not mid or mid in found:
                continue
            found.add(mid)

            name_el = box.select_one(".realtorName")
            spec: dict[str, str] = {}
            for tr in box.select(".realtorSpec tr"):
                th, td = tr.find("th"), tr.find("td")
                if th and td:
                    spec.setdefault(self._clean(th.get_text(strip=True)),
                                    self._clean(td.get_text(" ", strip=True)))
            cards.append({
                "id": mid,
                "url": url,
                "name": self._clean(name_el.get_text(" ", strip=True)) if name_el else "",
                "addr": spec.get("所在地", ""),
                "access": spec.get("交通", ""),
            })

        if cards:
            return cards

        # フォールバック: カードが無ければ詳細リンクだけを拾う
        for link in soup.select("a[href]"):
            href = link["href"].split("#")[0].split("?")[0]
            m = _RE_DETAIL.search(href)
            if not m or m.group(1) in found:
                continue
            found.add(m.group(1))
            cards.append({"id": m.group(1), "url": urljoin(base_url, href),
                          "name": "", "addr": "", "access": ""})
        return cards

    def _fetch_details(self, cards: Sequence[dict],
                       pref_ja: str) -> Generator[dict, None, None]:
        """カードごとにレコードを組み立てる。詳細ページは WORKERS 本で並行取得。"""
        def safe(card: dict) -> Optional[dict]:
            try:
                return self._build_item(card, pref_ja)
            except Exception as e:  # 1 店舗の失敗で全体を止めない
                self.logger.warning("詳細取得エラー: %s — %s", card["url"], e)
                return None

        with ThreadPoolExecutor(max_workers=self.WORKERS) as pool:
            pending: set = set()
            for card in cards:
                pending.add(pool.submit(safe, card))
                # in-flight を抑え、完了分を先に流す (メモリ・進捗の両面で有利)
                if len(pending) >= self.WORKERS * 2:
                    done, pending = wait(pending, return_when=FIRST_COMPLETED)
                    for f in done:
                        item = f.result()
                        if item:
                            yield item
            for f in as_completed(pending):
                item = f.result()
                if item:
                    yield item

    def _build_item(self, card: dict, pref_ja: str) -> Optional[dict]:
        """一覧カードの情報を土台に、詳細ページの情報を上書きして 1 レコードにする。"""
        post_code, addr = self._split_address(card["addr"])
        item = {
            Schema.URL: card["url"],
            Schema.NAME: card["name"],
            Schema.NAME_KANA: "",
            Schema.PREF: pref_ja,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: "",
            Schema.TIME: "",
            Schema.HOLIDAY: "",
            Schema.HP: "",
            "交通": card["access"],
            "FAX": "",
            "免許番号": "",
            "所属団体名": "",
            "保証協会": "",
            "取引物件（賃貸）": "",
            "取引物件（売買）": "",
            "特徴": "",
        }

        detail = self._scrape_detail(card["url"])
        if detail:
            for key, value in detail.items():
                if value:
                    item[key] = value
        elif not item[Schema.NAME]:
            return None   # 一覧にも詳細にも名前が無いレコードは破棄
        return item

    def _scrape_detail(self, url: str) -> Optional[dict]:
        """詳細ページから会社情報を取り出す (WAF 等で取れなければ None)。

        WAF に当たり続けたら DETAIL_BREAK 秒は詳細取得を止める (サーキットブレーカー)。
        止まっている間は一覧カード由来の情報だけでレコードを作る。
        """
        if time.monotonic() < self._detail_open_at:
            return None
        try:
            soup = self.get_soup(url, retries=self.DETAIL_RETRIES,
                                 cooldowns=self.DETAIL_COOLDOWN)
        except _WafChallenge:
            with self._net_lock:
                LifullHomeSScraper._detail_open_at = time.monotonic() + self.DETAIL_BREAK
            self.logger.warning("WAF のため詳細取得を %.0f 秒休止します (一覧の情報のみで継続)",
                                self.DETAIL_BREAK)
            return None
        if soup is None:
            return None

        # table.table-def の th→td を辞書化。
        data: dict[str, bs4.Tag] = {}
        for tbl in soup.select("table.table-def"):
            for tr in tbl.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if th and td:
                    data.setdefault(th.get_text(strip=True), td)

        detail: dict[str, str] = {}
        for label, column in _DETAIL_FIELDS:
            td = data.get(label)
            if td:
                detail[column] = self._clean(td.get_text(" ", strip=True))

        # 会社名 + カナ (ruby/rt)
        name_p = soup.select_one("p.realtorName")
        if name_p:
            rt = name_p.find("rt")
            if rt:
                detail[Schema.NAME_KANA] = self._clean(rt.get_text(" ", strip=True))
                rt.extract()   # rt を除去すると残りが基底テキスト
            detail[Schema.NAME] = self._clean(name_p.get_text(" ", strip=True))

        # HP リンク
        hp_a = soup.select_one("p.forOfficialSiteLink a[href]")
        if hp_a:
            detail[Schema.HP] = hp_a["href"].strip()

        # 所在地 → 郵便番号 / 住所 ("地図を見る" 等のリンク文言を除去)
        addr_td = data.get("所在地")
        if addr_td:
            td = bs4.BeautifulSoup(str(addr_td), "html.parser")
            for a in td.select("a"):
                a.extract()
            post_code, addr = self._split_address(self._clean(td.get_text(" ", strip=True)))
            detail[Schema.POST_CODE] = post_code
            detail[Schema.ADDR] = addr

        return detail or None

    @classmethod
    def _split_address(cls, raw: str) -> Tuple[str, str]:
        """住所文字列を (郵便番号, 郵便番号を除いた住所) に分ける。"""
        if not raw:
            return "", ""
        m = _RE_POST.search(raw)
        if not m:
            return "", raw
        return m.group(1), cls._clean(raw[m.end():])

    @staticmethod
    def _clean(s: Optional[str]) -> str:
        if not s:
            return ""
        s = s.replace("　", " ")
        return re.sub(r"\s+", " ", s).strip()


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = LifullHomeSScraper()
    # 🔒 sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.homes.co.jp/realtor/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
