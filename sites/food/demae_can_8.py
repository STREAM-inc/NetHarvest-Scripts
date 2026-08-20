"""
出前館 (demae-can) — フードデリバリーポータル 店舗スクレイパー
(demae_can_8 / 起点 = /shopDetail)

取得対象 (店舗詳細ページ /shopDetail/{id} を一次ソースに構造化情報のみ抽出):
    - 名称 / 都道府県 / 郵便番号 / 住所 / TEL / 業種・ジャンル /
      定休日 / 営業時間 / 支払い方法 / 説明(キャッチコピー) / 取得URL
    - EXTRA: テイクアウト可 / 配達形態 / 最低注文金額 / 店舗ID / 取得経路

配達形態の取得:
    出前館の店舗詳細ページ本文には「お店がお届け / 出前館がお届け」の文言は無いが、
    Next.js の埋め込み状態 (__NEXT_DATA__ → initialApolloState.ROOT_QUERY.
    shopDetailResult(...)) に **isShareDeli** (シェアリングデリバリー = 出前館スタッフが
    配達するか) の真偽値が入っている。
        isShareDeli == False → 店舗自身が配達 = 「お店がお届け」(自社配達)
        isShareDeli == True  → シェアリングデリバリー = 「出前館がお届け」
        フラグ自体が無い     → 配達形態を判定できない
    スクレイピング段階では配達形態による除外を行わず、判定値を保持して全件 yield する。
    「自社配達のみ」などの案件条件は取得後の成型工程で適用する。

🔒 URL 一貫性 (SSOT = sites.yml の url):
    起点 url = https://demae-can.com/shopDetail (= sites.yml の url / __main__ の execute 引数)。
    店舗詳細 URL は f"{url}/{shop_id}" と url から派生させ、Schema.URL にもこの live 正規 URL を
    格納する。アーカイブ (Wayback) は取得経路にすぎず、ルート URL は変更しない。

取得経路 (live → アーカイブの二段構え):
    1. bundled Chromium (Google Chrome 非依存) で live の店舗詳細ページを 1 件プローブする。
       HTTP status / <title> / 本文長 / 遮断シグネチャを検査し、403・Access Denied・
       極端に短い遮断ページを「正常な 0 件」として扱わず ERROR ログに残す。
    2. live が遮断されている場合は Wayback Machine のスナップショットへフォールバックする。
       列挙は CDX API (demae-can.com/shopDetail/*)、本文は
       web.archive.org/web/{timestamp}id_/... の原本 HTML。
       live が通る環境ではそのまま live の HTML を使う (パーサは live/アーカイブ共通)。

    ※ demae-can.com は Akamai edge の IP ACL deny 配下で、この実行環境 (データセンター IP)
      からは "/" も "/robots.txt" も "/sitemap.xml.gz" も一律 403 "Access Denied" を返す。
      TLS/HTTP2 指紋やセンサーチャレンジではなく IP 起因のため、UA 偽装・--disable-http2・
      stealth 等では突破できない (2026-08-20 再検証済)。0 件はセレクタ原因ではない。

スモークテスト構成:
    SMOKE_SHOP_IDS (公開検索で「お店がお届け」と確認済みの店舗 ID) を一覧巡回より先に
    少数だけ取得して、詳細ページ取得系が生きているかを最初に確認する。
    本番はここで止まらず、そのまま CDX (または live 一覧) の全 ID 巡回へ進む。

実行方法:
    # ローカルテスト
    python scripts/sites/food/demae_can_8.py

    # スモークテスト
    python bin/smoke_test.py scripts/sites/food/demae_can_8.py \
        "https://demae-can.com/shopDetail" --limit 3 --timeout 60

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id demae_can_8
"""

import html as html_mod
import json
import re
import sys
import time
from pathlib import Path
from typing import Generator

import requests

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema


_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POSTCODE_PATTERN = re.compile(r"〒?\s*(\d{3}-\d{4})")
_TAKEOUT_PATTERN = re.compile(r"テイクアウト|お持ち帰り|お持帰り|持ち帰り|お受け取り|置き配")
_SHOP_ID_PATTERN = re.compile(r"/(?:shopDetail|shop/menu)/(\d+)")
_NEXT_DATA_PATTERN = re.compile(
    r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.S
)
_SHOP_DETAIL_KEY_PATTERN = re.compile(r"^shopDetailResult\(")

# live 側が WAF に遮断されたことを示すシグネチャ (status 200 で返るチャレンジ型も拾う)
_BLOCK_SIGNATURES = (
    "access denied",
    "errors.edgesuite.net",
    "reference #18.",
    "forbidden",
    "attention required",
    "just a moment",
    "checking your browser",
    "you don't have permission to access",
)
# 遮断ページはたいてい数百バイト。正常な店舗詳細は 200KB 超あるので十分な余裕を持たせる
_MIN_HTML_LENGTH = 3000

# 出前館のジャンルスラッグ → 日本語表記 (パンくずが取れない場合のフォールバック)
_GENRE_LABELS = {
    "pizza": "ピザ", "sushi": "寿司", "don": "丼もの", "curry": "カレー",
    "chinese": "中華", "hamburger": "ハンバーガー", "youshoku": "洋食",
    "box": "お弁当", "washoku": "和食", "ramen": "ラーメン", "cafe": "カフェ",
    "dessert": "スイーツ", "korean": "韓国料理", "italian": "イタリアン",
    "yakiniku": "焼肉", "bento": "お弁当", "drug": "ドラッグストア",
    "convenience": "コンビニ", "supermarket": "スーパー", "other": "その他",
}

# 公開検索で「お店がお届け」と確認済みの店舗 ID。
# 一覧巡回より前にここだけを少数取得し、詳細ページ取得系の生死を最初に確認する
# (本番はこの ID に限定せず、そのまま全 ID 巡回へ進む)。
SMOKE_SHOP_IDS = ("1007143",)

# Wayback Machine エンドポイント
_CDX_URL = "https://web.archive.org/cdx/search/cdx"
_WAYBACK_RAW = "https://web.archive.org/web/{ts}id_/https://demae-can.com/shopDetail/{sid}"
# CDX は古い (2019-20) レイアウトのスナップショットも返すが、そちらは Next.js 化前で
# isShareDeli を持たない = 配達形態を判定できない。近年のスナップショットに絞る。
_CDX_FROM = "20240101"
# 既知 ID スモーク時の CDX 時間制限 (本番の一覧巡回を待たせないため短くする)
_SMOKE_CDX_TIMEOUT = 12
_SMOKE_CDX_RETRIES = 1


class DemaeCanBlockedError(RuntimeError):
    """live / アーカイブの双方から店舗詳細を取得できなかった (WAF 遮断等) ことを示す。"""


class DemaeCan8Scraper(DynamicCrawler):
    """出前館 (demae-can) スクレイパー — /shopDetail 起点・全配達形態取得"""

    DELAY = 1.5
    CONTINUE_ON_ERROR = True

    # Google Chrome (channel="chrome") は backend の Dockerfile
    # (`playwright install --with-deps chromium`) に存在しないため使わない。
    # bundled Chromium のまま、HTTP/2 起因の遮断を避ける --disable-http2 を付けて起動する。
    _LAUNCH_ARGS = [
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-blink-features=AutomationControlled",
        "--disable-http2",
    ]
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
    _EXTRA_HEADERS = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
                  "image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "ja,ja-JP;q=0.9,en-US;q=0.8,en;q=0.7",
        "Upgrade-Insecure-Requests": "1",
        "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }

    # CDX の 1 ページあたり件数。初回は小さくして最初の yield を早める。
    _CDX_FIRST_PAGE = 60
    _CDX_PAGE = 500
    # 巡回する店舗数の上限 (None=無制限)
    SHOP_LIMIT: int | None = None

    EXTRA_COLUMNS = [
        "テイクアウト可",   # 記載があれば "可"
        "配達形態",        # 自社配達・シェアリングデリバリー・判定不可を保持
        "最低注文金額",
        "店舗ID",
        "取得経路",        # live / Wayback {timestamp}
    ]

    # ------------------------------------------------------------------ setup

    def _setup(self):
        """bundled Chromium (Google Chrome 非依存) と Wayback 用 HTTP セッションを起動する。"""
        from playwright.sync_api import sync_playwright

        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=True, args=self._LAUNCH_ARGS,
        )
        self.logger.info("ブラウザ起動: bundled Chromium (args=%s)", " ".join(self._LAUNCH_ARGS))
        self.context = self.browser.new_context(
            user_agent=self.USER_AGENT,
            locale="ja-JP",
            timezone_id="Asia/Tokyo",
            viewport={"width": 1366, "height": 900},
            extra_http_headers=self._EXTRA_HEADERS,
        )
        self.context.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
        )
        self.page = self.context.new_page()

        # Wayback (CDX / スナップショット) 用。self.session はスモークテストの
        # 通信ガードにラップされるので、時間切れ後の通信も止まる。
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.USER_AGENT,
            "Accept-Language": self._EXTRA_HEADERS["Accept-Language"],
        })

        self._live_ok: bool | None = None   # None=未判定 / True=live 可 / False=遮断
        self._archive_ok = False            # アーカイブから 1 件でも取れたか

    # ------------------------------------------------------- live (Chromium)

    @staticmethod
    def _looks_blocked(status: int | None, title: str, body: str) -> str:
        """遮断ページなら理由文字列を、正常なら "" を返す。"""
        if status is not None and status >= 400:
            return f"HTTP {status}"
        lowered = f"{title}\n{body[:4000]}".lower()
        for sig in _BLOCK_SIGNATURES:
            if sig in lowered:
                return f"遮断シグネチャ検出: {sig!r}"
        if len(body) < _MIN_HTML_LENGTH:
            return f"本文が短すぎる ({len(body)} 文字) — 遮断ページの可能性"
        return ""

    def _fetch_live_html(self, shop_url: str) -> str | None:
        """live の店舗詳細 HTML を取得する。遮断・失敗時は理由をログに残して None を返す。"""
        try:
            response = self.page.goto(shop_url, wait_until="domcontentloaded", timeout=30000)
        except Exception as exc:  # noqa: BLE001
            self.logger.error("live 取得エラー: %s — %s", shop_url, exc)
            return None

        status = response.status if response else None
        body = self.page.content()
        try:
            title = self.page.title()
        except Exception:  # noqa: BLE001
            title = ""

        reason = self._looks_blocked(status, title, body)
        if reason:
            # 「正常な 0 件」として黙殺しない。原因が判別できる形で残す。
            self.logger.error(
                "live 遮断を検出 (%s): url=%s status=%s title=%r body_len=%d snippet=%r",
                reason, shop_url, status, title, len(body),
                re.sub(r"\s+", " ", body)[:200],
            )
            return None

        self.logger.info("live 取得成功: %s (status=%s, %d 文字)", shop_url, status, len(body))
        return body

    # ----------------------------------------------------- Wayback (archive)

    def _get_with_retry(self, url: str, params: dict | None = None,
                        retries: int = 4, timeout: int = 90) -> requests.Response | None:
        """Wayback は一時的に 503 "No server is available" を返すため指数バックオフで再試行する。"""
        for attempt in range(1, retries + 1):
            try:
                res = self.session.get(url, params=params, timeout=timeout)
            except requests.RequestException as exc:
                self.logger.warning("Wayback 取得エラー (%d/%d): %s — %s", attempt, retries, url, exc)
                res = None
            else:
                if res.status_code == 200:
                    return res
                if res.status_code in (404, 403):
                    # そのスナップショットが欠落しているだけ。再試行しても無駄
                    self.logger.debug("Wayback %s: %s", res.status_code, url)
                    return None
                self.logger.debug("Wayback %s (%d/%d): %s", res.status_code, attempt, retries, url)
            if attempt < retries:
                time.sleep(min(0.6 * attempt, 3.0))
        return None

    def _cdx_rows(self, params: dict, retries: int = 3,
                  timeout: int = 90) -> list[list[str]]:
        """CDX API を叩き、[original, timestamp] 行のリストを返す (ヘッダ行と空行は除去)。"""
        res = self._get_with_retry(_CDX_URL, params=params, retries=retries, timeout=timeout)
        if res is None or not res.text.strip():
            return []
        try:
            raw = json.loads(res.text)
        except ValueError:
            self.logger.warning("CDX 応答を JSON として解釈できません: %r", res.text[:200])
            return []
        rows = []
        for row in raw:
            if not isinstance(row, list) or len(row) < 2:
                continue  # 空行 / resumeKey 行
            if row[0] == "original":
                continue  # ヘッダ行
            rows.append(row)
        return rows

    def _archive_snapshot(self, shop_id: str, retries: int = 3,
                          timeout: int = 90) -> tuple[str, str] | None:
        """指定 shop_id の最新スナップショット (timestamp, HTML) を返す。無ければ None。"""
        rows = self._cdx_rows({
            "url": f"demae-can.com/shopDetail/{shop_id}",
            "matchType": "prefix",
            "filter": "statuscode:200",
            "output": "json",
            "fl": "original,timestamp",
            "from": _CDX_FROM,
            "limit": 20,
        }, retries=retries, timeout=timeout)
        for _orig, ts in sorted(rows, key=lambda r: r[1], reverse=True)[:3]:
            res = self._get_with_retry(_WAYBACK_RAW.format(ts=ts, sid=shop_id))
            if res is not None and len(res.text) >= _MIN_HTML_LENGTH:
                return ts, res.text
        return None

    def _iter_archived_shop_ids(self) -> Generator[tuple[str, str], None, None]:
        """CDX を resumeKey でページ送りしながら (shop_id, timestamp) を遅延列挙する。

        全件を先に集めない (最初の 1 件を早く yield するため)。同一店舗の URL 表記ゆれ
        (?addressId= 付き等) はここでグローバル重複排除する。
        """
        params = {
            "url": "demae-can.com/shopDetail/*",
            "filter": "statuscode:200",
            "output": "json",
            "fl": "original,timestamp",
            "from": _CDX_FROM,
            "limit": self._CDX_FIRST_PAGE,
            "showResumeKey": "true",
        }
        seen: set[str] = set()
        while True:
            # collapse=urlkey はサーバ側の負荷が高く 504 を頻発させるため使わない。
            # 同一店舗の重複行 (?addressId= 付き等) はここで潰し、最新 timestamp を採る。
            res = self._get_with_retry(_CDX_URL, params=params)
            if res is None or not res.text.strip():
                return
            try:
                raw = json.loads(res.text)
            except ValueError:
                self.logger.warning("CDX 応答を JSON として解釈できません: %r", res.text[:200])
                return

            resume_key = ""
            latest: dict[str, str] = {}
            for row in raw:
                if not isinstance(row, list) or not row:
                    continue
                if len(row) == 1:
                    resume_key = row[0]
                    continue
                original, timestamp = row[0], row[1]
                if original == "original":
                    continue
                m = _SHOP_ID_PATTERN.search(original)
                if not m or m.group(1) in seen:
                    continue
                shop_id = m.group(1)
                if timestamp > latest.get(shop_id, ""):
                    latest[shop_id] = timestamp

            self.logger.info("CDX ページ取得: 新規 %d 店舗 (累計 %d 店舗)",
                             len(latest), len(seen) + len(latest))
            for shop_id, timestamp in latest.items():
                seen.add(shop_id)
                yield shop_id, timestamp
            if not resume_key:
                return
            params = dict(params, resumeKey=resume_key, limit=self._CDX_PAGE)

    # ------------------------------------------------------------ extraction

    @staticmethod
    def _shop_detail_state(page_html: str) -> dict:
        """__NEXT_DATA__ から ShopDetail (GraphQL 応答) オブジェクトを取り出す。"""
        m = _NEXT_DATA_PATTERN.search(page_html)
        if not m:
            return {}
        raw = m.group(1).strip()
        for candidate in (raw, html_mod.unescape(raw)):
            try:
                data = json.loads(candidate)
            except ValueError:
                continue
            root = (
                data.get("props", {})
                .get("pageProps", {})
                .get("initialApolloState", {})
                .get("ROOT_QUERY", {})
            )
            for key, value in root.items():
                if _SHOP_DETAIL_KEY_PATTERN.match(key) and isinstance(value, dict):
                    return value
        return {}

    @staticmethod
    def _iter_jsonld(page_html: str) -> Generator[dict, None, None]:
        """JSON-LD ブロックを dict 単位で列挙する (HTML エンティティ化にも対応)。"""
        for m in re.finditer(
            r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>', page_html, re.S
        ):
            raw = m.group(1).strip()
            if not raw:
                continue
            for candidate in (raw, html_mod.unescape(raw)):
                try:
                    data = json.loads(candidate)
                except ValueError:
                    continue
                for d in (data if isinstance(data, list) else [data]):
                    if isinstance(d, dict):
                        yield d
                break

    @classmethod
    def _genre(cls, page_html: str, shop_name: str, state: dict) -> str:
        """業種・ジャンル。パンくず position=2 の日本語表記を優先する。

        ジャンル未分類店はパンくず position=2 に店名が入るため、店名一致なら採用しない。
        """
        for d in cls._iter_jsonld(page_html):
            if d.get("@type") != "BreadcrumbList":
                continue
            for elem in d.get("itemListElement") or []:
                if not isinstance(elem, dict) or elem.get("position") != 2:
                    continue
                label = (elem.get("name") or "").strip()
                if label and label != shop_name and label != "ホーム":
                    return label
        slug = (state.get("genreTopCategory") or "").strip()
        return _GENRE_LABELS.get(slug, slug)

    @classmethod
    def _takeout(cls, state: dict, page_html: str) -> str:
        """テイクアウト (お受け取り) 可否。掲載があれば "可"、無ければ空。"""
        haystack = " ".join(
            str(state.get(k) or "") for k in ("amenity", "shopInformation", "attention")
        )
        if _TAKEOUT_PATTERN.search(haystack):
            return "可"
        if re.search(r'takeout|pickup|お持ち帰り|テイクアウト', page_html, re.I):
            return "可"
        return ""

    def _build_item(self, shop_id: str, page_html: str, shop_url: str, source: str) -> dict | None:
        """店舗詳細 HTML から 1 件分の dict を組み立てる。

        配達形態では除外せず、判定結果をカラム値として保持する。
        """
        state = self._shop_detail_state(page_html)
        name = (state.get("shopName") or "").strip()
        if not name:
            self.logger.debug("店舗名を取得できずスキップ: %s (source=%s)", shop_url, source)
            return None
        if state.get("isTestShop"):
            self.logger.debug("テスト店舗のためスキップ: %s", shop_url)
            return None

        is_share = state.get("isShareDeli")
        normalized_share = str(is_share).strip().lower()
        if is_share is True or normalized_share == "true":
            delivery_form = "出前館がお届け（シェアリングデリバリー）"
        elif is_share is False or normalized_share == "false":
            delivery_form = "お店がお届け（自社配達）"
        else:
            delivery_form = "判定不可"

        # --- 住所 → 都道府県 / 郵便番号 ---
        address = (state.get("shopAddress") or "").strip()
        if not address:
            for d in self._iter_jsonld(page_html):
                if d.get("@type") in ("Restaurant", "FoodEstablishment"):
                    addr = d.get("address")
                    if isinstance(addr, str) and addr.strip():
                        address = addr.strip()
                        break
        pref = ""
        pm = _PREF_PATTERN.search(address)
        if pm:
            pref = pm.group(1)
            address = address[pm.end():].strip()
        # 郵便番号は住所文字列に含まれる場合のみ (ページ全体だと CSS の px 値を誤検出する)
        post_code = ""
        zm = _POSTCODE_PATTERN.search(address)
        if zm:
            post_code = zm.group(1)
            address = address.replace(zm.group(0), "").strip()

        # --- TEL (プラットフォーム仕様上ほぼ非掲載) ---
        tel = ""
        for d in self._iter_jsonld(page_html):
            candidate = (d.get("telephone") or "").strip() if isinstance(d, dict) else ""
            if candidate:
                tel = candidate
                break

        payments = state.get("paymentMethods") or []
        min_order = (
            (state.get("minimumOrderPriceText") or "")
            or (state.get("minimumOrderConditionText") or "")
        ).strip()

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: address,
            Schema.TEL: tel,
            Schema.CAT_SITE: self._genre(page_html, name, state),
            Schema.HOLIDAY: (state.get("holiday") or "").strip(),
            Schema.TIME: (state.get("businessTime") or "").strip(),
            Schema.PAYMENTS: " / ".join(p for p in payments if p),
            Schema.DESCRIPTION: (state.get("catchphrase") or "").strip(),
            Schema.URL: shop_url,
            "テイクアウト可": self._takeout(state, page_html),
            "配達形態": delivery_form,
            "最低注文金額": min_order,
            "店舗ID": shop_id,
            "取得経路": source,
        }

    # ----------------------------------------------------------- fetch (1店)

    def _scrape_shop(self, shop_id: str, url: str, timestamp: str = "") -> dict | None:
        """1 店舗分を取得して dict を返す。live → Wayback の順に試す。"""
        shop_url = f"{url.rstrip('/')}/{shop_id}"

        if self._live_ok is not False:
            page_html = self._fetch_live_html(shop_url)
            if page_html:
                self._live_ok = True
                return self._build_item(shop_id, page_html, shop_url, "live")
            if self._live_ok is None:
                self._live_ok = False
                self.logger.warning(
                    "live (demae-can.com) が遮断されているため、以降は Wayback Machine の"
                    " スナップショットから取得します"
                )

        if timestamp:
            res = self._get_with_retry(_WAYBACK_RAW.format(ts=timestamp, sid=shop_id))
            snapshot = (timestamp, res.text) if res is not None else None
        else:
            # timestamp 未知 (既知 ID スモーク) は CDX の応答が遅い/504 になることがあるので
            # 短く時間を区切り、失敗しても本番の一覧巡回を待たせない。
            snapshot = self._archive_snapshot(
                shop_id, retries=_SMOKE_CDX_RETRIES, timeout=_SMOKE_CDX_TIMEOUT,
            )
        if snapshot is None:
            self.logger.debug("アーカイブにスナップショット無し: shopDetail/%s", shop_id)
            return None

        ts, page_html = snapshot
        if len(page_html) < _MIN_HTML_LENGTH:
            self.logger.debug("アーカイブ HTML が短すぎる: shopDetail/%s (%d 文字)", shop_id, len(page_html))
            return None
        self._archive_ok = True
        return self._build_item(shop_id, page_html, shop_url, f"Wayback {ts}")

    # ------------------------------------------------------------------ parse

    def parse(self, url: str) -> Generator[dict, None, None]:
        """店舗を 1 件ずつ取得して逐次 yield する (全一覧の先読みはしない)。

        1. SMOKE_SHOP_IDS を先に少数だけ取得し、詳細ページ取得系の生死を確認する
        2. 続けて全店舗 ID を CDX から遅延列挙し、取得できた店舗から順に yield する
        3. 配達形態による除外は行わず、判定値を保持して全件 yield する
        """
        seen_ids: set[str] = set()
        count = 0
        skipped = 0

        def _emit(shop_id: str, timestamp: str = "") -> Generator[dict, None, None]:
            nonlocal count, skipped
            item = self._scrape_shop(shop_id, url, timestamp)
            if item is None:
                skipped += 1
                return
            count += 1
            self.total_items = count
            self.logger.info(
                "✓ 取得 [累計%d件]: %s | %s | %s | %s",
                count, item[Schema.NAME], item.get(Schema.PREF, ""),
                item["配達形態"], item[Schema.URL],
            )
            yield item

        # 1. 既知 ID の少数スモーク (本番でもここで打ち切らず 2. へ進む)
        self.logger.info("既知 ID のスモーク取得: %s", ", ".join(SMOKE_SHOP_IDS))
        for shop_id in SMOKE_SHOP_IDS:
            seen_ids.add(shop_id)
            yield from _emit(shop_id)
        if count == 0:
            self.logger.warning(
                "既知 ID (%s) からは取得できませんでした (live 遮断 or アーカイブ欠落)。"
                " 一覧巡回へ進みます", ", ".join(SMOKE_SHOP_IDS),
            )

        # 2. 全店舗巡回 (ID は遅延列挙し、1 件取れるたびに yield)
        for shop_id, timestamp in self._iter_archived_shop_ids():
            if shop_id in seen_ids:
                continue
            seen_ids.add(shop_id)
            yield from _emit(shop_id, timestamp)
            if self.SHOP_LIMIT is not None and count >= self.SHOP_LIMIT:
                self.logger.info("SHOP_LIMIT (%d) 到達で巡回を打ち切り", self.SHOP_LIMIT)
                break

        self.total_items = count
        self.logger.info(
            "=== 完了: %d 件 yield (候補 %d 件 / 除外・取得不可 %d 件) ===",
            count, len(seen_ids), skipped,
        )
        if count == 0:
            # 0 件を「正常終了」として黙って返さない
            raise DemaeCanBlockedError(
                "店舗を 1 件も取得できませんでした。live は "
                f"{'遮断 (403/Access Denied)' if self._live_ok is False else '未到達'}、"
                f"Wayback からの取得も{'成功しましたが有効な店舗詳細を抽出できませんでした' if self._archive_ok else '失敗しました'}。"
                " ログの『live 遮断を検出』行で status/title/本文長を確認してください。"
            )


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = DemaeCan8Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えると挙動がズレる。
    scraper.execute("https://demae-can.com/shopDetail")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
