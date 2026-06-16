"""
出前館 (demae-can) — フードデリバリーポータル 店舗スクレイパー
(demae_can_8 / 起点 = /shopDetail ・チェーンカタログ巡回型)

取得対象 (店舗詳細ページ /shopDetail/{id} を一次ソースに構造化情報のみ抽出):
    - 店舗名 / ジャンル / 都道府県 / 住所 / 郵便番号 / TEL /
      営業時間 / 定休日 / 支払い方法 / 取得URL
    - EXTRA: テイクアウト可否、配達形態
    ※ 名称は JSON-LD (Restaurant.name) を最優先で抽出する。h1 には店舗カードの
      価格・評価・送料・キャンペーン文言が混入するため一次ソースにしない。

🔒 URL 一貫性 (SSOT = sites.yml の url):
    起点 url = https://demae-can.com/shopDetail  (= sites.yml の url / __main__ の execute 引数)。
    店舗詳細 URL は f"{url}/{shop_id}" で url から直接派生させる。
    チェーン一覧等の URL も base (= url のスキーム+ホスト) から派生させ、別ホストをハードコードしない。

取得フロー (チェーンカタログ + エリア一覧巡回型 / CRAWL_CHAINS・CRAWL_AREAS で切替):
    1. [チェーン] base/chain/list 系から /chain/top/{chainId} を収集 → 各支店一覧から shop_id
    2. [エリア]   base/sitemap.xml.gz → address_detail サイトマップ →
                  /search/delivery/{areaCode} (エリア別の配達可能店一覧、個別店含む) から shop_id
    3. shop_id をソース横断でグローバル重複排除し、robots.txt Disallow 該当 ID は除外
    4. 各 /shopDetail/{id} を取得して構造化情報を抽出・yield (見つけ次第ストリーミング)

    ※ 出前館は住所を入力しないと個別 (非チェーン) 店が出ない「住所ゲート」方式だが、
      公式サイトマップに /search/delivery/{area} のエリア一覧 URL が列挙されており、これを
      辿ると個別店も SSR で取得できる (GraphQL 解析は不要)。
    ※ エリアは丁目レベルで数が膨大なため、エリア間の店舗重複をグローバル排除する。試験運用は
      AREA_LIMIT で件数を絞れる。
    ※ 旧実装の ID 総当たり (3M〜10M) は非現実的 (数ヶ月 + Akamai 遮断) かつチェーン店しか
      得られなかったため廃止した。

注意 (重要):
    - demae-can.com は Akamai WAF 配下。requests ベースは 403 全拒否 → Playwright 必須。
    - 利用規約 第12条(14) 営業妨害の懸念があるため DELAY を確保し高頻度アクセスを避ける。
    - robots.txt の /shop/menu/{id} Disallow をランタイムで読み取り尊重する。

実行方法:
    # ローカルテスト
    python scripts/sites/food/demae_can_8.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id demae_can_8
"""

import json
import re
import sys
from pathlib import Path
from typing import Generator

from playwright.sync_api import sync_playwright

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
_POSTCODE_PATTERN = re.compile(r"\d{3}-?\d{4}")
_TAKEOUT_PATTERN = re.compile(r"テイクアウト|お持ち帰り|お持帰り|持ち帰り")

# h1 フォールバック時に「店舗カード等の UI テキスト混入」を弾くための語/パターン。
# 例: "お店価格＋送料無料対象ピザーラ 盛岡店4.560分送料0円夏のプレミアム！"
_NAME_JUNK_TOKENS = (
    "送料", "クーポン", "ポイント", "無料対象", "お店価格", "最低注文",
    "キャンペーン", "プレミアム", "今だけ", "％OFF", "%OFF", "円～", "円〜",
)
_NAME_JUNK_PATTERN = re.compile(r"\d\.\d|\d+\s*円|\d+\s*分")  # 評価4.5 / 送料0円 / 60分 等
# エラー/404 ページで h1 や title に現れる文言
_ERROR_NAME_VALUES = {"エラー", "Error", "404"}

_PAYMENT_NAMES = {
    "visa": "VISA", "master": "Master", "jcb": "JCB", "amex": "AMEX",
    "diners": "Diners", "amazon": "Amazon Pay", "paypay": "PayPay",
    "docomo": "d払い", "carrier": "キャリア決済", "apple": "Apple Pay",
    "google": "Google Pay", "rakuten": "楽天ペイ", "linepay": "LINE Pay",
    "aupay": "au PAY", "merpay": "メルペイ",
}

_SECTION_HEADINGS = {
    "営業時間", "定休日", "住所", "ご利用できるお支払い方法", "お支払い方法",
    "配達員", "店舗からのコメント", "Information", "配達エリア", "店舗からのお知らせ",
    "最低注文条件", "最低注文金額", "ジャンル", "電話番号",
}

# チェーン一覧のジャンル (base/chain/list/{genre})。/chain/list の動的発見が
# 失敗した場合のフォールバック兼初期シード。
_CHAIN_GENRES = (
    "pizza", "sushi", "don", "curry", "chinese", "hamburger", "youshoku", "box",
)

# サイトマップ。エリア別配達一覧 (/search/delivery/{areaCode}) を列挙する起点。
# 個別 (非チェーン) 店もエリア一覧には出るため、全店網羅にはこちらを使う。
_SITEMAP_URL = "/sitemap.xml.gz"
# 個別店を含むエリア一覧 sitemap の <loc> 名 (チェーン用 sitemap は除外する)
_AREA_SITEMAP_KEYWORD = "address_detail"
_AREA_PATH_PATTERN = re.compile(r"/search/delivery/(\d+)")

# 404/エラーページ判定に使うタイトル文言
_ERROR_TITLE_PATTERNS = [
    "ページが見つかりません", "お探しのページは", "店舗が見つかりません",
    "404", "Not Found", "エラー",
]


class DemaeCan8Scraper(DynamicCrawler):
    """出前館 (demae-can) スクレイパー — /shopDetail 起点・チェーンカタログ巡回型"""

    DELAY = 2.0
    CONTINUE_ON_ERROR = True

    # 取得スコープ。
    #   CRAWL_CHAINS: /chain/list 経由でチェーン店を巡回 (高速・確実)
    #   CRAWL_AREAS : サイトマップの /search/delivery/{area} 経由で個別店含む全店を巡回
    #                 (網羅的だがエリア数が膨大で長時間ジョブになる)
    # 両方 True の場合はチェーン → エリアの順に巡回し shop_id をグローバル重複排除する。
    CRAWL_CHAINS = True
    CRAWL_AREAS = True
    # エリア巡回するエリア数の上限 (None=無制限)。試験運用や地域限定取得に使う。
    AREA_LIMIT: int | None = None

    # Akamai 回避用の launch 設定。bundled chromium は HTTP2/TLS 指紋で弾かれるため
    # 実 Google Chrome (channel="chrome") を優先し、無ければ chromium にフォールバックする。
    _LAUNCH_ARGS = ["--disable-blink-features=AutomationControlled", "--no-sandbox"]
    _EXTRA_HEADERS = {
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
                  "image/avif,image/webp,*/*;q=0.8",
    }

    EXTRA_COLUMNS = [
        "テイクアウト可",   # テキスト/JSON-LD 検出: 記載があれば "可"
        "配達形態",        # 「配達員」セクションの値 (例: 出前館スタッフ)
    ]

    # ------------------------------------------------------------------ setup

    def _setup(self):
        """Playwright を起動する。demae-can は Akamai WAF 配下で bundled chromium の
        HTTP2/TLS 指紋を拒否 (ERR_HTTP2_PROTOCOL_ERROR) するため、実 Google Chrome を
        優先利用する。Chrome 不在環境では chromium にフォールバックする
        (その場合 Akamai に弾かれて取得 0 件になりうる点に注意)。
        """
        self.playwright = sync_playwright().start()
        try:
            self.browser = self.playwright.chromium.launch(
                headless=True, channel="chrome", args=self._LAUNCH_ARGS,
            )
            self.logger.info("ブラウザ起動: Google Chrome (channel=chrome)")
        except Exception as exc:  # noqa: BLE001
            self.logger.warning(
                "Chrome 起動失敗のため bundled chromium にフォールバック "
                "(Akamai に弾かれる可能性あり): %s", exc,
            )
            self.browser = self.playwright.chromium.launch(
                headless=True, args=self._LAUNCH_ARGS,
            )
        self.context = self.browser.new_context(
            user_agent=self.USER_AGENT,
            locale="ja-JP",
            extra_http_headers=self._EXTRA_HEADERS,
        )
        # navigator.webdriver を隠蔽 (自動化検知の緩和)
        self.context.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
        )
        self.page = self.context.new_page()

    # ------------------------------------------------------------------ utils

    def _iter_jsonld(self, soup):
        """ページ内の JSON-LD ブロックを dict 単位で列挙する。"""
        for tag in soup.select('script[type="application/ld+json"]'):
            raw = (tag.string or tag.get_text() or "").strip()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except (ValueError, TypeError):
                continue
            candidates = data if isinstance(data, list) else [data]
            for d in candidates:
                if isinstance(d, dict):
                    if isinstance(d.get("@graph"), list):
                        for g in d["@graph"]:
                            if isinstance(g, dict):
                                yield g
                    yield d

    @staticmethod
    def _lines(soup) -> list[str]:
        text = soup.get_text("\n", strip=True)
        return [ln.strip() for ln in text.split("\n") if ln.strip()]

    @classmethod
    def _section_value(cls, lines: list[str], label: str) -> str:
        try:
            idx = next(i for i, ln in enumerate(lines) if ln == label)
        except StopIteration:
            return ""
        collected: list[str] = []
        for ln in lines[idx + 1:]:
            if ln in _SECTION_HEADINGS:
                break
            collected.append(ln)
        return " / ".join(collected).strip()

    @staticmethod
    def _first_text(soup, selectors: list[str]) -> str:
        for sel in selectors:
            el = soup.select_one(sel)
            if el:
                txt = el.get_text(" ", strip=True)
                if txt:
                    return txt
        return ""

    @classmethod
    def _extract_payments(cls, soup) -> str:
        found: list[str] = []
        for img in soup.select('img[src*="payment-method"], [class*="payment" i] img'):
            label = (img.get("alt") or "").strip()
            if not label:
                src = (img.get("src") or "").lower()
                for key, name in _PAYMENT_NAMES.items():
                    if key in src:
                        label = name
                        break
            if label and label not in found:
                found.append(label)
        return " / ".join(found)

    @classmethod
    def _extract_genre(cls, soup, name: str) -> str:
        for a in soup.select('a[href*="/chain/list/"]'):
            txt = a.get_text(" ", strip=True)
            if txt and "一覧" not in txt and len(txt) <= 30:
                return txt
        m = re.search(r"[（(]([^）)]{1,30})[）)]\s*の店舗詳細", name)
        if m:
            return m.group(1).strip()
        m = re.search(r"[（(]([^）)]{1,30})[）)]", name)
        return m.group(1).strip() if m else ""

    @staticmethod
    def _clean_name(raw: str) -> str:
        n = raw.strip()
        n = re.sub(r"[（(][^）)]*[）)]\s*の店舗詳細$", "", n)
        n = re.sub(r"\s*の店舗詳細$", "", n)
        m = re.search(r"店舗詳細[｜|]\s*(.+?)\s*の宅配", n)
        if m:
            n = m.group(1)
        return n.strip()

    @staticmethod
    def _restaurant_ld(jsonld_dicts: list[dict]) -> dict | None:
        """JSON-LD から Restaurant (店舗) ブロックを返す。無ければ None。"""
        for d in jsonld_dicts:
            if d.get("@type") == "Restaurant" and isinstance(d.get("name"), str) and d["name"].strip():
                return d
        return None

    @staticmethod
    def _name_from_breadcrumb(jsonld_dicts: list[dict]) -> str:
        """BreadcrumbList の末尾から店舗名を拾う。
        末尾が「〜の店舗詳細」(店舗詳細ページ署名) の場合のみ採用し、その接尾辞を除去する。
        非店舗ページ (chain/list 等) のパンくず末尾はこの署名を持たないため除外される。
        """
        for d in jsonld_dicts:
            if d.get("@type") != "BreadcrumbList":
                continue
            items = d.get("itemListElement")
            if not isinstance(items, list) or not items:
                continue
            last = items[-1]
            nm = last.get("name") if isinstance(last, dict) else None
            if isinstance(nm, str) and nm.strip().endswith("の店舗詳細"):
                return re.sub(r"\s*の店舗詳細$", "", nm.strip())
        return ""

    @classmethod
    def _looks_like_junk(cls, name: str) -> bool:
        """店舗カード等の UI テキストを巻き込んだ名称かどうかを判定する。"""
        if not name or name in _ERROR_NAME_VALUES:
            return True
        if any(tok in name for tok in _NAME_JUNK_TOKENS):
            return True
        if _NAME_JUNK_PATTERN.search(name):
            return True
        return len(name) > 60

    @classmethod
    def _detect_takeout(cls, soup, jsonld_dicts: list[dict], page_text: str) -> str:
        for d in jsonld_dicts:
            actions = d.get("potentialAction")
            action_list = actions if isinstance(actions, list) else [actions]
            for act in action_list:
                if not isinstance(act, dict):
                    continue
                dm = act.get("deliveryMethod")
                dm_str = json.dumps(dm, ensure_ascii=False) if dm is not None else ""
                if re.search(r"takeout|pickup|pick[-_]?up|お持ち帰り|テイクアウト", dm_str, re.I):
                    return "可"
        if soup.select_one(
            '[class*="takeout" i], [class*="takeOut" i], [class*="pickup" i], '
            'a[href*="takeout" i], a[href*="pickup" i]'
        ):
            return "可"
        if _TAKEOUT_PATTERN.search(page_text):
            return "可"
        return ""

    # --------------------------------------------------------------- catalog

    @staticmethod
    def _base_url(url: str) -> str:
        """url からスキーム+ホスト (例: https://demae-can.com) を取り出す。"""
        m = re.match(r"(https?://[^/]+)", url)
        return m.group(1) if m else url.rstrip("/")

    def _load_disallowed_menu_prefixes(self, base: str) -> tuple[str, ...]:
        """robots.txt を読み、/shop/menu/ ・ /shopDetail/ の Disallow を ID 接頭辞集合として返す。
        取得失敗時は空 (= 除外なし) を返す。
        """
        soup = self.get_soup(f"{base}/robots.txt", wait_until="domcontentloaded")
        if soup is None:
            self.logger.warning("robots.txt 取得失敗 — Disallow 判定をスキップ")
            return ()
        text = soup.get_text("\n")
        prefixes: set[str] = set()
        for line in text.splitlines():
            line = line.strip()
            if not line.lower().startswith("disallow:"):
                continue
            path = line.split(":", 1)[1].strip()
            m = re.search(r"/(?:shop/menu|shopDetail)/(\d+)", path)
            if m:
                prefixes.add(m.group(1))
        self.logger.info("robots.txt Disallow 店舗ID 接頭辞: %d 件", len(prefixes))
        return tuple(sorted(prefixes))

    def _collect_chain_top_urls(self, base: str) -> list[str]:
        """チェーン一覧系ページから /chain/top/{chainId} URL を収集する。"""
        seeds = [f"{base}/chain/list", f"{base}/"]
        seeds += [f"{base}/chain/list/{g}" for g in _CHAIN_GENRES]

        found: set[str] = set()
        visited: set[str] = set()
        for seed in seeds:
            if seed in visited:
                continue
            visited.add(seed)
            soup = self.get_soup(seed, wait_until="domcontentloaded")
            if soup is None:
                continue
            # ジャンル一覧ページを動的に発見してシードに追加
            for a in soup.select('a[href*="/chain/list/"]'):
                href = a.get("href") or ""
                full = href if href.startswith("http") else f"{base}{href}"
                if "/chain/list/" in full and full not in visited:
                    seeds.append(full)
            # チェーン詳細 (全支店を列挙するページ) を収集
            for a in soup.select('a[href*="/chain/top/"]'):
                m = re.search(r"/chain/top/(\d+)", a.get("href") or "")
                if m:
                    found.add(f"{base}/chain/top/{m.group(1)}")
        return sorted(found)

    @staticmethod
    def _extract_shop_ids(soup) -> list[str]:
        """ページ内の /shop/menu/{id} ・ /shopDetail/{id} リンクから shop_id を順序保持で抽出。"""
        ids: list[str] = []
        seen: set[str] = set()
        for a in soup.select('a[href*="/shop/menu/"], a[href*="/shopDetail/"]'):
            m = re.search(r"/(?:shop/menu|shopDetail)/(\d+)", a.get("href") or "")
            if m and m.group(1) not in seen:
                seen.add(m.group(1))
                ids.append(m.group(1))
        return ids

    def _collect_shop_ids(self, list_url: str) -> list[str]:
        """店舗一覧ページ (chain/top または search/delivery) から shop_id を収集する。"""
        soup = self.get_soup(list_url, wait_until="domcontentloaded")
        if soup is None:
            return []
        return self._extract_shop_ids(soup)

    def _fetch_text_via_page(self, abs_url: str) -> str:
        """ページ内 fetch で URL を取得しテキストを返す (gzip は DecompressionStream で解凍)。
        Akamai は Playwright 独自スタックの直接 HTTP を弾くため、ブラウザ (Chrome) の
        fetch を使う。呼び出し前に同一オリジンのページへ遷移済みである必要がある。
        """
        js = """
        async (url) => {
          const r = await fetch(url, {credentials:'include'});
          const buf = await r.arrayBuffer();
          const bytes = new Uint8Array(buf);
          if (bytes[0]===0x1f && bytes[1]===0x8b) {
            const ds = new DecompressionStream('gzip');
            const stream = new Response(buf).body.pipeThrough(ds);
            return await new Response(stream).text();
          }
          return new TextDecoder('utf-8').decode(buf);
        }
        """
        try:
            return self.page.evaluate(js, abs_url) or ""
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("sitemap 取得失敗: %s — %s", abs_url, exc)
            return ""

    def _iter_area_urls(self, base: str) -> Generator[str, None, None]:
        """サイトマップから個別店を含むエリア一覧 URL (/search/delivery/{area}) を
        重複排除しつつ列挙する。AREA_LIMIT 件で打ち切る。
        """
        index_xml = self._fetch_text_via_page(f"{base}{_SITEMAP_URL}")
        children = [
            loc for loc in re.findall(r"<loc>(.*?)</loc>", index_xml)
            if _AREA_SITEMAP_KEYWORD in loc
        ]
        self.logger.info("エリア sitemap: %d 個", len(children))

        seen_area: set[str] = set()
        emitted = 0
        for child in children:
            xml = self._fetch_text_via_page(child)
            if not xml:
                continue
            for loc in re.findall(r"<loc>(.*?)</loc>", xml):
                m = _AREA_PATH_PATTERN.search(loc)
                if not m:
                    continue
                area = m.group(1)  # ジャンル接尾辞を落としエリア単位で重複排除
                if area in seen_area:
                    continue
                seen_area.add(area)
                yield f"{base}/search/delivery/{area}"
                emitted += 1
                if self.AREA_LIMIT is not None and emitted >= self.AREA_LIMIT:
                    self.logger.info("AREA_LIMIT (%d) 到達でエリア列挙を打ち切り", self.AREA_LIMIT)
                    return
        self.logger.info("エリア URL 列挙完了: %d 件 (distinct area)", emitted)

    # ------------------------------------------------------------------ parse

    def parse(self, url: str) -> Generator[dict, None, None]:
        """店舗一覧を巡回し、各店の /shopDetail/{id} を取得して yield する。

        探索ソース (CRAWL_CHAINS / CRAWL_AREAS で切替):
          - チェーンカタログ: /chain/list → /chain/top/{chainId} → 支店一覧
          - エリア一覧: sitemap → /search/delivery/{area} (個別店含む全店)
        収集した shop_id はソース横断でグローバル重複排除し、見つけ次第ストリーミング取得する。
        """
        base = self._base_url(url)
        disallowed = self._load_disallowed_menu_prefixes(base)
        # 同一オリジンのページへ遷移してクリアランス確立 (sitemap の in-page fetch に必要)
        self.get_soup(base, wait_until="domcontentloaded")

        self._seen_ids: set[str] = set()
        self._count = 0

        # 1. チェーンカタログ (高速・確実)
        if self.CRAWL_CHAINS:
            chain_top_urls = self._collect_chain_top_urls(base)
            self.logger.info("発見したチェーン数: %d", len(chain_top_urls))
            if not chain_top_urls:
                self.logger.warning(
                    "チェーンが0件。サイト構造変更か Akamai 遮断の可能性 (base=%s)", base
                )
            for ci, chain_top_url in enumerate(chain_top_urls, 1):
                shop_ids = self._collect_shop_ids(chain_top_url)
                self.logger.info(
                    "[チェーン %d/%d] %s — 支店 %d 件",
                    ci, len(chain_top_urls), chain_top_url, len(shop_ids),
                )
                yield from self._scrape_new_ids(shop_ids, url, disallowed)

        # 2. エリア一覧 (個別店含む全店)
        if self.CRAWL_AREAS:
            for ai, area_url in enumerate(self._iter_area_urls(base), 1):
                shop_ids = self._collect_shop_ids(area_url)
                new_ids = [s for s in shop_ids if s not in self._seen_ids]
                self.logger.info(
                    "[エリア %d] %s — 店舗 %d 件 (新規 %d)",
                    ai, area_url, len(shop_ids), len(new_ids),
                )
                yield from self._scrape_new_ids(shop_ids, url, disallowed)

        self.total_items = self._count
        self.logger.info("=== 完了: 店舗 %d 件 ===", self._count)

    def _scrape_new_ids(
        self, shop_ids: list[str], url: str, disallowed: tuple[str, ...]
    ) -> Generator[dict, None, None]:
        """未取得の shop_id だけを /shopDetail から取得して yield する (グローバル重複排除)。"""
        for shop_id in shop_ids:
            if shop_id in self._seen_ids:
                continue
            self._seen_ids.add(shop_id)
            if disallowed and shop_id.startswith(disallowed):
                self.logger.debug("robots Disallow によりスキップ: %s", shop_id)
                continue

            shop_url = f"{url}/{shop_id}"
            try:
                item = self._scrape_shop(shop_url)
            except Exception as exc:  # noqa: BLE001
                self.logger.debug("例外スキップ: %s — %s", shop_url, exc)
                continue

            if item and item.get(Schema.NAME):
                self._count += 1
                self.total_items = self._count
                self.logger.info(
                    "✓ 取得 [累計%d件]: %s | %s | %s",
                    self._count, item[Schema.NAME], item.get(Schema.PREF, ""), shop_url,
                )
                yield item

    # --------------------------------------------------------------- detail

    def _scrape_shop(self, shop_url: str) -> dict | None:
        """店舗詳細ページ (/shopDetail/{id}) から構造化情報を抽出する。
        有効な店舗でない場合 (404・エラーページ) は None を返す。
        """
        soup = self.get_soup(shop_url, wait_until="domcontentloaded")
        if soup is None:
            return None

        # 404/エラーページの早期判定
        title_el = soup.select_one("title")
        title_text = title_el.get_text(strip=True) if title_el else ""
        if any(pat in title_text for pat in _ERROR_TITLE_PATTERNS):
            return None
        page_text = soup.get_text(" ", strip=True)
        if len(page_text) < 100:
            return None

        jsonld_dicts = list(self._iter_jsonld(soup))
        lines = self._lines(soup)

        # --- 名称 (JSON-LD Restaurant.name を最優先) ---
        # 店舗カードの価格・評価・送料・キャンペーン文言が h1/見出しに混入するため、
        # 構造化データ (JSON-LD / パンくず) を一次ソースにし、h1 は最終手段かつ
        # junk 判定で弾く。これにより名称ゴミと 404「エラー」行の混入を防ぐ。
        restaurant_ld = self._restaurant_ld(jsonld_dicts)
        name = restaurant_ld["name"].strip() if restaurant_ld else ""
        if not name:
            name = self._name_from_breadcrumb(jsonld_dicts)
        if not name:
            # h1 フォールバックは「〜の店舗詳細」という詳細ページ署名がある場合のみ採用。
            # これがないと chain/list 等の非店舗ページの h1 を誤って拾う。
            raw = self._first_text(soup, ["h1"])
            if raw.endswith("の店舗詳細"):
                cleaned = self._clean_name(raw)
                if not self._looks_like_junk(cleaned):
                    name = cleaned
        if not name:
            return None  # 構造化された店名なし = 有効な店舗ページではない (404/エラー含む)

        # --- ジャンル (JSON-LD servesCuisine 優先) ---
        cuisine = ""
        if restaurant_ld and isinstance(restaurant_ld.get("servesCuisine"), str):
            cuisine = restaurant_ld["servesCuisine"].strip()
        if not cuisine:
            cuisine = self._extract_genre(soup, self._first_text(soup, ["h1"]) or name)

        # --- 各セクション (見出しベース抽出) ---
        hours = self._section_value(lines, "営業時間")
        holiday = self._section_value(lines, "定休日")
        delivery_form = self._section_value(lines, "配達員")

        # --- 住所 (JSON-LD Restaurant.address を最優先) ---
        address = ""
        if restaurant_ld:
            addr = restaurant_ld.get("address")
            if isinstance(addr, str):
                address = addr.strip()
            elif isinstance(addr, dict):
                address = "".join(
                    (addr.get(k) or "").strip()
                    for k in ("addressRegion", "addressLocality", "streetAddress")
                )
        if not address:
            address = self._section_value(lines, "住所")
        if not address:
            address = self._first_text(
                soup, ['[class*="address" i]', '[itemprop="address"]']
            )
        # JSON-LD フォールバック (Restaurant 以外のブロック)
        if not address:
            for d in jsonld_dicts:
                addr = d.get("address")
                if isinstance(addr, dict):
                    parts = [
                        (addr.get("addressRegion") or "").strip(),
                        (addr.get("addressLocality") or "").strip(),
                        (addr.get("streetAddress") or "").strip(),
                    ]
                    address = "".join(p for p in parts if p)
                    if address:
                        break
                elif isinstance(addr, str) and addr.strip():
                    address = addr.strip()
                    break

        # --- 都道府県・郵便番号 ---
        pref = ""
        postcode = ""
        if address:
            pm = _PREF_PATTERN.search(address)
            if pm:
                pref = pm.group(1)
                address = address[pm.start():].strip()
            mc = _POSTCODE_PATTERN.search(address)
            if mc:
                postcode = mc.group(0)

        # --- TEL (JSON-LD 優先; プラットフォーム仕様上ほぼ非掲載) ---
        tel = ""
        for d in jsonld_dicts:
            tel = (d.get("telephone") or "").strip()
            if tel:
                break

        # --- 支払い方法 ---
        payments = self._extract_payments(soup)

        # --- テイクアウト可否 ---
        takeout = self._detect_takeout(soup, jsonld_dicts, page_text)

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: postcode,
            Schema.ADDR: address,
            Schema.TEL: tel,
            Schema.CAT_SITE: cuisine,
            Schema.TIME: hours,
            Schema.HOLIDAY: holiday,
            Schema.PAYMENTS: payments,
            Schema.URL: shop_url,
            "テイクアウト可": takeout,
            "配達形態": delivery_form,
        }


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
