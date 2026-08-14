"""
ゼヒトモ（Zehitomo）— ビジネスカテゴリー掲載プロの収集（市区町村まで巡回する版）

取得対象:
    - https://www.zehitomo.com/business/ 配下のプロ一覧に掲載されているプロ(事業者)
    - 在住地が中国地方・四国(香川/愛媛)・九州・沖縄の 15 都道府県のプロのみ

取得フロー:
    1. /business/ の __NEXT_DATA__ から
       - jobTypes.listByCategory … 小カテゴリ(jobType) 69 件と所属カテゴリ ["business", c1, c2]
       - categoryTree            … 大カテゴリ(c1) / 中カテゴリ(c2) の日本語名
       を一括取得する (カテゴリページを個別に巡回する必要が無い)
    2. 対象都道府県 × 小カテゴリで
       /business/{c1}/{c2}/{jobType}/{都道府県}?page=N を巡回し、
       続いて同ページから得た市区町村リンク
       /business/{c1}/{c2}/{jobType}/{都道府県}/{市区町村}?page=N も巡回する
    3. 一覧の __NEXT_DATA__ (initialState.marketingPages.pros) からプロを取り出し、
       在住都道府県が対象 15 県のものだけ詳細ページを取得して即 yield する

このサイト固有の注意点 (Phase 1 の実測で判明):
    - サイト全体が Cloudflare 配下。goto の度に clear_cookies() し、自動化フラグを
      無効化した実ブラウザ相当の設定で起動しないと 403 になる (requests では取得不可)。
    - 一覧のページ送りは **10 ページ(50 件)が上限**。totalPros が 171 でも
      ?page=11 は 404 になる。1 エリアページから 50 件しか辿れないため、
      都道府県ページに加えて市区町村ページも巡回して網羅性を上げている。
    - 一覧は「そのエリアに *対応可能* なプロ」を返す仕様で、他県在住のプロが多く混ざる。
      一覧 JSON の pros[].location.title に在住地が入っているので、
      詳細ページを取得する前に都道府県で絞り込み、無駄な通信を避けている。
    - 大カテゴリ(c1) の一部 (professional-writing / marketing-and-pr 等) は
      categoryTree に載っていないため、一覧ページのパンくず
      ([data-test-id="breadcrumbs"]) から日本語名を補完する。
    - 自由記述の紹介文(description / skills / interest / クチコミ本文)はカラムとして
      出力しない。Instagram アカウントの URL のみ、これらのテキストから抽出する。

実行方法:
    # ローカルテスト
    python scripts/sites/service/zehitomo_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id zehitomo_2
"""

import json
import logging
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup

from src.const.schema import Schema
from src.framework.dynamic import DynamicCrawler

logger = logging.getLogger(__name__)

# 対象 15 都道府県 (URL キー, 表示名)。掲載件数が多いと見込まれる順に並べ、早期に 1 件目を yield する。
TARGET_PREFECTURES: list[tuple[str, str]] = [
    ("fukuoka", "福岡県"),
    ("hiroshima", "広島県"),
    ("okayama", "岡山県"),
    ("kumamoto", "熊本県"),
    ("kagoshima", "鹿児島県"),
    ("okinawa", "沖縄県"),
    ("ehime", "愛媛県"),
    ("kagawa", "香川県"),
    ("nagasaki", "長崎県"),
    ("oita", "大分県"),
    ("yamaguchi", "山口県"),
    ("miyazaki", "宮崎県"),
    ("saga", "佐賀県"),
    ("shimane", "島根県"),
    ("tottori", "鳥取県"),
]

# 在住都道府県の判定に使う集合 (詳細ページの location.prefecture.name と照合)
TARGET_PREF_NAMES: frozenset[str] = frozenset(name for _key, name in TARGET_PREFECTURES)

_NEXT_DATA_RE = re.compile(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.DOTALL)
# 紹介文中に書かれた Instagram アカウント URL (ゼヒトモ公式アカウントは除外)
_INSTAGRAM_RE = re.compile(r"https?://(?:www\.)?instagram\.com/[A-Za-z0-9_.\-]+/?")
_ZIP_RE = re.compile(r"^(\d{3})(\d{4})$")


class Zehitomo2(DynamicCrawler):
    """ゼヒトモ（Zehitomo）スクレイパー — 市区町村ページまで巡回する版"""

    DELAY = 0.0  # リクエスト間隔は MIN_INTERVAL で自前制御する
    PAGE_SIZE = 5  # 一覧 1 ページあたりのプロ件数
    MAX_PAGES = 10  # サイト側の上限 (11 ページ目は 404)
    MIN_CITY_PAGES = 2  # 市区町村ページは最低このページ数まで見る
    MAX_RETRY = 4  # Cloudflare ブロック時のリトライ上限
    MIN_INTERVAL = 1.5  # サイトへの負荷を抑えるための最低リクエスト間隔 (秒)
    MAX_HARD_ERRORS = 2  # 再試行しても回復しない失敗の上限

    EXTRA_COLUMNS = ["rating", "review_count", "area_text"]

    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )

    _last_request_at: float = 0.0
    _cat_titles: dict[str, str] = {}  # parse() で categoryTree から構築する
    _last_error: str = ""  # 直前の goto 失敗理由 (回復しないエラーの判定用)

    # ------------------------------------------------------------------
    # ブラウザ設定 (Cloudflare 対策)
    # ------------------------------------------------------------------
    def _setup(self):
        """Cloudflare を通過できるブラウザ設定で Playwright を起動する。

        DynamicCrawler の既定設定 (自動化フラグ有効・古い UA) では 403 で弾かれるため
        _setup を上書きしている。
        """
        from playwright.sync_api import sync_playwright

        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        self.context = self._new_context()
        self.page = self.context.new_page()
        self._last_request_at = 0.0

    def _new_context(self):
        return self.browser.new_context(
            user_agent=self.USER_AGENT,
            locale="ja-JP",
            timezone_id="Asia/Tokyo",
            viewport={"width": 1440, "height": 900},
        )

    def _reset_context(self):
        """ブラウザコンテキストを作り直してセッションを取り直す。

        ブロックが続くときは Cookie の破棄だけでは復帰しないため、コンテキストごと作り直す。
        """
        logger.info("ブラウザコンテキストを再作成します")
        try:
            self.page.close()
            self.context.close()
        except Exception as e:  # 既に閉じている等は継続して差し支えない
            logger.warning("コンテキストの終了に失敗 (継続): %s", e)
        self.context = self._new_context()
        self.page = self.context.new_page()

    # ------------------------------------------------------------------
    # 取得ユーティリティ
    # ------------------------------------------------------------------
    @staticmethod
    def _is_blocked(soup: BeautifulSoup) -> bool:
        """Cloudflare のブロックページかどうかを判定する。"""
        title = soup.title.get_text(strip=True) if soup.title else ""
        return "Attention Required" in title or "Cloudflare" in title

    def _throttle(self):
        """直前のリクエストから MIN_INTERVAL 秒空ける。"""
        wait = self.MIN_INTERVAL - (time.monotonic() - self._last_request_at)
        if wait > 0:
            time.sleep(wait)
        self._last_request_at = time.monotonic()

    def _goto_direct(self, url: str) -> str | None:
        """キャッシュを介さず素の goto で取得する (リトライ用)。

        キャッシュにはブロックページも保存されるため、再試行では素の goto を使い、
        成功した HTML でキャッシュを上書きする。
        """
        try:
            self.page.goto(url, wait_until="domcontentloaded")
            self._last_error = ""
            return self.page.content()
        except Exception as e:
            self._last_error = str(e)
            logger.warning("ページ取得エラー: %s — %s", url, e)
            return None

    @staticmethod
    def _is_permanent_failure(error_text: str) -> bool:
        """再試行しても回復しないエラーかどうかを判定する。

        一部のプロフィールは無限リダイレクトになる (サイト側の不整合)。
        Cloudflare のブロックとは違い待っても直らないので、即スキップする。
        """
        return "ERR_TOO_MANY_REDIRECTS" in error_text

    def _fetch(self, url: str) -> BeautifulSoup | None:
        """Cloudflare ブロックを考慮してページを取得する。

        ブロック時はコンテキストを作り直しつつ指数バックオフを挟み、最大 MAX_RETRY 回
        まで再取得する。上限に達したら None を返す (スキップ扱い)。
        """
        hard_errors = 0
        for attempt in range(self.MAX_RETRY):
            self._throttle()
            # Cookie を保持したまま 2 回目以降のアクセスを行うと Cloudflare に 403 で
            # 弾かれる。毎回セッションを捨てることで安定して通過できる。
            try:
                self.context.clear_cookies()
            except Exception as e:
                logger.warning("Cookie クリアに失敗 (継続): %s", e)

            if attempt == 0:
                # 1 回目のみキャッシュ + 進捗ビーコン付きの標準経路を使う
                soup = self.get_soup(url)
                html = None
            else:
                html = self._goto_direct(url)
                soup = BeautifulSoup(html, "html.parser") if html else None

            if soup is not None and not self._is_blocked(soup):
                if html is not None:
                    # ブロックページがキャッシュされている可能性があるので上書きする
                    self._services.cache.put(url, html, variant="domcontentloaded")
                return soup

            if soup is None:
                if self._is_permanent_failure(self._last_error):
                    logger.warning("回復しないエラーのためスキップ: %s", url)
                    return None
                hard_errors += 1
                if hard_errors >= self.MAX_HARD_ERRORS:
                    logger.error("取得できないためスキップ: %s", url)
                    return None
            else:
                logger.warning(
                    "Cloudflare ブロック (%d/%d): %s", attempt + 1, self.MAX_RETRY, url
                )
                if attempt >= 1:
                    self._reset_context()
            time.sleep(min(5 * (attempt + 1), 30))

        logger.error("Cloudflare ブロックのためスキップ: %s", url)
        return None

    @staticmethod
    def _next_data(soup: BeautifulSoup) -> dict:
        """__NEXT_DATA__ の JSON を辞書で返す (無ければ空辞書)。"""
        tag = soup.find("script", id="__NEXT_DATA__")
        raw = tag.string if tag is not None else None
        if not raw:
            m = _NEXT_DATA_RE.search(str(soup))
            raw = m.group(1) if m else None
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except json.JSONDecodeError as e:
            logger.warning("__NEXT_DATA__ の解析に失敗: %s", e)
            return {}

    # ------------------------------------------------------------------
    # メイン
    # ------------------------------------------------------------------
    def parse(self, url: str):
        """引数 url (= sites.yml の url) を唯一の起点として巡回する。"""
        soup = self._fetch(url)
        if soup is None:
            raise RuntimeError(f"起点ページを取得できませんでした: {url}")

        page_props = self._next_data(soup).get("props", {}).get("pageProps", {})
        self._cat_titles = self._build_category_titles(page_props)
        job_types = (
            page_props.get("initialState", {}).get("jobTypes", {}).get("listByCategory")
            or []
        )
        if not job_types:
            raise RuntimeError("カテゴリー一覧 (listByCategory) を取得できませんでした")

        root = url.rstrip("/")  # https://www.zehitomo.com/business
        logger.info(
            "対象 %d 都道府県 × 小カテゴリ %d 件を巡回します",
            len(TARGET_PREFECTURES),
            len(job_types),
        )

        seen_pros: set[str] = set()  # プロ ID (詳細取得の重複防止)

        for pref_key, pref_name in TARGET_PREFECTURES:
            for job_type in job_types:
                categories = job_type.get("categories") or []
                job_type_id = job_type.get("_id") or ""
                if len(categories) < 3 or not job_type_id:
                    continue
                _vertical, cat_l1, cat_l2 = categories[:3]

                list_root = f"{root}/{cat_l1}/{cat_l2}/{job_type_id}/{pref_key}"
                yield from self._crawl_area(
                    list_root, job_type, cat_l1, cat_l2, pref_key, pref_name, seen_pros
                )

    def _crawl_area(
        self,
        list_root: str,
        job_type: dict,
        cat_l1: str,
        cat_l2: str,
        pref_key: str,
        pref_name: str,
        seen_pros: set[str],
    ):
        """1 小カテゴリ × 1 都道府県を、都道府県ページ → 市区町村ページの順に巡回する。"""
        # 1) 都道府県ページ。ここで市区町村リンクも回収する。
        city_urls: list[str] = []
        for item in self._crawl_pages(
            list_root, job_type, cat_l1, cat_l2, pref_name, seen_pros,
            city_sink=city_urls, is_city_page=False,
        ):
            yield item

        # 2) 市区町村ページ。都道府県ページは 50 件が上限なので、取りこぼしを拾う。
        for city_url in city_urls:
            for item in self._crawl_pages(
                city_url, job_type, cat_l1, cat_l2, pref_name, seen_pros,
                city_sink=None, is_city_page=True,
            ):
                yield item

    def _crawl_pages(
        self,
        list_url: str,
        job_type: dict,
        cat_l1: str,
        cat_l2: str,
        pref_name: str,
        seen_pros: set[str],
        city_sink: list[str] | None,
        is_city_page: bool,
    ):
        """1 エリアページのページ送りを巡回し、対象県のプロを都度 yield する。"""
        total = None
        category: dict = {}
        for page in range(1, self.MAX_PAGES + 1):
            page_url = list_url if page == 1 else f"{list_url}?page={page}"
            soup = self._fetch(page_url)
            if soup is None:
                return

            data = self._next_data(soup)
            if data.get("page") == "/404":
                # 掲載が無いカテゴリ×エリアの組み合わせ、またはページ送り上限
                return
            page_props = data.get("props", {}).get("pageProps", {})
            pros = (
                page_props.get("initialState", {})
                .get("marketingPages", {})
                .get("pros")
                or []
            )
            if not pros:
                return

            if page == 1:
                if total is None:
                    total = page_props.get("totalPros") or 0
                category = self._build_category(soup, job_type, cat_l1, cat_l2)
                if city_sink is not None:
                    city_sink.extend(self._city_links(soup, list_url))
            # category は page==1 で必ず作られる (page ループの先頭で必ず通過する)

            hit_in_target = False
            for pro in pros:
                pro_id = pro.get("id") or pro.get("seoSlug") or pro.get("slug") or ""
                # 一覧 JSON の在住地で先に絞り込み、対象外の詳細ページは取得しない
                if not self._is_target_by_list(pro):
                    continue
                hit_in_target = True
                if not pro_id or pro_id in seen_pros:
                    continue
                seen_pros.add(pro_id)

                detail_url = self._detail_url(page_url, pro, job_type)
                if not detail_url:
                    continue
                item = self._scrape_detail(detail_url)
                if item is None:
                    continue
                item.update(category)
                yield item

            # ページ送り継続判定
            if total and page * self.PAGE_SIZE >= total:
                return
            if is_city_page and page >= self.MIN_CITY_PAGES and not hit_in_target:
                # 市区町村ページは近い順に並ぶため、対象県のプロが尽きたら打ち切る
                return

    # ------------------------------------------------------------------
    # 一覧ページからの補助情報
    # ------------------------------------------------------------------
    @staticmethod
    def _is_target_by_list(pro: dict) -> bool:
        """一覧 JSON の location.title (「福岡県福岡市…」) で対象県か判定する。"""
        title = ((pro.get("location") or {}).get("title") or "").strip()
        return any(title.startswith(name) for name in TARGET_PREF_NAMES)

    @staticmethod
    def _detail_url(page_url: str, pro: dict, job_type: dict) -> str:
        """/profile/{seoSlug}/pro/{jobTypeId} を組み立てる。"""
        slug = (pro.get("seoSlug") or pro.get("slug") or "").strip()
        if not slug:
            return ""
        job_type_id = job_type.get("_id") or ""
        path = f"/profile/{slug}/pro/{job_type_id}" if job_type_id else f"/profile/{slug}/pro"
        return urljoin(page_url, path)

    @staticmethod
    def _city_links(soup: BeautifulSoup, list_url: str) -> list[str]:
        """都道府県ページから市区町村ページ (1 階層下) のリンクを回収する。"""
        base_path = urlparse(list_url).path.rstrip("/")
        found: list[str] = []
        seen: set[str] = set()
        for a in soup.select(f'a[href^="{base_path}/"]'):
            href = (a.get("href") or "").split("?")[0].split("#")[0].rstrip("/")
            # base_path の直下 1 階層のみ (市区町村 or 市区町村グループ)
            if href in seen or href.count("/") != base_path.count("/") + 1:
                continue
            seen.add(href)
            found.append(urljoin(list_url, href))
        return found

    def _build_category(
        self, soup: BeautifulSoup, job_type: dict, cat_l1: str, cat_l2: str
    ) -> dict:
        """業種カラム 5 つを組み立てる。

        大/中カテゴリ名は categoryTree を優先し、載っていない c1 (執筆 等) は
        一覧ページのパンくずから補完する。
        """
        crumbs = self._breadcrumb_titles(soup)
        l1_title = (
            self._cat_titles.get(cat_l1)
            or crumbs.get(f"/business/{cat_l1}")
            or cat_l1
        )
        l2_title = (
            self._cat_titles.get(f"{cat_l1}/{cat_l2}")
            or crumbs.get(f"/business/{cat_l1}/{cat_l2}")
            or cat_l2
        )
        translations = job_type.get("translations") or {}
        l3_title = translations.get("seoName") or job_type.get("_id") or ""
        detail_title = (
            translations.get("requestType")
            or translations.get("newRequestFormTitle")
            or ""
        )
        site_category = " > ".join(
            v for v in ("ビジネス", l1_title, l2_title, l3_title) if v
        )
        return {
            Schema.CAT_LV1: l1_title,
            Schema.CAT_LV2: l2_title,
            Schema.CAT_LV3: l3_title,
            Schema.CAT_NM: detail_title,
            Schema.CAT_SITE: site_category,
        }

    @staticmethod
    def _breadcrumb_titles(soup: BeautifulSoup) -> dict[str, str]:
        """パンくずから {href: 表示名} を作る。"""
        titles: dict[str, str] = {}
        for a in soup.select('[data-test-id="breadcrumbs"] a[href]'):
            href = (a.get("href") or "").rstrip("/")
            text = a.get_text(strip=True)
            if href and text and href not in titles:
                titles[href] = text
        return titles

    # ------------------------------------------------------------------
    # 詳細ページ
    # ------------------------------------------------------------------
    def _scrape_detail(self, url: str) -> dict | None:
        soup = self._fetch(url)
        if soup is None:
            return None

        page_props = self._next_data(soup).get("props", {}).get("pageProps", {})
        pro = (
            page_props.get("initialState", {})
            .get("users", {})
            .get("profiles", {})
            .get("pro")
        )
        if not isinstance(pro, dict):
            logger.warning("プロフィール情報が見つかりません: %s", url)
            return None

        profile = pro.get("profile") or {}
        pro_profile = pro.get("proProfile") or {}
        location = pro.get("location") or {}

        # 詳細ページの在住都道府県で最終判定する (一覧の絞り込みは前段の予選)
        pref = (location.get("prefecture") or {}).get("name", "").strip()
        if pref not in TARGET_PREF_NAMES:
            logger.debug("対象外の都道府県のため除外: %s (%s)", url, pref or "不明")
            return None

        name = (
            pro.get("name")
            or profile.get("companyNameKanji")
            or self._join_name(profile.get("fullNameKanji"))
        )
        if not name:
            return None

        local_business = page_props.get("localBusiness") or {}
        stats = local_business.get("reviewStatistics") or {}
        rating = stats.get("ratingValue")
        if rating is None:
            rating = pro_profile.get("rating")
        review_count = stats.get("reviewCount")
        if review_count is None:
            counts = pro.get("reviews", {}).get("counts") or []
            review_count = sum(counts) if counts else ""

        website = (pro_profile.get("website") or "").strip()
        instagram = self._pick_instagram(pro_profile, website)

        return {
            Schema.NAME: name,
            Schema.NAME_KANA: profile.get("companyNameFurigana")
            or self._join_name(profile.get("fullNameFurigana")),
            Schema.PREF: pref,
            Schema.POST_CODE: self._format_zip(
                location.get("zipCode") or (pro_profile.get("location") or {}).get("zipCode")
            ),
            Schema.ADDR: self._build_address(location, pro_profile, pref),
            Schema.REP_NM: self._join_name(profile.get("fullNameKanji")),
            Schema.LOB: self._enabled_job_titles(pro),
            Schema.INSTA: instagram,
            Schema.HP: "" if website == instagram else website,
            Schema.URL: url,
            "rating": rating if rating is not None else "",
            "review_count": review_count if review_count is not None else "",
            "area_text": self._area_text(pro),
        }

    # ------------------------------------------------------------------
    # 値の整形
    # ------------------------------------------------------------------
    @staticmethod
    def _build_category_titles(page_props: dict) -> dict[str, str]:
        """categoryTree から {c1key: 名称, "c1key/c2key": 名称} の辞書を作る。"""
        titles: dict[str, str] = {}
        children = (page_props.get("categoryTree") or {}).get("children") or {}
        for l1_key, l1 in children.items():
            if not isinstance(l1, dict):
                continue
            titles[l1_key] = l1.get("title") or l1_key
            for l2_key, l2 in (l1.get("children") or {}).items():
                if isinstance(l2, dict):
                    titles[f"{l1_key}/{l2_key}"] = l2.get("title") or l2_key
        return titles

    @staticmethod
    def _build_address(location: dict, pro_profile: dict, pref: str) -> str:
        """住所 (市区町村以降) を組み立てる。

        location の city + townArea を優先し、無ければ
        proProfile.location.title (「福岡県福岡市 中央区地行」) から都道府県を除いて使う。
        """
        city = (location.get("city") or {}).get("name", "").strip()
        town = (location.get("townArea") or {}).get("name", "").strip()
        if city:
            return f"{city}{town}"
        title = ((pro_profile.get("location") or {}).get("title") or "").strip()
        if title.startswith(pref):
            return title[len(pref):]
        return title

    @staticmethod
    def _join_name(name: dict | None) -> str:
        """{"last": "高山", "first": "直子"} を「高山 直子」に整形する。"""
        if not isinstance(name, dict):
            return ""
        last = (name.get("last") or "").strip()
        first = (name.get("first") or "").strip()
        return " ".join(p for p in (last, first) if p)

    @staticmethod
    def _format_zip(value: str | None) -> str:
        """8100064 → 810-0064。想定外の形式はそのまま返す。"""
        raw = re.sub(r"\D", "", value or "")
        m = _ZIP_RE.match(raw)
        return f"{m.group(1)}-{m.group(2)}" if m else (value or "")

    @staticmethod
    def _enabled_job_titles(pro: dict) -> str:
        """掲載中のサービス名 (enabledJobTypes の title) を連結して事業内容とする。"""
        titles: list[str] = []
        for job in pro.get("enabledJobTypes") or []:
            title = (job or {}).get("title")
            if title and title not in titles:
                titles.append(title)
        return "、".join(titles)

    @staticmethod
    def _area_text(pro: dict) -> str:
        """ページ上の「対応可能エリア」表記を再現する (例: 福岡県(72)、熊本県(49))。"""
        parts: list[str] = []
        for pref in pro.get("availablePrefectures") or []:
            name = (pref or {}).get("name")
            if not name:
                continue
            count = (pref or {}).get("citiesCount")
            parts.append(f"{name}({count})" if count is not None else str(name))
        return "、".join(parts)

    @staticmethod
    def _pick_instagram(pro_profile: dict, website: str) -> str:
        """website 欄、無ければ自己紹介テキスト中の Instagram URL を拾う。

        テキスト本文はカラムに出力せず、URL のみを抽出する。
        ゼヒトモ公式アカウント (instagram.com/zehitomo) は除外する。
        """
        candidates = [website]
        for key in ("description", "skills", "interest", "title"):
            value = pro_profile.get(key)
            if isinstance(value, str):
                candidates.extend(_INSTAGRAM_RE.findall(value))
        for candidate in candidates:
            if not candidate or "instagram.com" not in candidate:
                continue
            account = candidate.rstrip("/")
            if account.rsplit("/", 1)[-1].lower() == "zehitomo":
                continue
            return account
        return ""


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Zehitomo2()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.zehitomo.com/business/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
