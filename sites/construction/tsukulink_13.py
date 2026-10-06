"""
ツクリンク【佐賀】 (tsukulink.net) — 佐賀県の建設業者スクレイパー (掲載全社)

同一サイトの既存クローラー (北陸 tsukulink_4/_5、全国 tsukulink_6、関東 tsukulink_7、
佐賀プレミアム限定 tsukulink_8、長崎 tsukulink_10、近畿 tsukulink_12) と解析ロジックは
共通。tsukulink_8 が「佐賀県のプレミアム会員のみ」なのに対し、本クローラーは
**佐賀県に掲載されている全社** を対象とする点が異なる (備考にプレミアム限定の指示なし)。

取得対象:
    佐賀県の公開企業すべて。2026-10-06 時点の母集団は 5,336 社 (20 件/ページ = 267 ページ)。

起点 URL について (重要):
    sites.yml の url は ``https://tsukulink.net/saga/city_`` で、これは単体では 404 を返す
    **市区町村ページ (/saga/city_412082 等) のプレフィックス**である。
    parse() はこの url を唯一の正として扱い、
      1. 親パス (= 県インデックス /saga/) を url から導出して取得
      2. ``{url}{数字}`` に一致するリンク (= 市区町村一覧) を列挙
    という形でルートを導出する。別 URL のハードコードはしない。

取得フロー:
    1. 引数 url から県インデックス URL を導出し、市区町村ルート 20 件を列挙する
    2. [県インデックス, 市区町村×20] の各ルートを {root}?page=N で 1 ページずつ取得
       (県インデックス 5,336 社 ⊃ 市区町村合計 5,307 社。市区町村が未設定の 29 社は
        県インデックスにしか出ないので県インデックスが母集団の正。
        市区町村側はページ送りの打ち止め等で県インデックスが欠けた場合の保険として
        巡回し、会社ID で重複排除する)
    3. 詳細ページを 1 件取得するごとに即 yield (Pattern B / 全件バッファしない)
    4. 会社ID で重複排除し、詳細ページの住所が佐賀県でないものは除外する

備考への対応:
    - 「取得カラム: 名称/都道府県/住所/代表者名/HP/企業ラベル (プレミアム等の会員ラベル
      表記を含む)/認証・許可ラベル/サイト定義業種・ジャンル」
      → Schema.NAME / PREF / ADDR / REP_NM / HP / CAT_SITE と
        EXTRA「企業ラベル」「企業ラベルclass原値」「認証・許可ラベル」。
        会員ラベル (プレミアム等) は絞り込みに使わず、表示文字列を原値で保持する。
        許可系の構造化値として建設業許可番号・許可業種 (一般/特定)、
        名寄せ補助として郵便番号・会社ID・市区町村も併せて取得する
    - 「TEL は会員ログイン後のみ表示のためサイト非公開、取得不可でよい」
      → Schema.TEL は常に空文字 (セレクタの取りこぼしではない)
    - 「連続アクセスで HTTP400 が多発する / 低並列・リクエスト間隔・ブラウザ相当ヘッダ」
      → StaticCrawler の逐次アクセス (並列なし) + DELAY = 1.5 秒 +
        HTTP リクエスト単位の最小間隔 _REQUEST_INTERVAL + 指数バックオフ付き
        上限つきリトライ + _setup() でブラウザ相当の UA / Accept / Accept-Language を付与

サイト仕様メモ:
    - 一覧・詳細とも Accept ヘッダが無いと HTTP 400 を返すため _setup() で付与する
    - 一覧のページ送りは {root}?page=N。範囲外ページは 200 + 0 件で返るので終端判定に使う。
      総件数は .c-pagination-entries__total
    - 一覧カードは li.p-companies-list-item、会社ID は data-company-id 属性、
      詳細リンクは a.p-companies-list-item__name
    - 詳細ページの会社概要は table/dl ではなく
      h3.p-companies-show-detail__heading → h4.--small → h5.--xs と続き、
      見出しの後続兄弟要素が値という見出し駆動レイアウト
    - 企業ラベル・認証ラベルはサイドバーの「おすすめ会社」カードにも同じ class で出るため、
      必ず .p-companies-show-profile 配下 (一覧では li 配下) に限定して拾う
    - 認証ラベルは未認証でも span が出力され class に not-certified が付く
      (= 付いていないものだけが認証済み)
    - 住所は 〒8490302 のように郵便番号が前置され、ハイフンが無い会社もある
    - 「ウェブサイト」欄は未公開の会社で案内文が入るため URL 形式のみ採用する
    - 充実した会社は JSON-LD (Corporation) を持ち、郵便番号・都道府県・市区町村・
      ウェブサイト URL を構造化データで補完できる。無い会社は本文から取る
    - 利用規約 (/term) にスクレイピング・クローリングの禁止条項は無く
      (2026-10-06 時点で「スクレイピング」「クローリング」「自動」「収集」の語は不出現)、
      robots.txt も県別一覧・会社詳細を許可している

著作権配慮 (取得しない項目):
    ご挨拶 / 事業内容 / 担当者メッセージ / 主要取引先 / 募集案件本文 は
    自由記述の長文プロースのため取得しない。

実行方法:
    # ローカルテスト
    python scripts/sites/construction/tsukulink_13.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id tsukulink_13
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

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

# 住所から都道府県を切り出す。〒付き郵便番号が前置されることがあり
# 「.+?[都道府県]」のようなワイルドカードでは番号側を誤って拾うため明示列挙する。
_PREFECTURES = (
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県",
    "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
)
_PREF_PATTERN = re.compile("(" + "|".join(_PREFECTURES) + ")")

# 備考「佐賀県の建設業者一覧」で指定された対象都道府県
_TARGET_PREF = "佐賀県"

# 郵便番号 (〒849-0302 / 〒8490302 の双方)
_POSTCODE_RE = re.compile(r"〒?\s*(\d{3})-?(\d{4})")

# ページ送りの安全上限 (2026-10 時点の佐賀県インデックスは 267 ページ)
_MAX_PAGES = 1000

# 2026-10 時点で佐賀県に存在する市区町村ページ数。極端に減ったら構造変化を疑う
_MIN_CITY_ROOTS = 15

# 備考「連続アクセスで HTTP400 が多発する」対応のリクエスト間隔 (秒)。
# フレームワークの DELAY はアイテム書き出し間にしか効かず、一覧ページの連続取得は
# 素通しになるため、HTTP リクエスト単位の間隔をクローラー側で確保する。
_REQUEST_INTERVAL = 1.2

# ウェブサイト欄は未公開の会社で「現在公開済み自社ホームページはございません」等の
# 案内文が入る。URL 形式のものだけを HP として採用する。
_URL_RE = re.compile(r"https?://\S+")


class TsukulinkSagaAll(StaticCrawler):
    """ツクリンク【佐賀】 — 佐賀県の掲載企業 (全社) スクレイパー"""

    # 備考「連続アクセスで HTTP400 が多発する実績」への対応 (逐次 + 間隔)
    DELAY = 1.5
    # 通信エラーは get_soup から None で受け取り、_fetch 側で上限つきリトライを行う
    CONTINUE_ON_ERROR = True

    # 1 URL あたりのリトライ回数 (指数バックオフ)。上限到達時は None を返す
    FETCH_ATTEMPTS = 4

    # _fetch() のリクエスト間隔計測用 (parse 開始時にリセットする)
    _last_request_at = 0.0

    EXTRA_COLUMNS = [
        "会社ID",
        "市区町村",
        "企業ラベル",
        "企業ラベルclass原値",
        "認証・許可ラベル",
        "建設業許可番号",
        "建設業許可業種(一般)",
        "建設業許可業種(特定)",
        "業種",
    ]

    # ------------------------------------------------------------------ setup

    def _setup(self):
        """Accept ヘッダが無いと HTTP 400 を返すためブラウザ相当のヘッダを付与する。"""
        super()._setup()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
                ),
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,"
                    "image/avif,image/webp,*/*;q=0.8"
                ),
                "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
                "Upgrade-Insecure-Requests": "1",
            }
        )

    # --------------------------------------------------------------- fetch

    def _fetch(self, url: str):
        """リクエスト間隔を空けて取得し、失敗時は上限つき指数バックオフで再試行する。

        バースト由来の HTTP 400 / 一時的な 5xx を想定。FETCH_ATTEMPTS 回すべて
        失敗した場合は None を返し、呼び出し側で打ち切り/スキップを判断する
        (無限リトライはしない)。
        """
        for attempt in range(self.FETCH_ATTEMPTS):
            wait = _REQUEST_INTERVAL - (time.monotonic() - self._last_request_at)
            if wait > 0:
                time.sleep(wait)
            try:
                soup = self.get_soup(url)
            finally:
                self._last_request_at = time.monotonic()
            if soup is not None:
                return soup
            if attempt < self.FETCH_ATTEMPTS - 1:
                backoff = min(2 ** (attempt + 1), 30)
                logger.warning(
                    "取得に失敗したため %s 秒待って再試行 (%s/%s): %s",
                    backoff, attempt + 1, self.FETCH_ATTEMPTS - 1, url,
                )
                time.sleep(backoff)
        logger.warning("リトライ上限に到達したため取得を断念: %s", url)
        return None

    # ------------------------------------------------------------------ parse

    def parse(self, url: str):
        """佐賀県の掲載企業を、県インデックスと全市区町村一覧から取得する。"""
        self._last_request_at = 0.0
        roots = self._list_roots(url)
        seen_ids: set[str] = set()

        for root in roots:
            for page in range(1, _MAX_PAGES + 1):
                soup = self._fetch(f"{root}?page={page}")
                if soup is None:
                    raise RuntimeError(f"一覧の取得に失敗: {root} page={page}")

                items = soup.select("li.p-companies-list-item")
                if not items:
                    # 範囲外ページは 200 + 0 件で返る = 終端
                    logger.info("一覧の終端に到達: %s (page=%s)", root, page)
                    break

                if page == 1:
                    self._log_total(soup, root)

                for li in items:
                    company_id = (li.get("data-company-id") or "").strip()
                    link = li.select_one("a.p-companies-list-item__name[href]")
                    if not link:
                        continue
                    href = link.get("href", "")
                    if not href:
                        continue
                    if company_id and company_id in seen_ids:
                        # 県インデックスと市区町村一覧に重複掲載されるため ID で排除
                        continue
                    if company_id:
                        seen_ids.add(company_id)

                    detail_url = urljoin(root, href)
                    # 一覧カードのラベル原値 (詳細側に出ない会社があるためマージ用)
                    list_labels = _header_labels(li)
                    try:
                        item = self._scrape_detail(detail_url, company_id, list_labels)
                    except Exception as e:  # 個別アイテムの失敗は記録して継続
                        logger.warning("詳細の解析に失敗 (スキップ): %s — %s", detail_url, e)
                        continue
                    if not item:
                        logger.warning("詳細を取得できないためスキップ: %s", detail_url)
                        continue

                    # 佐賀県の一覧由来だが、詳細の住所が県外の会社は除外する
                    # (住所非公開で県が取れない会社は佐賀県一覧掲載を根拠に残す)
                    if item[Schema.PREF] and item[Schema.PREF] != _TARGET_PREF:
                        logger.debug("佐賀県外のため除外: %s (%s)", detail_url, item[Schema.PREF])
                        continue

                    yield item
            else:
                raise RuntimeError(f"ページ上限 {_MAX_PAGES} に到達: {root}")

    # ------------------------------------------------------- list roots

    def _list_roots(self, url: str) -> list[str]:
        """引数 url (市区町村ページのプレフィックス) から巡回すべき一覧ルートを導出する。

        url = https://tsukulink.net/saga/city_ は単体では 404 なので、
        親パス (= 県インデックス) を取得して ``{url}{数字}`` 形式のリンクを拾う。
        """
        prefix = url.rstrip("/")
        pref_index = prefix.rsplit("/", 1)[0] + "/"

        index = self._fetch(pref_index)
        if index is None:
            raise RuntimeError(f"県インデックスの取得に失敗: {pref_index}")

        prefix_path = urlparse(prefix).path
        city_pattern = re.compile(re.escape(prefix_path) + r"\d+/?$")

        city_roots = sorted(
            {
                urljoin(pref_index, a.get("href", "")).rstrip("/")
                for a in index.select("a[href]")
                if city_pattern.fullmatch(urlparse(urljoin(pref_index, a.get("href", ""))).path)
            }
        )
        if len(city_roots) < _MIN_CITY_ROOTS:
            raise RuntimeError(f"市区町村ページの列挙に失敗: {len(city_roots)} 件")
        logger.info("市区町村ページ: %s 件", len(city_roots))

        # 県インデックスが母集団の正 (市区町村未設定の会社はここにしか出ない)。
        # 市区町村側は取りこぼしの保険として後に回す (会社ID で重複排除される)。
        return [pref_index.rstrip("/"), *city_roots]

    def _log_total(self, soup, root: str) -> None:
        total_el = soup.select_one(".c-pagination-entries__total")
        if not total_el:
            return
        digits = re.sub(r"[^0-9]", "", total_el.get_text())
        if not digits:
            return
        logger.info("掲載企業数 (母集団): %s 件 — %s", digits, root)
        if self.total_items is None or int(digits) > (self.total_items or 0):
            self.total_items = int(digits)

    # ----------------------------------------------------------- detail page

    def _scrape_detail(self, url: str, company_id: str, list_labels: dict) -> dict | None:
        soup = self._fetch(url)
        if soup is None:
            return None

        name_el = soup.select_one("h1.p-companies-show-profile__title")
        if not name_el:
            logger.warning("会社名が見つからないためスキップ: %s", url)
            return None
        name = _clean(name_el.get_text(" ", strip=True))
        if not name:
            return None

        blocks = _labeled_blocks(soup)
        ld = _corporation_ld(soup)
        ld_addr = ld.get("address") or {}

        # --- 住所 / 郵便番号 / 都道府県 / 市区町村 --------------------------
        raw_addr = _text_of(soup, ".p-companies-show-profile__info-address")
        post_code = _normalize_postcode(ld_addr.get("postalCode") or "") or _normalize_postcode(raw_addr)
        address = _clean(_POSTCODE_RE.sub("", raw_addr, count=1))

        breadcrumbs = [
            _clean(a.get_text(" ", strip=True))
            for a in soup.select("ol.breadcrumbs .breadcrumbs__link")
        ]
        bc_pref = next((b for b in breadcrumbs if b in _PREFECTURES), "")

        m = _PREF_PATTERN.search(address)
        pref = (m.group(1) if m else "") or _clean(ld_addr.get("addressRegion") or "") or bc_pref

        city = _clean(ld_addr.get("addressLocality") or "")
        if not city and bc_pref:
            # パンくずは 「… > 佐賀県 > 佐賀市 > 会社名」 の並び
            idx = breadcrumbs.index(bc_pref)
            if idx + 1 < len(breadcrumbs):
                city = breadcrumbs[idx + 1]

        # --- 企業ラベル (プレミアム等の会員ラベル表記を含む) ------------------
        # サイドバー「おすすめ会社」にも同じ class のバッジが出るため、
        # 必ず .p-companies-show-profile 配下に限定する。
        # レイアウト差で詳細側に出ない会社があるので一覧側の原値とマージする。
        profile = soup.select_one(".p-companies-show-profile__info") or soup.select_one(
            ".p-companies-show-profile"
        )
        detail_labels = _header_labels(profile) if profile else {"texts": [], "classes": []}
        label_texts = _merge_unique(list_labels.get("texts", []), detail_labels["texts"])
        label_classes = _merge_unique(list_labels.get("classes", []), detail_labels["classes"])

        # --- 認証・許可ラベル (not-certified = 未認証なので除外) -------------
        certified_labels = _join_texts(
            span
            for span in (
                profile.select(".c-companies-certified-labels span.c-label-black-stroke")
                if profile
                else []
            )
            if "not-certified" not in (span.get("class") or [])
        )

        # --- HP -------------------------------------------------------------
        hp = ""
        for el in blocks.get(("会社概要", "ウェブサイト", ""), []):
            a = el.select_one("a[href]") if hasattr(el, "select_one") else None
            if a:
                hp = _as_url(a.get("href", ""))
                if hp:
                    break
        if not hp:
            # 未公開の会社は「現在公開済み自社ホームページはございません」という
            # 案内文が入るため、URL 形式でなければ空にする
            hp = _as_url(_first_text(blocks, ("会社概要", "ウェブサイト", ""))) or _as_url(
                ld.get("url") or ""
            )

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: address,
            # 備考どおり TEL は会員限定で公開ページに存在しないため常に空
            Schema.TEL: "",
            Schema.REP_NM: _strip_company_suffix(
                _joined(blocks, ("会社概要", "代表者", ""), sep=" "), name
            ),
            Schema.HP: hp,
            # サイト定義業種: プロフィール直下の業種表記 (例「管、電気」)
            Schema.CAT_SITE: _text_of(soup, ".p-companies-show-profile__info-job-type"),
            "会社ID": company_id,
            "市区町村": city,
            "企業ラベル": "、".join(label_texts),
            "企業ラベルclass原値": " / ".join(label_classes),
            "認証・許可ラベル": certified_labels,
            "建設業許可番号": _first_text(blocks, ("許認可", "建設業許可", "")),
            "建設業許可業種(一般)": _tidy(_joined(blocks, ("許認可", "建設業許可", "一般"))),
            "建設業許可業種(特定)": _tidy(_joined(blocks, ("許認可", "建設業許可", "特定"))),
            # 会社情報セクションの業種 (例「管工事業 電気工事業」)
            "業種": _tidy(_joined(blocks, ("会社情報", "業種", ""))),
        }


# ---------------------------------------------------------------- helpers


def _clean(text: str) -> str:
    """連続空白・NBSP を 1 スペースに畳む。"""
    return re.sub(r"[\s　\xa0]+", " ", text or "").strip()


def _tidy(text: str) -> str:
    """「A 、 B、」のように区切り前後の空白・末尾区切りが残る値を整形する。"""
    text = re.sub(r"\s*、\s*", "、", text or "")
    return text.strip("、 ")


def _text_of(soup, selector: str) -> str:
    el = soup.select_one(selector)
    return _clean(el.get_text(" ", strip=True)) if el else ""


def _join_texts(elements, sep: str = "、") -> str:
    return sep.join(t for t in (_clean(e.get_text(" ", strip=True)) for e in elements) if t)


def _header_labels(scope) -> dict:
    """企業ラベル (受発注区分 / プレミアム 等) の表示文字列と CSS クラスを原値で返す。

    scope には必ず一覧の li か詳細のプロフィール領域 (.p-companies-show-profile) を渡すこと。
    ページ全体を走査すると「おすすめ会社」カードのバッジを拾ってしまう。
    """
    texts: list[str] = []
    classes: list[str] = []
    for container in scope.select(".c-companies-header-labels__container"):
        for span in container.find_all("span", recursive=False):
            txt = _clean(span.get_text(" ", strip=True))
            cls = " ".join(span.get("class") or [])
            if txt and txt not in texts:
                texts.append(txt)
            if cls and cls not in classes:
                classes.append(cls)
    return {"texts": texts, "classes": classes}


def _merge_unique(*lists) -> list[str]:
    """複数の原値リストを順序を保ったまま重複なしで結合する。"""
    out: list[str] = []
    for values in lists:
        for v in values or []:
            if v and v not in out:
                out.append(v)
    return out


def _labeled_blocks(soup) -> dict:
    """詳細ページの見出し駆動レイアウトを {(h3, h4, h5): [値要素...]} に変換する。

    h3 (大見出し) / h4 (項目名) / h5 (一般・特定等) はいずれも
    div.p-companies-show__section の直下に兄弟として並んでおり、
    見出しの後ろに続く要素がその項目の値になる (table も dl も使われていない)。
    """
    blocks: dict[tuple[str, str, str], list] = {}
    for section in soup.select("div.p-companies-show__section"):
        h3 = h4 = h5 = ""
        for el in section.find_all(recursive=False):
            if el.name == "h3":
                h3, h4, h5 = _clean(el.get_text(" ", strip=True)), "", ""
            elif el.name == "h4":
                h4, h5 = _clean(el.get_text(" ", strip=True)), ""
            elif el.name == "h5":
                h5 = _clean(el.get_text(" ", strip=True))
            elif el.name in ("hr", "script", "style"):
                continue
            elif h4 or h3:
                blocks.setdefault((h3, h4, h5), []).append(el)
    return blocks


def _values(blocks: dict, key: tuple) -> list[str]:
    """指定見出しに属する値要素のテキスト (マスク注記・補足説明は除く) を返す。"""
    out = []
    for el in blocks.get(key, []):
        classes = el.get("class") or []
        if any("masked" in c or "description" in c for c in classes):
            continue
        txt = _clean(el.get_text(" ", strip=True))
        if txt:
            out.append(txt)
    return out


def _first_text(blocks: dict, key: tuple) -> str:
    vals = _values(blocks, key)
    return vals[0] if vals else ""


def _joined(blocks: dict, key: tuple, sep: str = "、") -> str:
    return sep.join(_values(blocks, key))


def _as_url(text: str) -> str:
    """URL 形式のときだけ値を返す (未公開案内文などのプレーンテキストは捨てる)。"""
    m = _URL_RE.search(_clean(text))
    return m.group(0) if m else ""


def _normalize_postcode(text: str) -> str:
    """「〒849-0302」「8490302」いずれも 849-0302 形式に正規化する。"""
    m = _POSTCODE_RE.search(_clean(text))
    return f"{m.group(1)}-{m.group(2)}" if m else ""


def _strip_company_suffix(value: str, company_name: str) -> str:
    """「小川 博司 株式会社オリバー」のように末尾へ会社名が連結された値から会社名を除く。"""
    if value and company_name and value.endswith(company_name) and value != company_name:
        return value[: -len(company_name)].strip()
    return value


def _corporation_ld(soup) -> dict:
    """JSON-LD の Corporation (住所・ウェブサイト) を返す。無ければ空 dict。"""
    for script in soup.select('script[type="application/ld+json"]'):
        raw = script.string or script.get_text()
        if not raw or "Corporation" not in raw:
            continue
        try:
            data = json.loads(raw)
        except (ValueError, TypeError):
            continue
        if isinstance(data, dict) and data.get("@type") == "Corporation":
            return data
    return {}


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    scraper = TsukulinkSagaAll()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://tsukulink.net/saga/city_")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
