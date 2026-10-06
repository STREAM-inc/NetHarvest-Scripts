"""
ツクリンク（近畿） (tsukulink.net) — 近畿 6 府県の建設業者スクレイパー

取得対象:
    備考に従い、近畿 6 府県
    (大阪府・京都府・兵庫県・奈良県・滋賀県・和歌山県) を拠点とする
    建設業者のみを取得する (三重県は対象外)。
    2026-10 時点 大阪 57,872 / 兵庫 30,288 / 京都 16,288 / 滋賀 8,582 /
    奈良 7,727 / 和歌山 7,030 = 約 127,787 社 (20 件/ページ = 約 6,390 ページ)。

取得フロー:
    1. 引数 url (https://tsukulink.net/osaka) を起点に、同一オリジンの
       県別一覧 URL (/kyoto /hyogo /nara /shiga /wakayama) を urljoin で導出する
       (県ページには他県への都道府県ナビが無いためスラッグから組み立てる)
    2. 6 府県の一覧 {pref_url}?page=N をラウンドロビンで 1 ページ (20 件) ずつ取得
       (大阪だけで 2,894 ページあるため、府県を順に回して近畿全体へ分散させる)
    3. li.p-companies-list-item から会社ID・詳細URLを拾う
    4. 詳細ページを 1 件取得するごとに即 yield (Pattern B)
    5. 会社IDで重複排除し、詳細ページの住所が近畿 6 府県でないものは除外する

サイト仕様メモ:
    - 一覧・詳細とも Accept ヘッダが無いと HTTP 400 を返すため _setup() で付与する
    - 備考「連続アクセスで 400」に対応するため、_fetch_soup() で
      リクエスト間隔を REQUEST_INTERVAL 秒 (既定 1.5 秒) 以上空け、
      さらにセッションの自動リトライ対象に 400 / 429 を追加している
      (フレームワークの DELAY はアイテム yield 間にしか効かないため別途実装)
    - 県別一覧の ?page=N は範囲外になると 200 + 0 件で返るので終端判定に使う
      (ページ上限・件数 cap は無く、最終ページまで 20 件ずつ返る)
    - 詳細ページの会社概要は table/dl ではなく
      h3.p-companies-show-detail__heading (大見出し)
      → h4.p-companies-show-detail__heading--small (項目名)
      → h5.p-companies-show-detail__heading--xs (一般/特定)
      → 続く兄弟要素が値 という見出し駆動レイアウト。
      見出し・値はいずれも div.p-companies-show__section の直下に並ぶので、
      このセクションに限定して走査する (限定しないとページ下部の
      「◯◯府の建設会社」= 他社カードまで拾ってしまう)
    - ヘッダーの認証ラベルは未認証でも span が出力され class に not-certified が付く
      (= 付いていないものだけが認証済み)。ラベル取得は
      .p-companies-show-profile__info 配下に限定する (他社カード混入の防止)
    - 充実した会社は JSON-LD (Corporation) を持ち、郵便番号・都道府県・市区町村・
      設立年月日・従業員数を構造化データで取得できる。無い会社は本文から補完する
    - 「インボイス登録の有無」の h4 は未登録企業では出力されないため、
      ヘッダーの認証ラベル側から判定する
    - 電話番号・FAX・法人番号は会員限定マスクのため公開ページに存在しない (常に空)
    - 利用規約 (/term) 第6条 禁止事項にスクレイピング・クローリングの禁止条項は無い
      (2026-10 時点で「スクレイピング」「クローリング」「自動」「収集」の語は不出現)

著作権配慮 (取得しない項目):
    ご挨拶 / 事業内容 / 担当者メッセージ / 協力業者・元請業者の募集案件本文 は
    自由記述の長文プロースのため取得しない。

実行方法:
    # ローカルテスト
    python scripts/sites/construction/tsukulink_12.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id tsukulink_12
"""

import json
import logging
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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

# 備考で指定された対象府県 (三重県は対象外)。key = 正式名 / value = URL スラッグ。
# 掲載社数の多い順ではなく、この定義順でラウンドロビンする。
_KINKI_PREFS = {
    "大阪府": "osaka",
    "京都府": "kyoto",
    "兵庫県": "hyogo",
    "奈良県": "nara",
    "滋賀県": "shiga",
    "和歌山県": "wakayama",
}

# 詳細ページ URL (/osaka/city_271161/990175)
_DETAIL_PATH = re.compile(r"^/[a-z]+/city_\d+/\d+$")

# 郵便番号 (〒544-0003 / 〒5300001)
_POSTCODE_RE = re.compile(r"〒?\s*(\d{3})-?(\d{4})")

# 設立年月日 (2002年04月01日)
_DATE_RE = re.compile(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日")

# 従業員数「204名 (施工管理職員数: 19名、…)」の内訳部分
_EMP_BREAKDOWN_RE = re.compile(r"^(.*?)\s*[（(](.+)[)）]\s*$")

# 1 府県あたりのページ送り安全上限 (2026-10 時点の最多が大阪府 2,894 ページ)
_MAX_PAGES = 10000


class TsukulinkKinki(StaticCrawler):
    """ツクリンク（近畿） — 近畿 6 府県の建設業者スクレイパー"""

    DELAY = 1.0

    # 備考「連続アクセスで 400 が出る」対策: HTTP リクエスト間の最小間隔 (秒)。
    # フレームワークの DELAY はアイテム yield 間にしか効かないため自前で制御する。
    REQUEST_INTERVAL = 1.5
    # 400 / 429 を踏んだ際のページ単位リトライ回数
    FETCH_ATTEMPTS = 4

    EXTRA_COLUMNS = [
        "会社ID",
        "市区町村",
        "評価点",
        "企業ラベル",
        "認証・許可ラベル",
        "本人確認",
        "業種の許認可確認",
        "保険加入状況",
        "インボイス登録有無",
        "建設業許可番号",
        "建設業許可業種(一般)",
        "建設業許可業種(特定)",
        "対応可能エリア",
    ]

    # ------------------------------------------------------------------ setup

    def _setup(self):
        """Accept ヘッダ付与 + 400/429 を含む自動リトライを設定する。"""
        super()._setup()
        self.session.headers.update(
            {
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,"
                    "image/avif,image/webp,*/*;q=0.8"
                ),
                "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            }
        )
        # 既定の status_forcelist は 5xx のみ。アクセス制限で返る 400 / 429 を加える。
        retries = Retry(
            total=4,
            backoff_factor=1.5,
            status_forcelist=[400, 429, 500, 502, 503, 504],
            respect_retry_after_header=True,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self._last_request_at = 0.0

    # ------------------------------------------------------------------ fetch

    def _fetch_soup(self, url: str):
        """REQUEST_INTERVAL 秒以上の間隔を空けて取得し、失敗時はバックオフして再試行する。

        トランスポート層のリトライ (400/429/5xx) を使い切って None が返った場合も、
        ページ単位でさらに FETCH_ATTEMPTS 回まで指数バックオフでやり直す。
        上限到達時は None を返し、呼び出し側で打ち切り/スキップを判断する。
        """
        for attempt in range(self.FETCH_ATTEMPTS):
            wait = self.REQUEST_INTERVAL - (time.monotonic() - self._last_request_at)
            if wait > 0:
                time.sleep(wait)
            soup = self.get_soup(url)
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
        """引数 url (大阪府の一覧) を唯一の起点に、近畿 6 府県の一覧→詳細を巡回する。"""
        pref_lists = self._kinki_list_urls(url)

        seen_ids: set[str] = set()
        # 府県ごとに「次に取得するページ番号」を持ち、ラウンドロビンで 1 ページずつ進める。
        # (大阪だけで 2,894 ページあるため、府県を順に回して近畿全体へ分散させる)
        pages = {pref: 1 for pref in pref_lists}
        total = 0

        while pages:
            for pref in list(pref_lists):
                page = pages.get(pref)
                if page is None:
                    continue
                if page > _MAX_PAGES:
                    logger.info("ページ上限に到達したため打ち切り: %s", pref)
                    pages.pop(pref)
                    continue

                soup = self._fetch_soup(f"{pref_lists[pref]}?page={page}")
                if soup is None:
                    logger.warning("一覧の取得に失敗したため次の府県へ: %s p%s", pref, page)
                    pages.pop(pref)
                    continue

                items = soup.select("li.p-companies-list-item")
                if not items:
                    # 範囲外ページは 200 + 0 件で返る = その府県の終端
                    logger.info("一覧の終端に到達: %s (page=%s)", pref, page)
                    pages.pop(pref)
                    continue

                if page == 1:
                    # 「N件中」表示から近畿 6 府県分の総件数を積み上げて ETA を有効にする
                    total_el = soup.select_one(".c-pagination-entries__total")
                    if total_el:
                        digits = re.sub(r"[^0-9]", "", total_el.get_text())
                        if digits:
                            total += int(digits)
                            self.total_items = total

                for li in items:
                    company_id = (li.get("data-company-id") or "").strip()
                    link = li.select_one("a.p-companies-list-item__name[href]")
                    if not link:
                        continue
                    href = link.get("href", "")
                    if not _DETAIL_PATH.match(href):
                        continue
                    if company_id and company_id in seen_ids:
                        # 複数府県の一覧に重複掲載される会社があるため ID で排除する
                        continue
                    if company_id:
                        seen_ids.add(company_id)

                    detail_url = urljoin(url, href)
                    try:
                        item = self._scrape_detail(detail_url, company_id, pref)
                    except Exception as e:  # 個別アイテムの失敗は記録して継続
                        logger.warning("詳細の解析に失敗 (スキップ): %s — %s", detail_url, e)
                        continue
                    if not item:
                        continue
                    # 備考「近畿 6 府県」: 詳細ページの住所が対象府県のものだけ採用する
                    # (三重県など対象外の府県を拠点とする会社はここで除外される)
                    if item[Schema.PREF] not in _KINKI_PREFS:
                        logger.debug("近畿外のため除外: %s (%s)", detail_url, item[Schema.PREF])
                        continue
                    yield item

                pages[pref] = page + 1

    # -------------------------------------------------------- list discovery

    def _kinki_list_urls(self, url: str) -> dict:
        """起点 url (大阪府の一覧) から近畿 6 府県の一覧 URL を導出する。

        戻り値は {"大阪府": "https://tsukulink.net/osaka", ...}。
        県別ページには他県への都道府県ナビが無いため、起点 url を基準に
        スラッグ (/kyoto /hyogo …) を urljoin して組み立てる。
        起点 url 自身が対象府県のページであれば、その URL をそのまま採用する。
        """
        base_path = (urlparse(url).path.rstrip("/") or "/").lower()
        found: dict[str, str] = {}
        for pref, slug in _KINKI_PREFS.items():
            if base_path == f"/{slug}":
                found[pref] = url.rstrip("/")
            else:
                found[pref] = urljoin(url, f"/{slug}")
        logger.info("巡回対象: %s", "、".join(f"{p}={u}" for p, u in found.items()))
        return found

    # ----------------------------------------------------------- detail page

    def _scrape_detail(self, url: str, company_id: str, list_pref: str = "") -> dict | None:
        soup = self._fetch_soup(url)
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
        # JSON-LD 側はハイフン無し (5300001) の会社があるため揃えて正規化する
        post_code = _normalize_postcode(ld_addr.get("postalCode") or "") or _normalize_postcode(raw_addr)
        address = _clean(_POSTCODE_RE.sub("", raw_addr, count=1))

        breadcrumbs = [
            _clean(a.get_text(" ", strip=True))
            for a in soup.select("ol.breadcrumbs .breadcrumbs__link")
        ]
        bc_pref = next((b for b in breadcrumbs if b in _PREFECTURES), "")
        city = _clean(ld_addr.get("addressLocality") or "")
        if not city and bc_pref in breadcrumbs:
            idx = breadcrumbs.index(bc_pref)
            if idx + 1 < len(breadcrumbs):
                city = breadcrumbs[idx + 1]

        m = _PREF_PATTERN.search(address)
        # 住所 → JSON-LD → パンくず → 一覧の府県 の順に解決する
        # (住所非公開の会社は住所が空になるため、掲載元の県別一覧の府県で補う)
        pref = (
            (m.group(1) if m else "")
            or _clean(ld_addr.get("addressRegion") or "")
            or bc_pref
            or list_pref
        )

        # --- 建設業許可 -----------------------------------------------------
        kensetsu_no = _first_text(blocks, ("許認可", "建設業許可", ""))
        ippan = _tidy(_joined(blocks, ("許認可", "建設業許可", "一般")))
        tokutei = _tidy(_joined(blocks, ("許認可", "建設業許可", "特定")))

        # --- ヘッダーのラベル (他社カード混入を避けプロフィール内に限定) ----
        # プレミアム会員は __info 側、非プレミアムは __title-container 側に
        # バッジが出るレイアウト差があるため、プロフィール全体 (1 ページに 1 つだけ存在)
        # を走査範囲にする。サイドバー「おすすめ会社」のバッジはこの外側なので混入しない。
        profile = soup.select_one(".p-companies-show-profile")
        company_labels = _join_texts(
            profile.select(".c-companies-header-labels__label-text") if profile else []
        )
        certified_labels = _join_texts(
            s
            for s in (
                profile.select(".c-companies-certified-labels span.c-label-black-stroke")
                if profile
                else []
            )
            if "not-certified" not in (s.get("class") or [])
        )

        # --- 従業員数 (本文優先、無ければ JSON-LD) --------------------------
        emp_raw = _first_text(blocks, ("会社概要", "従業員数", ""))
        if not emp_raw:
            value = (ld.get("numberOfEmployees") or {}).get("value")
            emp_raw = f"{value}名" if value else ""
        emp_num = _split_employees(emp_raw)

        # --- 設立年月日 (YYYY-MM-DD に正規化) -------------------------------
        open_date = _normalize_date(_first_text(blocks, ("会社概要", "設立年月日", "")))
        if not open_date:
            open_date = _clean(ld.get("foundingDate") or "")

        # --- HP -------------------------------------------------------------
        hp = ""
        for el in blocks.get(("会社概要", "ウェブサイト", ""), []):
            a = el.select_one("a[href]") if hasattr(el, "select_one") else None
            if a:
                hp = a.get("href", "").strip()
                break
        if not hp:
            # 未公開企業は「現在公開済み自社ホームページはございません」という
            # 案内文が入るため、URL 形式でなければ捨てる
            fallback = _first_text(blocks, ("会社概要", "ウェブサイト", "")) or _clean(ld.get("url") or "")
            hp = fallback if re.match(r"^https?://", fallback) else ""

        item = {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: address,
            Schema.REP_NM: _strip_company_suffix(
                _joined(blocks, ("会社概要", "代表者", ""), sep=" "), name
            ),
            Schema.EMP_NUM: emp_num,
            Schema.CAP: _first_text(blocks, ("会社情報", "資本金", "")),
            Schema.SALES: _joined(blocks, ("会社情報", "売上", ""), sep=" / "),
            Schema.OPEN_DATE: open_date,
            Schema.HP: hp,
            # サイト定義業種: プロフィール直下の業種表記
            Schema.CAT_SITE: _text_of(soup, ".p-companies-show-profile__info-job-type"),
            "会社ID": company_id,
            "市区町村": city,
            "評価点": _text_of(soup, ".p-companies-show-profile__rating .c-rating__score"),
            "企業ラベル": company_labels,
            "認証・許可ラベル": certified_labels,
            "本人確認": _certifications(soup, "本人確認"),
            "業種の許認可確認": _certifications(soup, "業種の許認可確認"),
            "保険加入状況": _certifications(soup, "保険加入状況"),
            "インボイス登録有無": _first_text(blocks, ("会社情報", "インボイス登録の有無", "")),
            "建設業許可番号": kensetsu_no,
            "建設業許可業種(一般)": ippan,
            "建設業許可業種(特定)": tokutei,
            "対応可能エリア": _job_areas(blocks),
        }

        # レイアウト差異で「会社情報」側に無い項目は「会社概要」側も見る
        if not item[Schema.CAP]:
            item[Schema.CAP] = _first_text(blocks, ("会社概要", "資本金", ""))
        if not item[Schema.SALES]:
            item[Schema.SALES] = _joined(blocks, ("会社概要", "売上", ""), sep=" / ")
        if not item["インボイス登録有無"]:
            # h4 が省略されている = 未登録。ヘッダーラベルから判定する
            item["インボイス登録有無"] = (
                "登録あり" if "インボイス登録あり" in certified_labels else "登録なし"
            )

        return item


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


def _labeled_blocks(soup) -> dict:
    """詳細ページの見出し駆動レイアウトを {(h3, h4, h5): [値要素...]} に変換する。

    h3 (大見出し) / h4 (項目名) / h5 (一般・特定等) はいずれも
    div.p-companies-show__section の直下に兄弟として並んでおり、
    見出しの後ろに続く要素がその項目の値になる。
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
    """指定見出しに属する値要素のテキスト (空要素・注記は除く) を返す。"""
    out = []
    for el in blocks.get(key, []):
        classes = el.get("class") or []
        if any("masked" in c or "description" in c for c in classes):
            continue  # 「＊＊＊＊」に関する注記や補足説明は値ではない
        txt = _clean(el.get_text(" ", strip=True))
        if txt:
            out.append(txt)
    return out


def _first_text(blocks: dict, key: tuple) -> str:
    vals = _values(blocks, key)
    return vals[0] if vals else ""


def _joined(blocks: dict, key: tuple, sep: str = "、") -> str:
    return sep.join(_values(blocks, key))


def _certifications(soup, category: str) -> str:
    """ツクリンク認証項目のうち、指定カテゴリで認証済 (is-active) の項目を返す。"""
    for h4 in soup.select("h4.p-companies-show-detail__heading--small"):
        if _clean(h4.get_text(strip=True)) != category:
            continue
        ul = h4.find_next_sibling("ul")
        if not ul:
            return ""
        return _join_texts(
            li for li in ul.select("li") if "is-active" in (li.get("class") or [])
        )
    return ""


def _split_employees(text: str) -> str:
    """「204名 (施工管理職員数: 19名、…)」から本体の人数のみを取り出す。"""
    if not text:
        return ""
    m = _EMP_BREAKDOWN_RE.match(text)
    return _clean(m.group(1)) if m else text


def _normalize_postcode(text: str) -> str:
    """「〒544-0003」「5300001」いずれも 544-0003 形式に正規化する。"""
    m = _POSTCODE_RE.search(_clean(text))
    return f"{m.group(1)}-{m.group(2)}" if m else ""


def _normalize_date(text: str) -> str:
    """「2002年04月01日」を 2002-04-01 に正規化する。"""
    m = _DATE_RE.search(text or "")
    if not m:
        return _clean(text)
    return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"


def _strip_company_suffix(value: str, company_name: str) -> str:
    """「小川 博司 株式会社オリバー」のように末尾へ会社名が連結された値から会社名を除く。"""
    if value and company_name and value.endswith(company_name) and value != company_name:
        return value[: -len(company_name)].strip()
    return value


def _job_areas(blocks: dict) -> str:
    """対応可能エリアを「地方: 都道府県、都道府県」形式で結合する。"""
    parts = []
    for el in blocks.get(("会社情報", "対応可能エリア", ""), []):
        divs = [
            _clean(d.get_text(" ", strip=True))
            for d in (el.find_all("div", recursive=False) if hasattr(el, "find_all") else [])
        ]
        divs = [d for d in divs if d]
        if len(divs) >= 2:
            parts.append(f"{divs[0]}: {_tidy(' '.join(divs[1:]))}")
        elif divs:
            parts.append(divs[0])
        else:
            txt = _clean(el.get_text(" ", strip=True))
            if txt:
                parts.append(txt)
    return " / ".join(parts)


def _corporation_ld(soup) -> dict:
    """JSON-LD の Corporation (住所・設立日・従業員数) を返す。無ければ空 dict。"""
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

    scraper = TsukulinkKinki()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://tsukulink.net/osaka")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
