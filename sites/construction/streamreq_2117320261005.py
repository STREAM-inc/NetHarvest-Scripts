"""
ツクリンク（京都・STREAMREQ-21173未完了再開20261005） (tsukulink.net) — 京都府の建設業者スクレイパー

取得対象:
    備考「対象は京都のみ / 都道府県は京都府のみ」に従い、
    京都府の公開企業一覧 (https://tsukulink.net/kyoto/) とその企業詳細の全件。
    2026-10 時点 16,288 社 / 20 件ページ = 815 ページ。

    名称・都道府県・住所・TEL・HP・プレミアム会員フラグ・企業詳細URL を必ず保持する。
    「自社HPリンクがある企業」「プレミアム会員」は *識別できる原値* を列として持たせるだけで、
    備考の指示どおり絞り込み (フィルタ) は行わず後続工程へ分離する:
      - Schema.HP          … 自社HPの URL (無い会社は空文字)
      - 「自社HP有無」      … あり / なし
      - 「プレミアム会員」  … はい / いいえ
      - 「企業ラベル」      … ヘッダーのラベル原文 (例「受発注両方、プレミアム」)
    唯一のフィルタは備考が明示する都道府県条件 (京都府以外を除外) のみ。

取得フロー:
    1. 引数 url (京都府の企業一覧) を唯一の起点として {url}?page=N を 1 ページずつ取得
    2. 1 ページ目の「N件中」表示から総ページ数を確定し、その最終ページで巡回を終える
    3. li.p-companies-list-item から会社ID・詳細URL・プレミアムラベル・主力工事・工事区分を拾う
    4. 詳細ページを 1 件取得するごとに即 yield (Pattern B)
    5. 会社IDで重複排除し、詳細ページの住所が京都府でないものは除外する

空ページを巡回しないための設計 (2026-10-07 改修):
    - 以前は京都府一覧に加えて市区町村別一覧 (/kyoto/city_NNNNNN/) 37 本も巡回していたが、
      市区町村別一覧は京都府一覧の部分集合でしかなく、新規会社が 1 件も出ない
      「空振りページ」を約 1,260 ページ踏むだけだった。実測 (2026-10-07):
        京都府一覧          16,289 件 / 815 ページ
        市区町村別一覧の合計 25,233 件 … ただし京都市 (9,023 件) = 11 行政区の合計で二重計上。
                                        実体は 16,210 件で京都府一覧に完全に含まれる
        市区町村別一覧の各社の詳細 URL は必ず自市区町村配下 (/kyoto/city_NNNNNN/{id}) で、
        対応可能エリアではなく所在地で分類されている = 京都府一覧と同一母集団
      よって起点 (京都府一覧) のみを巡回する。出力される会社の集合は従来と変わらない。
    - 範囲外ページ (page=816 以降) は 200 + 0 件で返るが、総件数から最終ページが判るので
      そもそも叩かない。総件数が取れなかった場合のみ従来どおり 0 件ページで終端判定する。
    - 保険として「新規会社が 1 件も増えないページ」が _MAX_STALE_PAGES 回続いたら打ち切る
      (一覧が範囲外で 1 ページ目に巻き戻る等の仕様変更で無限に空回りしないようにする)

サイト仕様メモ:
    - 一覧・詳細とも Accept ヘッダが無いと HTTP 400 を返すため _setup() で付与する
    - 一覧の ?page=N は範囲外になると 200 + 0 件で返る
      (2026-10 時点 page=815 が 9 件、page=816 以降は 0 件)
    - 短時間に高頻度でアクセスすると HTTP 403 を返す (約 2 分で自動解除)。
      815 ページ + 16,000 件の詳細という長丁場で 1 回の 403 が全滅に繋がるため、
      get_soup() を 403 のみバックオフ再試行するようにラップしている
    - 一覧は「プレミアム会員が先頭、以降は非会員」の並びになっており、
      プレミアムバッジ (span.c-label-premium) は一覧カード・詳細ヘッダーの双方に出る。
      ただしサイドバーの「おすすめ会社」カードにも同じ class が出るため、
      詳細側は .p-companies-show-profile__info 配下に限定して判定する
    - 詳細ページの会社概要は table/dl ではなく
      h3.p-companies-show-detail__heading (大見出し)
      → h4.p-companies-show-detail__heading--small (項目名)
      → h5.p-companies-show-detail__heading--xs (一般/特定)
      → 続く兄弟要素が値 という見出し駆動レイアウト。
      見出し・値は div.p-companies-show__section の直下に並ぶのでこのセクションに限定する
      (限定しないとページ下部の「京都府の建設会社」= 他社カードまで拾う)
    - **電話番号・FAX・法人番号は会員限定マスクのため公開ページに一切存在しない**
      (JSON-LD の telephone も null、tel: リンクも無し)。Schema.TEL は常に空文字になるが、
      備考で保持を指示された列のため列自体は必ず出力する
    - 充実した会社は JSON-LD (Corporation) を持ち、郵便番号・都道府県・市区町村・
      設立年月日・従業員数・自社HP URL を構造化データで取得できる。無い会社は本文から補完する
    - 「インボイス登録の有無」の h4 は未登録企業では出力されないため、
      ヘッダーの認証ラベル側から判定する
    - 住所は「〒6128492」のように郵便番号が前置され、ハイフン無しの会社もある
    - 利用規約 (/term) にスクレイピング・クローリングの禁止条項は無く、
      robots.txt も /kyoto 配下の一覧・会社詳細を許可している (2026-10-05 確認)

著作権配慮 (取得しない項目):
    ご挨拶 / 事業内容 (Schema.LOB) / 担当者メッセージ / 募集案件本文 は
    自由記述の長文プロースのため取得しない。主要取引先は取引先名の列挙のみ採用し、
    文章 (句点を含む or 300 文字超) の場合は破棄する

実行方法:
    # ローカルテスト
    python scripts/sites/construction/streamreq_2117320261005.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id streamreq_2117320261005
"""

import json
import logging
import math
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlencode, urljoin, urlparse, parse_qsl, urlunparse

import requests

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

# 備考「都道府県は京都府のみ」で指定された対象都道府県
_TARGET_PREF = "京都府"

# 詳細ページ URL (/kyoto/city_261092/989235)
_DETAIL_PATH = re.compile(r"^/[a-z]+/city_\d+/\d+$")

# 郵便番号 (〒610-0343 / 〒6128492)
_POSTCODE_RE = re.compile(r"〒?\s*(\d{3})-?(\d{4})")

# 設立年月日 (2017年04月03日)
_DATE_RE = re.compile(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日")

# 従業員数「5名 (施工管理職員数: 1名、…)」の内訳部分
_EMP_BREAKDOWN_RE = re.compile(r"^(.*?)\s*[（(](.+)[)）]\s*$")

# 一覧 1 ページあたりの掲載件数 (総件数 → 最終ページ番号の算出に使う)
_PER_PAGE = 20

# ページ送り安全上限 (2026-10 時点 16,289 件 = 815 ページ)。
# 通常は総件数から求めた最終ページで止まるため、ここまで到達することは無い。
_MAX_PAGES = 2000

# 新規会社が 1 件も増えないページがこの回数続いたら打ち切る (空回り防止の保険)
_MAX_STALE_PAGES = 3

# 高頻度アクセス時の HTTP 403 (約 2 分で自動解除) に対するバックオフ秒数
_FORBIDDEN_BACKOFF = (30, 60, 120)


class TsukulinkKyoto(StaticCrawler):
    """ツクリンク（京都） — 京都府の建設業者スクレイパー"""

    DELAY = 1.0
    CONTINUE_ON_ERROR = False

    EXTRA_COLUMNS = [
        "会社ID",
        "市区町村",
        "プレミアム会員",
        "自社HP有無",
        "企業ラベル",
        "認証・許可ラベル",
        "評価点",
        "本人確認",
        "業種の許認可確認",
        "保険加入状況",
        "インボイス登録有無",
        "建設業許可番号",
        "建設業許可業種(一般)",
        "建設業許可業種(特定)",
        "その他許認可",
        "許可業種",
        "主力工事",
        "工事区分",
        "対応可能工事種別",
        "スキル・技術",
        "主な施工工事区分",
        "主な建物種別",
        "保有建設機材",
        "技術者資格保有状況",
        "対応可能エリア",
        "主要取引先",
        "担当者役職",
        "担当者名",
        "従業員内訳",
        "表彰歴件数",
        "実績件数",
        "ブログ件数",
        "レビュー件数",
        "エリア",
        "業種",
        "法人番号",
        "代表者役職",
        "代表者",
        "設立日",
        "事業内容",
        "FAX",
        "メール",
        "Instagram",
        "Facebook",
        "X",
        "LINE公式",
    ]

    # ------------------------------------------------------------------ setup

    def _setup(self):
        """Accept ヘッダが無いと HTTP 400 を返すためブラウザ相当を付与する。"""
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

    # ------------------------------------------------------------------ parse

    def parse(self, url: str):
        """起点 url (京都府の企業一覧) の ?page=N を最終ページまで巡回して全社を yield する。

        市区町村別一覧は京都府一覧の部分集合 (モジュール冒頭「空ページを巡回しない
        ための設計」参照) なので巡回しない。終端は 1 ページ目の総件数から確定させ、
        範囲外の空ページを叩かないようにしている。
        """
        seen_ids: set[str] = set()
        last_page: int | None = None
        stale_pages = 0

        for page in range(1, _MAX_PAGES + 1):
            soup = self.get_soup(_page_url(url, page))
            if soup is None:
                raise RuntimeError(f"一覧の取得に失敗: {_page_url(url, page)}")

            if page == 1:
                # 「N件中」表示から総件数を取り、ETA と最終ページ番号の両方に使う
                total = _total_count(soup)
                if total:
                    self.total_items = total
                    last_page = math.ceil(total / _PER_PAGE)
                    logger.info("掲載件数 %s 件 / 全 %s ページ", total, last_page)
                else:
                    logger.warning("総件数を取得できないため 0 件ページで終端判定する")

            items = soup.select("li.p-companies-list-item")
            if not items:
                # 範囲外ページは 200 + 0 件で返る = 終端 (総件数が取れなかった場合の保険)
                logger.info("0 件ページのため終端と判断 (page=%s)", page)
                break

            new_on_page = 0
            for li in items:
                company_id = (li.get("data-company-id") or "").strip()
                link = li.select_one("a.p-companies-list-item__name[href]")
                if not link:
                    continue
                href = link.get("href", "")
                if not _DETAIL_PATH.match(href):
                    continue
                if company_id and company_id in seen_ids:
                    # プレミアム枠が先頭に再掲される等の重複は ID で排除する
                    continue
                if company_id:
                    seen_ids.add(company_id)
                new_on_page += 1

                detail_url = urljoin(url, href)
                try:
                    item = self._scrape_detail(detail_url, company_id, _list_extras(li))
                except Exception as e:
                    logger.warning("詳細の解析に失敗: %s — %s", detail_url, e)
                    raise
                if not item:
                    raise RuntimeError("Company detail missing: " + detail_url)
                # 備考「都道府県は京都府のみ」: 詳細ページの住所が京都府のものだけ採用する
                if item[Schema.PREF] != _TARGET_PREF:
                    logger.debug("京都府外のため除外: %s (%s)", detail_url, item[Schema.PREF])
                    continue
                yield item

            if last_page is not None and page >= last_page:
                # 次ページは必ず 0 件なので取得しない
                logger.info("最終ページ (%s) まで巡回完了: %s 社", last_page, len(seen_ids))
                break

            # 新規が 1 社も増えないページが続く = 一覧が巻き戻っている等の異常。空回りを止める
            stale_pages = stale_pages + 1 if new_on_page == 0 else 0
            if stale_pages >= _MAX_STALE_PAGES:
                logger.warning(
                    "新規会社が %s ページ連続で 0 件のため打ち切り (page=%s)", stale_pages, page
                )
                break

    # ------------------------------------------------------------- fetching

    def get_soup(self, url: str):
        """高頻度アクセス時に返る HTTP 403 (約 2 分で自動解除) だけはバックオフ再試行する。

        815 ページ + 16,000 件の詳細という長丁場で、一時的な 403 により
        クロール全体が落ちるのを防ぐ。403 以外のエラーは基底クラスの挙動のまま。
        """
        for wait in (*_FORBIDDEN_BACKOFF, None):
            try:
                return super().get_soup(url)
            except requests.exceptions.HTTPError as e:
                status = e.response.status_code if e.response is not None else None
                if status != 403 or wait is None:
                    raise
                logger.warning("HTTP 403 (レート制限)。%s 秒待って再試行: %s", wait, url)
                time.sleep(wait)
        return None


    # ----------------------------------------------------------- detail page

    def _scrape_detail(self, url: str, company_id: str, list_extras: dict) -> dict | None:
        soup = self.get_soup(url)
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
        # JSON-LD 側はハイフン無し (6128492) の会社があるため揃えて正規化する
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
        # 住所 → JSON-LD → パンくず → 一覧の県 の順に解決する
        # (住所非公開の会社は住所が空になるため、掲載元である京都府一覧の県で補う)
        pref = (
            (m.group(1) if m else "")
            or _clean(ld_addr.get("addressRegion") or "")
            or bc_pref
            or _TARGET_PREF
        )

        # --- 建設業許可 / その他許認可 -------------------------------------
        kensetsu_no = _first_text(blocks, ("許認可", "建設業許可", ""))
        ippan = _tidy(_joined(blocks, ("許認可", "建設業許可", "一般")))
        tokutei = _tidy(_joined(blocks, ("許認可", "建設業許可", "特定")))

        # --- ヘッダーのラベル (他社カード混入を避けプロフィール内に限定) ----
        info = soup.select_one(".p-companies-show-profile__info")
        company_labels = _join_texts(
            info.select(".c-companies-header-labels__label-text") if info else []
        )
        certified_labels = _join_texts(
            s
            for s in (info.select(".c-companies-certified-labels span.c-label-black-stroke") if info else [])
            if "not-certified" not in (s.get("class") or [])
        )

        # --- プレミアム会員フラグ (備考の必須項目) --------------------------
        # 詳細ヘッダーのバッジを優先し、取れない場合は一覧カードのバッジで補う。
        # サイドバー「おすすめ会社」にも同じ class が出るため info 配下に限定する。
        is_premium = bool(info.select_one(".c-companies-header-labels .c-label-premium")) if info else False
        if not is_premium and list_extras.get("プレミアム"):
            is_premium = True

        # --- 評価点 / 各種件数 ---------------------------------------------
        rating = _text_of(soup, ".p-companies-show-profile__rating .c-rating__score")
        counts = _profile_counts(soup)

        # --- 従業員数 (本文優先、無ければ JSON-LD) --------------------------
        emp_raw = _first_text(blocks, ("会社概要", "従業員数", ""))
        if not emp_raw:
            value = (ld.get("numberOfEmployees") or {}).get("value")
            emp_raw = f"{value}名" if value else ""
        emp_num, emp_breakdown = _split_employees(emp_raw)

        # --- 設立年月日 (YYYY-MM-DD に正規化) -------------------------------
        open_date = _normalize_date(_first_text(blocks, ("会社概要", "設立年月日", "")))
        if not open_date:
            open_date = _clean(ld.get("foundingDate") or "")

        # --- HP (自社ホームページ) ------------------------------------------
        hp = ""
        for el in blocks.get(("会社概要", "ウェブサイト", ""), []):
            a = el.select_one("a[href]") if hasattr(el, "select_one") else None
            if a:
                hp = a.get("href", "").strip()
                break
        if not hp:
            hp = _first_text(blocks, ("会社概要", "ウェブサイト", "")) or _clean(ld.get("url") or "")

        staff_position, staff_name = _staff(blocks)

        item = {
            # 企業詳細 URL (備考の必須項目)
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: address,
            # 電話番号は会員限定マスクのため公開ページに存在しない (常に空)。
            # 備考で保持を指示された列のため、空でも列として必ず出力する。
            Schema.TEL: "",
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
            # 絞込は後続工程へ分離するため、ここでは識別できる原値を持たせるだけにする
            "プレミアム会員": "はい" if is_premium else "いいえ",
            "自社HP有無": "あり" if hp else "なし",
            "企業ラベル": company_labels,
            "認証・許可ラベル": certified_labels,
            "評価点": rating,
            "本人確認": _certifications(soup, "本人確認"),
            "業種の許認可確認": _certifications(soup, "業種の許認可確認"),
            "保険加入状況": _certifications(soup, "保険加入状況"),
            "インボイス登録有無": _first_text(blocks, ("会社情報", "インボイス登録の有無", "")),
            "建設業許可番号": kensetsu_no,
            "建設業許可業種(一般)": ippan,
            "建設業許可業種(特定)": tokutei,
            "その他許認可": _other_permits(blocks),
            "許可業種": _list_items(blocks, ("会社情報", "業種", "")),
            "主力工事": list_extras.get("主力工事", ""),
            "工事区分": list_extras.get("工事区分", ""),
            "対応可能工事種別": _tidy(_joined(blocks, ("会社情報", "対応可能工事種別", ""))),
            "スキル・技術": _joined(blocks, ("会社情報", "スキル・技術", "")),
            "主な施工工事区分": _list_items(blocks, ("会社情報", "主な施工工事区分", "")),
            "主な建物種別": _list_items(blocks, ("会社情報", "主な建物種別", "")),
            "保有建設機材": _list_items(blocks, ("会社情報", "保有建設機材", "")),
            "技術者資格保有状況": _list_items(blocks, ("会社情報", "技術者資格保有状況", "")),
            "対応可能エリア": _job_areas(blocks),
            "主要取引先": _short_value(_first_text(blocks, ("会社概要", "主要取引先", ""))),
            "担当者役職": staff_position,
            "担当者名": staff_name,
            "従業員内訳": emp_breakdown,
            "表彰歴件数": counts.get("表彰歴", ""),
            "実績件数": counts.get("実績", ""),
            "ブログ件数": counts.get("ブログ", ""),
            "レビュー件数": counts.get("レビュー", ""),
        }

        # レイアウト差異で「会社情報」側に無い項目は「会社概要」側も見る
        if not item[Schema.CAP]:
            item[Schema.CAP] = _first_text(blocks, ("会社概要", "資本金", ""))
        if not item["インボイス登録有無"]:
            # h4 が省略されている = 未登録。ヘッダーラベルから判定する
            item["インボイス登録有無"] = (
                "登録あり" if "インボイス登録あり" in certified_labels else "登録なし"
            )

        return item


# ---------------------------------------------------------------- helpers


def _page_url(url: str, page: int) -> str:
    """起点 url に ?page=N を付与する (url 側の既存クエリは保持する)。"""
    if page <= 1:
        return url
    parsed = urlparse(url)
    query = [(k, v) for k, v in parse_qsl(parsed.query) if k != "page"]
    query.append(("page", str(page)))
    return urlunparse(parsed._replace(query=urlencode(query)))


def _total_count(soup) -> int:
    """一覧ヘッダーの「N件中」表示を総件数 (int) にする。取れなければ 0。"""
    total_el = soup.select_one(".c-pagination-entries__total")
    if not total_el:
        return 0
    digits = re.sub(r"[^0-9]", "", total_el.get_text())
    return int(digits) if digits else 0


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


def _list_extras(li) -> dict:
    """一覧アイテムにしか無い項目 (主力工事・工事区分) とプレミアムバッジを拾う。"""
    extras = {"主力工事": "", "工事区分": "", "プレミアム": False}
    for dl in li.select("dl.p-companies-list-item__job-list-item"):
        dt = dl.select_one(".p-companies-list-item__job-list-item__dt")
        dd = dl.select_one(".p-companies-list-item__job-list-item__dd")
        if not dt or not dd:
            continue
        label = dt.get_text(strip=True)
        if label in extras:
            extras[label] = _tidy(_clean(dd.get_text(" ", strip=True)))
    extras["プレミアム"] = bool(li.select_one(".c-companies-header-labels .c-label-premium"))
    return extras


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


def _list_items(blocks: dict, key: tuple) -> str:
    """値要素が ul の項目は li 単位で結合する (テキスト直結だと語が繋がるため)。"""
    parts = []
    for el in blocks.get(key, []):
        lis = el.select("li") if hasattr(el, "select") else []
        if lis:
            parts.extend(_clean(li.get_text(" ", strip=True)) for li in lis)
        else:
            parts.append(_clean(el.get_text(" ", strip=True)))
    return "、".join(p for p in parts if p)


def _other_permits(blocks: dict) -> str:
    """許認可セクションのうち建設業許可以外 (産業廃棄物収集運搬業許可 等) をまとめる。"""
    grouped: dict[str, list[str]] = {}
    for (h3, h4, h5), _els in blocks.items():
        if h3 != "許認可" or not h4 or h4 == "建設業許可":
            continue
        vals = _values(blocks, (h3, h4, h5))
        if not vals:
            continue
        label = f"{h4}({h5})" if h5 else h4
        grouped.setdefault(label, []).extend(vals)
    return " / ".join(f"{k}: {'、'.join(v)}" for k, v in grouped.items())


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


def _staff(blocks: dict) -> tuple[str, str]:
    """担当者ブロックから役職・氏名を取り出す (メッセージ本文は取得しない)。"""
    for el in blocks.get(("会社概要", "担当者", ""), []):
        children = el.find_all("div", recursive=False) if hasattr(el, "find_all") else []
        texts = []
        for child in children:
            classes = child.get("class") or []
            # 画像・長文メッセージ (accordion) ・注記は除外する
            if any(
                k in c
                for c in classes
                for k in ("content-image", "accordion", "masked", "lazyload")
            ):
                continue
            txt = _clean(child.get_text(" ", strip=True))
            if txt:
                texts.append(txt)
        if len(texts) >= 2:
            return texts[0], texts[1]
        if texts:
            return "", texts[0]
    return "", ""


def _split_employees(text: str) -> tuple[str, str]:
    """「5名 (施工管理職員数: 1名、…)」を 本体 / 内訳 に分ける。"""
    if not text:
        return "", ""
    m = _EMP_BREAKDOWN_RE.match(text)
    if m:
        return _clean(m.group(1)), _clean(m.group(2))
    return text, ""


def _normalize_postcode(text: str) -> str:
    """「〒610-0343」「6128492」いずれも 610-0343 形式に正規化する。"""
    m = _POSTCODE_RE.search(_clean(text))
    return f"{m.group(1)}-{m.group(2)}" if m else ""


def _normalize_date(text: str) -> str:
    """「2017年04月03日」を 2017-04-03 に正規化する。"""
    m = _DATE_RE.search(text or "")
    if not m:
        return _clean(text)
    return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"


def _short_value(text: str, limit: int = 300) -> str:
    """取引先名の列挙のみ採用する (文章化されている場合は著作権配慮で破棄)。"""
    text = _clean(text)
    if not text or len(text) > limit or "。" in text:
        return ""
    return text


def _strip_company_suffix(value: str, company_name: str) -> str:
    """「長尾 竜也 株式会社三弥」のように末尾へ会社名が連結された値から会社名を除く。"""
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


def _profile_counts(soup) -> dict:
    """プロフィール上部の「表彰歴 N件 / 実績 N件 / ブログ N件 / レビュー N件」を辞書化する。"""
    counts = {}
    container = soup.select_one(".p-companies-show-profile__counts")
    if not container:
        return counts
    for div in container.find_all("div", recursive=False):
        num_el = div.select_one(".p-companies-show-profile__counts-number")
        if not num_el:
            continue
        label = _clean(div.get_text(" ", strip=True)).split(" ")[0]
        counts[label] = _clean(num_el.get_text(strip=True))
    return counts


def _corporation_ld(soup) -> dict:
    """JSON-LD の Corporation (住所・設立日・従業員数・自社HP) を返す。無ければ空 dict。"""
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

    scraper = TsukulinkKyoto()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://tsukulink.net/kyoto/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
