"""
ツクリンク（滋賀） (tsukulink.net) — 滋賀県の建設業者スクレイパー

STREAMREQ-21176 の未完了分 (滋賀県) の再開用。既存の関東版
(construction/tsukulink_7.py)・北陸版 (tsukulink_4/_5) ・全国版 (tsukulink_6) とは
対象県が異なるだけで、詳細ページの解析ロジックは関東版を踏襲している。

取得対象:
    備考「対象は滋賀県のみ」に従い、滋賀県の公開企業一覧・企業詳細を全件取得する。
    2026-10 時点 8,582 社 / 20 件ページ = 430 ページ。

取得フロー:
    1. 引数 url (滋賀県の企業一覧) を唯一の起点として {url}?page=N を 1 ページずつ取得
    2. li.p-companies-list-item から会社ID・詳細URL・プレミアム会員ラベル・
       業種・主力工事・工事区分を拾う
    3. 詳細ページを 1 件取得するごとに即 yield (Pattern B)
    4. 会社IDで重複排除し、詳細ページの住所が滋賀県でないものは除外する

備考への対応:
    - 「名称/都道府県/住所/TEL/HP/プレミアム会員フラグ/企業詳細URL を保持」
      → Schema.NAME / PREF / ADDR / TEL / HP と
        EXTRA「プレミアム会員」「企業詳細URL」で保持する
    - 「プレミアム会員を識別できる原値を取得し、絞込は後続へ分離」
      → サイト上の表示ラベル文字列をそのまま「会員ラベル原値」、
        判定根拠の CSS クラス名を「会員ラベルclass原値」に入れ、
        正規化した 1/0 を「プレミアム会員」に入れる。
        プレミアム会員での絞り込みは **行わない** (全件 yield する)

サイト仕様メモ:
    - 一覧・詳細とも Accept ヘッダが無いと HTTP 400 を返すため _setup() で付与する
    - 一覧のページ送りは {url}?page=N。範囲外ページ (431 以降) は 200 + 0 件で
      返るので終端判定に使う。総件数は .c-pagination-entries__total
    - **電話番号・FAX・法人番号は会員限定マスクのため公開ページに一切出力されない**
      (詳細ページの HTML に「電話」「TEL」「tel:」の文字列自体が無い)。
      そのため Schema.TEL は常に空文字になる (セレクタの取りこぼしではない)
    - プレミアム会員バッジは span.c-label-premium。サイドバーの「おすすめ会社」
      カードにも同じ class が出るため、詳細ページでは .p-companies-show-profile
      配下に限定して判定する (一覧側は li 内に限定されるので安全)
    - 詳細ページの会社概要は table/dl ではなく
      h3.p-companies-show-detail__heading (大見出し)
      → h4.p-companies-show-detail__heading--small (項目名)
      → h5.p-companies-show-detail__heading--xs (一般/特定)
      → 続く兄弟要素が値 という見出し駆動レイアウト。
      見出し・値はいずれも div.p-companies-show__section の直下に並ぶ
    - 認証ラベルは未認証でも span が出力され class に not-certified が付く
      (= 付いていないものだけが認証済み)
    - 充実した会社は JSON-LD (Corporation) を持ち、郵便番号・都道府県・市区町村・
      設立年月日・従業員数・ウェブサイト URL を構造化データで取得できる。
      無い会社は本文から補完する
    - 「インボイス登録の有無」の h4 は未登録企業では出力されないため、
      ヘッダーの認証ラベル側から判定する
    - 利用規約 (/term) にスクレイピング・クローリングの禁止条項は無く、
      robots.txt も県別一覧・会社詳細を許可している (2026-10-05 時点で再確認)

著作権配慮 (取得しない項目):
    ご挨拶 / 事業内容 (Schema.LOB) / 担当者メッセージ / 協力業者・元請業者の募集案件本文 は
    自由記述の長文プロースのため取得しない。主要取引先は取引先名の列挙のみ採用し、
    文章 (句点を含む or 300 文字超) の場合は破棄する

実行方法:
    # ローカルテスト
    python scripts/sites/construction/streamreq_2117620261005.py
    # コンテナ実行
    docker compose exec worker python /app/bin/run_flow.py --site-id streamreq_2117620261005
"""

import json
import logging
import re
import sys
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

# 備考「対象は滋賀県のみ」で指定された対象都道府県
_TARGET_PREF = "滋賀県"

# 詳細ページ URL (/shiga/city_252085/894098)
_DETAIL_PATH = re.compile(r"^/[a-z]+/city_\d+/\d+$")

# プレミアム会員バッジの CSS クラス (= 識別できる原値)
_PREMIUM_CLASS = "c-label-premium"

# 郵便番号 (〒520-3035 / 〒5203035)
_POSTCODE_RE = re.compile(r"〒?\s*(\d{3})-?(\d{4})")

# 設立年月日 (2002年04月01日)
_DATE_RE = re.compile(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日")

# 従業員数「204名 (施工管理職員数: 19名、…)」の内訳部分
_EMP_BREAKDOWN_RE = re.compile(r"^(.*?)\s*[（(](.+)[)）]\s*$")

# ページ送りの安全上限 (2026-10 時点の滋賀県は 430 ページ)
_MAX_PAGES = 2000


class TsukulinkShiga(StaticCrawler):
    """ツクリンク（滋賀） — 滋賀県の建設業者スクレイパー"""

    DELAY = 1.0
    CONTINUE_ON_ERROR = False

    EXTRA_COLUMNS = [
        "会社ID",
        "企業詳細URL",
        "プレミアム会員",
        "会員ラベル原値",
        "会員ラベルclass原値",
        "市区町村",
        "評価点",
        "認証・許可ラベル",
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
        """引数 url (滋賀県一覧) を唯一の起点に {url}?page=N を辿り、詳細を 1 件ずつ yield する。

        市区町村別一覧 (/shiga/city_NNNNNN/) は県一覧の部分集合でしかない
        (大津市 1,835 件 ⊂ 滋賀県 8,584 件)。旧実装は県一覧の後に 19 市区町村を
        再走査していたため、全件が会社ID重複で捨てられる空ページを数千ページ
        送り続けていた。県一覧 1 系統のみを辿れば全件取得できる。
        """
        seen_ids: set[str] = set()
        root = url.rstrip("/") + "/"
        last_page = _MAX_PAGES  # 1 ページ目の総件数から算出し直す
        stale_pages = 0  # 新規会社が 1 件も無いページの連続数

        for page in range(1, _MAX_PAGES + 1):
            soup = self.get_soup(f"{root}?page={page}")
            if soup is None:
                logger.warning("一覧の取得に失敗したため打ち切り: page=%s", page)
                break

            items = soup.select("li.p-companies-list-item")
            if not items:
                # 範囲外ページは 200 + 0 件で返る = 終端
                logger.info("一覧の終端に到達 (page=%s)", page)
                break

            if page == 1:
                total_el = soup.select_one(".c-pagination-entries__total")
                if total_el:
                    digits = re.sub(r"[^0-9]", "", total_el.get_text())
                    if digits:
                        self.total_items = int(digits)
                        last_page = min(
                            _MAX_PAGES, -(-int(digits) // max(len(items), 1)) + 1
                        )
                        logger.info(
                            "滋賀県の掲載企業数: %s 件 (想定 %s ページ)", digits, last_page
                        )

            new_in_page = 0
            for li in items:
                company_id = (li.get("data-company-id") or "").strip()
                link = li.select_one("a.p-companies-list-item__name[href]")
                if not link:
                    continue
                href = link.get("href", "")
                if not _DETAIL_PATH.match(href):
                    continue
                if company_id and company_id in seen_ids:
                    continue
                if company_id:
                    seen_ids.add(company_id)
                new_in_page += 1

                detail_url = urljoin(root, href)
                try:
                    item = self._scrape_detail(detail_url, company_id, _list_extras(li))
                except Exception as e:  # 個別アイテムの失敗は記録して継続
                    logger.warning("詳細の解析に失敗 (スキップ): %s — %s", detail_url, e)
                    continue
                if not item:
                    logger.warning("詳細が取得できずスキップ: %s", detail_url)
                    continue
                # 備考「対象は滋賀県のみ」: 詳細ページの住所が滋賀県のものだけ採用する
                # (住所非公開で県が取れない会社は、滋賀県の一覧掲載である事実を根拠に残す)
                if item[Schema.PREF] and item[Schema.PREF] != _TARGET_PREF:
                    logger.debug("滋賀県外のため除外: %s (%s)", detail_url, item[Schema.PREF])
                    continue
                yield item

            # 同じページが返り続ける/全件重複する異常時に空走を止める
            stale_pages = stale_pages + 1 if new_in_page == 0 else 0
            if stale_pages >= 3:
                logger.info("新規企業の無いページが %s 回続いたため終了 (page=%s)", stale_pages, page)
                break

            if page >= last_page:
                logger.info("総件数から算出した最終ページに到達 (page=%s)", page)
                break
        else:
            logger.warning("ページ上限 %s に到達したため打ち切り", _MAX_PAGES)

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
        # JSON-LD 側はハイフン無し (5203035) の会社があるため揃えて正規化する
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
        # 住所 → JSON-LD → パンくず の順に解決する
        pref = (
            (m.group(1) if m else "")
            or _clean(ld_addr.get("addressRegion") or "")
            or bc_pref
        )

        # --- 建設業許可 / その他許認可 -------------------------------------
        kensetsu_no = _first_text(blocks, ("許認可", "建設業許可", ""))
        ippan = _tidy(_joined(blocks, ("許認可", "建設業許可", "一般")))
        tokutei = _tidy(_joined(blocks, ("許認可", "建設業許可", "特定")))

        # --- 会員ラベル (プレミアム判定の原値) ------------------------------
        # サイドバー「おすすめ会社」にも同じ class のバッジが出るため、
        # 必ず .p-companies-show-profile 配下に限定して走査する。
        # レイアウト差で一覧側にしか出ない会社があるので一覧の原値とマージする。
        profile = soup.select_one(".p-companies-show-profile")
        header_labels = _header_labels(profile) if profile else {"texts": [], "classes": []}
        label_texts = _merge_unique(list_extras.get("会員ラベル原値_list", []), header_labels["texts"])
        label_classes = _merge_unique(
            list_extras.get("会員ラベルclass原値_list", []), header_labels["classes"]
        )
        is_premium = any(_PREMIUM_CLASS in c.split() for c in label_classes)

        # --- 認証・許可ラベル ------------------------------------------------
        info = soup.select_one(".p-companies-show-profile__info")
        certified_labels = _join_texts(
            s
            for s in (info.select(".c-companies-certified-labels span.c-label-black-stroke") if info else [])
            if "not-certified" not in (s.get("class") or [])
        )

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

        # --- HP -------------------------------------------------------------
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
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: address,
            # 電話番号は会員限定マスクで公開ページに存在しないため常に空
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
            "企業詳細URL": url,
            "プレミアム会員": "1" if is_premium else "0",
            "会員ラベル原値": "、".join(label_texts),
            "会員ラベルclass原値": " / ".join(label_classes),
            "市区町村": city,
            "評価点": rating,
            "認証・許可ラベル": certified_labels,
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
    """一覧アイテムから拾う項目 (主力工事・工事区分・会員ラベル原値) を返す。

    会員ラベル (プレミアム等) は詳細ページのレイアウト差で出ない会社があるため、
    一覧側の原値も保持して詳細側とマージする。
    """
    labels = _header_labels(li)
    extras = {
        "主力工事": "",
        "工事区分": "",
        "会員ラベル原値_list": labels["texts"],
        "会員ラベルclass原値_list": labels["classes"],
    }
    for dl in li.select("dl.p-companies-list-item__job-list-item"):
        dt = dl.select_one(".p-companies-list-item__job-list-item__dt")
        dd = dl.select_one(".p-companies-list-item__job-list-item__dd")
        if not dt or not dd:
            continue
        label = dt.get_text(strip=True)
        if label in extras:
            extras[label] = _tidy(_clean(dd.get_text(" ", strip=True)))
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
    """「204名 (施工管理職員数: 19名、…)」を 本体 / 内訳 に分ける。"""
    if not text:
        return "", ""
    m = _EMP_BREAKDOWN_RE.match(text)
    if m:
        return _clean(m.group(1)), _clean(m.group(2))
    return text, ""


def _normalize_postcode(text: str) -> str:
    """「〒939-8211」「9398224」いずれも 939-8211 形式に正規化する。"""
    m = _POSTCODE_RE.search(_clean(text))
    return f"{m.group(1)}-{m.group(2)}" if m else ""


def _normalize_date(text: str) -> str:
    """「2002年04月01日」を 2002-04-01 に正規化する。"""
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



def _header_labels(scope) -> dict:
    """会員ラベル (受発注区分 / プレミアム 等) の表示文字列と CSS クラスを原値で返す。

    scope には必ずプロフィール領域 (.p-companies-show-profile) か一覧の li を渡すこと。
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


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    scraper = TsukulinkShiga()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://tsukulink.net/shiga/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
