"""
調達ポータル【法人】— 事業者情報公開機能【東京版】

運営: デジタル庁
URL: https://www.p-portal.go.jp/pps-web-biz/UAB01/OAB0103

このスクリプトは p_portal.py (全国版) の派生で、東京都に本社住所がある事業者を
できるだけ網羅的に取得することを目的とする。ユーザー要望を受けて以下を大幅改修した:

    1. 【東京都のみ】に対象を限定 (_TARGET_PREF = "13")。

    2. 【網羅性の強化】。従来は 950件程度で頭打ちになり、実件数に対して取得が少ない
       可能性があった。原因を切り分け・是正するため以下を追加した:

       (a) --count-only  : 詳細取得せず、市区町村ごとの検索件数・500件超・分割要否だけを
                           確認する「件数確認モード」。まずこれで「検索対象が少ない」のか
                           「取得処理で落ちている」のかを切り分ける。
       (b) --reset       : チェックポイント (.ckpt_p_portal_5.json) を削除して最初から取得。
                           チェックポイント使用有無・seen件数・done件数・今回新規取得件数・
                           最終件数をログに明示する。
       (c) 件数CSV       : 全市区町村の件数・500件超フラグ・取得方法・取得件数・未取得可能性を
                           p_portal_5_city_counts.csv に保存 (件数確認モード / 本取得の両方)。
       (d) 業者種別分割  : 500件超の市区町村を、まず【業者種別 (法人種別)】で分割する。
                           業者種別は "統一資格から検索する場合のみ" の条件では【ない】ため、
                           統一資格を持たない事業者も取りこぼさない (要望で最優先)。
       (e) 未完了の明示  : 最終分割後もなお500件超のバケットは done に入れず
                           overflow_unresolved.json に保存し「未完了: 追加分割必要」とログ。
                           先頭500件だけ取得して「完了扱い」にはしない。
       (f) 突合の強化    : 各バケットで検索条件/件数/ページ数/一覧法人番号数/詳細取得成功数/
                           重複除外数/500件超フラグ/完了・未完了 をログ。一覧数と詳細成功数が
                           一致しない場合は失敗法人番号を failed_detail.json に保存。

分割条件の調査結果 (要望4「営業品目以外の分割条件を調査・追加」への回答):
    OAB0101 フォームDOMを確認し、以下を評価した。
      ○ 業者種別 (法人種別 corporationCla): 株式会社/有限会社/合名・合資・合同会社/
         その他の設立登記法人/外国会社等/国の機関/地方公共団体/その他 の8区分。
         → "統一資格から検索する場合のみ" の枠の【外】にあり、全法人に適用可能。
           統一資格を持たない事業者も拾えるため【分割軸として最優先で採用】。
      ○ 資格の種類・営業品目 / 企業規模 / 資格等級 / 競争参加地域:
         → いずれもフォーム上「統一資格から検索する場合のみ、以下条件を選択してください」の
           枠内にあり、統一資格保有者のみが対象。非保有者を取りこぼすため補助的にのみ使用。
      × 商号・名称 (頭文字/50音/カナ):
         → フォーム注記「商号又は名称は部分一致での検索可能です。カナ名での検索はできません」。
           部分一致かつ法人名は漢字主体のため、頭文字による排他的な分割ができず
           重複・取りこぼしが多発する。分割軸として【不適】と判断し採用しない。

    ⇒ 採用した分割階層:
        L1 本社住所 (東京都 × 市区町村)
        L2 業者種別 (法人種別, 非統一資格・全法人)              ← 最優先の追加分割
        L3 資格の種類・営業品目 (統一資格のみ)                   ← L2でも500件超のとき
        L4 企業規模 (統一資格のみ)
        L5 資格等級 (統一資格のみ)
      L5後もなお500件超 → 完了扱いにせず overflow_unresolved.json に記録。

取得フロー:
    1. OAB0101 (検索フォーム) へアクセス
    2. 本社住所 (+ 分割条件) を設定して検索送信 → OAB0103
    3. フォーム送信後の実 URL をベースに ?page=N でページネーション (1ページ50件・最大500件)
    4. 1検索を2フェーズ処理 (一覧から新規法人番号収集 → OAB0108 へ POST して詳細取得)
    5. 基本情報・統一資格情報・落札実績件数を抽出して yield

設計メモ:
    - OAB0103/OAB0108 は直打ちアクセス不可 (JSESSIONID + CSRFトークン必須)。
      OAB0101 のフォーム送信で確立したセッションを維持したまま page.request.post() で
      OAB0108 を呼び出す。
    - 各入力要素は DOM に常在し name 属性を持つため、JS で値を設定し change を発火するだけで
      検索条件として送信される。市区町村は都道府県の change で AJAX 補充されるため待機。
      営業品目はモーダルのため確定ボタン (#qualificationKindBizItemSelected) の click が必要。
      企業規模・資格等級・業者種別はチェックボックスで確定ボタン不要。
    - 件数判定: "N件見つかりました" = 実件数 / "500件を超え" = 上限到達(500扱い) /
      "合致する事業者情報がありません" = 0件。
    - HTML の th「商品号又は名称」の実体は商号・名称 (Schema.NAME に対応)。
    - 代表者役職・代表者氏名が「－」の事業者は空文字に正規化する。

実行方法:
    # 件数確認モード (詳細取得せず市区町村ごとの件数だけ確認)
    python scripts/sites/government/p_portal_5.py --count-only

    # チェックポイントを消して最初から本取得
    python scripts/sites/government/p_portal_5.py --reset

    # 通常 (チェックポイントがあれば途中から再開)
    python scripts/sites/government/p_portal_5.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id p_portal_5
"""

import csv
import json
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup
from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_QUAL_CATEGORIES = ["物品の製造", "物品の販売", "役務の提供等", "物品の買い受け"]

# 【東京版】対象は東京都 (都道府県コード 13) のみ。
_TARGET_PREF = "13"
_PREF_CODES = [_TARGET_PREF]

# 企業規模 (統一資格分割 / L4)。値はフォームの companyScale チェックボックスの value。
_COMPANY_SCALES = [
    ("1", "大企業"),
    ("2", "中小企業"),
    ("4", "小規模企業"),
    ("3", "その他"),
]

# 資格等級 (統一資格分割 / L5)。値はフォームの qualificationGrade チェックボックスの value。
_QUAL_GRADES = [("A", "A"), ("B", "B"), ("C", "C"), ("D", "D")]

# サーバー側の1検索あたり上限。
_MAX_PER_SEARCH = 500

# --- 出力・状態ファイル (すべてスクリプトと同じディレクトリ) ---
_SCRIPT_DIR = Path(__file__).parent
# チェックポイントファイル (中断リジューム用)。key "seen"=取得済み法人番号,
# "done"=完了済みバケット, "version"=フォーマット版 (不一致なら無視して初期化)。
_CHECKPOINT_FILE = _SCRIPT_DIR / ".ckpt_p_portal_5.json"
_CHECKPOINT_VERSION = 3  # バケットキーを6要素 (+業者種別) に拡張したため版を更新
# 市区町村ごとの件数サマリ CSV (件数確認モード / 本取得の両方で出力)。
_CITY_COUNTS_CSV = _SCRIPT_DIR / "p_portal_5_city_counts.csv"
# 最終分割後もなお500件超で完全取得できなかったバケット。
_OVERFLOW_FILE = _SCRIPT_DIR / "overflow_unresolved.json"
# 一覧で見つけたが詳細取得に失敗した法人番号。
_FAILED_DETAIL_FILE = _SCRIPT_DIR / "failed_detail.json"

# --- フォーム要素セレクタ (2025年DOM調査で確定) ---
_CITY_READY_JS = (
    "() => { const s = document.querySelector('#city_select');"
    " return s && s.options.length > 1; }"
)

# 本社住所 (市区町村方式) を設定する JS。
_JS_SET_PREF = """
(pref) => {
    const radio = document.querySelector('#select-city');
    if (radio) { radio.checked = true; radio.dispatchEvent(new Event('change', {bubbles:true})); }
    const sel = document.querySelector('#presures_select');
    if (sel) { sel.value = pref; sel.dispatchEvent(new Event('change', {bubbles:true})); }
}
"""

# 市区町村を選択して「選択」確定ボタンを押す JS。
_JS_SET_CITY = """
(city) => {
    const sel = document.querySelector('#city_select');
    if (sel) { sel.value = city; sel.dispatchEvent(new Event('change', {bubbles:true})); }
    const ok = document.querySelector('#companyAdressSelected');
    if (ok) ok.click();
}
"""

# 業者種別 (法人種別 corporationCla) チェックボックスを1つ ON にする JS。
# 「統一資格から検索する場合のみ」の枠の外にある条件で、全法人に適用できる。
# value 指定のチェックボックスを ON にし change を発火する。確定ボタンは不要。
_JS_SET_CORP_CLA = """
(value) => {
    const cb = [...document.querySelectorAll('input[type=checkbox][name*="corporationCla"]')]
        .find(c => c.value === value);
    if (cb) { cb.checked = true; cb.dispatchEvent(new Event('change', {bubbles:true})); }
}
"""

# 業者種別 (法人種別) チェックボックスの value + ラベルを列挙する JS。
_JS_LIST_CORP_CLA = """
els => els.map(e => {
    let lbl = '';
    if (e.id) { const l = document.querySelector('label[for="' + e.id + '"]'); if (l) lbl = l.textContent.trim(); }
    if (!lbl) { const p = e.closest('label'); if (p) lbl = p.textContent.trim(); }
    if (!lbl) { const p = e.closest('td,li,span,div'); if (p) lbl = p.textContent.trim(); }
    return {value: e.value, label: lbl};
}).filter(o => o.value)
"""

# 資格の種類・営業品目モーダルで「資格の種類」と「営業品目」を両方 ON にし確定する JS。
_JS_SET_BIZ = """
(args) => {
    const [kind, value] = args;
    const kinds = [...document.querySelectorAll(
        'input[name="inputSearchCondtionBean.qualificationKindBizItemSelectBean.qualificationKind"]')];
    const kc = kinds.find(c => c.value === kind);
    if (kc) { kc.checked = true; kc.dispatchEvent(new Event('change', {bubbles:true})); }
    const items = [...document.querySelectorAll(
        'input[type=checkbox][name^="inputSearchCondtionBean.qualificationKindBizItemSelectBean.bizItem"]')];
    const cb = items.find(c => c.value === value);
    if (cb) { cb.checked = true; cb.dispatchEvent(new Event('change', {bubbles:true})); }
    const ok = document.querySelector('#qualificationKindBizItemSelected');
    if (ok) ok.click();
}
"""

# 企業規模チェックを ON にする JS。
_JS_SET_SCALE = """
(value) => {
    const cb = [...document.querySelectorAll('input[name="inputSearchCondtionBean.companyScale"]')]
        .find(c => c.value === value);
    if (cb) { cb.checked = true; cb.dispatchEvent(new Event('change', {bubbles:true})); }
}
"""

# 資格等級チェックを ON にする JS。
_JS_SET_GRADE = """
(value) => {
    const cb = [...document.querySelectorAll('input[name="inputSearchCondtionBean.qualificationGrade"]')]
        .find(c => c.value === value);
    if (cb) { cb.checked = true; cb.dispatchEvent(new Event('change', {bubbles:true})); }
}
"""

# 検索ボタンを押す JS。
_JS_CLICK_SEARCH = """
() => {
    const cands = ['input[name="OAB0103"]', 'input[type=submit][value*=検索]',
                   'button[type=submit]', 'input[type=submit]'];
    for (const sel of cands) { const b = document.querySelector(sel); if (b) { b.click(); return true; } }
    return false;
}
"""


def _clean(s: str) -> str:
    """空白正規化 + 「－」(全角ハイフン) を空文字に変換。"""
    if not s:
        return ""
    s = re.sub(r"\s+", " ", str(s)).strip()
    return "" if s == "－" else s


def _build_page_url(result_url: str, page_num: int, size: int) -> str:
    """フォーム送信後の実 URL にページ番号を合成する。"""
    if "page=" in result_url:
        url = re.sub(r"page=\d+", f"page={page_num}", result_url)
    elif "?" in result_url:
        url = f"{result_url}&page={page_num}&size={size}"
    else:
        url = f"{result_url}?page={page_num}&size={size}"
    if "size=" not in url:
        url = f"{url}&size={size}"
    return url


class PPortal5Scraper(DynamicCrawler):
    """調達ポータル【法人】事業者情報公開機能【東京版】スクレイパー"""

    DELAY = 1.0
    PAGE_SIZE = 50  # サーバーは 1ページ50件固定 (size= は無視される)
    EXTRA_COLUMNS = [
        "業者種別",      # 株式会社 / 合同会社 等 (構造化ラベル)
        "資格番号",      # 例: 0000100505
        "有効期間",      # 例: 令和07・08・09
        "企業規模",      # 大企業 / 中小企業 / 小規模企業 / その他
        "資格等級",      # 例: 役務の提供等:A / 物品の販売:A
        "競争参加地域",  # 例: 北海道 東北 関東・甲信越 ...
        "落札実績件数",  # 落札実績の総件数 (整数文字列)
    ]

    # __main__ / 呼び出し側で上書きするフラグ (既定=通常本取得)。
    reset_checkpoint: bool = False
    count_only: bool = False

    # ------------------------------------------------------------------ #
    #  フォーム / 市区町村                                                 #
    # ------------------------------------------------------------------ #

    def _load_search_form(self, search_url: str) -> bool:
        """検索フォーム (OAB0101) を開き、操作可能になるまで待つ。"""
        self.get_soup(search_url)
        try:
            self.page.wait_for_function(
                "() => { const s = document.querySelector('#presures_select');"
                " return s && s.options.length > 40; }",
                timeout=30000,
            )
            return True
        except Exception as e:
            self.logger.warning("検索フォーム初期化待ちタイムアウト: %s", e)
            return False

    def _get_cities(self, search_url: str, pref_code: str) -> list[dict[str, str]]:
        """指定都道府県の市区町村リストを取得する (失敗時は最大3回リトライ)。"""
        for attempt in range(1, 4):
            if not self._load_search_form(search_url):
                continue
            try:
                self.page.evaluate(_JS_SET_PREF, pref_code)
                self.page.wait_for_function(_CITY_READY_JS, timeout=20000)
                return self.page.eval_on_selector_all(
                    "#city_select option",
                    "els => els.map(o => ({code:o.value, name:o.text.trim()})).filter(o => o.code)",
                )
            except Exception as e:
                self.logger.warning(
                    "都道府県%s: 市区町村リスト取得失敗 (試行%d/3): %s", pref_code, attempt, e
                )
        self.logger.warning("都道府県%s: 市区町村リスト取得に3回失敗 → スキップ", pref_code)
        return []

    def _detect_split_conditions(self, search_url: str) -> None:
        """分割に使う条件 (業者種別 / 営業品目) をフォームから動的検出する。

        self._corp_clas : [{value, label}, ...] 業者種別 (法人種別)。非統一資格・全法人。
        self._biz_items : [{value, kind}, ...]   資格の種類・営業品目。統一資格のみ。
        """
        self._corp_clas = []
        self._biz_items = []
        for attempt in range(1, 4):
            self._load_search_form(search_url)
            try:
                self.page.wait_for_selector(
                    "input[type=checkbox][name*='bizItem']", timeout=15000
                )
            except Exception:
                pass
            try:
                self._corp_clas = self.page.eval_on_selector_all(
                    "input[type=checkbox][name*='corporationCla']", _JS_LIST_CORP_CLA
                )
            except Exception as e:
                self.logger.debug("業者種別検出失敗 (試行%d): %s", attempt, e)
            try:
                self._biz_items = self.page.eval_on_selector_all(
                    "input[type=checkbox][name*='bizItem']",
                    "els => els.map(e => ({value: e.value,"
                    " kind: (e.name.match(/bizItem(\\d+)$/) || ['',''])[1]}))"
                    ".filter(o => o.value)",
                )
            except Exception as e:
                self.logger.debug("営業品目検出失敗 (試行%d): %s", attempt, e)
            if self._corp_clas or self._biz_items:
                break
            self.logger.warning("分割条件検出 0件 (試行%d/3) → 再試行", attempt)

        self.logger.info(
            "分割条件検出: 業者種別(法人種別) %d件 / 資格の種類・営業品目 %d件",
            len(self._corp_clas), len(self._biz_items),
        )
        if self._corp_clas:
            self.logger.info(
                "業者種別(非統一資格・全法人): %s",
                ", ".join(f"{c['value']}={c.get('label', '')}" for c in self._corp_clas),
            )
        else:
            self.logger.warning(
                "業者種別(法人種別)を検出できませんでした。500件超の市区町村は"
                "統一資格フィルタ(営業品目→企業規模→資格等級)のみで分割します。"
                "この場合、統一資格を持たない事業者の超過分を取りこぼす可能性があります。"
            )
        if not self._biz_items:
            self.logger.warning(
                "資格の種類・営業品目コードを検出できませんでした。"
                "業者種別でも500件超のバケットは追加分割できず未完了になります。"
            )

    # ------------------------------------------------------------------ #
    #  チェックポイント / レジューム                                       #
    # ------------------------------------------------------------------ #

    def _load_checkpoint(self) -> tuple[set[str], set[tuple]]:
        """チェックポイントを読み込み (seen, done) を返す。

        reset_checkpoint=True の場合はファイルを削除して空で開始する。
        版が一致しない・壊れている場合も空で開始する。
        """
        if self.reset_checkpoint and _CHECKPOINT_FILE.exists():
            try:
                _CHECKPOINT_FILE.unlink()
                self.logger.info("--reset: チェックポイント %s を削除しました", _CHECKPOINT_FILE)
            except Exception as e:
                self.logger.warning("--reset: チェックポイント削除失敗: %s", e)

        self._used_checkpoint = False
        if not self.reset_checkpoint and _CHECKPOINT_FILE.exists():
            try:
                data = json.loads(_CHECKPOINT_FILE.read_text(encoding="utf-8"))
                if data.get("version") != _CHECKPOINT_VERSION:
                    self.logger.warning(
                        "チェックポイント版不一致 (file=%s expected=%s) → 初期化して最初から取得",
                        data.get("version"), _CHECKPOINT_VERSION,
                    )
                    return set(), set()
                seen = set(data.get("seen", []))
                done: set[tuple] = {tuple(k) for k in data.get("done", [])}
                self._used_checkpoint = True
                self.logger.info(
                    "チェックポイント使用: あり (seen=%d件 / done=%dバケット) → 完了済みをスキップ",
                    len(seen), len(done),
                )
                return seen, done
            except Exception as e:
                self.logger.warning("チェックポイント読込失敗: %s → 初期化して続行", e)
        else:
            self.logger.info("チェックポイント使用: なし (最初から取得)")
        return set(), set()

    def _save_checkpoint(self, seen: set[str]) -> None:
        """seen と self._done をチェックポイントファイルへ書き出す。"""
        try:
            _CHECKPOINT_FILE.write_text(
                json.dumps(
                    {
                        "version": _CHECKPOINT_VERSION,
                        "seen": list(seen),
                        "done": [list(k) for k in self._done],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
        except Exception as e:
            self.logger.warning("チェックポイント保存失敗: %s", e)

    # ------------------------------------------------------------------ #
    #  検索実行                                                            #
    # ------------------------------------------------------------------ #

    def _do_search(
        self,
        search_url: str,
        pref_code: str,
        city_code: str,
        corp_cla: str | None = None,
        biz_kind: str | None = None,
        biz_item: str | None = None,
        company_scale: str | None = None,
        qual_grade: str | None = None,
    ) -> tuple[BeautifulSoup, int, bool, str]:
        """検索条件を設定して検索を実行し、(soup, 件数, 上限超フラグ, 実 URL) を返す。

        corp_cla      : 業者種別 (法人種別) の value (L2分割。非統一資格・全法人)。
        biz_kind/item : 資格の種類コード + 営業品目コード (L3分割。統一資格のみ)。
        company_scale : 企業規模の value (L4分割。統一資格のみ)。
        qual_grade    : 資格等級の value (L5分割。統一資格のみ)。

        overflow=True のとき total は 500 (上限) を表す。
        """
        for _attempt in range(1, 4):
            if self._load_search_form(search_url):
                break
            self.logger.warning(
                "フォーム未確立 (pref=%s city=%s 試行%d/3) → 再取得", pref_code, city_code, _attempt
            )
        else:
            self.logger.warning(
                "フォーム確立失敗 (pref=%s city=%s) → この検索をスキップ", pref_code, city_code
            )
            return BeautifulSoup("", "html.parser"), 0, False, self.page.url

        # --- 本社住所 (都道府県 × 市区町村) ---
        self.page.evaluate(_JS_SET_PREF, pref_code)
        try:
            self.page.wait_for_function(_CITY_READY_JS, timeout=20000)
        except Exception as e:
            self.logger.debug("市区町村補充待ちタイムアウト %s: %s", pref_code, e)
        self.page.evaluate(_JS_SET_CITY, city_code)
        self.page.wait_for_timeout(300)

        # --- 業者種別 (法人種別 / L2) ---
        if corp_cla:
            self.page.evaluate(_JS_SET_CORP_CLA, corp_cla)
            self.page.wait_for_timeout(300)

        # --- 資格の種類・営業品目 (L3) ---
        if biz_item:
            self.page.evaluate(_JS_SET_BIZ, [biz_kind or "", biz_item])
            self.page.wait_for_timeout(300)

        # --- 企業規模 (L4) ---
        if company_scale:
            self.page.evaluate(_JS_SET_SCALE, company_scale)
            self.page.wait_for_timeout(300)

        # --- 資格等級 (L5) ---
        if qual_grade:
            self.page.evaluate(_JS_SET_GRADE, qual_grade)
            self.page.wait_for_timeout(300)

        # --- 検索送信 ---
        try:
            with self.page.expect_navigation(wait_until="domcontentloaded", timeout=30000):
                self.page.evaluate(_JS_CLICK_SEARCH)
        except Exception as e:
            self.logger.warning(
                "検索送信失敗 (pref=%s city=%s cla=%s kind=%s biz=%s scale=%s grade=%s): %s",
                pref_code, city_code, corp_cla, biz_kind, biz_item, company_scale, qual_grade, e,
            )
            return BeautifulSoup("", "html.parser"), 0, False, self.page.url
        self.page.wait_for_timeout(1200)

        result_url = self.page.url
        soup = BeautifulSoup(self.page.content(), "html.parser")
        text = soup.get_text()
        overflow = "500件を超え" in text
        m = re.search(r"(\d[\d,]*)件見つかりました", text)
        if m:
            total = int(m.group(1).replace(",", ""))
        else:
            total = _MAX_PER_SEARCH if overflow else 0
        return soup, total, overflow, result_url

    # ------------------------------------------------------------------ #
    #  ページネーション + 詳細取得 (突合ログ付き)                          #
    # ------------------------------------------------------------------ #

    def _paginate_and_yield(
        self,
        result_url: str,
        first_soup: BeautifulSoup,
        total: int,
        size: int,
        detail_url: str,
        seen: set[str],
        label: str = "",
        overflow: bool = False,
    ) -> Generator[dict, None, None]:
        """1検索を「一覧から新規法人番号収集 → 全件詳細取得」の2フェーズで処理する。

        突合のため、検索条件/件数/ページ数/一覧法人番号数/詳細取得成功数/重複除外数/
        500件超フラグ/完了・未完了 をログ出力する。一覧数と詳細成功数が一致しない場合は
        失敗法人番号を self._failed_detail に蓄積する (最後に failed_detail.json へ書き出し)。
        """
        max_pages = max(1, -(-min(total, _MAX_PER_SEARCH) // self.PAGE_SIZE))  # ceil
        prev_hrefs: list[str] | None = None
        entries: list[tuple[str, str, str, str]] = []  # (corp_no, art_qual_id, csrf, page_url)
        pages_visited = 0
        dup_skipped = 0

        # === フェッチ相: 全ページの一覧から新規法人番号を収集 ===
        for page_num in range(max_pages):
            if page_num == 0:
                soup = first_soup
                page_url = result_url
            else:
                page_url = _build_page_url(result_url, page_num, self.PAGE_SIZE)
                soup = self.get_soup(page_url)
                if soup is None:
                    break
            pages_visited += 1

            csrf_input = soup.find("input", {"name": "_csrf"})
            csrf = csrf_input["value"] if csrf_input else ""

            corp_links = soup.select("table tbody tr td:nth-child(3) a")
            if not corp_links:
                self.logger.debug("ページ%d: corp_links 0件 → ページネーション終了", page_num)
                break

            page_hrefs = [link.get("href", "") for link in corp_links]
            if prev_hrefs is not None and page_hrefs == prev_hrefs:
                self.logger.debug("ページ%d: 直前ページと同一 (クランプ) → 終了", page_num)
                pages_visited -= 1
                break
            prev_hrefs = page_hrefs

            for link in corp_links:
                href = link.get("href", "")
                m_corp = re.search(r"corporationNo', value:'(\d+)'", href)
                m_art = re.search(r"articleQualificationInfoId', value:'([^']*)'", href)
                if not m_corp:
                    continue
                corp_no = m_corp.group(1)
                if corp_no in seen:
                    dup_skipped += 1
                    continue
                seen.add(corp_no)
                entries.append((corp_no, m_art.group(1) if m_art else "", csrf, page_url))

        self._fetched_total += len(entries)

        # === 取得相: 収集した全件の詳細を取得 ===
        got = 0
        failures: list[dict] = []
        for corp_no, art_qual_id, csrf, page_url in entries:
            try:
                resp = self.page.request.post(
                    detail_url,
                    form={
                        "_csrf": csrf,
                        "articleQualificationInfoId": art_qual_id,
                        "corporationNo": corp_no,
                    },
                )
                if resp.status != 200:
                    self.logger.warning("OAB0108 error for %s: HTTP %d", corp_no, resp.status)
                    failures.append({"corp_no": corp_no, "reason": f"HTTP {resp.status}", "label": label})
                    continue

                detail_soup = BeautifulSoup(resp.text(), "html.parser")
                article_el = detail_soup.find("article")
                if article_el and "事業者情報を取得できません" in article_el.get_text():
                    self.logger.debug("corp %s: 情報なし", corp_no)
                    failures.append({"corp_no": corp_no, "reason": "情報なし", "label": label})
                    continue

                item = self._scrape_detail(detail_soup, page_url)
                if item:
                    self._retrieved_total += 1
                    got += 1
                    yield item
                else:
                    failures.append({"corp_no": corp_no, "reason": "詳細パース失敗", "label": label})

            except Exception as e:
                self.logger.warning("Error scraping corp %s: %s", corp_no, e)
                failures.append({"corp_no": corp_no, "reason": str(e), "label": label})

        self._dup_total += dup_skipped

        # === 突合ログ (要望5) ===
        completed = not overflow
        self.logger.info(
            "📊 %s | 検索件数=%d ページ数=%d 一覧法人番号=%d 詳細成功=%d 重複除外=%d "
            "500件超=%s 判定=%s",
            label or "(検索)", total, pages_visited, len(entries), got, dup_skipped,
            overflow, "完了" if completed else "未完了(先頭500件のみ)",
        )
        if len(entries) != got:
            # 一覧で見つけた新規法人番号数 ≠ 詳細取得成功数 → 失敗分を記録
            self.logger.warning(
                "⚠ %s | 一覧%d件 ≠ 詳細成功%d件 (差 %d件) → failed_detail.json に記録",
                label or "(検索)", len(entries), got, len(entries) - got,
            )
            self._failed_detail.extend(failures)

        self.logger.info(
            "累計 フェッチ%d件 / 取得%d件 / 重複除外%d件",
            self._fetched_total, self._retrieved_total, self._dup_total,
        )

    # ------------------------------------------------------------------ #
    #  parse (エントリポイント)                                            #
    # ------------------------------------------------------------------ #

    def parse(self, url: str) -> Generator[dict, None, None]:
        search_url = urljoin(url, "OAB0101")
        detail_url = urljoin(url, "OAB0108?")
        size = 50

        # 実行モード確定
        self.reset_checkpoint = bool(getattr(self, "reset_checkpoint", False))
        self.count_only = bool(getattr(self, "count_only", False))

        # 累計カウンタ / 記録用コンテナ初期化
        self._fetched_total = 0
        self._retrieved_total = 0
        self._dup_total = 0
        self._overflow_unresolved: list[dict] = []
        self._failed_detail: list[dict] = []
        # 市区町村ごとのサマリ: ccode -> {name, total, overflow, method, got, incomplete}
        self._city_stats: dict[str, dict] = {}

        # -------- 件数確認モード (--count-only) --------
        if self.count_only:
            self.logger.info("=== 件数確認モード (--count-only): 詳細取得は行いません ===")
            yield from self._run_count_only(search_url)
            self.total_items = 0
            return

        # -------- 本取得モード --------
        seen, self._done = self._load_checkpoint()
        # _harvest_overflow_leaf が参照する共有 seen を確定させる。
        self._current_seen = seen
        seen_at_start = len(seen)
        done_at_start = len(self._done)

        # 分割条件 (業者種別 / 営業品目) を動的検出
        self._detect_split_conditions(search_url)

        for pref_code in _PREF_CODES:
            cities = self._get_cities(search_url, pref_code)
            if not cities:
                continue
            self.logger.info("都道府県%s (東京都): 市区町村 %d件", pref_code, len(cities))
            self._num_cities = len(cities)

            for city in cities:
                yield from self._process_city(
                    search_url, detail_url, size, pref_code, city, seen
                )

        # -------- 出力・サマリ --------
        self._write_city_counts_csv()
        self._write_overflow_and_failures()

        self._used_checkpoint = getattr(self, "_used_checkpoint", False)
        skipped = done_at_start  # チェックポイントによりスキップされた完了済みバケット数
        self.logger.info("=" * 60)
        self.logger.info(
            "チェックポイント使用: %s / 開始時 seen=%d done=%d",
            "あり" if self._used_checkpoint else "なし", seen_at_start, done_at_start,
        )
        if self._used_checkpoint and skipped:
            self.logger.info(
                "チェックポイントにより %d バケットをスキップしました "
                "(スキップ分の件数は今回の新規取得には含まれません)", skipped,
            )
        self.logger.info(
            "東京都 市区町村数: %d", getattr(self, "_num_cities", len(self._city_stats))
        )
        target_total = sum(s["total"] for s in self._city_stats.values())
        any_overflow = any(s["overflow"] for s in self._city_stats.values())
        self.logger.info(
            "検索対象総件数: %d件%s", target_total, "以上" if any_overflow else "",
        )
        self.logger.info("取得成功(今回新規): %d件", self._retrieved_total)
        self.logger.info("重複除外: %d件", self._dup_total)
        self.logger.info("詳細取得失敗: %d件", len(self._failed_detail))
        self.logger.info("未完了バケット: %d件", len(self._overflow_unresolved))
        self.logger.info("最終 seen (ユニーク法人番号) 件数: %d件", len(seen))
        if self._overflow_unresolved:
            self.logger.warning(
                "未完了バケットあり → %s を確認し追加分割してください", _OVERFLOW_FILE
            )
        self.logger.info("チェックポイント: %s", _CHECKPOINT_FILE)
        self.logger.info("=" * 60)

        self.total_items = len(seen)

    # ------------------------------------------------------------------ #
    #  件数確認モード                                                      #
    # ------------------------------------------------------------------ #

    def _run_count_only(self, search_url: str) -> Generator[dict, None, None]:
        """詳細取得せず、市区町村ごとの件数・500件超・分割要否だけを確認する。

        本メソッドは item を yield しない (件数確認のみ)。結果は
        p_portal_5_city_counts.csv と最終ログにまとめる。
        """
        for pref_code in _PREF_CODES:
            cities = self._get_cities(search_url, pref_code)
            if not cities:
                self.logger.warning("都道府県%s: 市区町村が取得できませんでした", pref_code)
                continue
            self.logger.info("都道府県%s (東京都): 市区町村 %d件", pref_code, len(cities))
            self._num_cities = len(cities)

            for city in cities:
                ccode, cname = city["code"], city["name"]
                try:
                    _soup, total, overflow, _url = self._do_search(
                        search_url, pref_code, ccode
                    )
                except Exception as e:
                    self.logger.warning("%s %s: 件数確認失敗: %s", pref_code, cname, e)
                    total, overflow = 0, False
                need_split = overflow
                self.logger.info(
                    "件数確認 | %s %s(%s): %d件 500件超=%s 分割要否=%s",
                    pref_code, cname, ccode, total, overflow,
                    "要" if need_split else "不要",
                )
                self._city_stats[ccode] = {
                    "name": cname,
                    "total": total,
                    "overflow": overflow,
                    "method": "分割取得(要)" if need_split else "通常取得",
                    "got": 0,
                    "incomplete": overflow,
                }

        self._write_city_counts_csv(count_only=True)

        num = getattr(self, "_num_cities", len(self._city_stats))
        target_total = sum(s["total"] for s in self._city_stats.values())
        any_overflow = any(s["overflow"] for s in self._city_stats.values())
        over_cnt = sum(1 for s in self._city_stats.values() if s["overflow"])
        self.logger.info("=" * 60)
        self.logger.info("【件数確認モード サマリ】")
        self.logger.info("東京都 市区町村数: %d", num)
        self.logger.info("検索対象総件数: %d件%s", target_total, "以上" if any_overflow else "")
        self.logger.info("500件超(要分割)の市区町村: %d件", over_cnt)
        self.logger.info("件数CSV: %s", _CITY_COUNTS_CSV)
        self.logger.info(
            "→ この結果で「検索対象が少ない」のか「取得処理で落ちている」のかを切り分けてください"
        )
        self.logger.info("=" * 60)
        return
        yield  # (このメソッドをジェネレータにするためのダミー。実行されない)

    # ------------------------------------------------------------------ #
    #  市区町村処理 + 分割階層                                             #
    # ------------------------------------------------------------------ #

    def _process_city(
        self, search_url, detail_url, size, pref_code, city, seen
    ) -> Generator[dict, None, None]:
        """1市区町村を処理する (L1)。500件超なら業者種別分割 (L2) へ。"""
        ccode, cname = city["code"], city["name"]
        bucket = (pref_code, ccode, "", "", "", "")
        stat = {
            "name": cname, "total": 0, "overflow": False,
            "method": "通常取得", "got": 0, "incomplete": False,
        }
        self._city_stats[ccode] = stat
        ret_before = self._retrieved_total

        if bucket in self._done:
            self.logger.debug("スキップ (チェックポイント済): %s %s", pref_code, cname)
            stat["method"] = "スキップ(済)"
            return

        try:
            soup, total, overflow, result_url = self._do_search(search_url, pref_code, ccode)
        except Exception as e:
            self.logger.warning("%s %s: 検索失敗のためスキップ: %s", pref_code, cname, e)
            return

        stat["total"] = total
        stat["overflow"] = overflow

        if total == 0 and not overflow:
            self._done.add(bucket)
            self._save_checkpoint(seen)
            return

        if not overflow:
            # 500件以下 → 通常取得
            try:
                yield from self._paginate_and_yield(
                    result_url, soup, total, size, detail_url, seen,
                    label=f"{pref_code} {cname}", overflow=False,
                )
            except Exception as e:
                self.logger.warning("%s %s: ページ処理失敗: %s", pref_code, cname, e)
                return
            self._done.add(bucket)
            self._save_checkpoint(seen)
            stat["got"] = self._retrieved_total - ret_before
            return

        # 500件超 → L2 業者種別で分割
        self.logger.info("%s %s: 500件超 → 業者種別(法人種別)で分割", pref_code, cname)
        stat["method"] = "分割取得"
        yield from self._split_by_corp_cla(
            search_url, detail_url, size, pref_code, ccode, cname, seen
        )
        stat["got"] = self._retrieved_total - ret_before
        stat["incomplete"] = self._city_incomplete(ccode)

    def _split_by_corp_cla(
        self, search_url, detail_url, size, pref_code, ccode, cname, seen
    ) -> Generator[dict, None, None]:
        """L2: 市区町村 × 業者種別 (法人種別)。非統一資格・全法人に適用。

        業者種別が検出できない場合は営業品目分割 (L3) にフォールバックする。
        業者種別を試したが全件0件だった場合 (分割が機能していない疑い) も同様にフォールバック。
        """
        if not self._corp_clas:
            self.logger.warning(
                "%s %s: 業者種別が未検出 → 営業品目(統一資格のみ)分割にフォールバック",
                pref_code, cname,
            )
            yield from self._split_by_bizitem(
                search_url, detail_url, size, pref_code, ccode, cname, None, seen
            )
            return

        seen_total = 0
        for cla in self._corp_clas:
            cla_val, cla_lbl = cla["value"], cla.get("label", "")
            bucket = (pref_code, ccode, cla_val, "", "", "")
            if bucket in self._done:
                self.logger.debug("スキップ (済): %s %s 業者%s", pref_code, cname, cla_lbl)
                continue
            try:
                soup, total, overflow, result_url = self._do_search(
                    search_url, pref_code, ccode, corp_cla=cla_val
                )
            except Exception as e:
                self.logger.warning(
                    "%s %s 業者%s: 検索失敗: %s", pref_code, cname, cla_lbl, e
                )
                continue
            seen_total += total

            if total == 0 and not overflow:
                self._done.add(bucket)
                self._save_checkpoint(seen)
                continue

            lbl = f"{pref_code} {cname} 業者{cla_lbl}"
            if not overflow:
                try:
                    yield from self._paginate_and_yield(
                        result_url, soup, total, size, detail_url, seen,
                        label=lbl, overflow=False,
                    )
                except Exception as e:
                    self.logger.warning("%s: ページ処理失敗: %s", lbl, e)
                    continue
                self._done.add(bucket)
                self._save_checkpoint(seen)
            else:
                # 業者種別でも500件超 → L3 営業品目 (統一資格のみ) で細分化
                self.logger.info("%s: 500件超 → 資格の種類・営業品目で細分化 (統一資格のみ対象)", lbl)
                yield from self._split_by_bizitem(
                    search_url, detail_url, size, pref_code, ccode, cname, cla_val, seen
                )

        if self._corp_clas and seen_total == 0:
            # 全業者種別で0件 → 業者種別フィルタが機能していない疑い。営業品目にフォールバック。
            self.logger.warning(
                "%s %s: 業者種別分割の合計が0件でした。分割が機能していない可能性があります。"
                " → 営業品目(統一資格のみ)分割にフォールバック", pref_code, cname,
            )
            yield from self._split_by_bizitem(
                search_url, detail_url, size, pref_code, ccode, cname, None, seen
            )

    def _split_by_bizitem(
        self, search_url, detail_url, size, pref_code, ccode, cname, corp_cla, seen
    ) -> Generator[dict, None, None]:
        """L3: (市区町村 [× 業者種別]) × 資格の種類・営業品目 (統一資格のみ)。

        営業品目が未検出の場合は分割できないため、当該バケットを未完了として記録する
        (先頭500件のみ取得して完了扱いにはしない)。
        """
        cla_lbl = f" 業者{corp_cla}" if corp_cla else ""
        if not self._biz_items:
            # 営業品目が検出できない → これ以上分割できない。未完了として記録。
            self.logger.warning(
                "%s %s%s: 500件超だが営業品目コード未検出 → 分割不可。未完了: 追加分割必要",
                pref_code, cname, cla_lbl,
            )
            self._record_overflow(pref_code, ccode, cname, {"corp_cla": corp_cla},
                                  reason="営業品目コード未検出で分割不可")
            # データ収集のため先頭500件だけは取得する (ただし done には入れない)。
            yield from self._harvest_overflow_leaf(
                search_url, detail_url, size, pref_code, ccode, cname,
                dict(corp_cla=corp_cla),
                label=f"{pref_code} {cname}{cla_lbl}",
            )
            return

        for item in self._biz_items:
            biz, kind = item["value"], item.get("kind", "")
            bucket = (pref_code, ccode, corp_cla or "", biz, "", "")
            if bucket in self._done:
                self.logger.debug("スキップ (済): %s %s%s 品目%s", pref_code, cname, cla_lbl, biz)
                continue
            try:
                soup, total, overflow, result_url = self._do_search(
                    search_url, pref_code, ccode,
                    corp_cla=corp_cla, biz_kind=kind, biz_item=biz,
                )
            except Exception as e:
                self.logger.warning("%s %s%s 品目%s: 検索失敗: %s",
                                    pref_code, cname, cla_lbl, biz, e)
                continue

            if total == 0 and not overflow:
                self._done.add(bucket)
                self._save_checkpoint(seen)
                continue

            lbl = f"{pref_code} {cname}{cla_lbl} 品目{biz}"
            if not overflow:
                try:
                    yield from self._paginate_and_yield(
                        result_url, soup, total, size, detail_url, seen,
                        label=lbl, overflow=False,
                    )
                except Exception as e:
                    self.logger.warning("%s: ページ処理失敗: %s", lbl, e)
                    continue
                self._done.add(bucket)
                self._save_checkpoint(seen)
            else:
                self.logger.info("%s: 500件超 → 企業規模で細分化", lbl)
                yield from self._split_by_scale(
                    search_url, detail_url, size, pref_code, ccode, cname,
                    corp_cla, kind, biz, seen,
                )

    def _split_by_scale(
        self, search_url, detail_url, size, pref_code, ccode, cname,
        corp_cla, biz_kind, biz_item, seen,
    ) -> Generator[dict, None, None]:
        """L4: ... × 企業規模 (統一資格のみ)。"""
        cla_lbl = f" 業者{corp_cla}" if corp_cla else ""
        for scale_val, scale_name in _COMPANY_SCALES:
            bucket = (pref_code, ccode, corp_cla or "", biz_item, scale_val, "")
            if bucket in self._done:
                continue
            try:
                soup, total, overflow, result_url = self._do_search(
                    search_url, pref_code, ccode,
                    corp_cla=corp_cla, biz_kind=biz_kind, biz_item=biz_item,
                    company_scale=scale_val,
                )
            except Exception as e:
                self.logger.warning("%s %s%s 品目%s 規模%s: 検索失敗: %s",
                                    pref_code, cname, cla_lbl, biz_item, scale_name, e)
                continue

            if total == 0 and not overflow:
                self._done.add(bucket)
                self._save_checkpoint(seen)
                continue

            lbl = f"{pref_code} {cname}{cla_lbl} 品目{biz_item} 規模{scale_name}"
            if not overflow:
                try:
                    yield from self._paginate_and_yield(
                        result_url, soup, total, size, detail_url, seen,
                        label=lbl, overflow=False,
                    )
                except Exception as e:
                    self.logger.warning("%s: ページ処理失敗: %s", lbl, e)
                    continue
                self._done.add(bucket)
                self._save_checkpoint(seen)
            else:
                self.logger.info("%s: 500件超 → 資格等級で細分化", lbl)
                yield from self._split_by_grade(
                    search_url, detail_url, size, pref_code, ccode, cname,
                    corp_cla, biz_kind, biz_item, scale_val, scale_name, seen,
                )

    def _split_by_grade(
        self, search_url, detail_url, size, pref_code, ccode, cname,
        corp_cla, biz_kind, biz_item, scale_val, scale_name, seen,
    ) -> Generator[dict, None, None]:
        """L5 (最終): ... × 資格等級 (統一資格のみ)。

        資格等級でもなお500件超のバケットは done に入れず overflow_unresolved.json へ記録し、
        「未完了: 追加分割必要」とログする。先頭500件のみは取得するが完了扱いにはしない。
        """
        cla_lbl = f" 業者{corp_cla}" if corp_cla else ""
        for grade_val, grade_name in _QUAL_GRADES:
            bucket = (pref_code, ccode, corp_cla or "", biz_item, scale_val, grade_val)
            if bucket in self._done:
                continue
            try:
                soup, total, overflow, result_url = self._do_search(
                    search_url, pref_code, ccode,
                    corp_cla=corp_cla, biz_kind=biz_kind, biz_item=biz_item,
                    company_scale=scale_val, qual_grade=grade_val,
                )
            except Exception as e:
                self.logger.warning("%s %s%s 品目%s 規模%s 等級%s: 検索失敗: %s",
                                    pref_code, cname, cla_lbl, biz_item, scale_name, grade_name, e)
                continue

            if total == 0 and not overflow:
                self._done.add(bucket)
                self._save_checkpoint(seen)
                continue

            lbl = f"{pref_code} {cname}{cla_lbl} 品目{biz_item} 規模{scale_name} 等級{grade_name}"
            try:
                yield from self._paginate_and_yield(
                    result_url, soup, total, size, detail_url, seen,
                    label=lbl, overflow=overflow,
                )
            except Exception as e:
                self.logger.warning("%s: ページ処理失敗: %s", lbl, e)
                continue

            if overflow:
                # 最終分割でもなお500件超 → 完了扱いにしない。未完了として記録。
                self.logger.warning("%s: 500件超のまま。未完了: 追加分割必要", lbl)
                self._record_overflow(
                    pref_code, ccode, cname,
                    {"corp_cla": corp_cla, "biz_item": biz_item,
                     "company_scale": scale_val, "qual_grade": grade_val},
                    reason="全分割後もなお500件超",
                )
                # done には入れない
            else:
                self._done.add(bucket)
                self._save_checkpoint(seen)

    def _harvest_overflow_leaf(
        self, search_url, detail_url, size, pref_code, ccode, cname, cond, label,
    ) -> Generator[dict, None, None]:
        """未完了(分割不可)バケットの先頭500件だけをデータ確保のため取得する。

        done には入れない (呼び出し側で overflow 記録済み)。seen 経由で重複排除される。
        """
        try:
            soup, total, overflow, result_url = self._do_search(
                search_url, pref_code, ccode,
                corp_cla=cond.get("corp_cla"),
                biz_kind=cond.get("biz_kind"), biz_item=cond.get("biz_item"),
                company_scale=cond.get("company_scale"), qual_grade=cond.get("qual_grade"),
            )
        except Exception as e:
            self.logger.warning("%s: 先頭500件取得の再検索失敗: %s", label, e)
            return
        if total == 0 and not overflow:
            return
        seen = self._current_seen
        yield from self._paginate_and_yield(
            result_url, soup, total, size, detail_url, seen,
            label=label, overflow=overflow,
        )

    # ------------------------------------------------------------------ #
    #  記録ヘルパ (未完了 / 失敗 / 件数CSV)                                 #
    # ------------------------------------------------------------------ #

    def _record_overflow(self, pref_code, ccode, cname, cond, reason) -> None:
        self._overflow_unresolved.append({
            "pref_code": pref_code,
            "city_code": ccode,
            "city_name": cname,
            "condition": {k: v for k, v in cond.items() if v},
            "reason": reason,
        })
        st = self._city_stats.get(ccode)
        if st:
            st["incomplete"] = True

    def _city_incomplete(self, ccode) -> bool:
        return any(o["city_code"] == ccode for o in self._overflow_unresolved)

    def _write_overflow_and_failures(self) -> None:
        try:
            _OVERFLOW_FILE.write_text(
                json.dumps(self._overflow_unresolved, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            self.logger.info(
                "未完了バケット %d件 を %s に保存", len(self._overflow_unresolved), _OVERFLOW_FILE
            )
        except Exception as e:
            self.logger.warning("overflow_unresolved 保存失敗: %s", e)
        if self._failed_detail:
            try:
                _FAILED_DETAIL_FILE.write_text(
                    json.dumps(self._failed_detail, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                self.logger.info(
                    "詳細取得失敗 %d件 を %s に保存", len(self._failed_detail), _FAILED_DETAIL_FILE
                )
            except Exception as e:
                self.logger.warning("failed_detail 保存失敗: %s", e)

    def _write_city_counts_csv(self, count_only: bool = False) -> None:
        """市区町村ごとの件数サマリを CSV に書き出す (要望2)。"""
        try:
            with open(_CITY_COUNTS_CSV, "w", encoding="utf-8-sig", newline="") as f:
                w = csv.writer(f)
                w.writerow([
                    "都道府県コード", "市区町村コード", "市区町村名", "件数",
                    "500件超フラグ", "取得方法", "取得件数", "未取得可能性",
                ])
                for ccode, s in sorted(self._city_stats.items()):
                    w.writerow([
                        _TARGET_PREF, ccode, s["name"], s["total"],
                        "true" if s["overflow"] else "false",
                        s["method"],
                        "" if count_only else s["got"],
                        "true" if s["incomplete"] else "false",
                    ])
            self.logger.info("市区町村件数CSVを保存: %s", _CITY_COUNTS_CSV)
        except Exception as e:
            self.logger.warning("市区町村件数CSV保存失敗: %s", e)

    # ------------------------------------------------------------------ #
    #  詳細スクレイプ                                                      #
    # ------------------------------------------------------------------ #

    def _scrape_detail(self, soup: BeautifulSoup, source_url: str) -> dict | None:
        tables = soup.find_all("table")
        if not tables:
            return None

        basic: dict[str, str] = {}
        for row in tables[0].find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                basic[th.get_text(strip=True)] = _clean(td.get_text(strip=True))

        name = basic.get("商品号又は名称", "")
        if not name:
            return None

        full_addr = basic.get("本社住所", "")
        pref_m = _PREF_PATTERN.match(full_addr)
        pref = pref_m.group(1) if pref_m else ""
        addr = full_addr[len(pref):].strip() if pref else full_addr

        shikaku_bangou = ""
        yukokikan = ""
        kigyo_kibo = ""
        shikaku_tou = ""
        chiku = ""

        for t in tables[1:]:
            classes = set(t.get("class", []))

            if "bid-details" in classes or "change-details" in classes:
                continue

            if "main-table-pattern2" in classes:
                rows = t.find_all("tr")
                if not rows:
                    continue
                header_ths = [th.get_text(strip=True) for th in rows[0].find_all("th")]
                cats = header_ths[1:] if len(header_ths) > 1 else _QUAL_CATEGORIES
                for row in rows[1:]:
                    th_el = row.find("th")
                    if th_el and "資格等級" in th_el.get_text():
                        grades = [td.get_text(strip=True) for td in row.find_all("td")]
                        parts = [
                            f"{cat}:{g}"
                            for cat, g in zip(cats, grades)
                            if g and g not in ("ー", "－", "-")
                        ]
                        shikaku_tou = " / ".join(parts)
                        break

            elif "main-table-pattern3" in classes:
                rows = t.find_all("tr")
                if len(rows) >= 2:
                    region_hdrs = [th.get_text(strip=True) for th in rows[0].find_all("th")]
                    region_vals = [td.get_text(strip=True) for td in rows[1].find_all("td")]
                    chiku = " ".join(rh for rh, rv in zip(region_hdrs, region_vals) if rv == "○")

            else:
                for row in t.find_all("tr"):
                    th = row.find("th")
                    td = row.find("td")
                    if not (th and td):
                        continue
                    key = th.get_text(strip=True)
                    val = _clean(td.get_text(strip=True))
                    if key == "資格番号":
                        shikaku_bangou = val
                    elif key == "有効期間":
                        yukokikan = val
                    elif key == "企業規模":
                        kigyo_kibo = val

        bid_tables = [t for t in tables if "bid-details" in set(t.get("class", []))]
        rakusatsu_count = sum(
            max(0, len(t.find_all("tr")) - 1)
            for t in bid_tables
        )

        return {
            Schema.NAME: name,
            Schema.CO_NUM: basic.get("法人番号", ""),
            Schema.POST_CODE: basic.get("郵便番号", ""),
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.POS_NM: basic.get("代表者役職", ""),
            Schema.REP_NM: basic.get("代表者氏名", ""),
            Schema.URL: source_url,
            "業者種別": basic.get("業者種別", ""),
            "資格番号": shikaku_bangou,
            "有効期間": yukokikan,
            "企業規模": kigyo_kibo,
            "資格等級": shikaku_tou,
            "競争参加地域": chiku,
            "落札実績件数": str(rakusatsu_count) if rakusatsu_count else "",
        }


def _parse_cli_flags() -> dict:
    """コマンドライン引数を解析する。--reset / --count-only を受け付ける。"""
    args = set(sys.argv[1:])
    return {
        "reset": "--reset" in args,
        "count_only": "--count-only" in args,
    }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    flags = _parse_cli_flags()
    scraper = PPortal5Scraper()
    scraper.reset_checkpoint = flags["reset"]
    scraper.count_only = flags["count_only"]
    # _harvest_overflow_leaf が参照する共有 seen を parse 内で設定するためのフック
    scraper._current_seen = set()

    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.p-portal.go.jp/pps-web-biz/UAB01/OAB0103")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
