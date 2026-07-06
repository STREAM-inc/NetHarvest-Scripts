"""
調達ポータル【法人】— 事業者情報公開機能【東京版】

運営: デジタル庁
URL: https://www.p-portal.go.jp/pps-web-biz/UAB01/OAB0103

このスクリプトは p_portal.py (全国版) の派生で、以下2点をユーザー要望に沿って変更している:

    1. 【東京都のみ】に対象を限定 (_TARGET_PREF = "13")。
       parse() のループを東京都だけに絞り、23区+市町村を巡回する。

    2. 【資格の種類・営業品目フィルタを明示実装】。
       検索フォームの「資格の種類・営業品目」モーダルは
         - qualificationKind (資格の種類: 1=物品の製造 / 2=物品の販売 /
           3=役務の提供等 / 4=物品の買い受け) のチェックボックス
         - bizItem (営業品目コード: 101〜, 201〜, 301〜, 401〜 の75種)
       の二段構造になっており、各 bizItem チェックボックスの name 接尾辞
       (…bizItem{N}) がその品目の属する資格の種類 N を表す (DOM調査で確認)。
       検索を確実に効かせるため「資格の種類チェック + 営業品目チェック + 確定」を
       セットで送信する。

取得対象:
    - 調達ポータルに登録された事業者の基本情報・統一資格情報・落札実績件数

取得フロー:
    1. OAB0101 (事業者情報検索フォーム) へアクセス
    2. 本社住所 (東京都 + 市区町村) を設定して検索送信 → OAB0103
    3. フォーム送信後の実 URL をベースに ?page=N でページネーション
       (1ページ50件固定・最大500件/検索)。範囲外 page= は page0 にクランプされるため
       総件数からページ数を算出して停止する。
    4. 1検索ぶんを2フェーズ処理 (フェッチ相=全ページの一覧から新規法人番号を収集 →
       取得相=収集した全件を OAB0108 へ page.request.post() して詳細取得) で
       フェッチ/取得件数を一致させ、中断時の不整合を防ぐ。
    5. 基本情報・統一資格情報・落札実績件数を抽出して yield。

大量取得戦略 (本社住所 × 資格の種類・営業品目 × 企業規模 × 資格等級):
    - サーバーは1検索最大500件のため、条件を組み合わせて各バケットを500件以下に収める。
    - 第1レベル: 本社住所 = 東京都 × 市区町村 (500件以下ならそのまま全件取得)。
    - 第2レベル: 市区町村が500件超 → 資格の種類・営業品目 (75種) で分割。
    - 第3レベル: (市区町村 × 営業品目) でも500件超 → 企業規模 (大/中小/小規模/その他) で分割。
    - 第4レベル: (市区町村 × 営業品目 × 企業規模) でも500件超 → 資格等級 (A/B/C/D) で分割。
      ※全国版 (p_portal.py) は第3レベルまでで打ち切るが、本東京版では
        「315.その他 に数万件が該当し取りこぼす」というユーザー指摘を受け、
        資格等級による第4レベル分割を追加して回収率を最大化する。
    - 資格等級でもなお500件超のバケットは先頭500件のみ取得し次へ進む
      (WARNING "取りこぼしの可能性あり" を出力)。
    - 法人番号による重複排除を実施 (共通の seen で各パターンを自動マージ)。

    ⚠ 完全性の注意:
        「営業品目」「企業規模」「資格等級」フィルタは "統一資格から検索する場合のみ" の
        条件で統一資格保有者のみを返す。500件超の市区町村については無資格事業者の超過分を
        取りこぼす (本方針では統一資格保有者の網羅を優先)。サーバー側の500件上限のため、
        単一の営業品目 (例 315.その他) が全分割後もなお500件超なら完全網羅はできない。

設計メモ:
    - OAB0103/OAB0108 は直打ちアクセス不可 (JSESSIONID + CSRFトークン必須)。
      OAB0101 のフォーム送信で確立したセッションを維持したまま page.request.post() で
      OAB0108 を呼び出す。
    - ページネーション URL は _do_search() が返す「フォーム送信後の実 URL」を使う。
    - 本社住所・資格の種類/営業品目・企業規模・資格等級の入力要素は DOM に常在し
      name 属性を持つため、モーダルを開かずに JS で値を設定し change を発火するだけで
      検索条件として送信される。市区町村は都道府県の change で AJAX 補充されるため待機。
      営業品目はモーダルのため確定ボタン (#qualificationKindBizItemSelected) の click が
      必要だが、企業規模・資格等級はアコーディオン内のチェックボックスで確定ボタン不要。
    - 件数判定: "N件見つかりました" = 実件数 / "500件を超え" = 上限到達(500扱い) /
      "合致する事業者情報がありません" = 0件。
    - 統一資格情報・落札実績情報が存在しない事業者は対応フィールドを空文字で返す。
    - HTML の th「商品号又は名称」の実体は商号・名称 (Schema.NAME に対応)。
    - 代表者役職・代表者氏名が「－」の事業者は空文字に正規化する。

実行方法:
    # ローカルテスト
    python scripts/sites/government/p_portal_5.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id p_portal_5
"""

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

# 企業規模 (検索第3レベル分割)。値はフォームの companyScale チェックボックスの value。
# 大企業/中小企業/小規模企業/その他 の4区分で全事業者を分割する。
# (新規中小企業者=5 は中小/小規模の部分集合のため除外し、重複検索を避ける)
_COMPANY_SCALES = [
    ("1", "大企業"),
    ("2", "中小企業"),
    ("4", "小規模企業"),
    ("3", "その他"),
]

# 資格等級 (検索第4レベル分割)。値はフォームの qualificationGrade チェックボックスの value。
# 統一資格の等級 A/B/C/D。315.その他 のような巨大バケットをさらに細分化するために使う。
_QUAL_GRADES = [("A", "A"), ("B", "B"), ("C", "C"), ("D", "D")]

# チェックポイントファイル (中断リジューム用)。全国版とは別ファイルにする。
# key "seen": 取得済み法人番号リスト。key "done": 完了済みバケットのリスト。
# 削除すれば全バケットを先頭から再実行する。
_CHECKPOINT_FILE = Path(__file__).parent / ".ckpt_p_portal_5.json"

# --- フォーム要素セレクタ (2025年DOM調査で確定) ---
_CITY_READY_JS = (
    "() => { const s = document.querySelector('#city_select');"
    " return s && s.options.length > 1; }"
)

# 本社住所 (市区町村方式) を設定する JS。
# #select-city ラジオ(method=02)を ON にし、都道府県セレクトに値をセットして
# change を発火 → サーバーが市区町村セレクトを AJAX 補充する。
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

# 資格の種類・営業品目モーダルで「資格の種類」と「営業品目」を両方 ON にし確定する JS。
# args = [kind, value]。kind は qualificationKind の value ("1"〜"4")、
# value は bizItem の value (営業品目コード, 例 "315")。
# 二段構造 (資格の種類 → 営業品目) のため、両方をチェックしてから確定する。
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

# 企業規模チェックを ON にする JS。アコーディオン内だが確定ボタンは無く、
# チェック + change 発火のみで検索条件として送信される。
_JS_SET_SCALE = """
(value) => {
    const cb = [...document.querySelectorAll('input[name="inputSearchCondtionBean.companyScale"]')]
        .find(c => c.value === value);
    if (cb) { cb.checked = true; cb.dispatchEvent(new Event('change', {bubbles:true})); }
}
"""

# 資格等級チェックを ON にする JS。企業規模と同様に確定ボタン不要。
_JS_SET_GRADE = """
(value) => {
    const cb = [...document.querySelectorAll('input[name="inputSearchCondtionBean.qualificationGrade"]')]
        .find(c => c.value === value);
    if (cb) { cb.checked = true; cb.dispatchEvent(new Event('change', {bubbles:true})); }
}
"""

# 検索ボタンを押す JS (name 属性がサイト改修で変わることがあるため複数候補)。
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
    """フォーム送信後の実 URL にページ番号を合成する。

    result_url がすでに page= を含む場合は置換、なければ追記する。
    これにより検索コンテキスト (セッション紐付けパラメータ) を維持したまま
    ページネーションできる。
    """
    if "page=" in result_url:
        url = re.sub(r"page=\d+", f"page={page_num}", result_url)
    elif "?" in result_url:
        url = f"{result_url}&page={page_num}&size={size}"
    else:
        url = f"{result_url}?page={page_num}&size={size}"
    # size= がなければ追記
    if "size=" not in url:
        url = f"{url}&size={size}"
    return url


class PPortal5Scraper(DynamicCrawler):
    """調達ポータル【法人】事業者情報公開機能【東京版】スクレイパー"""

    DELAY = 1.0
    # サーバーは 1ページ50件固定 (size= パラメータは無視される)。
    PAGE_SIZE = 50
    EXTRA_COLUMNS = [
        "業者種別",      # 株式会社 / 合同会社 等 (構造化ラベル)
        "資格番号",      # 例: 0000100505
        "有効期間",      # 例: 令和07・08・09
        "企業規模",      # 大企業 / 中小企業 / 小規模企業 / その他
        "資格等級",      # 例: 役務の提供等:A / 物品の販売:A
        "競争参加地域",  # 例: 北海道 東北 関東・甲信越 東海・北陸 近畿 中国 四国 九州・沖縄
        "落札実績件数",  # 落札実績の総件数 (整数文字列)
    ]

    def _load_search_form(self, search_url: str) -> bool:
        """検索フォーム (OAB0101) を開き、操作可能になるまで待つ。

        固定時間待ち (wait_for_timeout) はワーカー環境が遅いと不足し、市区町村 AJAX や
        営業品目検出が空振りする。代わりに都道府県セレクトが populate されること
        (フォーム JS の初期化完了の指標) を待つ。成功で True を返す。
        """
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
        """指定都道府県の市区町村リストを取得する (失敗時は最大3回リトライ)。

        OAB0101 を開き、都道府県セレクトに値をセットして change を発火すると
        サーバーが市区町村セレクトを AJAX 補充する。補充完了を待って
        [{code, name}, ...] を返す。ワーカーの一時的な遅延・失敗に備えてリトライする。
        """
        for attempt in range(1, 4):
            if not self._load_search_form(search_url):
                continue
            try:
                self.page.evaluate(_JS_SET_PREF, pref_code)
                # 市区町村セレクトが AJAX 補充されるまで待機
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

    # ------------------------------------------------------------------ #
    #  チェックポイント / レジューム                                       #
    # ------------------------------------------------------------------ #

    def _load_checkpoint(self) -> tuple[set[str], set[tuple]]:
        """チェックポイントファイルを読み込み (seen, done) を返す。
        ファイルが存在しない・壊れている場合は空セットを返す。
        """
        if _CHECKPOINT_FILE.exists():
            try:
                data = json.loads(_CHECKPOINT_FILE.read_text(encoding="utf-8"))
                seen = set(data.get("seen", []))
                done: set[tuple] = {tuple(k) for k in data.get("done", [])}
                self.logger.info(
                    "チェックポイント読込: seen=%d件 done=%d件 → 完了済みバケットをスキップします",
                    len(seen), len(done),
                )
                return seen, done
            except Exception as e:
                self.logger.warning("チェックポイント読込失敗: %s → 初期化して続行", e)
        return set(), set()

    def _save_checkpoint(self, seen: set[str]) -> None:
        """seen と self._done をチェックポイントファイルへ書き出す。"""
        try:
            _CHECKPOINT_FILE.write_text(
                json.dumps(
                    {"seen": list(seen), "done": [list(k) for k in self._done]},
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
        except Exception as e:
            self.logger.warning("チェックポイント保存失敗: %s", e)

    def _do_search(
        self,
        search_url: str,
        pref_code: str,
        city_code: str,
        biz_kind: str | None = None,
        biz_item: str | None = None,
        company_scale: str | None = None,
        qual_grade: str | None = None,
    ) -> tuple[BeautifulSoup, int, bool, str]:
        """検索条件を設定して検索を実行し、(soup, 件数, 上限超フラグ, 実 URL) を返す。

        本社住所・資格の種類/営業品目・企業規模・資格等級の入力要素は DOM に常在し
        name 属性を持つため、モーダルを開かずに JS で値を設定して change を発火するだけで
        検索条件として送信される。市区町村は都道府県の change で AJAX 補充されるため待機する。

        biz_kind      : 資格の種類コード ("1"〜"4")。biz_item とペアで指定する。
        biz_item      : 営業品目コード (第2レベル分割時のみ)。
        company_scale : 企業規模の value (第3レベル分割時のみ。1/2/4/3)。
        qual_grade    : 資格等級の value (第4レベル分割時のみ。A/B/C/D)。

        戻り値の result_url はフォーム送信後にブラウザが表示した実 URL。
        これをベースにページネーションすることで検索コンテキストを維持する。

        overflow=True のとき total は 500 (上限) を表す。
        """
        # フォーム確立をリトライ。セッション切れ・一時的な遅延に備えて最大3回。
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

        # --- 資格の種類・営業品目 (第2レベル分割時のみ / 資格の種類 + 営業品目をセット送信) ---
        if biz_item:
            self.page.evaluate(_JS_SET_BIZ, [biz_kind or "", biz_item])
            self.page.wait_for_timeout(300)

        # --- 企業規模 (第3レベル分割時のみ) ---
        if company_scale:
            self.page.evaluate(_JS_SET_SCALE, company_scale)
            self.page.wait_for_timeout(300)

        # --- 資格等級 (第4レベル分割時のみ) ---
        if qual_grade:
            self.page.evaluate(_JS_SET_GRADE, qual_grade)
            self.page.wait_for_timeout(300)

        # --- 検索送信 (フォーム POST → OAB0103 へ遷移) ---
        try:
            with self.page.expect_navigation(wait_until="domcontentloaded", timeout=30000):
                self.page.evaluate(_JS_CLICK_SEARCH)
        except Exception as e:
            self.logger.warning(
                "検索送信失敗 (pref=%s city=%s kind=%s biz=%s scale=%s grade=%s): %s",
                pref_code, city_code, biz_kind, biz_item, company_scale, qual_grade, e,
            )
            return BeautifulSoup("", "html.parser"), 0, False, self.page.url
        self.page.wait_for_timeout(1200)

        # フォーム送信後の実 URL をキャプチャ (ページネーション URL 生成に使用)
        result_url = self.page.url

        soup = BeautifulSoup(self.page.content(), "html.parser")
        text = soup.get_text()
        overflow = "500件を超え" in text
        m = re.search(r"(\d[\d,]*)件見つかりました", text)
        if m:
            total = int(m.group(1).replace(",", ""))
        else:
            total = 500 if overflow else 0
        return soup, total, overflow, result_url

    def _paginate_and_yield(
        self,
        result_url: str,
        first_soup: BeautifulSoup,
        total: int,
        size: int,
        detail_url: str,
        seen: set[str],
        label: str = "",
    ) -> Generator[dict, None, None]:
        """1検索ぶんを「全ページのフェッチ → 全件の詳細取得」の2フェーズで処理する。

        フェッチ相: 全ページの一覧を先に巡回し、新規法人番号 (seen 未登録) を収集する。
        取得相    : 収集した全件の詳細をまとめて取得して yield する。
        こうすることで「フェッチしたものを全て取得してから次の検索へ」を保証し、
        中断してもフェッチ済みの検索は取得まで完了している状態になる。

        result_url: _do_search() が返したフォーム送信後の実 URL。
        label     : 進捗ログ用の検索ラベル。
        """
        # === フェッチ相: 全ページの一覧から新規法人番号を収集 ===
        # サーバーは1ページ50件固定で、範囲外の page= は page0 にクランプされ
        # 同じ結果を返す。したがって "corp_links 空" では止まらないため、
        # 総件数からページ数を算出して停止する。念のため直前ページと同一内容に
        # なった場合 (クランプ) も打ち切る。
        max_pages = max(1, -(-min(total, 500) // self.PAGE_SIZE))  # ceil
        prev_hrefs: list[str] | None = None
        entries: list[tuple[str, str, str, str]] = []  # (corp_no, art_qual_id, csrf, page_url)

        for page_num in range(max_pages):
            if page_num == 0:
                soup = first_soup
                page_url = result_url
            else:
                page_url = _build_page_url(result_url, page_num, self.PAGE_SIZE)
                soup = self.get_soup(page_url)
                if soup is None:
                    break

            csrf_input = soup.find("input", {"name": "_csrf"})
            csrf = csrf_input["value"] if csrf_input else ""

            corp_links = soup.select("table tbody tr td:nth-child(3) a")
            if not corp_links:
                self.logger.debug("ページ%d: corp_links 0件 → ページネーション終了", page_num)
                break

            # クランプ検出: 範囲外ページは直前ページと同一内容になるため打ち切る
            page_hrefs = [link.get("href", "") for link in corp_links]
            if prev_hrefs is not None and page_hrefs == prev_hrefs:
                self.logger.debug("ページ%d: 直前ページと同一 (クランプ) → 終了", page_num)
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
                    continue
                seen.add(corp_no)
                entries.append((corp_no, m_art.group(1) if m_art else "", csrf, page_url))

        self._fetched_total += len(entries)

        # === 取得相: 収集した全件の詳細を取得 ===
        got = 0
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
                    continue

                detail_soup = BeautifulSoup(resp.text(), "html.parser")
                article_el = detail_soup.find("article")
                if article_el and "事業者情報を取得できません" in article_el.get_text():
                    self.logger.debug("corp %s: 情報なし", corp_no)
                    continue

                item = self._scrape_detail(detail_soup, page_url)
                if item:
                    self._retrieved_total += 1
                    got += 1
                    yield item

            except Exception as e:
                self.logger.warning("Error scraping corp %s: %s", corp_no, e)

        # === 累計の突合ログ (検索単位でフェッチ/取得の合計を表示) ===
        if entries:
            self.logger.info(
                "📥 %s: フェッチ%d件 取得%d件 → 累計 フェッチ%d件 / 取得%d件",
                label or "(検索)", len(entries), got,
                self._fetched_total, self._retrieved_total,
            )

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 🔒 引数 url を唯一のルート (SSOT) として派生 URL を組み立てる。
        search_url = urljoin(url, "OAB0101")
        detail_url = urljoin(url, "OAB0108?")
        size = 50  # フォームに件数セレクトは無く、サーバー既定は 50件/ページ

        # チェックポイントから seen と完了済みバケットセットを復元。
        seen, self._done = self._load_checkpoint()
        # フェッチ(一覧で発見した新規法人番号)/取得(詳細取得に成功した件数)の累計
        self._fetched_total = 0
        self._retrieved_total = 0

        # 資格の種類・営業品目 一覧を動的取得 (第2レベル分割で使用)。
        # 各 bizItem チェックボックスから value(営業品目コード) と
        # name 接尾辞(資格の種類コード) を取得する。
        # フォーム JS 初期化 + チェックボックス出現を待ってから検出する。
        self._biz_items: list[dict[str, str]] = []
        for attempt in range(1, 4):
            self._load_search_form(search_url)
            try:
                self.page.wait_for_selector(
                    "input[type=checkbox][name*='bizItem']", timeout=15000
                )
            except Exception:
                pass
            self._biz_items = self.page.eval_on_selector_all(
                "input[type=checkbox][name*='bizItem']",
                "els => els.map(e => ({value: e.value,"
                " kind: (e.name.match(/bizItem(\\d+)$/) || ['',''])[1]}))"
                ".filter(o => o.value)",
            )
            if self._biz_items:
                break
            self.logger.warning("資格の種類・営業品目コード検出 0件 (試行%d/3) → 再試行", attempt)
        self.logger.info("資格の種類・営業品目コード %d件 を検出", len(self._biz_items))
        if not self._biz_items:
            self.logger.warning(
                "資格の種類・営業品目コードを検出できませんでした。500件超の市区町村は"
                "先頭500件のみ取得します (取りこぼしの可能性あり)"
            )

        # 第1レベル: 東京都 (_TARGET_PREF) × 市区町村
        for pref_code in _PREF_CODES:
            cities = self._get_cities(search_url, pref_code)
            if not cities:
                continue
            self.logger.info("都道府県%s (東京都): 市区町村 %d件", pref_code, len(cities))

            for city in cities:
                ccode, cname = city["code"], city["name"]
                bucket = (pref_code, ccode, "", "", "")

                if bucket in self._done:
                    self.logger.debug("スキップ (チェックポイント済): %s %s", pref_code, cname)
                    continue

                try:
                    soup, total, overflow, result_url = self._do_search(
                        search_url, pref_code, ccode
                    )
                except Exception as e:
                    self.logger.warning("%s %s: 検索失敗のためスキップ: %s", pref_code, cname, e)
                    continue

                if total == 0 and not overflow:
                    # 0件バケットも done 登録して次回の無駄な検索を省く。
                    self._done.add(bucket)
                    self._save_checkpoint(seen)
                    continue

                if not overflow:
                    try:
                        yield from self._paginate_and_yield(
                            result_url, soup, total, size, detail_url, seen,
                            label=f"{pref_code} {cname}",
                        )
                    except Exception as e:
                        self.logger.warning(
                            "%s %s: ページ処理失敗のためスキップ: %s", pref_code, cname, e
                        )
                        continue
                    self._done.add(bucket)
                    self._save_checkpoint(seen)

                elif not self._biz_items:
                    # 営業品目未検出で細分化不可 → 先頭500件のみ取得しスルー。
                    self.logger.warning(
                        "%s %s: 500件超だが営業品目未検出 → 先頭500件のみ (取りこぼしの可能性あり)",
                        pref_code, cname,
                    )
                    try:
                        yield from self._paginate_and_yield(
                            result_url, soup, total, size, detail_url, seen,
                            label=f"{pref_code} {cname}",
                        )
                    except Exception as e:
                        self.logger.warning(
                            "%s %s: ページ処理失敗のためスキップ: %s", pref_code, cname, e
                        )
                        continue
                    self._done.add(bucket)
                    self._save_checkpoint(seen)

                else:
                    # 第2レベル: 500件超 → 資格の種類・営業品目で細分化 (統一資格保有者が対象)。
                    self.logger.info("%s %s: 500件超 → 資格の種類・営業品目で細分化", pref_code, cname)
                    try:
                        yield from self._split_by_bizitem(
                            search_url, pref_code, ccode, cname, size, detail_url, seen
                        )
                    except Exception as e:
                        self.logger.warning(
                            "%s %s: 品目分割処理失敗のためスキップ: %s", pref_code, cname, e
                        )

        self.logger.info(
            "全バケット処理完了。チェックポイントを保持します: %s", _CHECKPOINT_FILE
        )
        self.total_items = len(seen)

    def _split_by_bizitem(
        self,
        search_url: str,
        pref_code: str,
        city_code: str,
        city_name: str,
        size: int,
        detail_url: str,
        seen: set[str],
    ) -> Generator[dict, None, None]:
        """市区町村 × 資格の種類・営業品目で検索を細分化する (第2レベル)。

        営業品目でも500件超の場合はさらに企業規模で分割する (第3レベル)。
        """
        for item in self._biz_items:
            biz, kind = item["value"], item.get("kind", "")
            bucket = (pref_code, city_code, biz, "", "")

            if bucket in self._done:
                self.logger.debug("スキップ (チェックポイント済): %s %s 品目%s",
                                  pref_code, city_name, biz)
                continue

            try:
                soup, total, overflow, result_url = self._do_search(
                    search_url, pref_code, city_code, biz_kind=kind, biz_item=biz
                )
            except Exception as e:
                self.logger.warning("%s %s 品目%s: 検索失敗のためスキップ: %s",
                                    pref_code, city_name, biz, e)
                continue

            if total == 0 and not overflow:
                self._done.add(bucket)
                self._save_checkpoint(seen)
                continue

            if not overflow:
                try:
                    yield from self._paginate_and_yield(
                        result_url, soup, total, size, detail_url, seen,
                        label=f"{pref_code} {city_name} 品目{biz}",
                    )
                except Exception as e:
                    self.logger.warning(
                        "%s %s 品目%s: ページ処理失敗のためスキップ: %s",
                        pref_code, city_name, biz, e,
                    )
                    continue
                self._done.add(bucket)
                self._save_checkpoint(seen)
            else:
                # 第3レベル: (市区町村 × 営業品目) でも500件超 → 企業規模で分割
                self.logger.info(
                    "%s %s 品目%s: 500件超 → 企業規模で細分化", pref_code, city_name, biz
                )
                try:
                    yield from self._split_by_scale(
                        search_url, pref_code, city_code, city_name,
                        kind, biz, size, detail_url, seen,
                    )
                except Exception as e:
                    self.logger.warning(
                        "%s %s 品目%s: 規模分割処理失敗のためスキップ: %s",
                        pref_code, city_name, biz, e,
                    )

    def _split_by_scale(
        self,
        search_url: str,
        pref_code: str,
        city_code: str,
        city_name: str,
        biz_kind: str,
        biz_item: str,
        size: int,
        detail_url: str,
        seen: set[str],
    ) -> Generator[dict, None, None]:
        """市区町村 × 営業品目 × 企業規模で検索を細分化する (第3レベル)。

        企業規模でもなお500件超の場合は資格等級で分割する (第4レベル)。
        """
        for scale_val, scale_name in _COMPANY_SCALES:
            bucket = (pref_code, city_code, biz_item, scale_val, "")

            if bucket in self._done:
                self.logger.debug("スキップ (チェックポイント済): %s %s 品目%s 規模%s",
                                  pref_code, city_name, biz_item, scale_name)
                continue

            try:
                soup, total, overflow, result_url = self._do_search(
                    search_url, pref_code, city_code,
                    biz_kind=biz_kind, biz_item=biz_item, company_scale=scale_val,
                )
            except Exception as e:
                self.logger.warning("%s %s 品目%s 規模%s: 検索失敗のためスキップ: %s",
                                    pref_code, city_name, biz_item, scale_name, e)
                continue

            if total == 0 and not overflow:
                self._done.add(bucket)
                self._save_checkpoint(seen)
                continue

            lbl = f"{pref_code} {city_name} 品目{biz_item} 規模{scale_name}"
            if not overflow:
                try:
                    yield from self._paginate_and_yield(
                        result_url, soup, total, size, detail_url, seen, label=lbl
                    )
                except Exception as e:
                    self.logger.warning("%s: ページ処理失敗のためスキップ: %s", lbl, e)
                    continue
                self._done.add(bucket)
                self._save_checkpoint(seen)
            else:
                # 第4レベル: (市区町村 × 営業品目 × 企業規模) でも500件超 → 資格等級で分割。
                # ※315.その他 のような巨大バケットの取りこぼし低減 (東京版で追加)。
                self.logger.info(
                    "%s %s 品目%s 規模%s: 500件超 → 資格等級で細分化",
                    pref_code, city_name, biz_item, scale_name,
                )
                try:
                    yield from self._split_by_grade(
                        search_url, pref_code, city_code, city_name,
                        biz_kind, biz_item, scale_val, scale_name,
                        size, detail_url, seen,
                    )
                except Exception as e:
                    self.logger.warning(
                        "%s: 等級分割処理失敗のためスキップ: %s", lbl, e
                    )

    def _split_by_grade(
        self,
        search_url: str,
        pref_code: str,
        city_code: str,
        city_name: str,
        biz_kind: str,
        biz_item: str,
        scale_val: str,
        scale_name: str,
        size: int,
        detail_url: str,
        seen: set[str],
    ) -> Generator[dict, None, None]:
        """市区町村 × 営業品目 × 企業規模 × 資格等級で細分化する (第4レベル / 最終)。

        資格等級 (A/B/C/D) の4区分で分割する。この区分でもなお500件超のバケットは
        先頭500件のみ取得し、追加の細分化は行わずに次のパターンへ進む
        (取りこぼしの可能性あり)。
        """
        for grade_val, grade_name in _QUAL_GRADES:
            bucket = (pref_code, city_code, biz_item, scale_val, grade_val)

            if bucket in self._done:
                self.logger.debug("スキップ (チェックポイント済): %s %s 品目%s 規模%s 等級%s",
                                  pref_code, city_name, biz_item, scale_name, grade_name)
                continue

            try:
                soup, total, overflow, result_url = self._do_search(
                    search_url, pref_code, city_code,
                    biz_kind=biz_kind, biz_item=biz_item,
                    company_scale=scale_val, qual_grade=grade_val,
                )
            except Exception as e:
                self.logger.warning("%s %s 品目%s 規模%s 等級%s: 検索失敗のためスキップ: %s",
                                    pref_code, city_name, biz_item, scale_name, grade_name, e)
                continue

            if total == 0 and not overflow:
                self._done.add(bucket)
                self._save_checkpoint(seen)
                continue

            lbl = f"{pref_code} {city_name} 品目{biz_item} 規模{scale_name} 等級{grade_name}"
            try:
                yield from self._paginate_and_yield(
                    result_url, soup, total, size, detail_url, seen, label=lbl
                )
            except Exception as e:
                self.logger.warning("%s: ページ処理失敗のためスキップ: %s", lbl, e)
                continue
            self._done.add(bucket)
            self._save_checkpoint(seen)
            if overflow:
                # 最終分割でもなお500件超 → 先頭500件のみ取得してスルー。
                self.logger.warning(
                    "%s: 500件超 → 先頭500件のみ (取りこぼしの可能性あり)", lbl
                )

    def _scrape_detail(self, soup: BeautifulSoup, source_url: str) -> dict | None:
        tables = soup.find_all("table")
        if not tables:
            return None

        # --- 基本情報テーブル (Table 0) ---
        basic: dict[str, str] = {}
        for row in tables[0].find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                basic[th.get_text(strip=True)] = _clean(td.get_text(strip=True))

        name = basic.get("商品号又は名称", "")
        if not name:
            return None

        # 都道府県 / 住所 の分割
        full_addr = basic.get("本社住所", "")
        pref_m = _PREF_PATTERN.match(full_addr)
        pref = pref_m.group(1) if pref_m else ""
        addr = full_addr[len(pref):].strip() if pref else full_addr

        # --- 統一資格情報 (Table 1以降を class で分類) ---
        shikaku_bangou = ""
        yukokikan = ""
        kigyo_kibo = ""
        shikaku_tou = ""
        chiku = ""

        for t in tables[1:]:
            classes = set(t.get("class", []))

            if "bid-details" in classes or "change-details" in classes:
                continue  # 落札実績・変更履歴は別処理

            if "main-table-pattern2" in classes:
                # 資格種類等テーブル: 資格等級行を抽出
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
                # 競争参加地域テーブル (2行: ヘッダ行 + ○/ー行)
                rows = t.find_all("tr")
                if len(rows) >= 2:
                    region_hdrs = [th.get_text(strip=True) for th in rows[0].find_all("th")]
                    region_vals = [td.get_text(strip=True) for td in rows[1].find_all("td")]
                    chiku = " ".join(rh for rh, rv in zip(region_hdrs, region_vals) if rv == "○")

            else:
                # 資格基本情報テーブル
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

        # --- 落札実績件数 (bid-details テーブル) ---
        bid_tables = [t for t in tables if "bid-details" in set(t.get("class", []))]
        rakusatsu_count = sum(
            max(0, len(t.find_all("tr")) - 1)  # ヘッダ行1行を除く
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
            # --- EXTRA ---
            "業者種別": basic.get("業者種別", ""),
            "資格番号": shikaku_bangou,
            "有効期間": yukokikan,
            "企業規模": kigyo_kibo,
            "資格等級": shikaku_tou,
            "競争参加地域": chiku,
            "落札実績件数": str(rakusatsu_count) if rakusatsu_count else "",
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = PPortal5Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.p-portal.go.jp/pps-web-biz/UAB01/OAB0103")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
