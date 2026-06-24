"""
調達ポータル【法人】— 事業者情報公開機能

運営: デジタル庁
URL: https://www.p-portal.go.jp/pps-web-biz/UAB01/OAB0103

取得対象:
    - 調達ポータルに登録された事業者の基本情報・統一資格情報・落札実績件数

取得フロー:
    1. OAB0101 (事業者情報検索フォーム) へアクセス
    2. 「本社住所を選択する」モーダルで 都道府県 + 市区町村 を設定して検索送信 → OAB0103
    3. フォーム送信後の実 URL をベースに ?page=N でページネーション (1ページ50件固定・最大500件/検索)
       ※ size= は無視され常に50件/ページ。範囲外 page= は page0 にクランプされるため
         総件数からページ数を算出して停止する (実機検証済)。
    4. 1検索ぶんを2フェーズで処理する (フェッチと取得の件数を一致させ、中断時の不整合を防ぐ):
       (フェッチ相) 全ページの一覧を先に巡回し新規法人番号を収集 → (取得相) 収集した
       全件の法人番号を OAB0108 へ page.request.post() して詳細ページを取得
    5. 基本情報・統一資格情報・落札実績件数を抽出して yield
       進捗は10件刻みではなく、検索単位で「フェッチ累計 / 取得累計」を突合ログ出力する。

大量取得戦略 (段階的に絞り込む = 必要な時だけ深掘りしリクエスト数を最小化):
    - 1回の検索で最大500件のサーバー制限を回避するため検索条件を段階的に分割する。
    - 第1レベル: 都道府県 (01〜47) × 市区町村 で分割。
                市区町村リストは都道府県選択時の AJAX 応答から動的に取得する。
    - 第2レベル: ある市区町村が500件超に達した場合のみ、以下2方式を併用して網羅する
                (両者は共通の seen で重複排除・自動マージ)。
                  (a) 「資格の種類・営業品目」(営業品目コード) で分割
                      → 統一資格保有者をカテゴリ別に取得。英数字始まり等かな分割で
                        当たらない商号も拾える。
                  (b) 市区町村 × 商号 (かな頭文字) で分割
                      → 無資格事業者を含む全事業者を網羅する。
    - 第3レベル: (市区町村 × 営業品目) でも500件超の場合のみ、商号 (かな頭文字1文字) で分割する。
    - 第4レベル: (市区町村 × かな1文字) でも500件超の場合のみ、商号を2文字
                (アア, アイ, …) に細分化する (_KANA_MAX_DEPTH=2)。
                商号プレフィックスは統一資格の有無に関わらず効くため無資格事業者も割れる。
    - 法人番号による重複排除を実施。

    ⚠ 完全性の注意:
        (a) の「営業品目」フィルタは "統一資格から検索する場合のみ" の条件であり統一資格
        保有者のみを返すが、(b) の市区町村かな分割で無資格事業者も併せて取得するため、
        両方式の併用で取りこぼしを最小化している。
        企業規模・資格等級・競争参加地域も統一資格条件のため無資格事業者の超過分を
        割れない (実機検証で札幌市中央区×かな「ア」は500超でも統一資格保有者は計50件のみ)。
        そのため無資格を含む超過分は商号2文字分割で割る方針とした。
        なお商号2文字でもなお500件超のケース (超過分は先頭500件のみ取得しスルー)、
        または商号がかな頭文字に当たらない事業者 (英数字始まり等) は依然取りこぼす
        可能性がある (ベストエフォート方針)。
        WARNING ログ "取りこぼしの可能性あり" を監視のこと。

設計メモ:
    - OAB0103/OAB0108 は直打ちアクセス不可 (JSESSIONID + CSRFトークン必須)。
      OAB0101 のフォーム送信で確立したセッションを維持したまま page.request.post() で
      OAB0108 を呼び出すため、1件ごとに画面遷移しない。
    - ページネーション URL は _do_search() が返す「フォーム送信後の実 URL」を使う。
      直接 OAB0103?page=N を構築するとサーバーが検索コンテキストを失い0件になる。
    - 本社住所・営業品目はカスタムスタイルのモーダル UI だが、フォーム入力要素自体は
      DOM に常在し name 属性を持つため、モーダルを開かずに JS で値を設定し change
      イベントを発火するだけで検索条件として送信される (DOM調査で確認済み)。
      市区町村セレクトは都道府県の change で AJAX 補充されるため待機が必要。
    - 件数判定: "N件見つかりました" = 実件数 / "500件を超え" = 上限到達(500扱い) /
      "合致する事業者情報がありません" = 0件。
    - 統一資格情報・落札実績情報が存在しない事業者は対応フィールドを空文字で返す。
    - HTML の th「商品号又は名称」の実体は商号・名称 (Schema.NAME に対応)。
    - 代表者役職・代表者氏名が「－」の事業者は空文字に正規化する。

実行方法:
    # ローカルテスト
    python scripts/sites/government/p_portal.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id p_portal
"""

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

# 都道府県コード 01〜47 (検索第1レベル分割)
_PREF_CODES = [f"{i:02d}" for i in range(1, 48)]

# 商号 (かな) 頭文字。市区町村が500件超のとき商号頭文字で分割する。
# なお (市区町村 × かな1文字) でも500件超の場合は、これらを2文字目として連結し
# 「アア」「アイ」… の2文字プレフィックスでさらに分割する (KANA_MAX_DEPTH=2)。
_KANA_PREFIXES = list(
    "アイウエオカキクケコサシスセソタチツテトナニヌネノ"
    "ハヒフヘホマミムメモヤユヨラリルレロワヲン"
    "ガギグゲゴザジズゼゾダヂヅデドバビブベボパピプペポ"
)

# 商号プレフィックス分割の最大文字数 (2 = 「ア」で割れなければ「アア」「アイ」…で割る)。
# 統一資格条件 (企業規模/資格等級/競争参加地域) は統一資格保有者しか返さず無資格事業者
# の超過分を割れないため、無資格も含めて割れる商号プレフィックスで分割する方針。
_KANA_MAX_DEPTH = 2

# --- フォーム要素セレクタ (2025年DOM調査で確定) ---
_NAME_FIELD = 'input[name="inputSearchCondtionBean.tradeName"]'  # 商号又は名称
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

# 資格の種類・営業品目モーダルで営業品目チェックを ON にし「選択」確定する JS。
_JS_SET_BIZ = """
(value) => {
    const cb = [...document.querySelectorAll('.modal-type-01 input[type=checkbox]')]
        .find(c => c.value === value);
    if (cb) { cb.checked = true; cb.dispatchEvent(new Event('change', {bubbles:true})); }
    const ok = document.querySelector('#qualificationKindBizItemSelected');
    if (ok) ok.click();
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


class PPortalScraper(DynamicCrawler):
    """調達ポータル【法人】事業者情報公開機能 スクレイパー"""

    DELAY = 1.0
    # サーバーは 1ページ50件固定 (size= パラメータは無視される / 実機検証済)。
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

    def _get_cities(self, search_url: str, pref_code: str) -> list[dict[str, str]]:
        """指定都道府県の市区町村リストを取得する。

        OAB0101 を開き、都道府県セレクトに値をセットして change を発火すると
        サーバーが市区町村セレクトを AJAX 補充する。補充完了を待って
        [{code, name}, ...] を返す。
        """
        self.get_soup(search_url)
        self.page.wait_for_timeout(1500)  # フォーム JS(イベントハンドラ)初期化待ち
        try:
            self.page.evaluate(_JS_SET_PREF, pref_code)
            # 市区町村セレクトが AJAX 補充されるまで待機
            self.page.wait_for_function(_CITY_READY_JS, timeout=15000)
        except Exception as e:
            self.logger.warning("都道府県%s: 市区町村リスト取得失敗: %s", pref_code, e)
            return []
        return self.page.eval_on_selector_all(
            "#city_select option",
            "els => els.map(o => ({code:o.value, name:o.text.trim()})).filter(o => o.code)",
        )

    def _do_search(
        self,
        search_url: str,
        pref_code: str,
        city_code: str,
        biz_item: str | None = None,
        name_prefix: str | None = None,
    ) -> tuple[BeautifulSoup, int, bool, str]:
        """検索条件を設定して検索を実行し、(soup, 件数, 上限超フラグ, 実 URL) を返す。

        本社住所・営業品目モーダルの入力要素は DOM に常在し name 属性を持つため、
        モーダルを開かずに JS で値を設定して change を発火するだけで検索条件として
        送信される。市区町村は都道府県の change で AJAX 補充されるため待機する。

        name_prefix: 商号 (tradeName) の頭文字。1〜2文字のかなプレフィックスを渡す。

        戻り値の result_url はフォーム送信後にブラウザが表示した実 URL。
        これをベースにページネーションすることで検索コンテキストを維持する。

        overflow=True のとき total は 500 (上限) を表す。
        """
        self.get_soup(search_url)
        self.page.wait_for_timeout(1500)  # フォーム JS 初期化待ち

        # --- 本社住所 (都道府県 × 市区町村) ---
        self.page.evaluate(_JS_SET_PREF, pref_code)
        try:
            self.page.wait_for_function(_CITY_READY_JS, timeout=15000)
        except Exception as e:
            self.logger.debug("市区町村補充待ちタイムアウト %s: %s", pref_code, e)
        self.page.evaluate(_JS_SET_CITY, city_code)
        self.page.wait_for_timeout(300)

        # --- 資格の種類・営業品目 (第2レベル分割時のみ) ---
        if biz_item:
            self.page.evaluate(_JS_SET_BIZ, biz_item)
            self.page.wait_for_timeout(300)

        # --- 商号 かなプレフィックス (1〜2文字。第3レベル以降の分割時のみ) ---
        if name_prefix:
            try:
                self.page.fill(_NAME_FIELD, name_prefix)
            except Exception as e:
                self.logger.debug("商号入力失敗 %s: %s", name_prefix, e)

        # --- 検索送信 (フォーム POST → OAB0103 へ遷移) ---
        try:
            with self.page.expect_navigation(wait_until="domcontentloaded", timeout=30000):
                self.page.evaluate(_JS_CLICK_SEARCH)
        except Exception as e:
            self.logger.warning(
                "検索送信失敗 (pref=%s city=%s biz=%s kana=%s): %s",
                pref_code, city_code, biz_item, name_prefix, e,
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
                    ページネーション URL の生成に使い、検索コンテキストを維持する。
        label     : 進捗ログ用の検索ラベル (例 "01 札幌市中央区 商号アイ")。
        """
        # === フェッチ相: 全ページの一覧から新規法人番号を収集 ===
        # サーバーは1ページ50件固定で、範囲外の page= は page0 にクランプされ
        # 同じ結果を返す (空ページにならない / 実機検証済)。したがって "corp_links 空"
        # では止まらないため、総件数からページ数を算出して停止する。
        # 念のため直前ページと同一内容になった場合 (クランプ) も打ち切る。
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

        # === 累計の突合ログ (10件刻みではなく、検索単位でフェッチ/取得の合計を表示) ===
        if entries:
            self.logger.info(
                "📥 %s: フェッチ%d件 取得%d件 → 累計 フェッチ%d件 / 取得%d件",
                label or "(検索)", len(entries), got,
                self._fetched_total, self._retrieved_total,
            )

    def parse(self, url: str) -> Generator[dict, None, None]:
        search_url = urljoin(url, "OAB0101")
        detail_url = urljoin(url, "OAB0108?")
        size = 50  # フォームに件数セレクトは無く、サーバー既定は 50件/ページ
        seen: set[str] = set()
        # フェッチ(一覧で発見した新規法人番号)/取得(詳細取得に成功した件数)の累計
        self._fetched_total = 0
        self._retrieved_total = 0

        # 営業品目コード一覧を動的取得 (第2レベル分割で使用)
        self.get_soup(search_url)
        self.page.wait_for_timeout(1500)
        self._biz_items: list[str] = self.page.eval_on_selector_all(
            ".modal-type-01 input[type=checkbox][name*='bizItem']",
            "els => els.map(e => e.value)",
        )
        self.logger.info("営業品目コード %d件 を検出", len(self._biz_items))

        # 第1レベル: 都道府県 (01〜47) × 市区町村
        for pref_code in _PREF_CODES:
            cities = self._get_cities(search_url, pref_code)
            if not cities:
                continue
            self.logger.info("都道府県%s: 市区町村 %d件", pref_code, len(cities))

            for city in cities:
                ccode, cname = city["code"], city["name"]
                try:
                    soup, total, overflow, result_url = self._do_search(
                        search_url, pref_code, ccode
                    )
                except Exception as e:
                    self.logger.warning("%s %s: 検索失敗のためスキップ: %s", pref_code, cname, e)
                    continue
                if total == 0 and not overflow:
                    continue

                if not overflow:
                    yield from self._paginate_and_yield(
                        result_url, soup, total, size, detail_url, seen,
                        label=f"{pref_code} {cname}",
                    )
                else:
                    # 第2レベル: 500件超 → 営業品目 と 商号かな を併用して網羅。
                    #   - 営業品目分割: 統一資格保有者をカテゴリ別に取得 (英数字始まりの
                    #     商号などかな分割で当たらない事業者も拾える)。
                    #   - 市区町村かな分割: 無資格事業者を含む全事業者を商号頭文字で取得。
                    #   両者は共通の seen で自動マージ・重複排除される。
                    self.logger.info(
                        "%s %s: 500件超 → 営業品目 + 商号かな を併用して細分化", pref_code, cname
                    )
                    yield from self._split_by_bizitem(
                        search_url, pref_code, ccode, cname, size, detail_url, seen
                    )
                    yield from self._split_by_kana(
                        search_url, pref_code, ccode, cname, size, detail_url, seen
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
        """市区町村 × 営業品目で検索を細分化する (第2レベル)。

        営業品目でも500件超の場合はさらに商号かな頭文字で分割する (第3レベル)。
        """
        for biz in self._biz_items:
            try:
                soup, total, overflow, result_url = self._do_search(
                    search_url, pref_code, city_code, biz_item=biz
                )
            except Exception as e:
                self.logger.warning("%s %s 品目%s: 検索失敗のためスキップ: %s",
                                    pref_code, city_name, biz, e)
                continue
            if total == 0 and not overflow:
                continue

            if not overflow:
                yield from self._paginate_and_yield(
                    result_url, soup, total, size, detail_url, seen,
                    label=f"{pref_code} {city_name} 品目{biz}",
                )
            else:
                # 第3レベル: (市区町村 × 営業品目) でも500件超 → 商号かな分割
                self.logger.info(
                    "%s %s 品目%s: 500件超 → 仮名で細分化", pref_code, city_name, biz
                )
                yield from self._split_by_kana(
                    search_url, pref_code, city_code, city_name,
                    size, detail_url, seen, biz_item=biz,
                )

    def _split_by_kana(
        self,
        search_url: str,
        pref_code: str,
        city_code: str,
        city_name: str,
        size: int,
        detail_url: str,
        seen: set[str],
        biz_item: str | None = None,
        prefix: str = "",
        depth: int = 1,
    ) -> Generator[dict, None, None]:
        """商号 (かな) プレフィックスで検索を分割する。再帰で文字数を深くする。

        biz_item=None : 市区町村全体 (無資格事業者を含む全事業者) を対象に分割。
        biz_item 指定: (市区町村 × 営業品目) が500件超だった場合の第3レベル分割。

        prefix : これまでの商号プレフィックス。各かなを連結して1文字深くする。
        depth  : 現在のプレフィックス文字数。

        あるプレフィックスでなお500件超なら、_KANA_MAX_DEPTH まで1文字深い
        プレフィックス (例: ア→アア,アイ,…) で再帰分割する。最大文字数でもなお
        500件超の場合は先頭500件のみ取得し取りこぼし警告を出す (スルー方針)。

        商号プレフィックスは統一資格の有無に関わらず効くため、無資格事業者を含めて
        分割できる (企業規模等の統一資格条件では無資格の超過分を割れない)。
        """
        for kana in _KANA_PREFIXES:
            np = prefix + kana
            try:
                k_soup, k_total, k_overflow, k_url = self._do_search(
                    search_url, pref_code, city_code, biz_item=biz_item, name_prefix=np
                )
            except Exception as e:
                self.logger.warning("%s %s 商号%s: 検索失敗のためスキップ: %s",
                                    pref_code, city_name, np, e)
                continue
            if k_total == 0 and not k_overflow:
                continue
            lbl = f"{pref_code} {city_name} 商号{np}"
            if biz_item:
                lbl = f"{pref_code} {city_name} 品目{biz_item} 商号{np}"
            if not k_overflow:
                yield from self._paginate_and_yield(
                    k_url, k_soup, k_total, size, detail_url, seen, label=lbl
                )
            elif depth < _KANA_MAX_DEPTH:
                # 500件超 → 商号をもう1文字深く分割。
                # 併せて現プレフィックスの先頭500件も取得する (商号が prefix と完全一致
                # する1文字社名など、深い分割で当たらない事業者の保険。seen で重複排除)。
                self.logger.info(
                    "%s %s 商号%s: 500件超 → 商号%d文字目で細分化",
                    pref_code, city_name, np, depth + 1,
                )
                yield from self._paginate_and_yield(
                    k_url, k_soup, k_total, size, detail_url, seen, label=lbl
                )
                yield from self._split_by_kana(
                    search_url, pref_code, city_code, city_name,
                    size, detail_url, seen, biz_item=biz_item,
                    prefix=np, depth=depth + 1,
                )
            else:
                # 最大文字数でもなお500件超 → 先頭500件のみ取得しスルー。
                self.logger.warning(
                    "%s %s 商号%s: 商号%d文字でも500件超 → 先頭500件のみ (取りこぼしの可能性あり)",
                    pref_code, city_name, np, _KANA_MAX_DEPTH,
                )
                yield from self._paginate_and_yield(
                    k_url, k_soup, k_total, size, detail_url, seen, label=lbl
                )

    def _scrape_detail(self, soup: BeautifulSoup, source_url: str) -> dict | None:
        tables = soup.find_all("table")
        if not tables:
            return None

        # --- 基本情報テーブル (Table 0: class=main-table-pattern1 のみ) ---
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
                # 資格基本情報テーブル (class=main-table-pattern1 のみ)
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

    scraper = PPortalScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.p-portal.go.jp/pps-web-biz/UAB01/OAB0103")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
