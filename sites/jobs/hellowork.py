"""
ハローワーク (ハローワークインターネットサービス) 求人情報スクレイパー

取得対象:
    - https://www.hellowork.mhlw.go.jp/ の公開求人 (一般求人)
    - 一覧ページ → 詳細ページの 2 段構成。求人カード 1 件ごとに 1 行のレコードを生成する。

取得フロー:
    1. Playwright で検索ページ (GECA110010) にアクセス
    2. 47 都道府県を 1 つずつ todohukenHidden に設定して検索 (件数の多い都道府県は
       一般求人内のフルタイム/パートで更にサブ分割)。各バッチで「次へ＞」ボタンで
       全ページ巡回し、各カードの詳細ページ URL (action=dispDetailBtn, jGSHNo 付き) を収集。
       バッチ単位で分割しているのは、全件 (約 100 万件超) を 1 セッションで巡回すると
       途中でセッション切れ／ナビゲーション失敗が発生し早期終了するため。
    3. requests で各詳細ページを取得し、74 個前後の <th>/<td> ペアを解析
    4. 事業所名・所在地・代表者・法人番号 等を Schema 列に、
       求人特有の属性 (求人番号、職種、賃金 等) を EXTRA_COLUMNS に格納

実行方法:
    python scripts/sites/jobs/hellowork.py
    python bin/run_flow.py --site-id hellowork
"""

import re
import sys
import time
from pathlib import Path

root_path = Path(__file__).resolve().parent.parent.parent.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

BASE_URL = "https://www.hellowork.mhlw.go.jp/kensaku"
SEARCH_URL = f"{BASE_URL}/GECA110010.do?action=initDisp&screenId=GECA110010"

# 都道府県コード (hellowork 検索フォームの skCheckXX value / todohukenHidden に渡す値)
_PREFECTURES: list[tuple[str, str]] = [
    ("01", "北海道"), ("02", "青森県"), ("03", "岩手県"), ("04", "宮城県"),
    ("05", "秋田県"), ("06", "山形県"), ("07", "福島県"), ("08", "茨城県"),
    ("09", "栃木県"), ("10", "群馬県"), ("11", "埼玉県"), ("12", "千葉県"),
    ("13", "東京都"), ("14", "神奈川県"), ("15", "新潟県"), ("16", "富山県"),
    ("17", "石川県"), ("18", "福井県"), ("19", "山梨県"), ("20", "長野県"),
    ("21", "岐阜県"), ("22", "静岡県"), ("23", "愛知県"), ("24", "三重県"),
    ("25", "滋賀県"), ("26", "京都府"), ("27", "大阪府"), ("28", "兵庫県"),
    ("29", "奈良県"), ("30", "和歌山県"), ("31", "鳥取県"), ("32", "島根県"),
    ("33", "岡山県"), ("34", "広島県"), ("35", "山口県"), ("36", "徳島県"),
    ("37", "香川県"), ("38", "愛媛県"), ("39", "高知県"), ("40", "福岡県"),
    ("41", "佐賀県"), ("42", "長崎県"), ("43", "熊本県"), ("44", "大分県"),
    ("45", "宮崎県"), ("46", "鹿児島県"), ("47", "沖縄県"),
]

# 都道府県内をさらにサブ分割するしきい値 (件)
# 1 セッションで「次へ＞」ボタンを連打して安定して取得しきれる目安 (経験則)
_SUBDIVIDE_THRESHOLD = 25_000

# 一般求人 (kjKbnRadioBtn=1) の内訳: フルタイム / パート
_IPPAN_SUBTYPES: list[tuple[str, str]] = [
    ("1", "フルタイム"),
    ("2", "パート"),
]

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_POST_CODE_PATTERN = re.compile(r"〒?\s*(\d{3}-\d{4})")
_PHONE_PATTERN = re.compile(r"電話番号\s*([0-9０-９\-－]{8,})")


class HelloworkScraper(DynamicCrawler):
    """ハローワークインターネットサービス 求人情報スクレイパー"""

    DELAY = 0.5
    EXTRA_COLUMNS = [
        "求人番号",
        "受付年月日",
        "紹介期限日",
        "受理安定所",
        "求人区分",
        "事業所番号",
        "職種",
        "仕事内容",
        "雇用形態",
        "雇用期間",
        "就業場所",
        "賃金",
        "就業時間",
        "休日等",
        "加入保険等",
        "会社の特長",
        "担当者",
    ]

    def prepare(self):
        """詳細ページ取得用 requests セッションを初期化"""
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self.session.headers.update({"User-Agent": self.USER_AGENT})

    def finalize(self):
        if hasattr(self, "session") and self.session:
            self.session.close()

    def parse(self, url: str):
        # --- 1. Playwright で 47 都道府県を順に検索し、全ページの詳細URLを収集 ---
        seen: set[str] = set()
        all_urls: list[str] = []

        for code, name in _PREFECTURES:
            try:
                urls = self._collect_detail_urls_for_pref(code, name)
            except Exception as e:
                self.logger.warning("[%s] 検索全体でエラー: %s", name, e)
                continue

            new_count = 0
            for u in urls:
                if u not in seen:
                    seen.add(u)
                    all_urls.append(u)
                    new_count += 1
            self.logger.info(
                "[%s] 取得 %d 件 (新規 %d / 累計 %d)",
                name, len(urls), new_count, len(all_urls),
            )

        self.total_items = len(all_urls)
        self.logger.info("詳細ページURL収集完了: %d 件", len(all_urls))

        # --- 2. requests で各詳細ページを取得 ---
        for i, detail_url in enumerate(all_urls):
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning(
                    "詳細ページ取得失敗 [%d/%d]: %s (%s)",
                    i + 1, len(all_urls), detail_url, e,
                )
                continue

    # ------------------------------------------------------------------
    # 一覧ページ (Playwright)
    # ------------------------------------------------------------------
    def _collect_detail_urls_for_pref(self, pref_code: str, pref_name: str) -> list[str]:
        """指定都道府県の全求人 URL を収集する。

        件数が _SUBDIVIDE_THRESHOLD を超える場合は、一般求人 (フルタイム/パート/その他) と
        新卒・季節・出稼ぎ・障害者向け を別バッチで検索しサブ分割する。
        """
        # 件数を確認するための予備検索 (条件なし、kjKbnRadioBtn=1 がデフォルト)
        total = self._search_with_pref(pref_code, ippan_subtype=None)
        if total is None:
            self.logger.warning("[%s] 検索結果総数の取得に失敗", pref_name)
            total = 0

        self.logger.info("[%s] 検索結果: 約 %d 件", pref_name, total)

        # しきい値以下ならそのまま全件取得
        if total <= _SUBDIVIDE_THRESHOLD:
            return self._paginate_collect(label=pref_name)

        # しきい値超: 一般求人 (kjKbnRadioBtn=1) をフルタイム/パートで分割。
        # 一般求人以外 (新卒/季節/出稼ぎ/障害者) は件数が小さいので 1 バッチで取得。
        self.logger.info(
            "[%s] %d 件 > %d 件のためサブ分割で取得",
            pref_name, total, _SUBDIVIDE_THRESHOLD,
        )

        urls: list[str] = []
        seen: set[str] = set()

        for sub_code, sub_name in _IPPAN_SUBTYPES:
            self._search_with_pref(pref_code, ippan_subtype=sub_code)
            label = f"{pref_name}/一般求人/{sub_name}"
            for u in self._paginate_collect(label=label):
                if u not in seen:
                    seen.add(u)
                    urls.append(u)

        return urls

    def _search_with_pref(self, pref_code: str, ippan_subtype: str | None) -> int | None:
        """検索ページを開き、都道府県と (任意で) 一般求人サブ種別を指定して検索を実行。

        Args:
            pref_code: skCheck の値 ("01" 〜 "47")
            ippan_subtype: ippanCKBox の値。"1"=フルタイム, "2"=パート, None=指定なし

        Returns:
            int | None: 検索結果の総件数。読み取り失敗時は None。
        """
        self.page.goto(SEARCH_URL, wait_until="domcontentloaded")

        # JS から hidden を直接設定して検索 (本来 UI はモーダルで都道府県を選ばせる仕組みだが、
        # サーバーは todohukenHidden の値だけを参照するため直接代入で十分)
        js = (
            f"document.querySelector('#ID_todohukenHidden').value = '{pref_code}';"
        )
        if ippan_subtype is not None:
            # 一般求人ラジオを選択 (デフォルトのままだが念のため)
            js += "document.querySelector('#ID_kjKbnRadioBtn1').checked = true;"
            # ippanCKBox は複数選択可だが、ここでは 1 つだけ ON にする
            js += "document.querySelectorAll('input[name=\"ippanCKBox\"]').forEach(el => el.checked = false);"
            js += (
                f"document.querySelector('#ID_ippanCKBox{ippan_subtype}').checked = true;"
            )
        self.page.evaluate(js)

        try:
            with self.page.expect_navigation(wait_until="domcontentloaded", timeout=60000):
                self.page.click("#ID_searchBtn")
        except Exception as e:
            self.logger.warning("検索ボタン押下後の遷移失敗 [pref=%s sub=%s]: %s",
                                pref_code, ippan_subtype, e)
            return None

        # 50 件/ページに切替 (失敗してもデフォルトで続行)
        try:
            self.page.select_option("#ID_fwListNaviDispTop", "50")
            with self.page.expect_navigation(wait_until="domcontentloaded", timeout=60000):
                self.page.evaluate(
                    "document.forms['form_1'].submit && document.forms['form_1'].submit()"
                )
        except Exception:
            self.logger.debug("表示件数の切替に失敗。デフォルト件数で続行")

        return self._read_total_count()

    def _paginate_collect(self, label: str) -> list[str]:
        """現在表示中の一覧ページを起点に、最終ページまで詳細 URL を収集する。

        Args:
            label: ログ出力用のバッチ識別ラベル (例: "東京都/一般求人/フルタイム")
        """
        detail_urls: list[str] = []
        seen: set[str] = set()
        page_num = 1
        consecutive_failures = 0
        MAX_FAILURES = 3

        while True:
            soup = BeautifulSoup(self.page.content(), "html.parser")
            before = len(detail_urls)
            for a in soup.select('a[href*="action=dispDetailBtn"]'):
                href = a.get("href", "").strip()
                if not href:
                    continue
                if href.startswith("./"):
                    href = href[2:]
                if not href.startswith("http"):
                    href = f"{BASE_URL}/{href}"
                if href not in seen:
                    seen.add(href)
                    detail_urls.append(href)
            if page_num % 20 == 1 or page_num == 1:
                self.logger.info("[%s] ページ %d 解析 (累計 %d 件)",
                                 label, page_num, len(detail_urls))

            next_btn = self.page.query_selector('input[name="fwListNaviBtnNext"]')
            if not next_btn:
                self.logger.info("[%s] 次へボタンなし。終了", label)
                break

            try:
                disabled = next_btn.is_disabled()
            except Exception:
                disabled = False
            if disabled:
                self.logger.info("[%s] 最終ページに到達 (次へ無効)", label)
                break

            # 同一ページから新規 URL が 1 件も増えなければ末尾と判断
            if len(detail_urls) == before and page_num > 1:
                self.logger.info("[%s] 新規 URL がなくなったため終了", label)
                break

            try:
                with self.page.expect_navigation(wait_until="domcontentloaded", timeout=60000):
                    next_btn.click()
                consecutive_failures = 0
            except Exception as e:
                consecutive_failures += 1
                self.logger.warning(
                    "[%s] ページ %d 遷移失敗 (連続 %d/%d): %s",
                    label, page_num, consecutive_failures, MAX_FAILURES, e,
                )
                if consecutive_failures >= MAX_FAILURES:
                    self.logger.warning("[%s] リトライ上限到達のためバッチ打ち切り", label)
                    break
                time.sleep(2)
                continue

            page_num += 1
            time.sleep(0.3)

        return detail_urls

    def _read_total_count(self) -> int | None:
        try:
            text = self.page.inner_text("body")
        except Exception:
            return None
        # "1142398件中 1～30 件を表示" のようなパターンを優先
        m = re.search(r"([\d,]+)\s*件中", text)
        if m:
            return int(m.group(1).replace(",", ""))
        m = re.search(r"全\s*([\d,]+)\s*件|該当件数\s*([\d,]+)\s*件|([\d,]+)\s*件", text)
        if not m:
            return None
        digits = next((g for g in m.groups() if g), None)
        return int(digits.replace(",", "")) if digits else None

    # ------------------------------------------------------------------
    # 詳細ページ (requests)
    # ------------------------------------------------------------------
    def _scrape_detail(self, url: str) -> dict | None:
        response = self.session.get(url, timeout=20)
        response.raise_for_status()
        if "charset=" not in response.headers.get("Content-Type", "").lower():
            response.encoding = response.apparent_encoding
        soup = BeautifulSoup(response.text, "html.parser")

        # ページ内の全 <th>/<td> ペアを {label: value} に畳み込む
        # 同じラベルが複数回現れるため、リスト化してから後段で振り分ける
        pairs: dict[str, list[str]] = {}
        for tr in soup.find_all("tr"):
            ths = tr.find_all("th", recursive=False)
            tds = tr.find_all("td", recursive=False)
            if not ths or not tds:
                continue
            label = " ".join(th.get_text(" ", strip=True) for th in ths).strip()
            # 「ＰＲロゴマーク」等のテキスト混じりラベルを正規化
            label = re.split(r"[\s　]+", label)[0]
            value = " ".join(td.get_text(" ", strip=True) for td in tds).strip()
            if label and value:
                pairs.setdefault(label, []).append(value)

        if not pairs:
            return None

        item: dict = {Schema.URL: url}

        # --- 事業所基本情報 ---
        # 「事業所名」は 2 か所 (求人カード上部 + 事業所情報) に出現する。
        # 後者 (フリガナ + 漢字名) を優先する。
        names = pairs.get("事業所名", [])
        if names:
            # フリガナ＋漢字 が連結された方を採用
            full = max(names, key=len)
            # 先頭にフリガナ (カタカナ) があれば分離
            kana_match = re.match(r"^([゠-ヿ\s　]+)\s+(.*)$", full)
            if kana_match:
                item[Schema.NAME_KANA] = kana_match.group(1).strip()
                item[Schema.NAME] = kana_match.group(2).strip()
            else:
                item[Schema.NAME] = full

        # --- 所在地 (本社) ---
        addr_raw = self._first(pairs.get("所在地"))
        if addr_raw:
            self._fill_address(item, addr_raw)

        # --- ホームページ ---
        hp = self._first(pairs.get("ホームページ"))
        if hp:
            item[Schema.HP] = hp

        # --- 担当者 (TEL を抽出) ---
        contact = self._first(pairs.get("担当者"))
        if contact:
            item["担当者"] = contact
            tel_match = _PHONE_PATTERN.search(contact)
            if tel_match:
                item[Schema.TEL] = tel_match.group(1)

        # --- 役職／代表者名 ---
        rep = self._first(pairs.get("役職／代表者名"))
        if rep:
            pos_match = re.search(r"役職\s*([^\s代]+)", rep)
            nm_match = re.search(r"代表者名\s*(.+)$", rep)
            if pos_match:
                item[Schema.POS_NM] = pos_match.group(1).strip()
            if nm_match:
                item[Schema.REP_NM] = nm_match.group(1).strip()
            elif not pos_match:
                item[Schema.REP_NM] = rep

        # --- 単純マッピング ---
        simple = {
            "法人番号": Schema.CO_NUM,
            "従業員数": Schema.EMP_NUM,
            "資本金": Schema.CAP,
            "事業内容": Schema.LOB,
            "産業分類": Schema.CAT_SITE,
        }
        for label, schema_key in simple.items():
            v = self._first(pairs.get(label))
            if v:
                item[schema_key] = v

        # --- 設立年 ---
        founded = self._first(pairs.get("設立年")) or self._first(pairs.get("設立年月日"))
        if founded:
            item[Schema.OPEN_DATE] = founded

        # --- EXTRA_COLUMNS ---
        extras_simple = [
            "求人番号", "受付年月日", "紹介期限日", "受理安定所",
            "求人区分", "事業所番号", "職種", "仕事内容",
            "雇用形態", "雇用期間", "就業場所", "就業時間",
            "休日等", "加入保険等", "会社の特長",
        ]
        for label in extras_simple:
            v = self._first(pairs.get(label))
            if v:
                item[label] = v

        # 賃金は複数ラベルに分散している (基本給 (a) など)。最初の合算行を採用
        wage = self._first(pairs.get("ａ ＋ ｂ（固定残業代がある場合はａ ＋ ｂ ＋ ｃ）"))
        if wage:
            item["賃金"] = wage

        if Schema.NAME not in item:
            return None

        return item

    @staticmethod
    def _first(values: list[str] | None) -> str | None:
        return values[0] if values else None

    @staticmethod
    def _fill_address(item: dict, raw: str) -> None:
        """〒xxx-xxxx 都道府県... の形式を分解して item に書き込む"""
        post_match = _POST_CODE_PATTERN.search(raw)
        if post_match:
            item[Schema.POST_CODE] = post_match.group(1)
            tail = raw[post_match.end():].strip()
        else:
            tail = raw

        pref_match = _PREF_PATTERN.match(tail)
        if pref_match:
            item[Schema.PREF] = pref_match.group(1)
        item[Schema.ADDR] = tail


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = HelloworkScraper()
    scraper.execute(SEARCH_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
