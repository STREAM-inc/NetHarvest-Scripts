"""
ハローワーク (ハローワークインターネットサービス) 求人情報スクレイパー

取得対象:
    - https://www.hellowork.mhlw.go.jp/ の公開求人 (デフォルト=一般求人)
    - 一覧ページ → 詳細ページの 2 段構成。求人カード 1 件ごとに 1 行のレコードを生成する。

取得フロー:
    1. Playwright で検索ページ (GECA110010) にアクセスし、空条件で検索ボタン押下
    2. 検索結果一覧 (50件/ページ) のページネーションを「次へ＞」ボタンで巡回し、
       各カードの詳細ページ URL (action=dispDetailBtn, jGSHNo 付き) を収集
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
        # --- 1. Playwright で検索 → 全ページの詳細URLを収集 ---
        detail_urls = self._collect_detail_urls()
        self.total_items = len(detail_urls)
        self.logger.info("詳細ページURL収集完了: %d 件", len(detail_urls))

        # --- 2. requests で各詳細ページを取得 ---
        for i, detail_url in enumerate(detail_urls):
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning(
                    "詳細ページ取得失敗 [%d/%d]: %s (%s)",
                    i + 1, len(detail_urls), detail_url, e,
                )
                continue

    # ------------------------------------------------------------------
    # 一覧ページ (Playwright)
    # ------------------------------------------------------------------
    def _collect_detail_urls(self) -> list[str]:
        """検索フォームを送信し、ページ送りで全詳細URLを収集"""
        detail_urls: list[str] = []
        seen: set[str] = set()

        self.logger.info("検索ページにアクセス中...")
        self.page.goto(SEARCH_URL, wait_until="domcontentloaded")

        # 1ページ目を表示する: 「検索する」を submit
        self.logger.info("検索ボタンをクリック (デフォルト条件) ...")
        with self.page.expect_navigation(wait_until="domcontentloaded", timeout=60000):
            self.page.click("#ID_searchBtn")

        # 50件/ページに切替えて巡回数を減らす
        try:
            self.page.select_option("#ID_fwListNaviDispTop", "50")
            with self.page.expect_navigation(wait_until="domcontentloaded", timeout=60000):
                self.page.evaluate(
                    "document.forms['form_1'].submit && document.forms['form_1'].submit()"
                )
        except Exception:
            # 50件切替に失敗してもデフォルト30件で続行
            self.logger.debug("表示件数の切替に失敗。デフォルト件数で続行")

        # 総件数 (例: "全 268 件")
        total = self._read_total_count()
        if total:
            self.logger.info("検索結果: 約 %d 件", total)

        page_num = 1
        while True:
            self.logger.info("一覧ページ %d を解析中... (累計: %d件)", page_num, len(detail_urls))
            soup = BeautifulSoup(self.page.content(), "html.parser")

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

            # 「次へ＞」ボタン: input[name=fwListNaviBtnNext]
            next_btn = self.page.query_selector('input[name="fwListNaviBtnNext"]')
            if not next_btn:
                self.logger.info("次へボタンが見つからないため終了")
                break

            try:
                disabled = next_btn.is_disabled()
            except Exception:
                disabled = False
            if disabled:
                self.logger.info("最終ページに到達 (次へが無効)")
                break

            try:
                with self.page.expect_navigation(wait_until="domcontentloaded", timeout=60000):
                    next_btn.click()
            except Exception as e:
                self.logger.info("次へボタン押下後の遷移なし。終了 (%s)", e)
                break
            page_num += 1
            time.sleep(0.3)

        return detail_urls

    def _read_total_count(self) -> int | None:
        try:
            text = self.page.inner_text("body")
        except Exception:
            return None
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
