"""
ハローワーク 新規求人スクレイパー (直近3日間)

取得対象:
    - https://www.hellowork.mhlw.go.jp/ の公開求人のうち「新着求人(直近3日間)」のみ
    - 求人区分: 一般 (必須) + ID_jyoukenBox1 (新着求人直近3日) チェックで全国約15万件を1回の検索で取得

取得フロー:
    1. Playwright で検索ページ (GECA110010) にアクセス
    2. 一般区分 + 新着フラグを指定して全国検索 (都道府県指定なし)
    3. 「次へ＞」で全ページ巡回し、詳細ページ URL を収集
    4. requests で各詳細ページを取得し、<th>/<td> ペアを解析

実行方法:
    python scripts/sites/jobs/hellowork_shinnkikyuujin.py
    python bin/run_flow.py --site-id hellowork_shinnkikyuujin
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
_EMAIL_PATTERN = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

# 従業員数内訳パターン: "企業全体 393人 就業場所 195人 うち女性 151人 うちパート 35人"
_EMP_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("従業員数_企業全体",  re.compile(r"企業全体\s*([\d,]+)\s*人")),
    ("従業員数_就業場所",  re.compile(r"就業場所\s*([\d,]+)\s*人")),
    ("従業員数_うち女性",  re.compile(r"うち女性\s*([\d,]+)\s*人")),
    ("従業員数_うちパート", re.compile(r"うちパート\s*([\d,]+)\s*人")),
]


class HelloworkShinnkikyuujinScraper(DynamicCrawler):
    """ハローワーク 新規求人（直近3日間・全国一括）スクレイパー"""

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
        "担当者メール",
        "従業員数_企業全体",
        "従業員数_就業場所",
        "従業員数_うち女性",
        "従業員数_うちパート",
    ]

    def prepare(self):
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self.session.headers.update({"User-Agent": self.USER_AGENT})

    def finalize(self):
        if hasattr(self, "session") and self.session:
            self.session.close()

    def parse(self, url: str):
        self._search_nationwide()
        all_urls = self._paginate_collect()

        self.total_items = len(all_urls)
        self.logger.info("詳細ページURL収集完了: %d 件", len(all_urls))

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

    def _search_nationwide(self) -> None:
        """一般区分 + 新着フラグで全国検索を実行する"""
        self.page.goto(SEARCH_URL, wait_until="domcontentloaded")

        # 一般区分はデフォルトで選択済み
        # checkbox は label が上を覆っているため label 経由でクリックする
        self.page.evaluate("document.querySelector('#ID_LjyoukenBox1').click()")

        try:
            with self.page.expect_navigation(wait_until="domcontentloaded", timeout=60000):
                self.page.click("#ID_searchBtn")
        except Exception as e:
            self.logger.warning("検索ボタン押下後の遷移失敗: %s", e)
            return

        self.logger.info("検索結果ページ: %s", self.page.url)

    def _paginate_collect(self) -> list[str]:
        """現在表示中の一覧ページを起点に、最終ページまで詳細 URL を収集する"""
        detail_urls: list[str] = []
        seen: set[str] = set()
        page_num = 1
        consecutive_failures = 0
        MAX_FAILURES = 3
        nav_succeeded = True  # 直前の遷移が成功したかどうか

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
                self.logger.info("ページ %d 解析 (累計 %d 件)", page_num, len(detail_urls))

            next_btn = self.page.query_selector('input[name="fwListNaviBtnNext"]')
            if not next_btn:
                self.logger.info("次へボタンなし。終了")
                break

            try:
                disabled = next_btn.is_disabled()
            except Exception:
                disabled = False
            if disabled:
                self.logger.info("最終ページに到達")
                break

            # 遷移成功後のみ「新規URLゼロ」を終了条件にする
            # （遷移失敗後の continue でループ先頭に戻った場合は同一ページを再パースするため除外）
            if nav_succeeded and len(detail_urls) == before and page_num > 1:
                self.logger.info("新規 URL がなくなったため終了")
                break

            try:
                with self.page.expect_navigation(wait_until="domcontentloaded", timeout=60000):
                    next_btn.click()
                consecutive_failures = 0
                nav_succeeded = True
            except Exception as e:
                consecutive_failures += 1
                nav_succeeded = False
                self.logger.warning(
                    "ページ %d 遷移失敗 (連続 %d/%d): %s",
                    page_num, consecutive_failures, MAX_FAILURES, e,
                )
                if consecutive_failures >= MAX_FAILURES:
                    self.logger.warning("リトライ上限到達のためバッチ打ち切り")
                    break
                time.sleep(2)
                continue

            page_num += 1
            time.sleep(0.3)

        return detail_urls

    def _scrape_detail(self, url: str) -> dict | None:
        response = self.session.get(url, timeout=20)
        response.raise_for_status()
        if "charset=" not in response.headers.get("Content-Type", "").lower():
            response.encoding = response.apparent_encoding
        soup = BeautifulSoup(response.text, "html.parser")

        pairs: dict[str, list[str]] = {}
        for tr in soup.find_all("tr"):
            ths = tr.find_all("th", recursive=False)
            tds = tr.find_all("td", recursive=False)
            if not ths or not tds:
                continue
            label = " ".join(th.get_text(" ", strip=True) for th in ths).strip()
            label = re.split(r"[\s　]+", label)[0]
            value = " ".join(td.get_text(" ", strip=True) for td in tds).strip()
            if label and value:
                pairs.setdefault(label, []).append(value)

        if not pairs:
            return None

        item: dict = {Schema.URL: url}

        names = pairs.get("事業所名", [])
        if names:
            full = max(names, key=len)
            kana_match = re.match(r"^([゠-ヿ\s　]+)\s+(.*)$", full)
            if kana_match:
                item[Schema.NAME_KANA] = kana_match.group(1).strip()
                item[Schema.NAME] = kana_match.group(2).strip()
            else:
                item[Schema.NAME] = full

        addr_raw = self._first(pairs.get("所在地"))
        if addr_raw:
            self._fill_address(item, addr_raw)

        # 郵便番号セルが独立して存在する場合は所在地抽出より優先
        post_cell = self._first(pairs.get("郵便番号"))
        if post_cell:
            pm = _POST_CODE_PATTERN.search(post_cell)
            item[Schema.POST_CODE] = pm.group(1) if pm else post_cell.strip()

        hp = self._first(pairs.get("ホームページ"))
        if hp:
            item[Schema.HP] = hp

        contact = self._first(pairs.get("担当者"))
        if contact:
            item["担当者"] = contact
            tel_match = _PHONE_PATTERN.search(contact)
            if tel_match:
                item[Schema.TEL] = tel_match.group(1)
            email_match = _EMAIL_PATTERN.search(contact)
            if email_match:
                item["担当者メール"] = email_match.group(0)

        # 電話番号セルが独立して存在する場合は担当者抽出より優先
        tel_cell = self._first(pairs.get("電話番号"))
        if tel_cell:
            item[Schema.TEL] = tel_cell.strip()

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

        simple = {
            "法人番号": Schema.CO_NUM,
            "資本金": Schema.CAP,
            "事業内容": Schema.LOB,
            "産業分類": Schema.CAT_SITE,
        }
        for label, schema_key in simple.items():
            v = self._first(pairs.get(label))
            if v:
                item[schema_key] = v

        emp_raw = self._first(pairs.get("従業員数"))
        if emp_raw:
            for col, pat in _EMP_PATTERNS:
                m = pat.search(emp_raw)
                if m:
                    item[col] = m.group(1).replace(",", "")

        founded = self._first(pairs.get("設立年")) or self._first(pairs.get("設立年月日"))
        if founded:
            item[Schema.OPEN_DATE] = founded

        for label in [
            "求人番号", "受付年月日", "紹介期限日", "受理安定所",
            "求人区分", "事業所番号", "職種", "仕事内容",
            "雇用形態", "雇用期間", "就業場所", "就業時間",
            "休日等", "加入保険等", "会社の特長",
        ]:
            v = self._first(pairs.get(label))
            if v:
                item[label] = v

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

    scraper = HelloworkShinnkikyuujinScraper()
    scraper.execute(SEARCH_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
