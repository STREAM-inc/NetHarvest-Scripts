# scripts/sites/jobs/jinzai_hellowork.py
"""
人材サービス総合サイト（職業紹介事業） — 全国事業所一覧スクレイパー

取得対象:
    - 全国の有料職業紹介事業所（約34,000件）
    - 一覧ページから詳細ページURLを収集 → 各詳細ページでデータ取得

取得フロー:
    1. Playwright で検索フォーム操作（全国 + 有料職業紹介事業 → 検索実行）
    2. 一覧ページを全ページ巡回し、詳細ページURLを収集（約1,700ページ）
    3. requests で各詳細ページにアクセスしてデータ取得（Playwrightより高速）

実行方法:
    python scripts/sites/jobs/jinzai_hellowork.py
    python bin/run_flow.py --site-id jinzai_hellowork
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

BASE_URL = "https://jinzai.hellowork.mhlw.go.jp/JinzaiWeb"
SEARCH_URL = f"{BASE_URL}/GICB101010.do?screenId=GICB101010&action=initDisp"

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


class JinzaiHelloworkScraper(DynamicCrawler):
    """人材サービス総合サイト 職業紹介事業所スクレイパー"""

    DELAY = 0.5
    EXTRA_COLUMNS = [
        "許可届出受理番号",
        "許可届出受理年月日",
        "事業所名称",
        "取扱職種",
        "取扱地域",
        "得意とする職種",
        "備考",
    ]

    def prepare(self):
        """requests セッションを初期化（詳細ページ取得用）"""
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self.session.headers.update({
            "User-Agent": self.USER_AGENT,
        })

    def finalize(self):
        """requests セッションを閉じる"""
        if hasattr(self, "session") and self.session:
            self.session.close()

    def parse(self, url: str):
        # --- 1. Playwright で検索 → 全詳細URLを収集 ---
        detail_urls = self._execute_search_and_collect()
        self.total_items = len(detail_urls)
        self.logger.info("詳細ページURL収集完了: %d 件", len(detail_urls))

        # Playwright は詳細取得では不要なのでブラウザのページ遷移コストを削減
        # 以降は requests で高速取得

        # --- 2. 各詳細ページからデータ取得 (requests) ---
        for i, detail_url in enumerate(detail_urls):
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("詳細ページ取得失敗 [%d/%d]: %s (%s)", i + 1, len(detail_urls), detail_url, e)
                continue

    def _post_and_wait(self, js_expr: str):
        """JSでフォームsubmitを発火し、ナビゲーション完了を確実に待つ"""
        with self.page.expect_navigation(wait_until="networkidle", timeout=60000):
            self.page.evaluate(js_expr)

    def _navigate_to_page(self, page_num: int):
        """ページネーションで指定ページへ遷移し、読み込み完了を待つ"""
        self._post_and_wait(f"doPostAction('page','{page_num}')")

    def _execute_search_and_collect(self) -> list[str]:
        """Playwright で検索フォームを操作し、全ページの詳細ページURLを収集する"""
        detail_urls = []
        detail_urls_set = set()

        # トップページへアクセス
        self.logger.info("検索ページにアクセス中...")
        self.page.goto(SEARCH_URL, wait_until="networkidle")

        # 「職業紹介事業」リンクをクリック
        self._post_and_wait("doPostAction('transition','1')")

        # 「全国」チェックボックスをチェック
        self.page.check("#ID_cbZenkoku1")
        time.sleep(0.3)

        # 「有料職業紹介事業」チェックボックスをチェック
        self.page.check("#ID_cbJigyoshoKbnYu1")
        time.sleep(0.3)

        # 検索実行
        self.logger.info("検索実行中（全国 + 有料職業紹介事業）...")
        self._post_and_wait("doPostAction('search', '')")

        # 初回ページで総件数を取得
        soup = BeautifulSoup(self.page.content(), "html.parser")
        count_text = soup.get_text()
        total = 0
        count_match = re.search(r"検索結果\s+([\d,]+)\s*件", count_text)
        if count_match:
            total = int(count_match.group(1).replace(",", ""))
            self.logger.info("検索結果: %d 件", total)

        # --- 全ページの詳細URLを収集 ---
        page_num = 1
        total_pages = (total + 19) // 20 if total else None
        while True:
            if total_pages:
                self.logger.info("一覧ページ %d/%d からURL収集中... (累計: %d件)", page_num, total_pages, len(detail_urls))
            else:
                self.logger.info("一覧ページ %d からURL収集中... (累計: %d件)", page_num, len(detail_urls))

            soup = BeautifulSoup(self.page.content(), "html.parser")

            # 詳細ページリンクを抽出
            page_urls = set()
            for link in soup.select('a[href*="action=detail"]'):
                href = link.get("href", "")
                if "action=detail" in href:
                    if not href.startswith("http"):
                        href = f"https://jinzai.hellowork.mhlw.go.jp/JinzaiWeb/{href}"
                    page_urls.add(href)

            new_urls = [u for u in page_urls if u not in detail_urls_set]
            detail_urls.extend(new_urls)
            detail_urls_set.update(new_urls)

            # 次ページへの遷移
            next_page = page_num + 1
            has_next = f"doPostAction('page','{next_page}')" in str(soup)

            if has_next:
                self._navigate_to_page(next_page)
                page_num = next_page
            else:
                self.logger.info("最終ページ (%d) に到達。合計 %d 件のURL収集完了", page_num, len(detail_urls))
                break

        return detail_urls

    def _scrape_detail(self, url: str) -> dict | None:
        """requests で詳細ページを取得し、事業所情報を解析する"""
        response = self.session.get(url, timeout=20)
        response.raise_for_status()
        content_type = response.headers.get("Content-Type", "")
        if "charset=" not in content_type.lower():
            response.encoding = response.apparent_encoding
        soup = BeautifulSoup(response.text, "html.parser")

        table = soup.find("table", id="searchDet")
        if not table:
            return None

        item = {Schema.URL: url}

        rows = table.select("tr")
        for row in rows:
            tds = row.select("td")
            if len(tds) < 2:
                continue

            label = tds[0].get_text(strip=True)
            value_td = tds[1]
            value = value_td.get_text(strip=True)

            if label == "許可・届出受理番号":
                item["許可届出受理番号"] = value

            elif label == "許可届出受理年月日":
                item["許可届出受理年月日"] = value

            elif label == "事業主名称":
                item[Schema.NAME] = value
                hp_link = value_td.select_one("a#ID_linkJigyoshoURL")
                if hp_link:
                    href = hp_link.get("href", "").strip()
                    if href and href.startswith("http"):
                        item[Schema.HP] = href

            elif label == "事業所名称":
                item["事業所名称"] = value
                # 事業所名称にもHP URLリンクがある場合（事業主名称で取れなかった場合のフォールバック）
                if Schema.HP not in item:
                    hp_link = value_td.select_one("a#ID_linkJigyoshoURL")
                    if hp_link:
                        href = hp_link.get("href", "").strip()
                        if href and href.startswith("http"):
                            item[Schema.HP] = href

            elif label == "事業所所在地":
                full_addr = value
                m = _PREF_PATTERN.match(full_addr)
                if m:
                    item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = full_addr

            elif label == "電話番号":
                item[Schema.TEL] = value

            elif label == "取扱職種":
                if len(tds) >= 3:
                    item["取扱職種"] = tds[2].get_text(strip=True)
                else:
                    item["取扱職種"] = value

            elif label == "取扱地域":
                item["取扱地域"] = value

            elif label == "得意とする職種":
                item["得意とする職種"] = value

            elif label == "備考":
                biko = re.sub(r"\s+", " ", value).strip()
                item["備考"] = biko

        if Schema.NAME not in item:
            return None

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = JinzaiHelloworkScraper()
    scraper.execute(SEARCH_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
