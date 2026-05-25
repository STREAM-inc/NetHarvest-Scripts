"""
Web幹事 — ホームページ制作会社ディレクトリ（web-kanji.com）

取得対象:
    - 東京都・埼玉県・千葉県・神奈川県・茨城県・栃木県・群馬県・山梨県・大阪府

取得フロー:
    1. /search/{prefecture}[/page/{N}] を巡回し詳細URLを収集
    2. 各詳細ページ /companies/{slug} から企業情報を抽出
    3. 都道府県またぎの重複はURLベースで排除

CAPTCHA対応:
    実行するとChromiumブラウザが画面に表示される。
    「ユーザーが人間であることを確認する」画面が出た場合は、
    ブラウザウィンドウ上で手動で解決すれば自動的に処理が続行される（最大2分待機）。

実行方法:
    python scripts/sites/corporate/web_kanji.py
    python bin/run_flow.py --site-id web_kanji
"""

import random
import re
import sys
import time
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

BASE_URL = "https://web-kanji.com"

_PREFECTURES = [
    "tokyo",
    "saitama",
    "chiba",
    "kanagawa",
    "ibaraki",
    "tochigi",
    "gunma",
    "yamanashi",
    "osaka",
]

_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|"
    r"静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|"
    r"奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|"
    r"熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_POST_RE = re.compile(r"〒\s*(\d{3}-\d{4})")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


def _dl_value(soup, key: str) -> str:
    for dt in soup.find_all("dt"):
        if dt.get_text(strip=True) == key:
            dd = dt.find_next_sibling("dd")
            return _clean(dd.get_text()) if dd else ""
    return ""


def _is_blocked(soup) -> bool:
    if soup is None:
        return True
    text = soup.get_text()
    return "ユーザーが人間であることを確認する" in text or (
        "Just a moment" in text and "Cloudflare" in text
    )


class WebKanjiScraper(DynamicCrawler):
    """Web幹事 ホームページ制作会社スクレイパー（Playwright実ブラウザ方式）"""

    DELAY = 2.0
    EXTRA_COLUMNS: list[str] = []

    def _setup(self):
        """headless=False でブラウザを起動。CAPTCHAが出た場合に手動解決できるよう画面表示する。"""
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=False)
        self.context = self.browser.new_context(user_agent=self.USER_AGENT)
        self.page = self.context.new_page()

    def _navigate(self, url: str) -> BeautifulSoup | None:
        """ページ遷移してBeautifulSoupを返す。CAPTCHAが出た場合は最大2分待機する。"""
        time.sleep(self.DELAY + random.uniform(0.5, 1.5))
        soup = self.get_soup(url, wait_until="load")

        if _is_blocked(soup):
            self.logger.warning(
                "CAPTCHA検知 — ブラウザウィンドウで手動解決してください（最大2分）: %s", url
            )
            try:
                # 会社一覧 or 会社詳細のいずれかのセレクタが出るまで待つ
                self.page.wait_for_selector(
                    ".companies-item, dl dt", timeout=120_000
                )
            except Exception:
                self.logger.error("CAPTCHA解決タイムアウト。スキップします: %s", url)
                return None
            soup = BeautifulSoup(self.page.content(), "html.parser")

        return soup

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls = self._collect_detail_urls()
        self.total_items = len(detail_urls)
        self.logger.info("詳細URL収集完了: %d 件", len(detail_urls))

        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
            except Exception as e:
                self.logger.warning("詳細取得失敗 %s: %s", detail_url, e)
                continue
            if item and item.get(Schema.NAME):
                yield item

    def _collect_detail_urls(self) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()

        for pref in _PREFECTURES:
            base = f"{BASE_URL}/search/{pref}"
            self.logger.info("都道府県巡回: %s", pref)
            page = 1
            max_page = 1

            while page <= max_page:
                page_url = base if page == 1 else f"{base}/page/{page}"
                soup = self._navigate(page_url)

                if soup is None:
                    self.logger.warning("%s の %d ページ目をスキップ", pref, page)
                    break

                cards = soup.select(".companies-item")
                if not cards:
                    break

                for a in soup.select('.companies-item a[href*="/companies/"]'):
                    href = a.get("href", "")
                    if not href or href in seen:
                        continue
                    seen.add(href)
                    urls.append(href)

                if page == 1:
                    for a in soup.select(".pagination-item a"):
                        m = re.search(r"/page/(\d+)$", a.get("href", ""))
                        if m:
                            max_page = max(max_page, int(m.group(1)))
                    self.logger.info("  %s: %d ページ", pref, max_page)

                page += 1

        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self._navigate(url)
        if soup is None:
            return None

        name = _dl_value(soup, "会社名")
        if not name:
            h1 = soup.select_one(".company-name")
            name = _clean(h1.get_text()) if h1 else ""
        if not name:
            return None

        item: dict = {Schema.URL: url, Schema.NAME: name}

        rep = _dl_value(soup, "代表")
        if rep:
            item[Schema.REP_NM] = rep

        established = _dl_value(soup, "設立")
        if established:
            item[Schema.OPEN_DATE] = established

        cap = _dl_value(soup, "資本金")
        if cap:
            item[Schema.CAP] = cap

        emp = _dl_value(soup, "社員数")
        if emp:
            item[Schema.EMP_NUM] = emp

        hp = _dl_value(soup, "URL")
        if hp:
            item[Schema.HP] = hp

        address_raw = _dl_value(soup, "本社所在地")
        if address_raw:
            m_post = _POST_RE.search(address_raw)
            if m_post:
                item[Schema.POST_CODE] = m_post.group(1)
            addr_text = re.sub(r"〒\s*\d{3}-\d{4}\s*", "", address_raw).strip()
            m_pref = _PREF_RE.match(addr_text)
            if m_pref:
                item[Schema.PREF] = m_pref.group(1)
                item[Schema.ADDR] = addr_text[m_pref.end():].strip()
            else:
                item[Schema.ADDR] = addr_text

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = WebKanjiScraper()
    scraper.execute(f"{BASE_URL}/search/tokyo")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
