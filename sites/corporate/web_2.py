"""
Web幹事 — ホームページ制作会社ディレクトリ（web-kanji.com）／全国版

取得対象:
    - 全国のホームページ制作会社（/search を起点に全ページ巡回）

取得フロー (一覧 → 詳細, Pattern B = 詳細を1件取得するごとに即 yield):
    1. /search[/page/{N}] を巡回し、各ページのカードから詳細URL (/companies/{slug}) を収集
    2. 収集した詳細URLごとに即座に詳細ページを取得し、企業情報を抽出して yield
    3. ページまたぎの重複は URL ベースで排除

CAPTCHA対応:
    Cloudflare の「ユーザーが人間であることを確認する」画面が出る場合がある。
    XServer の無い実行環境（CI/サーバー）でも動作させるため headless=True で起動する。
    CAPTCHA が出た場合は最大2分待機し、解決されなければ該当URLをスキップする。

実行方法:
    python scripts/sites/corporate/web_2.py
    docker compose exec worker python /app/bin/run_flow.py --site-id web_2
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

_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|"
    r"静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|"
    r"奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|"
    r"熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_POST_RE = re.compile(r"〒\s*(\d{3}-\d{4})")
# 「(3080件中 1〜20件)」のような表示から総件数（最初の数字）を取得する
_TOTAL_RE = re.compile(r"([\d,]+)\s*件中")

# /search は「地域から探す」の都道府県インデックスで企業カードを持たない。
# 企業カードは都道府県別ページ /search/{slug}[/page/{N}] に存在するため、
# 全国版として 47 都道府県を順に巡回する。
PREFECTURE_SLUGS = [
    "hokkaido", "aomori", "iwate", "miyagi", "akita", "yamagata", "fukushima",
    "ibaraki", "tochigi", "gunma", "saitama", "chiba", "tokyo", "kanagawa",
    "niigata", "toyama", "ishikawa", "fukui", "yamanashi", "nagano", "gifu",
    "shizuoka", "aichi", "mie", "shiga", "kyoto", "osaka", "hyogo", "nara",
    "wakayama", "tottori", "shimane", "okayama", "hiroshima", "yamaguchi",
    "tokushima", "kagawa", "ehime", "kochi", "fukuoka", "saga", "nagasaki",
    "kumamoto", "oita", "miyazaki", "kagoshima", "okinawa",
]


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


def _dl_value(soup, key: str) -> str:
    """プロフィールの dl/dt/dd から、dt のラベルに一致する dd の値を返す。"""
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


class WebKanjiNationwideScraper(DynamicCrawler):
    """Web幹事 全国版 ホームページ制作会社スクレイパー（Playwright実ブラウザ方式）"""

    DELAY = 2.0
    EXTRA_COLUMNS: list[str] = []

    def _setup(self):
        """headless=True でブラウザを起動。XServer の無い実行環境でも動作させるためヘッドレスにする。"""
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=True)
        self.context = self.browser.new_context(user_agent=self.USER_AGENT)
        self.page = self.context.new_page()

    def _navigate(self, url: str) -> BeautifulSoup | None:
        """ページ遷移して BeautifulSoup を返す。CAPTCHA が出た場合は最大2分待機する。"""
        time.sleep(self.DELAY + random.uniform(0.5, 1.5))
        soup = self.get_soup(url, wait_until="load")

        if _is_blocked(soup):
            self.logger.warning(
                "CAPTCHA検知 — ブラウザウィンドウで手動解決してください（最大2分）: %s", url
            )
            try:
                self.page.wait_for_selector(".companies-item, dl dt", timeout=120_000)
            except Exception:
                self.logger.error("CAPTCHA解決タイムアウト。スキップします: %s", url)
                return None
            soup = BeautifulSoup(self.page.content(), "html.parser")

        return soup

    def parse(self, url: str) -> Generator[dict, None, None]:
        """47都道府県の一覧ページを巡回し、詳細URLを見つけ次第すぐに詳細を取得して yield する (Pattern B)。

        起点の /search は「地域から探す」の都道府県インデックスであり企業カードを持たない。
        企業カードは都道府県別ページ /search/{slug}[/page/{N}] にのみ存在するため、
        全国版として PREFECTURE_SLUGS の各都道府県を順にページ巡回する。
        """
        seen: set[str] = set()

        for pref in PREFECTURE_SLUGS:
            page = 1
            max_page = 1

            while page <= max_page:
                page_url = (
                    f"{BASE_URL}/search/{pref}"
                    if page == 1
                    else f"{BASE_URL}/search/{pref}/page/{page}"
                )
                soup = self._navigate(page_url)

                if soup is None:
                    self.logger.warning("%s %d ページ目の取得に失敗。次の都道府県へ", pref, page)
                    break

                # カードは <a class="companies-item" href="/companies/{slug}"> 自体が詳細リンク
                cards = soup.select("a.companies-item")
                if not cards:
                    self.logger.info("%s %d ページ目にカードが無いため終了", pref, page)
                    break

                # 各都道府県の初回ページで総件数（加算）と総ページ数を確定する
                if page == 1:
                    self._add_total_items(soup)
                    for a in soup.select(".pagination-item a"):
                        m = re.search(r"/page/(\d+)$", a.get("href", ""))
                        if m:
                            max_page = max(max_page, int(m.group(1)))
                    self.logger.info("%s 総ページ数: %d", pref, max_page)

                # カード自身の href が詳細URL。1件ずつ即時に詳細取得 → yield
                for a in cards:
                    href = a.get("href", "")
                    if not href or "/companies/" not in href:
                        continue
                    detail_url = href if href.startswith("http") else BASE_URL + href
                    if detail_url in seen:
                        continue
                    seen.add(detail_url)

                    try:
                        item = self._scrape_detail(detail_url)
                    except Exception as e:
                        self.logger.warning("詳細取得失敗 %s: %s", detail_url, e)
                        continue
                    if item and item.get(Schema.NAME):
                        yield item

                page += 1

    def _add_total_items(self, soup) -> None:
        """都道府県別一覧の「(N件中 …)」表示から総件数を取得し、進捗表示用に加算する。"""
        m = _TOTAL_RE.search(soup.get_text())
        if not m:
            return
        try:
            count = int(m.group(1).replace(",", ""))
        except ValueError:
            return
        self.total_items = (self.total_items or 0) + count
        self.logger.info("累計総件数: %d 件", self.total_items)

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

        # 事業内容: サービス種別の列挙（構造化された短い項目の集合）
        lob = _dl_value(soup, "事業内容")
        if lob:
            item[Schema.LOB] = lob

        address_raw = _dl_value(soup, "本社所在地")
        if address_raw:
            m_post = _POST_RE.search(address_raw)
            if m_post:
                item[Schema.POST_CODE] = m_post.group(1)
            addr_text = re.sub(r"〒\s*\d{3}-\d{4}\s*", "", address_raw).strip()
            m_pref = _PREF_RE.search(addr_text)
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

    scraper = WebKanjiNationwideScraper()
    scraper.execute(f"{BASE_URL}/search")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
