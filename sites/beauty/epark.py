"""
EPARKリラク＆エステ (mitsuraku.jp) — リラクゼーション・マッサージ・エステサロン検索ポータル

取得対象:
    - 全国のリラク・エステサロン基本情報 (名称・住所・電話番号・営業時間・定休日・支払い方法・HP・メニューなど)

取得フロー:
    1. カテゴリ (マッサージ/エステ/フィットネス) × 全47都道府県 を直接構築 (計141URL)
         /{pref}/          → マッサージ・リラク
         /esthe/{pref}/    → エステ
         /fitness/{pref}/  → フィットネス
    2. 各一覧ページを ?page=N でページネーション (パネル消失でページ終了)
    3. パネル検出: div.panel.result-panel.js-salon-panel → [class*=js-salon-panel]
                   → h2.search_shopname の直接親要素 の順で3段フォールバック
    4. 詳細URLは seen_detail で重複排除し、ThreadPoolExecutor (20並列) で並行取得・即yield

実行方法:
    # ローカルテスト
    python scripts/sites/beauty/epark.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id epark
"""

import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import bs4
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 全47都道府県スラグ (mitsuraku.jp URLスラグ)
_ALL_PREFS = [
    # 北海道
    "hokkaido",
    # 東北
    "aomori", "iwate", "miyagi", "akita", "yamagata", "fukushima",
    # 関東
    "ibaraki", "tochigi", "gumma", "saitama", "chiba", "tokyo", "kanagawa",
    # 中部 (北信越・東海)
    "niigata", "toyama", "ishikawa", "fukui", "yamanashi", "nagano",
    "gifu", "shizuoka", "aichi",
    # 近畿
    "mie", "shiga", "kyoto", "osaka", "hyogo", "nara", "wakayama",
    # 中国
    "tottori", "shimane", "okayama", "hiroshima", "yamaguchi",
    # 四国
    "tokushima", "kagawa", "ehime", "kochi",
    # 九州・沖縄
    "fukuoka", "saga", "nagasaki", "kumamoto", "oita", "miyazaki",
    "kagoshima", "okinawa",
]

# カテゴリプレフィックス: マッサージ/リラク=空文字, エステ, フィットネス
_GENRE_PREFIXES = ["", "esthe/", "fitness/"]

_N_WORKERS = 20

_thread_local = threading.local()


def _get_thread_session() -> requests.Session:
    """各ワーカースレッドに固有のセッションを返す"""
    if not hasattr(_thread_local, "session"):
        sess = requests.Session()
        retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
        sess.mount("https://", HTTPAdapter(max_retries=retries))
        sess.headers.update({"User-Agent": StaticCrawler.USER_AGENT})
        _thread_local.session = sess
    return _thread_local.session


class EparkScraper(StaticCrawler):
    """EPARKリラク＆エステ スクレイパー"""

    DELAY = 0.0
    EXTRA_COLUMNS = ["メニュー"]

    def parse(self, url: str):
        """
        カテゴリ×都道府県の全141組み合わせを直接スキャン。
        ナビゲーション再帰を廃止し、全都道府県・全カテゴリを確実に処理する。
        """
        base = url.rstrip("/")  # "https://mitsuraku.jp"
        seen_detail: set[str] = set()

        total = len(_GENRE_PREFIXES) * len(_ALL_PREFS)
        idx = 0
        for genre_prefix in _GENRE_PREFIXES:
            for pref in _ALL_PREFS:
                idx += 1
                list_url = f"{base}/{genre_prefix}{pref}/"
                self.logger.info(f"[{idx}/{total}] {list_url}")
                yield from self._scrape_list(list_url, seen_detail=seen_detail)

    def _find_panels(self, soup) -> list:
        """
        サロンパネル要素を3段フォールバックで検出。
        1. div.panel.result-panel.js-salon-panel (元セレクタ)
        2. [class*='js-salon-panel'] (クラス部分一致)
        3. h2.search_shopname の直接親要素 (最汎用フォールバック)
        """
        panels = soup.select("div.panel.result-panel.js-salon-panel")
        if panels:
            return panels

        panels = soup.select("[class*='js-salon-panel']")
        if panels:
            return panels

        h2s = soup.select("h2.search_shopname")
        if not h2s:
            return []
        seen_ids: set[int] = set()
        panels = []
        for h2 in h2s:
            parent = h2.parent
            pid = id(parent)
            if pid not in seen_ids:
                seen_ids.add(pid)
                panels.append(parent)
        return panels

    def _scrape_list(self, list_url: str, first_soup=None, seen_detail: set[str] | None = None):
        """一覧ページをページネーションし、各ページの詳細を並行取得してyield"""
        if seen_detail is None:
            seen_detail = set()
        page = 1
        while True:
            if page == 1 and first_soup is not None:
                soup = first_soup
            else:
                page_url = f"{list_url}?page={page}" if page > 1 else list_url
                soup = self.get_soup(page_url)
                if soup is None:
                    break

            panels = self._find_panels(soup)
            if not panels:
                break

            batch: list[tuple[dict, str]] = []
            for panel in panels:
                basic, detail_url = self._extract_panel_basic(panel)
                if not detail_url or detail_url in seen_detail:
                    continue
                seen_detail.add(detail_url)
                batch.append((basic, detail_url))

            if batch:
                detail_urls = [d for _, d in batch]
                details = self._fetch_details_concurrent(detail_urls)
                for (basic, _), detail in zip(batch, details):
                    item = {**(detail or {})}
                    for k, v in basic.items():
                        if v:
                            item[k] = v
                    if item.get(Schema.NAME):
                        yield item

            page += 1

    def _extract_panel_basic(self, panel) -> tuple[dict, str]:
        """パネル要素からリストページ取得可能な基本情報と詳細URLを返す"""
        name_a = panel.select_one("h2.search_shopname a.js-salon-link")
        if not name_a:
            return {}, ""
        detail_url = name_a.get("href", "")
        if not detail_url:
            return {}, ""

        name_el = name_a.select_one("span[itemprop='name']")
        name = name_el.get_text(strip=True) if name_el else ""

        kana_el = panel.select_one("small[itemprop='alternateName']")
        kana = kana_el.get_text(strip=True) if kana_el else ""

        cat_els = panel.select("span.list-category")
        cat_site = " / ".join(el.get_text(strip=True) for el in cat_els)

        hours_el = panel.select_one("div[itemprop='openingHours']")
        hours_panel = hours_el.get_text(strip=True) if hours_el else ""

        basic: dict = {
            Schema.NAME: name,
            Schema.NAME_KANA: kana,
            Schema.CAT_SITE: cat_site,
            Schema.URL: detail_url,
        }
        if hours_panel:
            basic[Schema.TIME] = hours_panel

        return basic, detail_url

    def _fetch_details_concurrent(self, urls: list[str]) -> list[dict | None]:
        """複数の詳細URLを ThreadPoolExecutor で並行取得する"""
        results: list[dict | None] = [None] * len(urls)
        with ThreadPoolExecutor(max_workers=_N_WORKERS) as ex:
            fut2idx = {ex.submit(self._fetch_detail_threaded, u): i for i, u in enumerate(urls)}
            for fut in as_completed(fut2idx):
                idx = fut2idx[fut]
                try:
                    results[idx] = fut.result()
                except Exception as e:
                    self.logger.warning(f"詳細取得エラー (並行): {e}")
        return results

    def _fetch_detail_threaded(self, url: str) -> dict | None:
        """スレッド内から詳細ページを取得して構造化データを返す"""
        try:
            sess = _get_thread_session()
            resp = sess.get(url, timeout=self.TIMEOUT)
            resp.raise_for_status()
            content_type = resp.headers.get("Content-Type", "")
            if "charset=" not in content_type.lower():
                resp.encoding = resp.apparent_encoding
            soup = bs4.BeautifulSoup(resp.text, "html.parser")
            return self._parse_detail_soup(soup, url)
        except Exception as e:
            self.logger.warning(f"詳細取得エラー ({url}): {e}")
            return None

    def _parse_detail_soup(self, soup, url: str = "") -> dict | None:
        """詳細ページのBeautifulSoupから構造化データを抽出する"""
        try:
            # TEL: data-phone-number 属性
            tel_el = soup.select_one("p.js-shop-phone-number[data-phone-number]")
            tel = tel_el["data-phone-number"] if tel_el else ""

            # 住所 (itemprop)
            region_el = soup.select_one("span[itemprop='addressRegion']")
            locality_el = soup.select_one("span[itemprop='addressLocality']")
            street_el = soup.select_one("span[itemprop='streetAddress']")

            pref = region_el.get_text(strip=True) if region_el else ""
            locality = locality_el.get_text(strip=True) if locality_el else ""
            street = street_el.get_text(strip=True).lstrip("\xa0").strip() if street_el else ""
            addr = f"{locality} {street}".strip() if street else locality

            if not pref and addr:
                m = _PREF_PATTERN.match(addr)
                if m:
                    pref = m.group(1)
                    addr = addr[m.end():].strip()
            elif pref and addr.startswith(pref):
                addr = addr[len(pref):].strip()

            # 営業時間・定休日・支払い方法・HP (panel-list)
            hours = ""
            holiday = ""
            pay = ""
            hp = ""
            for ul in soup.select("ul.row.panel-list"):
                header_el = ul.select_one("li.col-xs-4, li.col-sm-3")
                value_el = ul.select_one("li.col-xs-8, li.col-sm-9")
                if not header_el or not value_el:
                    continue
                header = "".join(
                    t.strip() for t in header_el.find_all(string=True, recursive=False)
                ).strip()
                if header == "営業時間":
                    p_el = value_el.select_one("p")
                    if p_el:
                        hours = p_el.get_text(strip=True)
                elif header == "定休日":
                    p_el = value_el.select_one("p")
                    holiday = p_el.get_text(strip=True) if p_el else value_el.get_text(strip=True)
                elif header == "クレジットカード":
                    pay = value_el.get_text(separator=" ", strip=True)
                elif header in ("ホームページ", "HP", "ウェブサイト"):
                    a_el = value_el.select_one("a[href]")
                    hp = a_el["href"] if a_el else value_el.get_text(strip=True)

            # メニュー
            menu_items = []
            for row in soup.select("div.menu-list__item, li.menu-list__item, tr.js-menu-row"):
                name_el = row.select_one(".menu-list__name, .menu__name, td.menu-name")
                price_el = row.select_one(".menu-list__price, .menu__price, td.menu-price")
                item_name = name_el.get_text(strip=True) if name_el else ""
                item_price = price_el.get_text(strip=True) if price_el else ""
                if item_name:
                    menu_items.append(f"{item_name} {item_price}".strip())
            if not menu_items:
                for section in soup.select("section.menu, div.menu-wrap, div#menu"):
                    for row in section.select("tr, li"):
                        text = row.get_text(separator=" ", strip=True)
                        if text:
                            menu_items.append(text)
                        if len(menu_items) >= 20:
                            break
                    if menu_items:
                        break
            menu = " / ".join(menu_items[:20])

            return {
                Schema.PREF: pref,
                Schema.ADDR: addr,
                Schema.TEL: tel,
                Schema.TIME: hours,
                Schema.HOLIDAY: holiday,
                Schema.PAYMENTS: pay,
                Schema.HP: hp,
                "メニュー": menu,
            }
        except Exception as e:
            self.logger.warning(f"詳細パースエラー ({url}): {e}")
            return None


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = EparkScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://mitsuraku.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
