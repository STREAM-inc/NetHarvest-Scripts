"""
EPARKリラク＆エステ (mitsuraku.jp) — リラクゼーション・マッサージ・エステサロン検索ポータル

取得対象:
    - 全国のリラク・エステサロン基本情報 (名称・住所・電話番号・営業時間・定休日・支払い方法・HP・メニューなど)

取得フロー:
    1. カテゴリ (マッサージ/エステ/フィットネス) × 全47都道府県 を直接構築 (計141URL)
         /{pref}/          → マッサージ・リラク
         /esthe/{pref}/    → エステ
         /fitness/{pref}/  → フィットネス
    2. 各一覧ページを ?page=N でページネーション (パネル消失 or 404 でページ終了)
    3. パネル検出: div.panel.result-panel.js-salon-panel → [class*=js-salon-panel]
                   → h2.search_shopname の直接親要素 の順で3段フォールバック
    4. 詳細URLは seen_detail で重複排除し、ThreadPoolExecutor (20並列) で並行取得・即yield
    5. メニューは詳細ページに無いため /reserve/menu/{id}/ から別取得 (任意・失敗許容)

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

# 詳細URLから salon ID を抽出 (例: https://mitsuraku.jp/salon/47915/ → 47915)
_SALON_ID_PATTERN = re.compile(r"/salon/(\d+)")

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

# メニューを別ページ (/reserve/menu/{id}/) からも取得するか。
# 1サロンあたり追加リクエストが1件増えるため、件数優先で軽量化したい場合は False にする。
_FETCH_MENU_PAGE = True

_thread_local = threading.local()


def _get_thread_session() -> requests.Session:
    """各ワーカースレッドに固有のセッションを返す"""
    if not hasattr(_thread_local, "session"):
        sess = requests.Session()
        retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
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
        grand_total = 0  # 全URL通算のyield件数 (どこで止まるか可視化用)
        for genre_prefix in _GENRE_PREFIXES:
            for pref in _ALL_PREFS:
                idx += 1
                list_url = f"{base}/{genre_prefix}{pref}/"
                self.logger.info(f"[{idx}/{total}] {list_url}")
                # 1県の失敗で全141URLの巡回を止めないよう、県単位で例外を握りつぶす
                pref_count = 0
                try:
                    for item in self._scrape_list(list_url, seen_detail=seen_detail):
                        pref_count += 1
                        grand_total += 1
                        yield item
                except Exception as e:
                    self.logger.warning(f"一覧巡回エラー (継続) {list_url}: {e}")
                self.logger.info(f"[{idx}/{total}] 完了 {pref}: {pref_count}件 (通算 {grand_total}件)")

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

    # 一覧1ページあたりの掲載件数 (mitsuraku.jp は20件固定)
    _PAGE_SIZE = 20
    _COUNT_PATTERN = re.compile(r"([\d,]+)\s*件中")

    def _fetch_list_soup(self, url: str):
        """
        一覧ページを直接 requests で取得して soup を返す。

        フレームワークの self.get_soup() は ?page=N 付きURLで None を返す事例があり
        (URL正規化 / 訪問済みdedup / 内部ページ上限など)、ページネーションが1ページで
        打ち切られて取得件数が激減する。一覧取得は curl で動作確認済みの直接セッションに
        統一し、フレームワーク依存を排除する。

        戻り値:
            soup  … 取得成功
            None  … 404 (=最終ページ超過。巡回終了の合図)
            例外  … 一時エラー (呼び出し側でリトライ/スキップ)
        """
        sess = _get_thread_session()
        resp = sess.get(url, timeout=self.TIMEOUT)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        content_type = resp.headers.get("Content-Type", "")
        if "charset=" not in content_type.lower():
            resp.encoding = resp.apparent_encoding
        return bs4.BeautifulSoup(resp.text, "html.parser")

    def _estimate_max_page(self, soup) -> int:
        """一覧ページの「◯◯件中」表記から想定総ページ数を推定する (取得失敗時の終了判定用)"""
        m = self._COUNT_PATTERN.search(soup.get_text(" ", strip=True))
        if not m:
            return 0
        try:
            total = int(m.group(1).replace(",", "").replace("，", ""))
        except ValueError:
            return 0
        if total <= 0:
            return 0
        return (total + self._PAGE_SIZE - 1) // self._PAGE_SIZE

    def _scrape_list(self, list_url: str, first_soup=None, seen_detail: set[str] | None = None):
        """一覧ページをページネーションし、各ページの詳細を並行取得してyield"""
        if seen_detail is None:
            seen_detail = set()
        page = 1
        # 一覧ページの想定総ページ数 ("◯◯件中" から算出)。0 のうちは未確定。
        max_page = 0
        while True:
            if page == 1 and first_soup is not None:
                soup = first_soup
            else:
                page_url = f"{list_url}?page={page}" if page > 1 else list_url
                # _fetch_list_soup は 404(=最終ページ超過) で None、一時エラーで例外を返す。
                # 一時エラーは数回リトライし、それでもダメなら(総ページ数が分かっていれば)
                # 次ページへスキップして途中終了を避ける。
                soup = None
                fetched_ok = False
                for attempt in range(3):
                    try:
                        soup = self._fetch_list_soup(page_url)
                        fetched_ok = True
                        break  # None(404) も成功扱い(=終了判定へ)
                    except Exception as e:
                        self.logger.info(f"  retry page={page} ({attempt + 1}/3): {page_url} ({e})")
                if not fetched_ok:
                    # 一時エラー。総ページ数が分かっていて未到達なら次ページへスキップ。
                    if max_page and page < max_page:
                        self.logger.warning(f"  page={page} 取得失敗をスキップ: {page_url}")
                        page += 1
                        continue
                    break
                if soup is None:
                    break  # 404 = 最終ページ超過。正常終了。

            # 総件数から最終ページを推定 (例: "1,940件中 1～20件を表示")
            if not max_page:
                max_page = self._estimate_max_page(soup)

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
        name = name_el.get_text(strip=True) if name_el else name_a.get_text(strip=True)

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
            soup = self._get_soup_with_session(sess, url)
            if soup is None:
                return None
            data = self._parse_detail_soup(soup, url)
            if data is None:
                return None

            # メニューは詳細ページに掲載されないため /reserve/menu/{id}/ から取得 (任意)
            if _FETCH_MENU_PAGE and not data.get("メニュー"):
                m = _SALON_ID_PATTERN.search(url)
                if m:
                    data["メニュー"] = self._fetch_menu(sess, m.group(1))
            return data
        except Exception as e:
            self.logger.warning(f"詳細取得エラー ({url}): {e}")
            return None

    def _get_soup_with_session(self, sess: requests.Session, url: str):
        """セッションを使ってHTMLを取得しBeautifulSoupを返す (文字化け対策込み)"""
        resp = sess.get(url, timeout=self.TIMEOUT)
        resp.raise_for_status()
        content_type = resp.headers.get("Content-Type", "")
        if "charset=" not in content_type.lower():
            resp.encoding = resp.apparent_encoding
        return bs4.BeautifulSoup(resp.text, "html.parser")

    def _fetch_menu(self, sess: requests.Session, salon_id: str) -> str:
        """
        /reserve/menu/{id}/ からメニュー(コース名+EPARK料金)を取得する。
        詳細ページにはメニューが無く、予約メニューページに集約されているため別取得。
        失敗しても本体取得を止めない (空文字を返す)。
        """
        try:
            url = f"https://mitsuraku.jp/reserve/menu/{salon_id}/"
            soup = self._get_soup_with_session(sess, url)
        except Exception as e:
            self.logger.warning(f"メニュー取得エラー (id={salon_id}): {e}")
            return ""

        items: list[str] = []
        for name_el in soup.select("div.menu-name.fb, .menu-name"):
            name = name_el.get_text(strip=True)
            if not name:
                continue
            # コース名の近傍 (同一 reserve-menu-title / panel) にある EPARK料金 を探す
            price = ""
            container = name_el.find_parent(class_=re.compile(r"reserve-menu-title|reserve-menu-panel|panel-body"))
            if container is not None:
                price_el = container.select_one("span.ml5.fb, span.fb")
                if price_el and "円" in price_el.get_text():
                    price = price_el.get_text(strip=True)
            items.append(f"{name} {price}".strip())
            if len(items) >= 20:
                break
        return " / ".join(items)

    def _parse_detail_soup(self, soup, url: str = "") -> dict | None:
        """詳細ページのBeautifulSoupから構造化データを抽出する"""
        try:
            # TEL: data-phone-number 属性 (要素は <div>/<li>。<p> ではない点に注意)
            tel_el = soup.select_one("[data-phone-number]")
            tel = tel_el.get("data-phone-number", "").strip() if tel_el else ""

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
                    hours = p_el.get_text(strip=True) if p_el else value_el.get_text(strip=True)
                elif header == "定休日":
                    p_el = value_el.select_one("p")
                    holiday = p_el.get_text(strip=True) if p_el else value_el.get_text(strip=True)
                elif header in ("クレジットカード", "キャッシュレス決済"):
                    val = value_el.get_text(separator=" ", strip=True)
                    pay = f"{pay} / {val}".strip(" /") if pay else val
                elif header in ("ホームページ", "HP", "ウェブサイト", "オフィシャルサイト", "公式サイト"):
                    a_el = value_el.select_one("a[href]")
                    hp = a_el["href"] if a_el else value_el.get_text(strip=True)

            # メニュー: 詳細ページの「今回体験のメニュー」(report_menu) を軽量に拾う。
            # 完全なメニュー一覧は _fetch_menu で別ページから取得する。
            menu = ""
            report_menu = soup.select_one("div.report_menu h4")
            if report_menu:
                menu = report_menu.get_text(separator=" ", strip=True)

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
