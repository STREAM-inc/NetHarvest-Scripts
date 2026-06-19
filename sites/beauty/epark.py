"""
EPARKリラク＆エステ (mitsuraku.jp) — リラクゼーション・マッサージ・エステサロン検索ポータル

取得対象:
    - 全国のリラク・エステサロン基本情報 (名称・住所・電話番号・営業時間・定休日・支払い方法・HP・メニューなど)

取得フロー:
    1. ルートページ (https://mitsuraku.jp/) からナビゲーションリンクを抽出
       (都道府県・ジャンル(esthe/massage/fitness等)・エリア/路線 を含む全トップナビ)
    2. 各ナビページを再帰的に辿り、サロンパネルが出現した時点でリストとして処理
    3. サロンパネルがないページは子ナビゲーションリンクへ再帰 (深さ7段まで)
    4. 各リストページを ?page=N でページネーション
    5. 詳細URLはグローバル seen_detail で重複排除し、ThreadPoolExecutor で並行取得・即yield

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
from urllib.parse import urljoin, urlparse

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

# ナビゲーションとして辿らないスラグ (機能ページ・静的アセットなど)
# NOTE: "esthe"/"fitness"/"massage"/"genre"/"area"/"railway" はジャンル/エリアナビなので許可。
#       これらを除外すると /tokyo/shinjuku/esthe/ 等のジャンル別一覧に到達できず件数が激減する。
_NON_NAV_SLUGS = {
    "inquiry", "search",
    "salon", "sitemap", "column", "salonRequest", "news", "login", "mypage",
    "corporate", "special", "term", "publish", "guide", "agreement",
    "faq", "policy", "company", "images", "css", "js", "ajax", "reserve",
    "coupon", "review", "access", "blog", "menu", "photo", "shop",
}

_SLUG_RE = re.compile(r"^[a-z][a-z0-9-]*$")

# 詳細ページ並行取得のスレッド数
_N_WORKERS = 20

# スレッドごとの requests.Session (thread-safe)
_thread_local = threading.local()


def _get_thread_session() -> requests.Session:
    """各ワーカースレッドに固有のセッションを返す"""
    if not hasattr(_thread_local, "session"):
        sess = requests.Session()
        retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
        sess.mount("https://", HTTPAdapter(max_retries=retries))
        sess.headers.update({
            "User-Agent": StaticCrawler.USER_AGENT,
        })
        _thread_local.session = sess
    return _thread_local.session


class EparkScraper(StaticCrawler):
    """EPARKリラク＆エステ スクレイパー"""

    # DELAY=0: 詳細ページは ThreadPoolExecutor で並行取得するため
    # フレームワーク側の per-item sleep を排除し、スループットを最大化する
    DELAY = 0.0
    EXTRA_COLUMNS = ["メニュー"]

    def parse(self, url: str):
        root_soup = self.get_soup(url)

        nav_urls = self._find_nav_children(root_soup, url)
        self.logger.info(f"ルートナビゲーション数: {len(nav_urls)}")

        seen: set[str] = set()
        seen_detail: set[str] = set()
        for nav_url in nav_urls:
            yield from self._traverse(nav_url, depth=1, seen=seen, seen_detail=seen_detail)

    def _find_nav_children(self, soup, base_url: str) -> list[str]:
        """現在URLより1段深いナビゲーションリンクを抽出する"""
        base_segs = [s for s in urlparse(base_url).path.split("/") if s]
        target_depth = len(base_segs) + 1

        urls: list[str] = []
        seen: set[str] = set()
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
                continue
            full = urljoin(base_url, href).split("?")[0].split("#")[0]
            parsed = urlparse(full)
            if parsed.netloc != "mitsuraku.jp":
                continue
            segs = [s for s in parsed.path.split("/") if s]
            if len(segs) != target_depth:
                continue
            slug = segs[-1]
            if slug in _NON_NAV_SLUGS or not _SLUG_RE.match(slug):
                continue
            normalized = f"https://mitsuraku.jp/{'/'.join(segs)}/"
            if normalized not in seen and normalized != base_url:
                urls.append(normalized)
                seen.add(normalized)
        return urls

    def _traverse(self, url: str, depth: int, seen: set[str], seen_detail: set[str]):
        """再帰的に階層を辿り、サロンパネルが出現したページをリストとして処理する"""
        if url in seen or depth > 7:
            return
        seen.add(url)

        soup = self.get_soup(url)
        if soup is None:
            return

        panels = soup.select("div.panel.result-panel.js-salon-panel")
        if panels:
            # リストページ: 1ページ目のsoupを再利用してページネーション
            yield from self._scrape_list(url, first_soup=soup, seen_detail=seen_detail)
        else:
            child_urls = self._find_nav_children(soup, url)
            self.logger.info(f"{'  ' * depth}[depth={depth}] {url}: 子リンク数 {len(child_urls)}")
            for child_url in child_urls:
                yield from self._traverse(child_url, depth + 1, seen, seen_detail)

    def _scrape_list(self, list_url: str, first_soup=None, seen_detail: set[str] | None = None):
        """リスト(一覧)ページをページネーションし、各ページの詳細を並行取得してyield"""
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

            panels = soup.select("div.panel.result-panel.js-salon-panel")
            if not panels:
                break

            # パネルから基本情報と詳細URLを抽出
            batch: list[tuple[dict, str]] = []
            for panel in panels:
                basic, detail_url = self._extract_panel_basic(panel)
                if not detail_url or detail_url in seen_detail:
                    continue
                seen_detail.add(detail_url)
                batch.append((basic, detail_url))

            # 詳細ページを並行取得してyield
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
