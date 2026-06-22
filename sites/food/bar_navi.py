"""
バーナビ (BAR-NAVI) — サントリー公式バー検索サイト (日本最大級)

取得対象:
    - 全国のバー・酒場情報
    - 店名 / フリガナ / 都道府県 / 住所 / TEL /
      営業時間 / 定休日 / ジャンル / HP / エリア / 席数

取得フロー:
    1. {url}/sitemap.html から都道府県ページの URL 一覧を収集
    2. 各都道府県ページ (/{pref}/?page=N) をページネーションしながら
       店舗詳細 URL (/shop/{id}/) を取得
    3. 各店舗詳細ページからデータを抽出・即 yield

備考:
    - Akamai WAF により requests では 403。Playwright (DynamicCrawler) を使用。
    - ショップ ID は電話番号そのまま (例: 0351598008) または S/0X 接頭辞 ID。

実行方法:
    # ローカルテスト
    python scripts/sites/food/bar_navi.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id bar_navi
"""

import re
import sys
import urllib.parse
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup
from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema


# 都道府県ページの href パターン: /tokyo/, /osaka/, /kanagawa/ ...
_PREF_HREF_RE = re.compile(r"^/[a-z][a-z]+/$")
# 除外すべき非都道府県パス
_PREF_EXCLUDE = {"/search/", "/keisai/", "/sitemap/", "/blog/", "/shop/", "/"}

# 店舗詳細ページの href パターン: /shop/0351598008/, /shop/S000006676/
_SHOP_HREF_RE = re.compile(r"^/shop/[^/]+/$")

# 都道府県抽出
_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|"
    r"三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_WS_RE = re.compile(r"\s+")

# HP として採用しない URL パターン (bar-navi 内部 / SNS / 電話リンク)
_HP_EXCLUDE_RE = re.compile(
    r"^(tel:|mailto:|javascript:|#|https?://bar-navi\.suntory\.co\.jp|"
    r"https?://(line\.me|instagram\.com|twitter\.com|x\.com|facebook\.com|tiktok\.com))"
)


class BarNaviScraper(DynamicCrawler):
    """バーナビ (BAR-NAVI) スクレイパー"""

    DELAY = 2.0
    EXTRA_COLUMNS = ["エリア", "席数"]

    # dt/dd あるいは th/td のキー → Schema 定数 または EXTRA_COLUMNS 名
    _FIELD_MAP: dict[str, str | None] = {
        "住所": Schema.ADDR,
        "TEL": Schema.TEL,
        "電話番号": Schema.TEL,
        "営業時間": Schema.TIME,
        "定休日": Schema.HOLIDAY,
        "ジャンル": Schema.CAT_SITE,
        "業態": Schema.CAT_SITE,
        "ホームページ": Schema.HP,
        "HP": Schema.HP,
        "エリア": "エリア",
        "席数": "席数",
        "収容人数": "席数",
    }

    # ---------------------------------------------------------- #
    #  parse() — sites.yml の url をそのまま受け取る              #
    # ---------------------------------------------------------- #
    def parse(self, url: str) -> Generator[dict, None, None]:
        # 1. サイトマップ or ルートページから都道府県 URL を収集
        pref_urls = self._collect_pref_urls(url)
        self.logger.info("都道府県ページ: %d 件", len(pref_urls))

        # 2. 各都道府県のページを巡回しながら即 yield
        for pref_url in pref_urls:
            yield from self._scrape_pref(pref_url, url)

    # ---------------------------------------------------------- #
    #  都道府県 URL 収集                                          #
    # ---------------------------------------------------------- #
    def _collect_pref_urls(self, base_url: str) -> list[str]:
        # サイトマップを優先
        sitemap_url = urllib.parse.urljoin(base_url, "sitemap.html")
        soup = self.get_soup(sitemap_url)
        if soup is None:
            soup = self.get_soup(base_url)
        if soup is None:
            return []

        seen: set[str] = set()
        urls: list[str] = []
        for a in soup.find_all("a", href=True):
            href: str = a["href"].strip()
            if _PREF_HREF_RE.match(href) and href not in _PREF_EXCLUDE:
                full = urllib.parse.urljoin(base_url, href)
                if full not in seen:
                    seen.add(full)
                    urls.append(full)
        return urls

    # ---------------------------------------------------------- #
    #  都道府県ページのページネーション + 店舗即 yield            #
    # ---------------------------------------------------------- #
    def _scrape_pref(self, pref_url: str, base_url: str) -> Generator[dict, None, None]:
        page_num = 1
        seen_shops: set[str] = set()

        while True:
            list_url = pref_url if page_num == 1 else f"{pref_url}?page={page_num}"
            soup = self.get_soup(list_url)
            if soup is None:
                break

            # 店舗リンクを収集 (このページ分のみ)
            shop_hrefs: list[str] = []
            for a in soup.find_all("a", href=True):
                href: str = a["href"].strip()
                if _SHOP_HREF_RE.match(href) and href not in seen_shops:
                    seen_shops.add(href)
                    shop_hrefs.append(href)

            if not shop_hrefs:
                self.logger.info("店舗リンクなし: %s", list_url)
                break

            # 詳細ページを 1 件ずつ取得 → 即 yield
            for href in shop_hrefs:
                shop_url = urllib.parse.urljoin(base_url, href)
                try:
                    item = self._scrape_detail(shop_url)
                    if item:
                        yield item
                except Exception as exc:
                    self.logger.warning("詳細取得エラー %s: %s", shop_url, exc)

            # 次ページ有無を確認
            if not self._has_next_page(soup, page_num):
                break
            page_num += 1

    # ---------------------------------------------------------- #
    #  次ページ判定                                               #
    # ---------------------------------------------------------- #
    def _has_next_page(self, soup: BeautifulSoup, current: int) -> bool:
        next_n = current + 1
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if f"page={next_n}" in href or f"p={next_n}" in href:
                return True
        for a in soup.find_all("a"):
            txt = a.get_text(strip=True)
            if txt in ("次へ", "次のページ", "→", ">", "▶", "次"):
                return True
        return False

    # ---------------------------------------------------------- #
    #  店舗詳細ページのスクレイピング                             #
    # ---------------------------------------------------------- #
    def _scrape_detail(self, shop_url: str) -> dict | None:
        soup = self.get_soup(shop_url)
        if soup is None:
            return None

        data: dict = {Schema.URL: shop_url}

        # ---- 店名・フリガナ ----
        # タイトル形式: "店名(フリガナ) エリア -BAR-NAVI"
        h1 = soup.find("h1")
        title_el = soup.find("title")
        name_raw = (
            h1.get_text(" ", strip=True) if h1
            else (title_el.get_text(strip=True) if title_el else "")
        )
        # BAR-NAVI サフィックスを除去
        name_raw = re.sub(r"\s*[-－]\s*BAR-NAVI\s*$", "", name_raw).strip()

        # "(フリガナ)" または "（フリガナ）" を抽出
        m = re.match(r"^(.+?)\s*[（(]([^）)]+)[）)]\s*(.*)", name_raw)
        if m:
            data[Schema.NAME] = m.group(1).strip()
            data[Schema.NAME_KANA] = m.group(2).strip()
            if m.group(3):
                data.setdefault("エリア", m.group(3).strip())
        else:
            data[Schema.NAME] = name_raw or ""

        if not data.get(Schema.NAME):
            return None

        # ---- dl (dt/dd) + table (th/td) からフィールド抽出 ----
        kv: dict[str, str] = {}
        self._extract_dl(soup, kv)
        self._extract_table(soup, kv)

        for raw_key, val in kv.items():
            schema_or_col = self._FIELD_MAP.get(raw_key)
            if schema_or_col is not None:
                data.setdefault(schema_or_col, val)

        # ---- 住所から都道府県を分離 ----
        addr = data.get(Schema.ADDR, "")
        if addr:
            pm = _PREF_RE.match(addr)
            if pm:
                data[Schema.PREF] = pm.group(1)
                data[Schema.ADDR] = addr[pm.end():].strip()

        # ---- HP (data に未セットの場合、外部リンクから補足) ----
        if not data.get(Schema.HP):
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                if href.startswith("http") and not _HP_EXCLUDE_RE.match(href):
                    data[Schema.HP] = href
                    break

        return data

    # ---- dl/dt-dd ペア抽出ヘルパー ----
    def _extract_dl(self, soup: BeautifulSoup, kv: dict) -> None:
        for dl in soup.find_all("dl"):
            dts = dl.find_all("dt")
            dds = dl.find_all("dd")
            for i, dt in enumerate(dts):
                key = _WS_RE.sub("", dt.get_text(strip=True))
                if i < len(dds):
                    val = _WS_RE.sub(" ", dds[i].get_text(" ", strip=True)).strip()
                    if key and val:
                        kv.setdefault(key, val)

    # ---- table/th-td ペア抽出ヘルパー ----
    def _extract_table(self, soup: BeautifulSoup, kv: dict) -> None:
        for table in soup.find_all("table"):
            for tr in table.find_all("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if th and td:
                    key = _WS_RE.sub("", th.get_text(strip=True))
                    val = _WS_RE.sub(" ", td.get_text(" ", strip=True)).strip()
                    if key and val:
                        kv.setdefault(key, val)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BarNaviScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://bar-navi.suntory.co.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
