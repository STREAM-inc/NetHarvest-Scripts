"""
ガルステ（girlsbar-station.com）— ガールズバー・スナック店舗情報スクレイパー

取得フロー:
    post-sitemap.xml を起点に2系統のURLを収集する。
    1. /shop/{slug}/ — 単独店舗の詳細ページ
    2. /{都道府県slug}/{都市slug}/ — 1記事に複数店舗がtable形式で並ぶ記事ページ
    両者ともtable(tr/td)構造で店舗情報を抽出し、TELで重複排除する。

実行方法:
    python scripts/sites/portal/girlsbar_station.py
"""

import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Generator

root_path = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(root_path))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

BASE_URL = "https://girlsbar-station.com"
SITEMAP_URL = f"{BASE_URL}/post-sitemap.xml"

PREF_RE = re.compile(r"(東京都|北海道|(?:京都|大阪)府|.+?県)")

# 47都道府県のslug（girlsbar-station.com の URL第1セグメントで使われるもの）
# 大分は表記ゆれで oita / ooita 両方を含める
PREFECTURE_SLUGS = {
    "hokkaido", "aomori", "iwate", "miyagi", "akita", "yamagata", "fukushima",
    "ibaraki", "tochigi", "gunma", "saitama", "chiba", "tokyo", "kanagawa",
    "niigata", "toyama", "ishikawa", "fukui", "yamanashi", "nagano", "gifu",
    "shizuoka", "aichi", "mie", "shiga", "kyoto", "osaka", "hyogo", "nara",
    "wakayama", "tottori", "shimane", "okayama", "hiroshima", "yamaguchi",
    "tokushima", "kagawa", "ehime", "kochi", "fukuoka", "saga", "nagasaki",
    "kumamoto", "oita", "ooita", "miyazaki", "kagoshima", "okinawa",
}

ARTICLE_URL_RE = re.compile(r"^https?://girlsbar-station\.com/([^/]+)/([^/]+)/?$")
SHOP_URL_RE = re.compile(r"^https?://girlsbar-station\.com/shop/[^/]+/?$")


class GirlsBarStationScraper(StaticCrawler):
    """ガルステ（ガールズバー・スナック検索サイト）スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["エリア"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # url 引数は post-sitemap.xml を想定
        sitemap_url = url if url.endswith(".xml") else SITEMAP_URL

        shop_urls, article_urls = self._collect_urls_from_sitemap(sitemap_url)
        self.logger.info("/shop/ URL: %d件, 記事ページ URL: %d件", len(shop_urls), len(article_urls))

        seen_keys: set[str] = set()  # TELまたは(name+addr)で重複排除

        # 1. /shop/ 系の単独店舗ページ
        for shop_url in shop_urls:
            item = self._scrape_detail(shop_url)
            if not item:
                continue
            key = self._dedup_key(item)
            if key and key in seen_keys:
                continue
            if key:
                seen_keys.add(key)
            yield item

        # 2. /{pref}/{city}/ 系の記事ページ（1ページに複数店舗）
        for article_url in article_urls:
            for item in self._scrape_article_page(article_url):
                key = self._dedup_key(item)
                if key and key in seen_keys:
                    continue
                if key:
                    seen_keys.add(key)
                yield item

    @staticmethod
    def _dedup_key(item: dict) -> str:
        tel = (item.get(Schema.TEL) or "").strip()
        if tel:
            return f"tel:{tel}"
        name = (item.get(Schema.NAME) or "").strip()
        addr = (item.get(Schema.ADDR) or "").strip()
        if name and addr:
            return f"na:{name}|{addr}"
        return ""

    # ------------------------------------------------------------------
    # サイトマップからのURL収集
    # ------------------------------------------------------------------

    def _collect_urls_from_sitemap(self, sitemap_url: str) -> tuple[list[str], list[str]]:
        """post-sitemap.xml から /shop/ と /{pref}/{city}/ のURLを分類して返す"""
        response = self.session.get(sitemap_url, timeout=self.TIMEOUT)
        response.raise_for_status()
        root = ET.fromstring(response.text)

        shop_urls: list[str] = []
        article_urls: list[str] = []
        seen_shop: set[str] = set()
        seen_article: set[str] = set()

        for elem in root.iter():
            if not str(elem.tag).endswith("loc"):
                continue
            u = (elem.text or "").strip()
            if not u:
                continue

            if SHOP_URL_RE.match(u):
                if u not in seen_shop:
                    seen_shop.add(u)
                    shop_urls.append(u)
                continue

            m = ARTICLE_URL_RE.match(u)
            if m and m.group(1) in PREFECTURE_SLUGS:
                if u not in seen_article:
                    seen_article.add(u)
                    article_urls.append(u)

        return shop_urls, article_urls

    # ------------------------------------------------------------------
    # /shop/ 単独店舗ページ
    # ------------------------------------------------------------------

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        item: dict[str, str] = {Schema.URL: url}

        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(strip=True)
            if name:
                item[Schema.NAME] = name

        # タグ: div.entry-footer-tags 内の /tag/..._area/ → エリア、その他 → ジャンル
        genres: list[str] = []
        areas: list[str] = []
        for tag_box in soup.select("div.entry-footer-tags"):
            for a in tag_box.find_all("a", href=True):
                href = a["href"].strip()
                if not href.startswith(f"{BASE_URL}/tag/"):
                    continue
                text = a.get_text(strip=True)
                if not text:
                    continue
                if href.rstrip("/").endswith("_area"):
                    if text not in areas:
                        areas.append(text)
                else:
                    if text not in genres:
                        genres.append(text)
        if genres:
            item[Schema.CAT_SITE] = ",".join(genres)
        if areas:
            item["エリア"] = ",".join(areas)

        self._fill_item_from_tables(soup, item)

        if Schema.NAME not in item:
            self.logger.warning("店舗名取得失敗のためスキップ: %s", url)
            return None
        return item

    # ------------------------------------------------------------------
    # /{pref}/{city}/ 記事ページ（1ページ複数店舗）
    # ------------------------------------------------------------------

    def _scrape_article_page(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)
        if soup is None:
            return

        for table in soup.find_all("table"):
            item: dict[str, str] = {Schema.URL: url}
            self._fill_item_from_table(table, item)

            # 記事ページの各table = 各店舗。「店名」または「店舗名」で開始することを期待
            if Schema.NAME not in item:
                continue
            # 電話番号も店名も無い場合はスキップ（表が店舗以外の用途のとき）
            if Schema.TEL not in item:
                continue
            yield item

    # ------------------------------------------------------------------
    # 共通: tableからの抽出
    # ------------------------------------------------------------------

    def _fill_item_from_tables(self, soup, item: dict) -> None:
        """ページ内の全 <table> を順に走査し item を埋める（/shop/ 詳細ページ用）"""
        for table in soup.find_all("table"):
            self._fill_item_from_table(table, item, override_name=False)

    def _fill_item_from_table(self, table, item: dict, override_name: bool = True) -> None:
        """単一 <table> から店舗情報を item に埋める"""
        for tr in table.find_all("tr"):
            tds = tr.find_all("td", recursive=False)
            if len(tds) < 2:
                continue
            label = tds[0].get_text(strip=True)
            value_td = tds[1]
            value = value_td.get_text(" ", strip=True)
            # 「‐」「-」「ー」のみのプレースホルダは無視
            if value in ("", "‐", "-", "ー", "—", "－"):
                continue

            if label in ("店名", "店舗名"):
                if override_name or Schema.NAME not in item:
                    item[Schema.NAME] = value

            elif label == "住所":
                m = PREF_RE.match(value)
                if m:
                    item[Schema.PREF] = m.group(1)
                    item[Schema.ADDR] = value[len(m.group(1)):].strip()
                else:
                    item[Schema.ADDR] = value

            elif label == "電話番号":
                a = value_td.find("a", href=True)
                if a and a["href"].startswith("tel:"):
                    item[Schema.TEL] = a["href"][4:].strip()
                else:
                    item[Schema.TEL] = value

            elif label == "営業時間":
                item[Schema.TIME] = value

            elif label == "定休日":
                item[Schema.HOLIDAY] = value

            elif label in ("公式HP", "ホームページ", "Webサイト", "WEBサイト", "HP"):
                a = value_td.find("a", href=True)
                if a and a["href"].startswith(("http://", "https://")):
                    item[Schema.HP] = a["href"]
                elif value.startswith(("http://", "https://")):
                    item[Schema.HP] = value


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = GirlsBarStationScraper()
    scraper.execute(SITEMAP_URL)

    print("\n" + "=" * 60)
    print("実行結果サマリ")
    print("=" * 60)
    print(f"  出力ファイル:     {scraper.output_filepath}")
    print(f"  取得件数:         {scraper.item_count}")
    print(f"  観測カラム数:     {len(scraper.observed_columns)}")
    print(f"  観測カラム:       {scraper.observed_columns}")
    print("=" * 60)
