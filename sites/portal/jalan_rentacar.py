"""
じゃらんレンタカー (jalan.net) — レンタカー営業所スクレイパー

取得対象:
    - 営業所名、都道府県、住所、営業時間、アクセス

取得フロー:
    市区町村一覧ページ → 各市区町村の店舗一覧（ページネーション）→ 店舗情報取得

HTML構造:
    - 店舗カード: div.p-store
    - 店舗名:    ul.p-store__header > li > a[href*=SHOP_]
    - 店舗情報:  dt.p-storeInfo__label + dd.p-storeInfo__text
    - ページネーション: a (text="次へ"), ?p=N (0始まり)

対象:
    - 全国（札幌市〜竹富町）

実行方法:
    python scripts/sites/portal/jalan_rentacar.py
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

BASE_URL = "https://www.jalan.net"
CITY_LIST_URL = f"{BASE_URL}/rentacar/city/"

PREF_RE = re.compile(r"(.+?)のレンタカー一覧")
SHOP_LINK_RE = re.compile(r"/SHOP_\d+/$")


class JalanRentacarScraper(StaticCrawler):
    """じゃらんレンタカー 営業所スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["アクセス"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        city_entries = self._get_city_entries()
        self.logger.info("対象市区町村数: %d", len(city_entries))

        seen_shops: set[str] = set()

        for pref, city_url in city_entries:
            self.logger.info("=== %s ===", city_url)
            yield from self._scrape_city(pref, city_url, seen_shops)

    # ------------------------------------------------------------------
    # 市区町村一覧
    # ------------------------------------------------------------------

    def _get_city_entries(self) -> list[tuple[str, str]]:
        """市区町村一覧ページから (都道府県名, URL) のリストを取得"""
        soup = self.get_soup(CITY_LIST_URL)
        if soup is None:
            return []

        entries: list[tuple[str, str]] = []
        current_pref = ""

        for h3 in soup.find_all("h3"):
            match = PREF_RE.search(h3.get_text(strip=True))
            if not match:
                continue
            current_pref = match.group(1)

            ul = h3.find_next_sibling("ul")
            if ul is None:
                continue
            for a_tag in ul.find_all("a", href=True):
                href = a_tag["href"]
                if "/rentacar/search/" in href and "/CTY_" in href:
                    full_url = BASE_URL + href if href.startswith("/") else href
                    entries.append((current_pref, full_url))

        return entries

    # ------------------------------------------------------------------
    # 店舗一覧ページ（ページネーション）
    # ------------------------------------------------------------------

    def _scrape_city(
        self, pref: str, city_url: str, seen_shops: set[str]
    ) -> Generator[dict, None, None]:
        """市区町村の店舗一覧をページネーションしながらスクレイピング"""
        page = 0

        while True:
            current_url = city_url if page == 0 else f"{city_url}?p={page}"
            soup = self.get_soup(current_url)
            if soup is None:
                break

            # div.p-store を店舗カードとして取得
            cards = soup.find_all("div", class_="p-store")
            if not cards:
                break

            for card in cards:
                item = self._parse_card(card, pref, seen_shops)
                if item:
                    yield item

            # 次ページへ
            next_link = soup.find("a", string="次へ")
            if next_link and next_link.get("href"):
                page += 1
                time.sleep(self.DELAY)
            else:
                break

    # ------------------------------------------------------------------
    # 店舗カードのパース
    # ------------------------------------------------------------------

    def _parse_card(self, card, pref: str, seen_shops: set[str]) -> dict | None:
        """div.p-store から店舗情報を抽出"""
        # 店舗名: ul.p-store__header 内の SHOP_ リンク
        shop_link = card.find("a", href=SHOP_LINK_RE)
        if shop_link is None:
            return None

        href = shop_link["href"]
        if href in seen_shops:
            return None
        seen_shops.add(href)

        name = shop_link.get_text(strip=True)
        if not name:
            return None

        item: dict[str, str] = {
            Schema.NAME: name,
            Schema.URL: BASE_URL + href,
            Schema.PREF: pref,
            Schema.CAT_SITE: "レンタカー",
        }

        # dt.p-storeInfo__label + dd.p-storeInfo__text から情報取得
        for dt in card.find_all("dt", class_="p-storeInfo__label"):
            label = dt.get_text(strip=True)
            dd = dt.find_next_sibling("dd", class_="p-storeInfo__text")
            if dd is None:
                continue
            value = dd.get_text(" ", strip=True)
            if not value:
                continue

            if label == "住所":
                item[Schema.ADDR] = value
            elif label == "営業時間":
                # 改行・余白を整形: "平日 09:00 - 18:00 / 土日祝 09:00 - 18:00"
                item[Schema.TIME] = re.sub(r"\s+", " ", value).strip()
            elif label == "アクセス":
                item["アクセス"] = value

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = JalanRentacarScraper()
    scraper.execute(CITY_LIST_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
