"""
ニフティ不動産（不動産会社・不動産屋を探す） — myhome.nifty.com/shop

取得対象:
    - 全国の不動産会社・不動産屋（店舗）情報

取得フロー（一覧が無いため階層を辿って探索する）:
    /shop/ (ルート)
      → 都道府県ページ      /shop/{pref}/
        → 市区町村ページ    /shop/{pref}/{city}_ct/  （ページ送り /{n}/）
          → 店舗詳細ページ  /shop/shopinfo_{id}/     （th/td テーブル）
    詳細ページを 1 件取得するごとに即 yield する（Pattern B）。

実行方法:
    # ローカルテスト
    python scripts/sites/realestate/myhome.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id myhome
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 都道府県リンク: /shop/aomori/ のような 1 セグメント
_PREF_LINK = re.compile(r"^/shop/([a-z]+)/$")
# 市区町村リンク: /shop/aomori/aomorishi_ct/
_CITY_LINK = re.compile(r"^/shop/[a-z]+/[a-z0-9]+_ct/$")
# 店舗詳細リンク（正規形のみ）: /shop/shopinfo_xxxx/
_SHOP_LINK = re.compile(r"^/shop/(shopinfo_[a-f0-9]+)/$")

# 住所先頭の都道府県を抜き出す
_PREF_PATTERN = re.compile(
    r"^(東京都|北海道|(?:京都|大阪)府|"
    r"(?:神奈川|和歌山|鹿児島|"
    r"青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|新潟|富山|"
    r"石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|鳥取|島根|"
    r"岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|沖縄)県)"
)


class MyhomeScraper(StaticCrawler):
    """ニフティ不動産（不動産会社・不動産屋を探す） スクレイパー"""

    DELAY = 1.5

    EXTRA_COLUMNS = [
        "アクセス",  # 最寄駅＋徒歩分（構造化された短い情報のみ）
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url を唯一のルート（起点）とする
        root = self.get_soup(url)

        # 都道府県ページを列挙
        pref_urls = []
        for a in root.select("a[href]"):
            href = a.get("href", "")
            if _PREF_LINK.match(href):
                full = urljoin(url, href)
                if full not in pref_urls:
                    pref_urls.append(full)

        for pref_url in pref_urls:
            self.logger.info(f"都道府県ページ取得: {pref_url}")
            try:
                pref_soup = self.get_soup(pref_url)
            except Exception as e:
                self.logger.warning(f"都道府県ページのスキップ: {pref_url} ({e})")
                continue

            # 市区町村ページを列挙
            city_urls = []
            for a in pref_soup.select("a[href]"):
                href = a.get("href", "")
                if _CITY_LINK.match(href):
                    full = urljoin(pref_url, href)
                    if full not in city_urls:
                        city_urls.append(full)

            for city_url in city_urls:
                yield from self._scrape_city(city_url)
                time.sleep(self.DELAY)

    def _scrape_city(self, city_url: str) -> Generator[dict, None, None]:
        """市区町村ページをページ送りしながら店舗詳細を取得して即 yield する"""
        page = 1
        while True:
            page_url = city_url if page == 1 else urljoin(city_url, f"{page}/")
            self.logger.info(f"市区町村ページ取得: {page_url}")
            try:
                soup = self.get_soup(page_url)
            except Exception as e:
                self.logger.warning(f"市区町村ページのスキップ: {page_url} ({e})")
                return

            # この市区町村の店舗詳細リンク（正規形のみ・出現順で重複排除）
            shop_urls = []
            for a in soup.select('a[href*="shopinfo_"]'):
                href = a.get("href", "")
                if _SHOP_LINK.match(href):
                    full = urljoin(page_url, href)
                    if full not in shop_urls:
                        shop_urls.append(full)

            if not shop_urls:
                return

            for shop_url in shop_urls:
                time.sleep(self.DELAY)
                item = self._scrape_detail(shop_url)
                if item:
                    yield item

            # 次ページの有無を判定（/{page+1}/ へのリンクがあるか）
            has_next = any(
                re.search(rf"_ct/{page + 1}/$", a.get("href", ""))
                for a in soup.select("a[href]")
            )
            if not has_next:
                return
            page += 1
            time.sleep(self.DELAY)

    def _scrape_detail(self, url: str) -> dict | None:
        try:
            soup = self.get_soup(url)

            data = {Schema.URL: url}

            # 店舗名（H1 から「の詳細情報」を除去）
            h1 = soup.select_one("h1")
            if h1:
                name = h1.get_text(strip=True)
                name = re.sub(r"の詳細情報$", "", name)
                data[Schema.NAME] = name

            # th/td テーブルから基本情報を取得
            for th in soup.select("th.is-width-150px"):
                key = th.get_text(strip=True)
                td = th.find_next_sibling("td")
                if not td:
                    continue
                val = " ".join(td.get_text(" ", strip=True).split())

                if key == "住所":
                    m = _PREF_PATTERN.match(val)
                    if m:
                        data[Schema.PREF] = m.group(1)
                        data[Schema.ADDR] = val[m.end():].strip()
                    else:
                        data[Schema.ADDR] = val
                elif key == "電話番号":
                    data[Schema.TEL] = val
                elif key == "営業時間":
                    data[Schema.TIME] = val
                elif key == "定休日":
                    data[Schema.HOLIDAY] = val
                elif key == "アクセス":
                    data["アクセス"] = val

            # 名称が取れなければ無効レコード扱い
            if not data.get(Schema.NAME):
                return None

            return data

        except Exception as e:
            self.logger.warning(f"詳細ページのスキップ: {url} ({e})")
            return None


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = MyhomeScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://myhome.nifty.com/shop/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
