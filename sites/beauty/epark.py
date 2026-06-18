"""
EPARKリラク＆エステ (mitsuraku.jp) — リラクゼーション・マッサージ・エステサロン検索ポータル

取得対象:
    - 全国のリラク・エステサロン基本情報 (名称・住所・電話番号・営業時間・定休日・支払い方法・HP・メニューなど)

取得フロー:
    1. ルートページ (https://mitsuraku.jp/) から都道府県リンクを抽出
    2. 各都道府県ページを ?page=N でページネーション
    3. 各サロンの詳細ページから構造化データを取得・即yield

実行方法:
    # ローカルテスト
    python scripts/sites/beauty/epark.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id epark
"""

import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# スラグが都道府県ページのものかどうかの判定用除外リスト
_NON_PREF_SLUGS = {
    "area", "railway", "genre", "inquiry", "esthe", "fitness", "search",
    "salon", "sitemap", "column", "salonRequest", "news", "login", "mypage",
    "corporate", "special", "massage", "term", "publish", "guide", "agreement",
    "faq", "policy", "company", "images", "css", "js", "ajax", "reserve",
    "coupon", "review", "access", "blog", "menu", "photo",
}


class EparkScraper(StaticCrawler):
    """EPARKリラク＆エステ スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["メニュー"]

    def parse(self, url: str):
        root_soup = self.get_soup(url)

        # 都道府県リンクをルートページから動的に抽出
        pref_urls = []
        seen = set()
        for a in root_soup.select("a[href]"):
            href = a["href"]
            m = re.match(r"https://mitsuraku\.jp/([a-z]+)/?$", href)
            if m and m.group(1) not in _NON_PREF_SLUGS and href not in seen:
                pref_urls.append(href.rstrip("/") + "/")
                seen.add(href)

        for pref_url in pref_urls:
            page = 1
            while True:
                page_url = f"{pref_url}?page={page}" if page > 1 else pref_url
                soup = self.get_soup(page_url)
                panels = soup.select("div.panel.result-panel.js-salon-panel")
                if not panels:
                    break

                for panel in panels:
                    try:
                        name_a = panel.select_one("h2.search_shopname a.js-salon-link")
                        if not name_a:
                            continue
                        detail_url = name_a.get("href", "")
                        if not detail_url:
                            continue

                        # 一覧ページから取れる情報
                        name_el = name_a.select_one("span[itemprop='name']")
                        name = name_el.get_text(strip=True) if name_el else ""

                        kana_el = panel.select_one("small[itemprop='alternateName']")
                        kana = kana_el.get_text(strip=True) if kana_el else ""

                        cat_els = panel.select("span.list-category")
                        cat_site = " / ".join(el.get_text(strip=True) for el in cat_els)

                        # 詳細ページを取得して即yield
                        item = self._scrape_detail(detail_url)
                        if item is None:
                            item = {}

                        item[Schema.NAME] = name or item.get(Schema.NAME, "")
                        item[Schema.NAME_KANA] = kana
                        item[Schema.CAT_SITE] = cat_site
                        item[Schema.URL] = detail_url

                        yield item

                    except Exception as e:
                        self.logger.warning(f"パネル取得エラー: {e}")
                        continue

                page += 1

    def _scrape_detail(self, url: str) -> dict | None:
        try:
            soup = self.get_soup(url)

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

            # PREF を住所から確認・補完
            if not pref and addr:
                m = _PREF_PATTERN.match(addr)
                if m:
                    pref = m.group(1)
                    addr = addr[m.end():].strip()
            elif pref and addr.startswith(pref):
                addr = addr[len(pref):].strip()

            # 営業時間・定休日・支払い方法・HP (panel-list)
            # HTML の <li> は閉じタグ省略のためネスト構造が混在する。
            # col-xs-4/col-sm-3 がヘッダー、col-xs-8/col-sm-9 が値を保持する。
            hours = ""
            holiday = ""
            pay = ""
            hp = ""
            for ul in soup.select("ul.row.panel-list"):
                header_el = ul.select_one("li.col-xs-4, li.col-sm-3")
                value_el = ul.select_one("li.col-xs-8, li.col-sm-9")
                if not header_el or not value_el:
                    continue
                # ヘッダー直接テキスト (子要素のテキストを除く)
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

            # メニュー: 詳細ページのメニューセクションからコース名と料金を取得
            menu_items = []
            for row in soup.select("div.menu-list__item, li.menu-list__item, tr.js-menu-row"):
                name_el = row.select_one(".menu-list__name, .menu__name, td.menu-name")
                price_el = row.select_one(".menu-list__price, .menu__price, td.menu-price")
                item_name = name_el.get_text(strip=True) if name_el else ""
                item_price = price_el.get_text(strip=True) if price_el else ""
                if item_name:
                    menu_items.append(f"{item_name} {item_price}".strip())
            # フォールバック: 汎用メニューテーブル
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
            self.logger.warning(f"詳細取得エラー ({url}): {e}")
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
