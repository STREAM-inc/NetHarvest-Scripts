"""
ノックスグループ — 岩手県盛岡市のナイトグループ店舗一覧

取得対象:
    - グループ傘下の各店舗 (4店舗)

取得フロー:
    1. グループTOPページ (https://noxgroup.jp/) から店舗リストを取得
    2. 各店舗の公式サイトへ遷移して詳細情報を取得

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/noxgroup.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id noxgroup
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
    r"^(北海道|東京都|大阪府|京都府|"
    r"(?:神奈川|埼玉|千葉|愛知|福岡|北海道|岩手|宮城|秋田|山形|福島|"
    r"茨城|栃木|群馬|新潟|富山|石川|福井|山梨|長野|静岡|三重|滋賀|"
    r"兵庫|奈良|和歌山|鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|"
    r"佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県)"
)

_GENRE_PATTERN = re.compile(r"盛岡([\w・ー/（）]+?)(?:「|$)")


class NoxgroupScraper(StaticCrawler):
    """ノックスグループ スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = []

    def parse(self, url: str):
        soup = self.get_soup(url)
        items = soup.select(".shop-list ul li")
        self.total_items = len(items)

        for item in items:
            a = item.select_one("a[href]")
            if not a:
                continue
            shop_url = a["href"]
            try:
                record = self._scrape_detail(shop_url)
                if record:
                    yield record
            except Exception as e:
                self.logger.warning("detail error %s: %s", shop_url, e)

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)

        # 店舗名: タイトルの「」内から抽出
        title = soup.title.get_text(strip=True) if soup.title else ""
        name_m = re.search(r"「([^」]+)」", title)
        name = name_m.group(1) if name_m else title.split("|")[0].strip()

        # ジャンル (カテゴリ): タイトルから抽出
        genre_m = _GENRE_PATTERN.search(title)
        genre = genre_m.group(1) if genre_m else ""

        # TEL
        tel_a = soup.select_one("a[href^='tel:']")
        tel = tel_a["href"].replace("tel:", "") if tel_a else ""

        # 住所
        addr_el = soup.select_one("address.footer__location")
        full_addr = addr_el.get_text(strip=True) if addr_el else ""
        pref = ""
        addr = full_addr
        pref_m = _PREF_PATTERN.match(full_addr)
        if pref_m:
            pref = pref_m.group(1)
            addr = full_addr[pref_m.end():].strip()

        # 営業時間
        open_dl = soup.select_one("dl.footer__business-hours-open-time")
        time_open = ""
        if open_dl:
            dd = open_dl.select_one("dd")
            time_open = dd.get_text(strip=True) if dd else ""

        # 定休日
        close_dl = soup.select_one("dl.footer__business-hours-store-holiday")
        holiday = ""
        if close_dl:
            dd = close_dl.select_one("dd")
            holiday = dd.get_text(strip=True) if dd else ""

        # SNS
        insta_a = soup.select_one("a[href*='instagram.com']")
        insta = insta_a["href"] if insta_a else ""

        x_a = soup.select_one("a[href*='x.com'], a[href*='twitter.com']")
        x_url = x_a["href"] if x_a else ""

        tiktok_a = soup.select_one("a[href*='tiktok.com']")
        tiktok = tiktok_a["href"] if tiktok_a else ""

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: url,
            Schema.URL: url,
            Schema.TIME: time_open,
            Schema.HOLIDAY: holiday,
            Schema.INSTA: insta,
            Schema.X: x_url,
            Schema.TIKTOK: tiktok,
            Schema.CAT_SITE: genre,
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = NoxgroupScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://noxgroup.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
