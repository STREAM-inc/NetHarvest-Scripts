"""
ホスタグラム — ホストクラブ店舗情報サイト（群馬・栃木・新潟）

取得対象:
    - 全エリア（群馬/栃木/新潟）の掲載店舗（11件）

取得フロー:
    1. トップページからエリア名一覧を取得
    2. /area/{area}/shops から店舗IDを収集
    3. /shop/{shop_id} の詳細ページを個別取得

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/hostgram.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id hostgram
"""

import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_BASE = "https://hostgram.jp"

_PREF_RE = re.compile(
    r"^(北海道|東京都|大阪府|京都府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|"
    r"新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|"
    r"和歌山|鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|"
    r"熊本|大分|宮崎|鹿児島|沖縄)県)"
)


class HostgramCrawler(StaticCrawler):
    """ホスタグラム スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["エリア"]

    def parse(self, url: str):
        # Step 1: トップページからエリア一覧を取得
        top_soup = self.get_soup(url)
        areas = []
        for a in top_soup.select("a[href]"):
            href = a["href"]
            m = re.match(r"^/area/([^/]+)$", href)
            if m and m.group(1) not in areas:
                areas.append(m.group(1))

        # Step 2: 各エリアの店舗ID収集
        shop_ids = []
        for area in areas:
            list_soup = self.get_soup(f"{_BASE}/area/{area}/shops")
            for a in list_soup.select("a[href]"):
                href = a["href"]
                m2 = re.match(r"^/shop/([^/]+)$", href)
                if m2 and m2.group(1) not in shop_ids:
                    shop_ids.append(m2.group(1))

        self.total_items = len(shop_ids)

        # Step 3: 各店舗詳細ページを取得
        for shop_id in shop_ids:
            detail_url = f"{_BASE}/shop/{shop_id}"
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.error("Failed %s: %s", detail_url, e)
                continue

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)

        # 店舗名
        h1 = soup.find("h1", class_="h1_title")
        name = h1.get_text(strip=True) if h1 else ""
        if not name:
            return None

        # 店舗情報テーブル（section.table_layout 内の th/td）
        info = {}
        for section in soup.find_all("section", class_="table_layout"):
            for tr in section.find_all("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if th and td:
                    info[th.get_text(strip=True)] = td.get_text(strip=True)

        addr_raw = info.get("住所", "")
        pref = ""
        addr = addr_raw
        m = _PREF_RE.match(addr_raw)
        if m:
            pref = m.group(1)
            addr = addr_raw[m.end():].strip()

        # 店舗SNS（h1親セクション内の a.Instagram / a.Twitter / a.TikTok）
        insta = twitter = tiktok = ""
        if h1:
            section = h1.find_parent("section") or h1.find_parent("div")
            if section:
                for a in section.find_all("a", href=True):
                    if "hostgram_official" in a["href"]:
                        continue
                    a_cls = a.get("class", [])
                    if "Instagram" in a_cls and not insta:
                        insta = a["href"]
                    elif "Twitter" in a_cls and not twitter:
                        twitter = a["href"]
                    elif "TikTok" in a_cls and not tiktok:
                        tiktok = a["href"]

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: info.get("電話番号", ""),
            Schema.TIME: info.get("営業時間", ""),
            Schema.HOLIDAY: info.get("店休日", info.get("定休日", "")),
            Schema.CAT_SITE: info.get("業種", ""),
            Schema.INSTA: insta,
            Schema.X: twitter,
            Schema.TIKTOK: tiktok,
            Schema.URL: url,
            "エリア": info.get("エリア", ""),
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = HostgramCrawler()
    scraper.execute("https://hostgram.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
