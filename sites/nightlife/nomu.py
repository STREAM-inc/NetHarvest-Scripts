"""
ノムノム — バー検索サイト クローラー

取得対象:
    - ノムノム掲載のバー・カラオケバー等の店舗情報 (カテゴリ /c8)
    - 店名 / 住所 / 都道府県 / TEL / 営業時間 / 定休日 /
      SNS(Instagram/X) / 支払方法 / ジャンル / エリア / 料金目安 / アクセス / 座席 / タバコ

取得フロー:
    1. /c8 (インデックス) で総掲載数を取得
    2. /c8/search?page=N で一覧を巡回 (15件/ページ、計10ページ前後)
    3. 各店舗の /shop/{id} 詳細ページで全フィールドを抽出 → 即 yield

実行方法:
    python scripts/sites/nightlife/nomu.py
    docker compose exec worker python /app/bin/run_flow.py --site-id nomu
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_ITEMS_PER_PAGE = 15


class NomuScraper(StaticCrawler):
    """ノムノム バー検索スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["エリア", "料金目安", "アクセス", "座席", "タバコ"]

    def parse(self, url: str):
        # 総掲載数をインデックスページから取得
        index_soup = self.get_soup(url)
        if index_soup:
            m = re.search(r"総掲載数[：:]\s*(\d+)件", index_soup.get_text())
            if m:
                self.total_items = int(m.group(1))

        search_url = f"{url}/search"
        page = 1
        while True:
            page_url = f"{search_url}?page={page}" if page > 1 else search_url
            soup = self.get_soup(page_url)
            if soup is None:
                break
            items = soup.select("div.store-list")
            if not items:
                break

            for item in items:
                link = item.select_one("a[href]")
                if not link:
                    continue
                href = link.get("href", "")
                if "/shop/" not in href:
                    continue
                detail_url = urljoin(url, href)

                # ジャンルは一覧の table.new-table から取得
                genre = ""
                new_table = item.select_one("table.new-table")
                if new_table:
                    for row in new_table.select("tr"):
                        th = row.select_one("th")
                        td = row.select_one("td")
                        if th and td and th.get_text(strip=True) == "ジャンル":
                            genre = td.get_text(strip=True)
                            break

                record = self._scrape_detail(detail_url)
                if record:
                    if genre:
                        record[Schema.CAT_SITE] = genre
                    yield record

            # 取得件数が1ページ未満なら最終ページ
            if len(items) < _ITEMS_PER_PAGE:
                break
            page += 1

    def _scrape_detail(self, url: str) -> dict | None:
        try:
            soup = self.get_soup(url)
            if soup is None:
                return None
            table = soup.select_one("table.store")
            if not table:
                return None

            record: dict = {Schema.URL: url}
            for row in table.select("tr"):
                th = row.select_one("th")
                td = row.select_one("td")
                if not th or not td:
                    continue
                header = th.get_text(strip=True)
                value = td.get_text(strip=True)

                if header == "店名":
                    record[Schema.NAME] = value
                elif header == "住所":
                    a = td.select_one("a")
                    full_addr = a.get_text(strip=True) if a else value
                    m = _PREF_RE.match(full_addr)
                    if m:
                        record[Schema.PREF] = m.group(1)
                        record[Schema.ADDR] = full_addr[m.end():].strip()
                    else:
                        record[Schema.ADDR] = full_addr
                elif header == "TEL":
                    tel_link = td.select_one("a[href^='tel:']")
                    if tel_link:
                        record[Schema.TEL] = tel_link["href"][4:]
                elif header == "営業時間":
                    record[Schema.TIME] = value
                elif header == "定休日":
                    record[Schema.HOLIDAY] = value
                elif header == "SNS":
                    insta = td.select_one("a[href*='instagram.com']")
                    if insta:
                        record[Schema.INSTA] = insta.get("href", "")
                    x_link = td.select_one("a[href*='x.com']")
                    if x_link:
                        record[Schema.X] = x_link.get("href", "")
                elif header == "利用可能なお支払い方法":
                    record[Schema.PAY] = re.sub(r"[\s・]+", "／", value).strip("／")
                elif header == "エリア":
                    record["エリア"] = value
                elif header == "料金目安":
                    a = td.select_one("a")
                    record["料金目安"] = a.get_text(strip=True) if a else value
                elif header == "アクセス":
                    record["アクセス"] = value
                elif header == "座席":
                    record["座席"] = value
                elif header == "タバコ":
                    record["タバコ"] = value

            return record if Schema.NAME in record else None
        except Exception as e:
            self.logger.warning("詳細取得失敗: %s — %s", url, e)
            return None


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = NomuScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://nomu.tsuku2.jp/c8")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
