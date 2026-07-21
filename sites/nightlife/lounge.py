"""
ナイエコ出張所 (lounge.cxc-kumamoto.com) — 九州地方のラウンジ/ナイトクラブ紹介ウェブログ

取得対象:
    - 各店舗紹介記事に掲載された店舗の基本情報 (店名/所在地/電話/営業時間/店休日/HP 等)

取得フロー:
    1. post-sitemap.xml から全記事 URL を列挙 (ルート URL から派生)
    2. 各記事を取得し、記事内の「店舗情報テーブル」(所在地/住所 行を持つ table) を検出
       - 単独店舗記事 (見出し「基本情報」) は H1 を店名とする
       - 「○○市繁華街のナイトスポットまとめ」型記事は table 直前の見出しを店名とする
         (1 記事に複数店舗テーブルが並ぶ)
    3. 店舗テーブル 1 件ごとに即 yield (Pattern B)

除外フィールド (著作権リスク: 運営者による自由記述プロースのため):
    - 料金システム / 特徴 / コメント

実行方法:
    python scripts/sites/nightlife/lounge.py
    docker compose exec worker python /app/bin/run_flow.py --site-id lounge
"""

import re
import sys
import urllib.parse
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# カテゴリスラッグ → 都道府県 (記事は都道府県別カテゴリに属する)
_PREF_MAP = {
    "kumamoto": "熊本県",
    "kagoshima": "鹿児島県",
    "saga": "佐賀県",
    "oita": "大分県",
    "nagasaki": "長崎県",
    "miyazaki": "宮崎県",
    "fukuoka": "福岡県",
}

# 店名見出しとして採用しない汎用ラベル (単独店舗記事はこれ→H1 を店名とする)
_GENERIC_HEADINGS = {
    "基本情報", "店舗情報", "店舗概要", "店舗データ", "概要", "詳細", "データ", "DATA", "INFO",
}

# 住所 (店舗テーブル判定にも使う)
_ADDR_LABELS = {"所在地", "住所"}


class Lounge(StaticCrawler):
    """ナイエコ出張所 スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["地図URL"]

    def parse(self, url: str):
        # ルート URL から sitemap を派生 (URL 一貫性: 引数 url が唯一のルート)
        sitemap_url = urllib.parse.urljoin(url, "post-sitemap.xml")
        sm = self.get_soup(sitemap_url)
        if sm is None:
            return

        post_urls = [loc.get_text(strip=True) for loc in sm.find_all("loc")]
        post_urls = [u for u in post_urls if u]
        self.total_items = len(post_urls)

        for post_url in post_urls:
            try:
                yield from self._scrape_post(post_url)
            except Exception as e:  # 個別記事のエラーはログして継続
                self.logger.warning("記事解析エラー (スキップ): %s — %s", post_url, e)
                continue

    def _scrape_post(self, post_url: str):
        soup = self.get_soup(post_url)
        if soup is None:
            return

        h1 = soup.find("h1")
        page_name = h1.get_text(strip=True) if h1 else ""

        # 都道府県: 記事のカテゴリリンクのスラッグから判定
        pref = ""
        for a in soup.select('a[href*="/category/"]'):
            m = re.search(r"/category/([^/]+)/?", a.get("href", ""))
            if m and m.group(1) in _PREF_MAP:
                pref = _PREF_MAP[m.group(1)]
                break

        # 記事本文内の店舗テーブル (所在地/住所 行を持つ table) を収集
        container = soup.find("article") or soup
        venue_tables = []
        for table in container.find_all("table"):
            rows = self._table_rows(table)
            if _ADDR_LABELS & set(rows.keys()):
                venue_tables.append((table, rows))

        # 1 記事 1 店舗 (単独店舗記事) は H1 を店名とする。
        # 複数店舗が並ぶ「まとめ」記事は table 直前の見出しを各店名とする。
        single = len(venue_tables) == 1
        for table, rows in venue_tables:
            if single:
                name = page_name
            else:
                heading = table.find_previous(["h2", "h3", "h4"])
                name = heading.get_text(strip=True) if heading else ""
                if not name or name in _GENERIC_HEADINGS:
                    name = page_name
            if not name:
                continue

            item = self._build_item(name, pref, post_url, table, rows)
            if item:
                yield item

    @staticmethod
    def _table_rows(table):
        """table の各行を {ラベル: 値セル(bs4 Tag)} に変換して返す。"""
        rows = {}
        for tr in table.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            if len(cells) < 2:
                continue
            label = cells[0].get_text(strip=True)
            if label and label not in rows:
                rows[label] = cells[1]
        return rows

    def _build_item(self, name, pref, post_url, table, rows):
        def txt(*labels):
            for lb in labels:
                if lb in rows:
                    return rows[lb].get_text(" ", strip=True)
            return ""

        def link(*labels):
            for lb in labels:
                if lb in rows:
                    a = rows[lb].find("a", href=True)
                    if a:
                        return a["href"].strip()
            return ""

        hp_cell = rows.get("ホームページ") or rows.get("Webサイト") or rows.get("HP")
        hp = ""
        if hp_cell is not None:
            a = hp_cell.find("a", href=True)
            if a:
                hp = a["href"].strip()

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: pref + txt("所在地", "住所"),
            Schema.TEL: txt("電話番号"),
            Schema.TIME: txt("営業時間"),
            Schema.HOLIDAY: txt("店休日"),
            Schema.HP: hp,
            Schema.URL: post_url,
            "地図URL": link("地図"),
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = Lounge()
    # 🔒 sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://lounge.cxc-kumamoto.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
