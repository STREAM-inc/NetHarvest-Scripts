"""
全日本トラック協会 加盟事業者リンク集 — トラック運送事業者の名称・都道府県・HP取得

取得対象:
    - 全日本トラック協会に加盟する事業者の名称・都道府県・HP URL（約266件）

取得フロー:
    1. 単一ページを取得
    2. <h3> 見出しから都道府県名を取得
    3. 直後の <ul><li><a> から事業者名と HP URL を取得

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/jta_jigyou.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id jta_jigyou
"""

import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class JtaJigyouCrawler(StaticCrawler):
    """全日本トラック協会 加盟事業者リンク集 スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = []

    def parse(self, url: str):
        soup = self.get_soup(url)

        items = []
        for section in soup.select("div.jigyo-list"):
            pref_el = section.select_one("p.member-name")
            if not pref_el:
                continue
            pref = pref_el.get_text(strip=True)
            for a in section.select("ul li a[href]"):
                items.append((pref, a.get_text(strip=True), a["href"]))

        self.total_items = len(items)

        for pref, name, hp in items:
            yield {
                Schema.NAME: name,
                Schema.PREF: pref,
                Schema.HP: hp,
                Schema.URL: url,
            }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = JtaJigyouCrawler()
    scraper.execute("https://jta.or.jp/association/link/jigyou.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
