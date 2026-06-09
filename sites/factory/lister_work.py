"""
Lister製造業 (lister.work) — 工業団地一覧スクレイパー

取得対象:
    - 全国の工業団地 (工業団地名・都道府県・緯度・経度)
    - 合計約 1,466 件

取得フロー:
    1. /all_districts/0/ から全工業団地の (ID・都道府県・名称) を収集
    2. 各 /?d={id} ページから緯度・経度を抽出

実行方法:
    python scripts/sites/factory/lister_work.py
    python bin/run_flow.py --site-id lister_work
"""

import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


BASE_URL = "https://www.lister.work"
ALL_DISTRICTS_URL = f"{BASE_URL}/all_districts/0/"


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


class ListerWorkScraper(StaticCrawler):
    """Lister製造業 (lister.work) 工業団地一覧スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "緯度",
        "経度",
    ]

    def parse(self, url: str):
        # Step 1: 全工業団地リストを取得
        soup = self.get_soup(ALL_DISTRICTS_URL)
        if soup is None:
            return

        parks: list[tuple[int, str, str]] = []
        for tr in soup.select("table tbody tr"):
            cells = tr.select("td")
            if len(cells) < 3:
                continue
            a = cells[2].find("a", href=True)
            if not a:
                continue
            href = a.get("href", "")
            m = re.search(r"\?d=(\d+)", href)
            if not m:
                continue
            park_id = int(m.group(1))
            pref = _clean(cells[1].get_text())
            name = _clean(a.get_text())
            if name:
                parks.append((park_id, pref, name))

        self.total_items = len(parks)
        self.logger.info("工業団地一覧: %d 件収集", len(parks))

        # Step 2: 各パークページから緯度・経度を取得
        for park_id, pref, name in parks:
            detail_url = f"{BASE_URL}/?d={park_id}"
            try:
                item = self._scrape_detail(detail_url, park_id, pref, name)
                if item:
                    yield item
            except Exception:
                self.logger.exception("詳細取得失敗: %s", detail_url)
                continue

    def _scrape_detail(self, url: str, park_id: int, pref: str, name: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        lat_el = soup.find("input", {"id": "input-latitude"})
        lng_el = soup.find("input", {"id": "input-longitude"})
        lat = lat_el.get("value", "") if lat_el else ""
        lng = lng_el.get("value", "") if lng_el else ""

        return {
            Schema.URL:  url,
            Schema.NAME: name,
            Schema.PREF: pref,
            "緯度":       lat,
            "経度":       lng,
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = ListerWorkScraper()
    scraper.execute(BASE_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
