"""
QLIFE (株式会社QLife / www.qlife.co.jp) — 企業ニュースリリース一覧クローラー

取得対象:
    - 年度別アーカイブ (/date/{YYYY}) にある全ニュースリリース
      公開年 2007〜現在年。総件数は約 280 件前後。

取得フロー:
    1. /date/{year} をクロール (year = 現在年 〜 2007)。
       各年内で /date/{year}/{page} (1 ページ 20 件) を 0 件になるまで巡回。
    2. ul.release_box > li の a[href] を詳細 URL として収集。
    3. 各詳細ページ (/news/{id}.html) から h2.title と p.data を抽出。

実行方法:
    # ローカルテスト
    python scripts/sites/service/qlife.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id qlife
"""

import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://www.qlife.co.jp"
START_YEAR = 2007
MAX_PAGES_PER_YEAR = 20  # 安全上限 (1 ページ 20 件、通常 1〜2 ページで完結)


_DATE_RE = re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


def _normalize_date(raw: str) -> str:
    """'2025年12月18日 [木]' -> '2025-12-18' / 解析できなければ原文を返す"""
    if not raw:
        return ""
    m = _DATE_RE.search(raw)
    if not m:
        return _clean(raw)
    y, mo, d = m.groups()
    return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"


class QlifeScraper(StaticCrawler):
    """QLife 企業ニュースリリーススクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["公開日"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        current_year = datetime.now().year
        seen: set[str] = set()
        detail_urls: list[str] = []

        for year in range(current_year, START_YEAR - 1, -1):
            for page in range(1, MAX_PAGES_PER_YEAR + 1):
                list_url = (
                    f"{BASE_URL}/date/{year}"
                    if page == 1
                    else f"{BASE_URL}/date/{year}/{page}"
                )
                try:
                    soup = self.get_soup(list_url)
                except Exception as e:
                    self.logger.warning("一覧取得失敗: %s — %s", list_url, e)
                    break
                if soup is None:
                    break

                release_box = soup.select_one("ul.release_box")
                if release_box is None:
                    break

                anchors = release_box.select("li a[href]")
                if not anchors:
                    break

                new_count = 0
                for a in anchors:
                    href = a.get("href", "").strip()
                    if not href:
                        continue
                    if href.startswith("/"):
                        href = BASE_URL + href
                    if href in seen:
                        continue
                    seen.add(href)
                    detail_urls.append(href)
                    new_count += 1

                if new_count == 0:
                    break

                time.sleep(self.DELAY)

        self.total_items = len(detail_urls)
        self.logger.info("収集済み詳細URL: %d 件", self.total_items)

        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning(
                    "詳細取得失敗 (スキップ): %s — %s", detail_url, e
                )
                continue
            time.sleep(self.DELAY)

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        title_el = soup.select_one("div.title_box h2.title") or soup.select_one(
            "h2.title"
        )
        title = _clean(title_el.get_text()) if title_el else ""

        date_el = soup.select_one("div.title_box p.data") or soup.select_one(
            "p.data"
        )
        date_raw = _clean(date_el.get_text()) if date_el else ""

        if not title:
            return None

        return {
            Schema.NAME: title,
            Schema.URL: url,
            "公開日": _normalize_date(date_raw),
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = QlifeScraper()
    scraper.execute(f"{BASE_URL}/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
