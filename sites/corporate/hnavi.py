"""
発注ナビ — システム/アプリ開発会社マッチングサイト（hnavi.co.jp）

取得対象:
    - 登録開発会社の企業情報

取得フロー:
    1. /corporation/?page=N を巡回し詳細URLを収集
    2. 各詳細ページから企業情報を抽出

実行方法:
    python scripts/sites/corporate/hnavi.py
    python bin/run_flow.py --site-id hnavi
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://hnavi.co.jp"
LIST_URL = "https://hnavi.co.jp/corporation/"

_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|"
    r"静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|"
    r"奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|"
    r"熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("\u3000", " ")).strip()


class HnaviScraper(StaticCrawler):
    """発注ナビ 開発会社スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS: list[str] = []

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls = self._collect_detail_urls()
        self.total_items = len(detail_urls)
        self.logger.info("詳細URL収集完了: %d 件", len(detail_urls))

        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
            except Exception as e:
                self.logger.warning("詳細取得失敗 %s: %s", detail_url, e)
                continue
            if item and item.get(Schema.NAME):
                yield item

    def _collect_detail_urls(self) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        page = 1

        while True:
            page_url = LIST_URL if page == 1 else f"{LIST_URL}?page={page}"
            try:
                soup = self.get_soup(page_url)
            except Exception as e:
                self.logger.warning("一覧取得失敗 page=%d: %s", page, e)
                break

            anchors = soup.select('a[href^="/corporation/"]')
            page_hits = 0
            for a in anchors:
                href = a.get("href", "")
                if not re.match(r"^/corporation/\d+/?$", href):
                    continue
                full = urljoin(BASE_URL, href)
                if full in seen:
                    continue
                seen.add(full)
                urls.append(full)
                page_hits += 1

            if page_hits == 0:
                break

            # 最終ページ判定（ページネーション内の最大ページ番号）
            if page == 1:
                max_page = page
                for pa in soup.select("ul.app-pagination a[href*='page=']"):
                    m = re.search(r"page=(\d+)", pa.get("href", ""))
                    if m:
                        max_page = max(max_page, int(m.group(1)))
                self._max_page = max_page
                self.logger.info("総ページ数: %d", max_page)

            if page >= getattr(self, "_max_page", 1):
                break
            page += 1

        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)

        name_el = soup.select_one(".header-title")
        if not name_el:
            return None
        name = _clean(name_el.get_text())

        item: dict = {
            Schema.URL: url,
            Schema.NAME: name,
        }

        for block in soup.select(".d-flex.managed"):
            title_el = block.select_one(".managed-title")
            body_el = block.select_one(".managed-body")
            if not title_el or not body_el:
                continue
            title = _clean(title_el.get_text())
            body = _clean(body_el.get_text())

            if title == "設立":
                item[Schema.OPEN_DATE] = body
            elif title == "資本金":
                item[Schema.CAP] = body
            elif title == "従業員数":
                item[Schema.EMP_NUM] = body
            elif title == "代表者":
                item[Schema.REP_NM] = body
            elif title == "所在地":
                # 複数拠点ある場合は最初の拠点をメインに、全体も保持
                first = body_el.select_one("div")
                first_text = _clean(first.get_text()) if first else body
                m = _PREF_RE.match(first_text)
                if m:
                    item[Schema.PREF] = m.group(1)
                    item[Schema.ADDR] = _clean(first_text[m.end():])
                else:
                    item[Schema.ADDR] = first_text
            elif title == "ホームページ":
                item[Schema.HP] = body

        return item


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = HnaviScraper()
    scraper.execute(LIST_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
