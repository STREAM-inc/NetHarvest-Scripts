"""
DOMO (Domo, Inc. Japan) — 顧客導入事例スクレイパー

取得対象:
    - Domoを導入している企業の成功事例 (/jp/customers 配下)
    - 企業名・HP・業種・部署・タグ・事例タイトル等を取得

取得フロー:
    1. /jp/customers 一覧ページから Webflow ページネーション (?8079afd6_page=N) で巡回
    2. /jp/customers/{slug} 詳細ページへ遷移して企業情報を取得

実行方法:
    python scripts/sites/corporate/domo.py
    python bin/run_flow.py --site-id domo
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

BASE_URL = "https://www.domo.com"
LIST_URL = "https://www.domo.com/jp/customers"
MAX_PAGES = 50


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class DomoScraper(StaticCrawler):
    """Domo顧客導入事例スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["事例タイトル", "企業説明", "部署", "タグ"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        detail_urls: list[str] = []

        for page in range(1, MAX_PAGES + 1):
            page_url = LIST_URL if page == 1 else f"{LIST_URL}?8079afd6_page={page}"
            urls = self._collect_detail_urls(page_url)
            new_urls = [u for u in urls if u not in seen]
            if not new_urls:
                self.logger.info("ページ %d で新規URLなし。巡回終了。", page)
                break
            for u in new_urls:
                seen.add(u)
                detail_urls.append(u)

        self.total_items = len(detail_urls)
        self.logger.info("取得対象件数: %d", self.total_items)

        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
                if item and item.get(Schema.NAME):
                    yield item
            except Exception as e:
                self.logger.warning("詳細ページ取得失敗: %s — %s", detail_url, e)
                continue

    def _collect_detail_urls(self, list_url: str) -> list[str]:
        soup = self.get_soup(list_url)
        if soup is None:
            return []
        urls: list[str] = []
        for a in soup.select('a[href*="/jp/customers/"]'):
            href = a.get("href", "")
            if not href:
                continue
            full = urljoin(BASE_URL, href)
            # /jp/customers トップと URL クエリ付きを除外
            if re.search(r"/jp/customers/?(\?.*)?$", full):
                continue
            # クエリを除去
            full = full.split("?")[0].rstrip("/")
            if full not in urls:
                urls.append(full)
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}

        # 企業名: ページタイトル "パナソニック株式会社 成功事例 - Domo | ドーモ株式会社"
        title_tag = soup.find("title")
        if title_tag:
            raw_title = _clean(title_tag.get_text())
            name = re.split(r"\s*[ 　]*(?:成功事例|Domo導入事例|導入事例)\s*", raw_title, maxsplit=1)[0]
            name = name.split(" - ")[0].strip()
            if name:
                data[Schema.NAME] = name

        # 事例タイトル (h1)
        h1 = soup.find("h1")
        if h1:
            data["事例タイトル"] = _clean(h1.get_text())

        # customer-info-block 群から企業説明・業種・部署・HPを抽出
        info_blocks = soup.select(".customer-info-block")
        for block in info_blocks:
            cls = " ".join(block.get("class", []))
            # 不可視ブロックはスキップ
            if "w-condition-invisible" in cls:
                continue

            text = _clean(block.get_text(" "))
            if not text or text == "お客様事例をダウンロード":
                continue

            # 業種 (customer-info-block w-dyn-items だが department-wrapper は除く)
            if "w-dyn-items" in cls and "department-wrapper" not in cls:
                if not data.get(Schema.CAT_LV1):
                    data[Schema.CAT_LV1] = text
                continue

            # 部署
            if "department-wrapper" in cls:
                data["部署"] = text
                continue

            # 企業説明 + HP (最初の info-block)
            if "企業説明" not in data:
                # HP リンクを抽出
                hp_link = block.find("a", href=lambda h: h and h.startswith("http"))
                if hp_link and not data.get(Schema.HP):
                    data[Schema.HP] = hp_link.get("href", "").strip()
                data["企業説明"] = text

        # タグ (customer-tag-wrapper .rc-card-tag)
        tags = [_clean(t.get_text()) for t in soup.select(".customer-tag-wrapper .rc-card-tag")]
        tags = [t for t in tags if t]
        if tags:
            data["タグ"] = " / ".join(tags)

        if not data.get(Schema.NAME):
            return None

        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = DomoScraper()
    scraper.execute(LIST_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
