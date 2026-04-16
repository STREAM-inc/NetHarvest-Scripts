# scripts/sites/agency_franchise/bahn_rep.py
"""
バーン代理店 (bahn-rep.com) — 代理店・フランチャイズ企業情報スクレイパー

取得対象:
    - 名称（会社名）、住所、設立年月日、資本金、売上、従業員数、HP、事業内容、部署

取得フロー:
    一覧ページ（ページネーション）→ 詳細ページURL収集 → 各詳細ページからデータ取得

実行方法:
    python scripts/sites/agency_franchise/bahn_rep.py
    python bin/run_flow.py --site-id bahn_rep
"""

import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class BahnRepScraper(StaticCrawler):
    """バーン代理店 スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["部署"]

    def parse(self, url: str):
        """
        一覧ページ → 詳細ページ → 企業情報取得

        Args:
            url: 一覧ページのURL（例: https://bahn-rep.com/category/allarea/page/1）
        """
        # --- 1. 全詳細ページURLを収集（ページネーション）---
        detail_urls = self._collect_urls(url)
        self.total_items = len(detail_urls)
        self.logger.info("詳細ページURL収集完了: %d 件", len(detail_urls))

        # --- 2. 各詳細ページからデータ取得 ---
        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("詳細ページ取得失敗: %s (%s)", detail_url, e)
                continue

    def _collect_urls(self, base_url: str) -> list[str]:
        """一覧ページをページネーションしながら詳細URLを収集する"""
        # /page/N 形式のURLからベースを取得
        base = re.sub(r"/page/\d+", "", base_url).rstrip("/")

        seen: set[str] = set()
        urls: list[str] = []
        page = 1

        while True:
            page_url = f"{base}/page/{page}"
            self.logger.info("一覧ページ取得: page=%d", page)

            soup = self.get_soup(page_url)
            if not soup:
                break

            # 詳細リンク: <a class="image" href="...">
            links = soup.select("a.image")
            if not links:
                break

            for a in links:
                href = a.get("href", "")
                if not href or href in seen:
                    continue
                seen.add(href)
                urls.append(href)

            page += 1

        return urls

    def _scrape_detail(self, detail_url: str) -> dict | None:
        """詳細ページから企業情報を取得する"""
        soup = self.get_soup(detail_url)
        if not soup:
            return None

        item = {Schema.URL: detail_url}

        # テーブル構造: <td（ラベル）><td（値）> の2列
        # ラベル列は background-color: #f2f2f2 のスタイルが付いている
        for row in soup.select("table tr"):
            tds = row.select("td")
            if len(tds) < 2:
                continue

            label = tds[0].get_text(strip=True)
            td_val = tds[1]

            if label == "名称":
                item[Schema.NAME] = td_val.get_text(strip=True)
            elif label == "住所":
                item[Schema.ADDR] = td_val.get_text(" ", strip=True)
            elif label == "設立":
                item[Schema.OPEN_DATE] = td_val.get_text(strip=True)
            elif label == "資本金":
                item[Schema.CAP] = td_val.get_text(strip=True)
            elif label == "年商":
                item[Schema.SALES] = td_val.get_text(strip=True)
            elif label == "部署":
                item["部署"] = td_val.get_text(strip=True)
            elif label == "従業員":
                item[Schema.EMP_NUM] = td_val.get_text(strip=True)
            elif label == "HP":
                a_tag = td_val.select_one("a")
                if a_tag:
                    item[Schema.HP] = a_tag.get("href", "").strip()
            elif label == "事業":
                item[Schema.LOB] = td_val.get_text(" ", strip=True)

        # 会社名が取れなかった場合はスキップ
        if Schema.NAME not in item:
            return None

        return item


# =============================================================================
# ローカル実行用エントリーポイント
# =============================================================================
if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    BahnRepScraper().execute("https://bahn-rep.com/category/allarea/page/1")
