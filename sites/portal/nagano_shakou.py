# scripts/sites/portal/nagano_shakou.py
"""
長野県社交飲食業生活衛生同業組合 (naganosyakouinshyoku.com) — 加入店舗スクレイパー

取得対象:
    - 一覧ページから詳細ページURLを収集
    - 詳細ページの dl/dt/dd から店舗名、住所、TEL、営業時間、定休日等を取得

取得フロー:
    一覧ページ (ページネーション /page/N/) → 詳細ページリンク収集 → 各詳細ページからデータ取得

実行方法:
    python scripts/sites/portal/nagano_shakou.py
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

root_path = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(root_path))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class NaganoShakouScraper(StaticCrawler):
    """長野県社交飲食業組合 加入店舗スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["エリア", "FAX", "メール", "価格帯"]

    def parse(self, url: str):
        # --- 1. 全詳細ページ URL を収集 ---
        detail_urls = self._collect_detail_urls(url)
        self.total_items = len(detail_urls)
        self.logger.info("詳細ページ URL 収集完了: %d 件", len(detail_urls))

        # --- 2. 各詳細ページからデータ取得 ---
        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("詳細ページ取得失敗: %s (%s)", detail_url, e)
                continue

    def _collect_detail_urls(self, base_url: str) -> list[str]:
        """一覧ページをページネーションしながら詳細ページ URL を収集する"""
        detail_urls = []
        current_url = base_url

        while current_url:
            self.logger.info("一覧ページ取得: %s", current_url)
            soup = self.get_soup(current_url)
            if soup is None:
                break

            # 詳細ページリンクを収集（/shoppost/ を含むリンク、重複排除）
            for a_tag in soup.select("a[href*='/shoppost/']"):
                href = a_tag.get("href", "")
                if "/shoppost-cat/" in href:
                    continue  # カテゴリページは除外
                abs_url = urljoin(current_url, href)
                if abs_url not in detail_urls:
                    detail_urls.append(abs_url)

            # 次のページを探す
            next_url = None
            # /page/N/ 形式のページネーション
            current_page = 1
            page_match = re.search(r"/page/(\d+)", current_url)
            if page_match:
                current_page = int(page_match.group(1))

            next_page_url = re.sub(r"/page/\d+/?", f"/page/{current_page + 1}/", current_url)
            if next_page_url == current_url:
                # 初回（/page/ なし）の場合
                next_page_url = current_url.rstrip("/") + f"/page/2/"

            # 次のページリンクが存在するか確認
            next_link = soup.select_one(f"a[href*='/page/{current_page + 1}']")
            if next_link:
                next_url = urljoin(current_url, next_link["href"])
            else:
                next_url = None

            current_url = next_url

        return detail_urls

    def _scrape_detail(self, detail_url: str) -> dict | None:
        """詳細ページから店舗情報を取得する"""
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        item = {Schema.URL: detail_url}

        # 都道府県は固定
        item[Schema.PREF] = "長野県"

        # --- div.shop-name 内の shop-info-title / shop-info-post からデータ取得 ---
        for row in soup.select("div.shop-name"):
            title_div = row.select_one("div.shop-info-title")
            post_div = row.select_one("div.shop-info-post")
            if not title_div or not post_div:
                continue
            # ラベルからハイフンを除去
            label = title_div.get_text(strip=True).rstrip("-").strip()
            value = post_div.get_text(strip=True)

            if label == "店名":
                item[Schema.NAME] = value
            elif label == "住所":
                item[Schema.ADDR] = value
            elif label == "電話番号":
                item[Schema.TEL] = value
            elif label == "営業時間":
                item[Schema.TIME] = value
            elif label == "定休日":
                item[Schema.HOLIDAY] = value
            elif label == "価格帯":
                item["価格帯"] = value
            elif label == "店舗カテゴリ":
                item[Schema.CAT_SITE] = value
            elif label == "ホームページ":
                a_tag = post_div.select_one("a[href]")
                if a_tag and a_tag["href"]:
                    item[Schema.HP] = a_tag["href"]
            elif label == "FAX":
                if value:
                    item["FAX"] = value
            elif label in ("E-MAIL", "メール", "Eメール"):
                if value:
                    item["メール"] = value

        # 店舗名が取れなかった場合はスキップ
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

    scraper = NaganoShakouScraper()
    scraper.execute("https://naganosyakouinshyoku.com/shoppost-cat/naganoshi")

    print("\n" + "=" * 60)
    print("実行結果サマリ")
    print("=" * 60)
    print(f"  出力ファイル:     {scraper.output_filepath}")
    print(f"  取得件数:         {scraper.item_count}")
    print(f"  観測カラム数:     {len(scraper.observed_columns)}")
    print(f"  観測カラム:       {scraper.observed_columns}")
    print("=" * 60)
