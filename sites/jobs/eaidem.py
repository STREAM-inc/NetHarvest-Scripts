# scripts/sites/jobs/eaidem.py
"""
イーアイデム (e-aidem.com) — 企業情報スクレイパー

取得対象:
    - 詳細ページの「企業情報」セクション (.companyBox)
        社名, 企業概要, 本社所在地, URL
    - 詳細ページの「応募情報」セクション (.applicationBox)
        連絡先TEL

取得フロー:
    一覧ページ (pagination) → 詳細ページリンク収集 → 各詳細ページからデータ取得

実行方法:
    # ローカルテスト（鳥取県のみ）
    python scripts/sites/eaidem.py

    # Prefect Flow 経由
    python bin/run_flow.py --site eaidem --url "https://www.e-aidem.com/aps/list.htm?region_id=06&district_id=31"
"""

import re
import sys
from pathlib import Path

# プロジェクトルートを sys.path に追加（ローカル実行用）
root_path = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(root_path))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class EaidemScraper(StaticCrawler):
    """イーアイデム 企業情報スクレイパー"""

    DELAY = 0.1  # サーバー負荷軽減（秒）
    EXTRA_COLUMNS = ["企業概要"]

    def parse(self, url):
        """
        一覧ページ → 詳細ページ → 企業情報取得

        Args:
            url: 一覧ページの URL (例: https://www.e-aidem.com/aps/list.htm?region_id=06&district_id=31)
        """
        # --- 1. 全詳細ページ URL を収集 (ページネーション) ---
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
        """一覧ページをページネーションしながら詳細ページの URL を収集する"""
        detail_urls = []
        page = 1

        # 既存の page= パラメータを削除
        clean_url = re.sub(r"[&?]page=\d+", "", base_url)

        while True:
            # ページ番号付き URL を構築
            sep = "&" if "?" in clean_url else "?"
            list_url = f"{clean_url}{sep}page={page}"

            self.logger.info("一覧ページ取得: page=%d", page)
            soup = self.get_soup(list_url)

            # 詳細リンクを抽出 (「詳細を見る」リンク)
            links = soup.select("a[href*='_detail.htm']")
            if not links:
                break

            for link in links:
                href = link.get("href", "")
                if "_detail.htm" in href:
                    # 相対 URL を絶対 URL に変換
                    if href.startswith("/"):
                        href = "https://www.e-aidem.com" + href
                    if href not in detail_urls:
                        detail_urls.append(href)

            # 「次へ」リンクがなければ最終ページ
            next_link = soup.select_one("a[href*='page={}']".format(page + 1))
            if not next_link:
                break

            page += 1

        return detail_urls

    def _scrape_detail(self, detail_url: str) -> dict | None:
        """詳細ページから企業情報と連絡先TELを取得する"""
        soup = self.get_soup(detail_url)

        item = {Schema.URL: detail_url}

        # --- 企業情報 (.companyBox) ---
        company_box = soup.select_one(".companyBox")
        if company_box:
            rows = company_box.select("tr")
            for row in rows:
                th = row.select_one("th")
                td = row.select_one("td")
                if not th or not td:
                    continue

                label = th.get_text(strip=True)

                if label == "社名":
                    # <span class="other"> を除外して社名だけ取得
                    other = td.select_one(".other")
                    if other:
                        other.extract()  # decompose() より安全（DOM ツリーを破壊しない）
                    item[Schema.NAME] = td.get_text(strip=True)

                elif label == "本社所在地":
                    # 〒を除いた住所テキスト
                    addr_text = td.get_text("\n", strip=True)
                    # 郵便番号行と住所行を結合
                    item[Schema.ADDR] = re.sub(r"\s+", " ", addr_text).strip()

                elif label == "URL":
                    a_tag = td.select_one("a")
                    if a_tag:
                        item[Schema.HP] = a_tag.get("href", "").strip()

                elif label == "企業概要":
                    item["企業概要"] = td.get_text("\n", strip=True)

        # --- 連絡先TEL (.applicationBox) ---
        app_box = soup.select_one(".applicationBox")
        if app_box:
            tel_row = None
            for row in app_box.select("tr"):
                th = row.select_one("th")
                if th and "TEL" in th.get_text():
                    tel_row = row
                    break
            if tel_row:
                td = tel_row.select_one("td")
                if td:
                    item[Schema.TEL] = td.get_text(strip=True)

        # 社名が取れなかった場合はスキップ
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

    scraper = EaidemScraper()
    # テスト: 鳥取県 (region_id=06, district_id=31) — 約400件
    scraper.execute(
        "https://www.e-aidem.com/aps/list.htm?region_id=06&district_id=31"
    )

    print("\n" + "=" * 60)
    print("📊 実行結果サマリ")
    print("=" * 60)
    print(f"  出力ファイル:     {scraper.output_filepath}")
    print(f"  取得件数:         {scraper.item_count}")
    print(f"  観測カラム数:     {len(scraper.observed_columns)}")
    print(f"  観測カラム:       {scraper.observed_columns}")
    print("=" * 60)

    # CSV の先頭5行を表示
    if scraper.output_filepath:
        print("\n📄 CSV 先頭5行:")
        print("-" * 60)
        with open(scraper.output_filepath, encoding="utf-8-sig") as f:
            for i, line in enumerate(f):
                if i >= 6:  # ヘッダー + 5行
                    break
                print(line.rstrip())
