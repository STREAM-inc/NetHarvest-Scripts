# scripts/sites/agency_franchise/kkhashi.py
"""
カケハシ (kkhashi.com) — フランチャイズ・代理店募集案件スクレイパー

取得対象:
    - 名称（会社名）、案件名、設立年月日、従業員数、資本金、売上、事業内容
    - 案件情報（初期費用/加盟金/ロイヤリティ）
    - 契約形態、募集対象、報酬タイプ、カテゴリー、募集エリア

取得フロー:
    一覧ページ（ページネーション）→ 詳細ページURL収集 → 各詳細ページからデータ取得

実行方法:
    python scripts/sites/agency_franchise/kkhashi.py
    python bin/run_flow.py --site-id kkhashi
"""

import re
import sys
from pathlib import Path

# プロジェクトルートを sys.path に追加（ローカル実行用）
root_path = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(root_path))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://www.kkhashi.com"


class KkhashiScraper(StaticCrawler):
    """カケハシ フランチャイズ・代理店募集案件スクレイパー"""

    DELAY = 1.5  # サーバー負荷軽減（秒）
    EXTRA_COLUMNS = [
        "案件名",
        "契約形態",
        "初期費用",
        "加盟金",
        "ロイヤリティ",
        "報酬タイプ",
        "募集対象",
        "募集エリア",
    ]

    def parse(self, url: str):
        """
        一覧ページ → 詳細ページ → 案件情報取得

        Args:
            url: 一覧ページのURL
        """
        # --- 1. 全詳細ページURLを収集（ページネーション）---
        detail_urls = self._collect_matter_urls(url)
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

    def _collect_matter_urls(self, base_url: str) -> list[str]:
        """一覧ページをページネーションしながら詳細URLを収集する"""
        # 既存の page= パラメータを除去
        clean_url = re.sub(r"[&?]page=\d+", "", base_url)

        seen: set[str] = set()
        urls: list[str] = []
        page = 1

        while True:
            page_url = f"{clean_url}&page={page}" if page > 1 else clean_url
            self.logger.info("一覧ページ取得: page=%d", page)

            soup = self.get_soup(page_url)
            if not soup:
                break

            links = soup.select('a[href*="/matters/detail/"]')
            if not links:
                break

            for a in links:
                href = a.get("href", "")
                if not href:
                    continue
                full_url = BASE_URL + href if href.startswith("/") else href
                if full_url not in seen:
                    seen.add(full_url)
                    urls.append(full_url)

            # 次ページリンクがなければ最終ページ
            next_link = soup.select_one(f'a[href*="page={page + 1}"]')
            if not next_link:
                break

            page += 1

        return urls

    def _scrape_detail(self, detail_url: str) -> dict | None:
        """詳細ページから案件情報を取得する"""
        soup = self.get_soup(detail_url)
        if not soup:
            return None

        item = {Schema.URL: detail_url}

        # --- 案件名（タイトル）---
        title_el = soup.select_one("h2.sp-bold")
        if title_el:
            item["案件名"] = title_el.get_text(strip=True)

        # --- 案件情報テーブル (.tab-section01): 初期費用/加盟金/ロイヤリティ ---
        tab1 = soup.select_one(".tab-section01 table")
        if tab1:
            for row in tab1.select("tr"):
                th = row.select_one("th")
                td = row.select_one("td")
                if not th or not td:
                    continue
                label = th.get_text(strip=True)
                value = td.get_text(" ", strip=True)
                if label == "初期費用":
                    item["初期費用"] = value
                elif label == "加盟金":
                    item["加盟金"] = value
                elif label == "ロイヤリティ":
                    item["ロイヤリティ"] = value

        # --- 企業情報テーブル (.tab-section02): 会社名/設立年/従業員/資本金/年商 ---
        tab2 = soup.select_one(".tab-section02 table")
        if tab2:
            for row in tab2.select("tr"):
                th = row.select_one("th")
                td = row.select_one("td")
                if not th or not td:
                    continue
                label = th.get_text(strip=True)
                value = td.get_text(" ", strip=True)
                if label == "会社名":
                    item[Schema.NAME] = value
                elif label == "事業内容":
                    item[Schema.LOB] = value
                elif label == "設立年":
                    item[Schema.OPEN_DATE] = value
                elif label == "従業員":
                    item[Schema.EMP_NUM] = value
                elif label == "資本金":
                    item[Schema.CAP] = value
                elif label == "年商":
                    item[Schema.SALES] = value

        # --- 契約形態（アクティブなもの: gray クラスなし）---
        contract_el = soup.select_one(".list.contract")
        if contract_el:
            active = [
                li.get_text(strip=True)
                for li in contract_el.select("li")
                if "gray" not in li.get("class", [])
            ]
            if active:
                item["契約形態"] = "・".join(active)

        # --- 募集対象 ---
        target_el = soup.select_one(".list.target")
        if target_el:
            active = [
                li.get_text(strip=True)
                for li in target_el.select("li")
                if "gray" not in li.get("class", [])
            ]
            if active:
                item["募集対象"] = "・".join(active)

        # --- 報酬タイプ ---
        reward_el = soup.select_one(".list.reward")
        if reward_el:
            active = [
                li.get_text(strip=True)
                for li in reward_el.select("li")
                if "gray" not in li.get("class", [])
            ]
            if active:
                item["報酬タイプ"] = "・".join(active)

        # --- カテゴリー ---
        genre_el = soup.select_one(".arrow.genre")
        if genre_el:
            categories = [
                a.get_text(strip=True)
                for a in genre_el.select("li a")
                if a.get_text(strip=True)
            ]
            if categories:
                item[Schema.CAT_SITE] = "・".join(categories)

        # --- 募集エリア ---
        area_el = soup.select_one(".section__wrap__center")
        if area_el:
            areas = [dt.get_text(strip=True) for dt in area_el.select("dt")]
            if areas:
                item["募集エリア"] = "・".join(areas)

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

    scraper = KkhashiScraper()
    scraper.execute(
        "https://www.kkhashi.com/matters?ref=%E5%85%A8%E3%81%A6%E3%81%AE%E6%A1%88%E4%BB%B6%E3%81%8B%E3%82%89%E6%8E%A2%E3%81%99"
    )

    print("\n" + "=" * 60)
    print("実行結果サマリ")
    print("=" * 60)
    print(f"  出力ファイル:     {scraper.output_filepath}")
    print(f"  取得件数:         {scraper.item_count}")
    print(f"  観測カラム数:     {len(scraper.observed_columns)}")
    print(f"  観測カラム:       {scraper.observed_columns}")
    print("=" * 60)

    if scraper.output_filepath:
        print("\n CSV 先頭5行:")
        print("-" * 60)
        with open(scraper.output_filepath, encoding="utf-8-sig") as f:
            for i, line in enumerate(f):
                if i >= 6:  # ヘッダー + 5行
                    break
                print(line.rstrip())
