# scripts/sites/portal/okinawa_shakou.py
"""
沖縄県社交飲食業生活衛生同業組合 (okinawasyako.ti-da.net) — 加盟店スクレイパー

取得対象:
    - ジャンル別カテゴリページから詳細ページURLを収集
    - 詳細ページの table.tenpo (th/td) から店舗情報を取得

取得フロー:
    4つのジャンルカテゴリ (居酒屋, バー, キャバクラ・スナック, ラウンジ・クラブ)
    → ページネーション巡回 → 詳細ページリンク収集 → 各詳細ページからデータ取得

実行方法:
    python scripts/sites/portal/okinawa_shakou.py
"""

import sys
from pathlib import Path
from urllib.parse import urljoin

root_path = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(root_path))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://okinawasyako.ti-da.net/"

# 全カテゴリページ（ジャンル別 + エリア別）
# 両方を巡回して重複排除することで全店舗を網羅する
CATEGORY_PAGES = [
    # ジャンル別
    "c409463.html",  # 居酒屋・お食事処
    "c409461.html",  # バー
    "c409465.html",  # キャバクラ・スナック
    "c409912.html",  # ラウンジ・クラブ
    # エリア別
    "c409476.html",  # 北部
    "c409459.html",  # 中部
    "c409475.html",  # 南部
    "c409477.html",  # 那覇・浦添
    "c409913.html",  # 宮古島
    "c409914.html",  # 石垣島
]


class OkinawaShakouScraper(StaticCrawler):
    """沖縄県社交飲食業組合 加盟店スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["エリア"]

    def parse(self, url: str):
        # --- 1. 全詳細ページ URL を収集 ---
        detail_urls = self._collect_detail_urls()
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

    def _collect_detail_urls(self) -> list[str]:
        """ジャンル別+エリア別カテゴリをページネーションしながら詳細ページURLを収集する"""
        detail_urls = []

        for category in CATEGORY_PAGES:
            current_url = urljoin(BASE_URL, category)

            while current_url:
                self.logger.info("一覧ページ取得: %s", current_url)
                soup = self.get_soup(current_url)
                if soup is None:
                    break

                # li.archiveli 内の a タグから詳細ページリンクを収集
                for li in soup.select("li.archiveli"):
                    a_tag = li.find("a", href=True)
                    if a_tag:
                        href = urljoin(current_url, a_tag["href"])
                        if href not in detail_urls:
                            detail_urls.append(href)

                # 「次のページ」リンクで次のページへ
                next_link = soup.select_one("a.amenu.next")
                if next_link and next_link.get("href"):
                    current_url = urljoin(current_url, next_link["href"])
                else:
                    current_url = None

        return detail_urls

    def _scrape_detail(self, detail_url: str) -> dict | None:
        """詳細ページから店舗情報を取得する"""
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        item = {Schema.URL: detail_url}

        # 都道府県は固定
        item[Schema.PREF] = "沖縄県"

        # --- table.tenpo の th/td からデータ取得 ---
        table = soup.find("table", class_="tenpo")
        if not table:
            return None

        for tr in table.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            label = th.get_text(strip=True)
            value = td.get_text(" ", strip=True)
            if not value:
                continue

            if label == "店名":
                item[Schema.NAME] = value
            elif label == "所在地":
                item[Schema.ADDR] = value
            elif label == "TEL(店)":
                item[Schema.TEL] = value
            elif label == "ジャンル":
                item[Schema.CAT_SITE] = value
            elif label == "営業時間":
                item[Schema.TIME] = value
            elif label == "定休日":
                item[Schema.HOLIDAY] = value
            elif label == "ブログURL":
                a = td.find("a", href=True)
                if a and a["href"] and not a["href"].startswith("【"):
                    item[Schema.HP] = a["href"]

        # --- エリア情報をパンくずから取得 ---
        area = self._extract_area(soup)
        if area:
            item["エリア"] = area

        # 店舗名が取れなかった場合はスキップ
        if Schema.NAME not in item:
            return None

        return item

    def _extract_area(self, soup) -> str | None:
        """パンくずリスト (.topicpass) からエリア情報を取得する"""
        topicpass = soup.find("div", class_="topicpass")
        if not topicpass:
            return None

        # エリアカテゴリのURLパターン
        area_categories = {
            "c409476.html": "北部",
            "c409459.html": "中部",
            "c409475.html": "南部",
            "c409477.html": "那覇・浦添",
            "c409913.html": "宮古島",
            "c409914.html": "石垣島",
            "c408984.html": "北部",
            "c408976.html": "中部",
            "c408985.html": "南部",
            "c408986.html": "那覇・浦添",
            "c408987.html": "宮古島",
            "c408988.html": "石垣島",
        }

        for a_tag in topicpass.find_all("a", href=True):
            href = a_tag["href"].lstrip("/")
            if href in area_categories:
                return area_categories[href]

        return None


# =============================================================================
# ローカル実行用エントリーポイント
# =============================================================================
if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = OkinawaShakouScraper()
    scraper.execute(BASE_URL)

    print("\n" + "=" * 60)
    print("実行結果サマリ")
    print("=" * 60)
    print(f"  出力ファイル:     {scraper.output_filepath}")
    print(f"  取得件数:         {scraper.item_count}")
    print(f"  観測カラム数:     {len(scraper.observed_columns)}")
    print(f"  観測カラム:       {scraper.observed_columns}")
    print("=" * 60)
