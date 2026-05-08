# scripts/sites/portal/niigata_shakou.py
"""
新潟県社交飲食業生活衛生同業組合 (niigataken-shakou.com) — 加入店舗スクレイパー

取得対象:
    - 一覧ページ (/search?keyword=) から詳細ページURLを収集
    - 詳細ページの table th/td から店舗名、住所、TEL、営業時間、定休日等を取得
    - ul.meta の li から業種・ジャンル・エリアを取得

取得フロー:
    一覧ページ (20件/ページ, ?page=N) → 詳細ページリンク収集 → 各詳細ページからデータ取得

実行方法:
    python scripts/sites/portal/niigata_shakou.py
"""

import sys
from pathlib import Path
from urllib.parse import urljoin

root_path = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(root_path))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class NiigataShakouScraper(StaticCrawler):
    """新潟県社交飲食業組合 加入店舗スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["エリア", "ジャンル", "平均予算", "総席数", "カラオケ", "店舗説明"]

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
        page = 1

        while True:
            if page == 1:
                current_url = base_url
            else:
                current_url = f"{base_url}&page={page}" if "?" in base_url else f"{base_url}?page={page}"

            self.logger.info("一覧ページ取得: %s", current_url)
            soup = self.get_soup(current_url)
            if soup is None:
                break

            # 各店舗の詳細リンクを収集（/store/ を含むリンク）
            found_new = False
            for a_tag in soup.select("a[href*='/store/']"):
                href = a_tag.get("href", "")
                abs_url = urljoin(current_url, href)
                if abs_url not in detail_urls:
                    detail_urls.append(abs_url)
                    found_new = True

            # 新しいリンクが見つからなければ最終ページ
            if not found_new:
                break

            # 次のページリンクが存在するか確認
            next_link = soup.select_one(f"a[href*='page={page + 1}']")
            if next_link:
                page += 1
            else:
                break

        return detail_urls

    def _scrape_detail(self, detail_url: str) -> dict | None:
        """詳細ページから店舗情報を取得する"""
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        item = {Schema.URL: detail_url}

        # 都道府県は固定
        item[Schema.PREF] = "新潟県"

        # --- 店舗名: h1.title から取得 ---
        h1 = soup.select_one("h1.title")
        if h1:
            item[Schema.NAME] = h1.get_text(strip=True)

        # --- ul.meta の li から業種・ジャンル・エリアを取得 ---
        for ul in soup.find_all("ul", class_="meta"):
            # 業種: li.genre
            genre_li = ul.select_one("li.genre")
            if genre_li:
                text = genre_li.get_text(strip=True)
                item[Schema.CAT_SITE] = text.replace("業種：", "").replace("業種:", "").strip()

            # ジャンル: li.sub-genre
            sub_genre_li = ul.select_one("li.sub-genre")
            if sub_genre_li:
                text = sub_genre_li.get_text(strip=True)
                item["ジャンル"] = text.replace("ジャンル：", "").replace("ジャンル:", "").strip()

            # エリア: li.area
            area_li = ul.select_one("li.area")
            if area_li:
                text = area_li.get_text(strip=True)
                # "新潟エリア： 古町" → "古町"
                if "：" in text:
                    item["エリア"] = text.split("：")[-1].strip()
                else:
                    item["エリア"] = text.strip()

        # --- 店舗説明文: h2.title の次の p タグ ---
        for h2 in soup.find_all("h2", class_="title"):
            for sibling in h2.find_next_siblings():
                if sibling.name in ("table", "dl", "h2", "h1", "footer", "div"):
                    break
                if sibling.name == "p":
                    text = sibling.get_text(strip=True)
                    if text and len(text) > 5:
                        item["店舗説明"] = text
                        break

        # --- table の th/td からデータ取得 ---
        for table in soup.find_all("table"):
            for tr in table.find_all("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                label = th.get_text(strip=True)
                value = td.get_text(strip=True)
                if not value:
                    continue

                if label == "店名":
                    if Schema.NAME not in item:
                        item[Schema.NAME] = value
                elif label == "住所":
                    item[Schema.ADDR] = value
                elif label == "電話番号":
                    item[Schema.TEL] = value
                elif label == "営業時間":
                    item[Schema.TIME] = value
                elif label == "定休日":
                    item[Schema.HOLIDAY] = value
                elif label == "平均予算":
                    item["平均予算"] = value
                elif label == "総席数":
                    item["総席数"] = value
                elif label == "カラオケ":
                    item["カラオケ"] = value
                elif label in ("Webサイト", "WEBサイト", "ホームページ"):
                    a = td.select_one("a[href]")
                    if a and a.get("href"):
                        href = a["href"]
                        if "google.com/maps" not in href:
                            item[Schema.HP] = href

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

    scraper = NiigataShakouScraper()
    scraper.execute("https://niigataken-shakou.com/search?keyword=")

    print("\n" + "=" * 60)
    print("実行結果サマリ")
    print("=" * 60)
    print(f"  出力ファイル:     {scraper.output_filepath}")
    print(f"  取得件数:         {scraper.item_count}")
    print(f"  観測カラム数:     {len(scraper.observed_columns)}")
    print(f"  観測カラム:       {scraper.observed_columns}")
    print("=" * 60)
