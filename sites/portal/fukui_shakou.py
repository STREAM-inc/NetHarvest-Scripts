# scripts/sites/portal/fukui_shakou.py
"""
福井県社交飲食業生活衛生同業組合 (fukui-syakou.com) — 加盟店スクレイパー

取得対象:
    - 一覧ページ (/guide) から詳細ページURLを収集
    - 詳細ページの table.spotinfo (th/td) から店舗情報を取得

取得フロー:
    一覧ページ (10件/ページ, /guide/page/N) → 詳細ページリンク収集 → 各詳細ページからデータ取得

実行方法:
    python scripts/sites/portal/fukui_shakou.py
"""

import sys
from pathlib import Path
from urllib.parse import urljoin

root_path = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(root_path))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class FukuiShakouScraper(StaticCrawler):
    """福井県社交飲食業組合 加盟店スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["アクセス", "座席数", "価格帯", "お支払い",
                     "SNS", "口コミサイト", "店舗説明"]

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

            # a.spotpage から詳細ページリンクを収集
            found_new = False
            for a_tag in soup.find_all("a", class_="spotpage"):
                href = a_tag.get("href", "")
                if href and href not in detail_urls:
                    detail_urls.append(href)
                    found_new = True

            if not found_new:
                break

            # 「次へ」リンクで次のページへ
            next_link = soup.find("a", string="次へ")
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
        item[Schema.PREF] = "福井県"

        # --- 店舗名: h2.spotname ---
        h2 = soup.find("h2", class_="spotname")
        if h2:
            item[Schema.NAME] = h2.get_text(strip=True)

        # --- カテゴリ: p.spotcate ---
        p_cate = soup.find("p", class_="spotcate")
        if p_cate:
            item[Schema.CAT_SITE] = p_cate.get_text(strip=True)

        # --- 紹介文: h2.spotname の次のクラスなし div ---
        if h2:
            for sib in h2.find_next_siblings():
                if sib.name == "table":
                    break
                if sib.name == "div" and not sib.get("class"):
                    text = sib.get_text(strip=True)
                    if text and len(text) > 3:
                        item["店舗説明"] = text
                        break

        # --- table.spotinfo の th/td からデータ取得 ---
        table = soup.find("table", class_="spotinfo")
        if table:
            for tr in table.find_all("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                label = th.get_text(strip=True)
                value = td.get_text(strip=True)
                if not value:
                    continue

                if label == "住所":
                    item[Schema.ADDR] = value
                elif label == "TEL":
                    item[Schema.TEL] = value
                elif label == "営業時間":
                    item[Schema.TIME] = value
                elif label == "定休日":
                    item[Schema.HOLIDAY] = value
                elif label == "アクセス":
                    item["アクセス"] = value
                elif label == "座席数":
                    item["座席数"] = value
                elif label == "価格帯":
                    item["価格帯"] = value
                elif label == "お支払い":
                    item["お支払い"] = value
                elif label == "SNS":
                    # リンクがあればURLを取得
                    a = td.find("a", href=True)
                    if a:
                        item["SNS"] = a["href"]
                    else:
                        item["SNS"] = value
                elif label == "口コミサイト":
                    a = td.find("a", href=True)
                    if a:
                        item["口コミサイト"] = a["href"]
                    else:
                        item["口コミサイト"] = value
                elif label in ("ホームページ", "HP", "Webサイト", "WEBサイト", "参考サイト"):
                    a = td.find("a", href=True)
                    if a:
                        item[Schema.HP] = a["href"]
                    elif value.startswith("http"):
                        item[Schema.HP] = value

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

    scraper = FukuiShakouScraper()
    scraper.execute("https://fukui-syakou.com/guide")

    print("\n" + "=" * 60)
    print("実行結果サマリ")
    print("=" * 60)
    print(f"  出力ファイル:     {scraper.output_filepath}")
    print(f"  取得件数:         {scraper.item_count}")
    print(f"  観測カラム数:     {len(scraper.observed_columns)}")
    print(f"  観測カラム:       {scraper.observed_columns}")
    print("=" * 60)
