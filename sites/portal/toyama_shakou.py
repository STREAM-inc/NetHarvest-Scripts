# scripts/sites/portal/toyama_shakou.py
"""
富山県社交飲食業生活衛生同業組合 (toyama-syakou.com) — 加入店舗スクレイパー

取得対象:
    - 一覧ページ (/shop/) から詳細ページURLを収集
    - 詳細ページの dl.p-shop-single__detail (dt/dd) から店舗情報を取得

取得フロー:
    一覧ページ (10件/ページ, /shop/page/N/) → 詳細ページリンク収集 → 各詳細ページからデータ取得

注意:
    - SSL証明書が不正なためverify=Falseで通信する
    - 詳細ページのHTMLが壊れており、「休日」のdd内に後続のdt/ddがネストされている
      → dl内の全dt/ddを順番に取得して対処する

実行方法:
    python scripts/sites/portal/toyama_shakou.py
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

root_path = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(root_path))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class ToyamaShakouScraper(StaticCrawler):
    """富山県社交飲食業組合 加入店舗スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["キャッチコピー", "席数", "セット料金", "クレジットカード",
                     "カラオケ", "飲み放題プラン"]

    def _setup(self):
        """SSL証明書エラー回避のため verify=False を設定"""
        super()._setup()
        self.session.verify = False

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
                current_url = f"{base_url.rstrip('/')}/page/{page}/"

            self.logger.info("一覧ページ取得: %s", current_url)
            soup = self.get_soup(current_url)
            if soup is None:
                break

            shop_list = soup.find("ul", class_="p-shop__list")
            if not shop_list:
                break

            # 各店舗の詳細リンクを収集
            # li 内の a タグのうち /shop/ を含み shop_category/shop_budget/shop_seat を含まないもの
            found_new = False
            for li in shop_list.find_all("li", recursive=False):
                for a_tag in li.find_all("a", href=True):
                    href = a_tag["href"]
                    if "/shop/" in href and not any(
                        x in href for x in ["shop_category", "shop_budget", "shop_seat", "/page/"]
                    ):
                        abs_url = urljoin(current_url, href)
                        if abs_url not in detail_urls:
                            detail_urls.append(abs_url)
                            found_new = True

            if not found_new:
                break

            # 次のページリンクが存在するか確認
            nav = soup.find("div", class_="nav-links")
            if nav:
                next_link = nav.find("a", string=re.compile(r"^>$|^›$|^次"))
                if not next_link:
                    # ページ番号リンクで次ページを探す
                    next_link = nav.find("a", href=re.compile(rf"/page/{page + 1}/"))
                if next_link:
                    page += 1
                    continue
            break

        return detail_urls

    def _scrape_detail(self, detail_url: str) -> dict | None:
        """詳細ページから店舗情報を取得する"""
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        item = {Schema.URL: detail_url}

        # 都道府県は固定
        item[Schema.PREF] = "富山県"

        # --- 店舗名: h1.p-shop-single__title ---
        h1 = soup.find("h1", class_="p-shop-single__title")
        if h1:
            item[Schema.NAME] = h1.get_text(strip=True)

        # --- キャッチコピー ---
        catchcopy = soup.find("div", class_="p-shop-single__catchcopy")
        if catchcopy:
            text = catchcopy.get_text(strip=True)
            if text:
                item["キャッチコピー"] = text

        # --- カテゴリ (業種・予算): div.p-shop-single__category 内の a タグ ---
        cat_div = soup.find("div", class_="p-shop-single__category")
        if cat_div:
            for a_tag in cat_div.find_all("a", href=True):
                href = a_tag["href"]
                text = a_tag.get_text(strip=True)
                if "shop_category" in href:
                    item[Schema.CAT_SITE] = text

        # --- dl.p-shop-single__detail から dt/dd ペアでデータ取得 ---
        # HTMLが壊れてddの中にdt/ddがネストされるため、dl内の全dt/ddを順番に取得
        dl = soup.find("dl", class_="p-shop-single__detail")
        if dl:
            all_dt = dl.find_all("dt")
            all_dd = dl.find_all("dd")
            for dt, dd in zip(all_dt, all_dd):
                label = dt.get_text(strip=True)
                # dd 内にネストされた dt/dd がある場合、直接テキストのみ取得
                # dd の最初のテキストノードだけを取り出す
                value = ""
                for content in dd.children:
                    if hasattr(content, "name") and content.name in ("dt", "dd", "dl"):
                        break
                    if hasattr(content, "name"):
                        value += content.get_text(strip=True)
                    else:
                        value += str(content).strip()
                value = value.strip()
                if not value:
                    continue

                if label == "住所":
                    item[Schema.ADDR] = value
                elif label == "電話番号":
                    item[Schema.TEL] = value
                elif label == "営業時間":
                    item[Schema.TIME] = value
                elif label in ("休日", "定休日"):
                    item[Schema.HOLIDAY] = value
                elif label == "席数":
                    item["席数"] = value
                elif label == "セット料金":
                    item["セット料金"] = value
                elif label == "クレジットカード":
                    item["クレジットカード"] = value
                elif label == "カラオケ":
                    item["カラオケ"] = value
                elif label == "飲み放題プラン":
                    item["飲み放題プラン"] = value

        # 店舗名が取れなかった場合はスキップ
        if Schema.NAME not in item:
            return None

        return item


# =============================================================================
# ローカル実行用エントリーポイント
# =============================================================================
if __name__ == "__main__":
    import logging
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = ToyamaShakouScraper()
    scraper.execute("http://toyama-syakou.com/shop/")

    print("\n" + "=" * 60)
    print("実行結果サマリ")
    print("=" * 60)
    print(f"  出力ファイル:     {scraper.output_filepath}")
    print(f"  取得件数:         {scraper.item_count}")
    print(f"  観測カラム数:     {len(scraper.observed_columns)}")
    print(f"  観測カラム:       {scraper.observed_columns}")
    print("=" * 60)
