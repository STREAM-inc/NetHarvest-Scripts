# scripts/sites/portal/kagoshima_shakou.py
"""
鹿児島県社交飲食業生活衛生同業組合 (kagoshima-pub.or.jp) — 加盟店スクレイパー

取得対象:
    - 一覧ページ (/category/kagoshima/) から詳細ページURLを収集
    - 詳細ページの ul > li > strong からラベル/値ペアで店舗情報を取得

取得フロー:
    一覧ページ (約4件/ページ, /category/kagoshima/page/N/) → 詳細ページリンク収集 → 各詳細ページからデータ取得

実行方法:
    python scripts/sites/portal/kagoshima_shakou.py
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

root_path = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(root_path))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class KagoshimaShakouScraper(StaticCrawler):
    """鹿児島県社交飲食業組合 加盟店スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["FAX"]

    # 全支部カテゴリURL
    BRANCH_PATHS = [
        "kagoshima",  # 鹿児島市
        "sendai",     # 川内支部
        "okuchi",     # 伊佐支部
        "kanoya",     # 鹿屋支部
        "ibusuki",    # 指宿支部
        "amami",      # 奄美支部
        "tokunoshima", # 徳之島支部
    ]

    def parse(self, url: str):
        # --- 1. 全支部から詳細ページ URL を収集 ---
        base = url.rstrip("/").rsplit("/category", 1)[0]
        detail_urls = []
        for branch in self.BRANCH_PATHS:
            branch_url = f"{base}/category/{branch}/"
            self.logger.info("支部: %s", branch_url)
            urls = self._collect_detail_urls(branch_url)
            for u in urls:
                if u not in detail_urls:
                    detail_urls.append(u)
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

            # h3 > a から詳細ページリンクを収集
            for h3 in soup.find_all("h3"):
                a_tag = h3.find("a", href=True)
                if not a_tag:
                    continue
                href = a_tag["href"]
                abs_url = urljoin(current_url, href)
                # カテゴリページや固定ページを除外
                if "/category/" in abs_url or "/page/" in abs_url:
                    continue
                if abs_url not in detail_urls:
                    detail_urls.append(abs_url)

            # 「次へ »」リンクで次のページへ
            next_link = soup.find("a", string=re.compile(r"次へ"))
            if next_link and next_link.get("href"):
                next_url = urljoin(current_url, next_link["href"])
                if next_url != current_url:
                    current_url = next_url
                else:
                    current_url = None
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
        item[Schema.PREF] = "鹿児島県"

        # --- 店舗名: h3 ---
        h3 = soup.find("h3")
        if h3:
            item[Schema.NAME] = h3.get_text(strip=True)

        # --- ラベル/値ペアを収集 (ul>li>strong 形式 + dl>dt/dd 形式) ---
        pairs: list[tuple[str, str, object]] = []  # (label, value, element)

        # (1) ul > li > strong 形式
        for li in soup.find_all("li"):
            strong = li.find("strong")
            if not strong:
                continue
            label = strong.get_text(strip=True)
            value = li.get_text(strip=True)
            value = value[len(label):].strip()
            if value:
                pairs.append((label, value, li))

        # (2) dl > dt / dd 形式 (フォールバック)
        if not pairs:
            for dt in soup.find_all("dt"):
                dd = dt.find_next_sibling("dd")
                if not dd:
                    continue
                label = dt.get_text(strip=True)
                value = dd.get_text(strip=True)
                if value and value != "–":
                    pairs.append((label, value, dd))

        # --- ラベルに応じてマッピング ---
        for label, value, elem in pairs:
            if label == "住所":
                # 郵便番号を分離 (〒XXX-XXXX または数字7-8桁)
                post_match = re.search(r"〒?(\d{3}-?\d{4})", value)
                if post_match:
                    item[Schema.POST_CODE] = post_match.group(1)
                    addr = re.sub(r"〒?\d{3}-?\d{4}\s*", "", value).strip()
                    item[Schema.ADDR] = addr
                else:
                    item[Schema.ADDR] = value
            elif label == "TEL":
                item[Schema.TEL] = value
            elif label == "FAX":
                item["FAX"] = value
            elif label in ("ホームページ", "HP", "Webサイト", "URL"):
                a_tag = elem.find("a", href=True)
                if a_tag:
                    item[Schema.HP] = a_tag["href"]
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

    scraper = KagoshimaShakouScraper()
    scraper.execute("https://www.kagoshima-pub.or.jp/category/kagoshima/")

    print("\n" + "=" * 60)
    print("実行結果サマリ")
    print("=" * 60)
    print(f"  出力ファイル:     {scraper.output_filepath}")
    print(f"  取得件数:         {scraper.item_count}")
    print(f"  観測カラム数:     {len(scraper.observed_columns)}")
    print(f"  観測カラム:       {scraper.observed_columns}")
    print("=" * 60)
