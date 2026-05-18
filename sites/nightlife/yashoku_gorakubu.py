import sys
from pathlib import Path
import re
import time
from typing import Generator
from bs4 import BeautifulSoup

# sys.path を調整（4階層上へ）
base_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(base_dir))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class YashokuGorakubuCrawler(StaticCrawler):
    """
    夜職ゴラクブ (https://yorushoku.jp/)
    ナイトワーク求人サイト
    """

    SITE_ID = "yashoku_gorakubu"
    BASE_URL = "https://yorushoku.jp"
    START_URL = "https://yorushoku.jp/job-list/"
    DELAY = 1.5
    EXTRA_COLUMNS = ["job_category", "position", "salary", "location", "article_id"]

    def prepare(self):
        self.job_categories = []

    def parse(self, url: str) -> Generator[dict, None, None]:
        """
        一覧ページ → 職種別ページ → 個別求人をスクレイピング
        """
        # ステップ1: 職種リストページを取得
        soup = self.get_soup(url)
        if not soup:
            self.logger.error(f"Failed to fetch {url}")
            return

        # ステップ2: 職種別ページのURLを抽出
        job_category_cards = soup.select("div.term-card-list.term-card-list-job .term-card.parent-term a")

        if not job_category_cards:
            self.logger.warning("No job category cards found")
            return

        self.logger.info(f"Found {len(job_category_cards)} job categories")

        job_category_urls = []
        for card in job_category_cards:
            href = card.get("href")
            if href:
                if not href.startswith("http"):
                    href = self.BASE_URL + href
                job_category_urls.append(href)

        # ステップ3: 各職種ページから求人を抽出
        item_count = 0
        for category_url in job_category_urls:
            self.logger.info(f"Processing category: {category_url}")
            page_num = 1

            while True:
                if page_num == 1:
                    url = category_url
                else:
                    url = category_url.rstrip("/") + f"/page/{page_num}/"

                soup = self.get_soup(url)
                if not soup:
                    self.logger.warning(f"Failed to fetch {url}")
                    break

                # 求人投稿（article）を抽出
                articles = soup.select("article[id^='post-'][class*='introduce']")

                if not articles:
                    self.logger.info(f"No articles found on page {page_num}, stopping pagination")
                    break

                self.logger.info(f"Found {len(articles)} articles on page {page_num}")

                for article in articles:
                    try:
                        item = self._parse_article(article, category_url)
                        if item:
                            yield item
                            item_count += 1
                    except Exception as e:
                        self.logger.warning(f"Error parsing article: {e}")
                        continue

                # ページネーション判定
                next_page_link = soup.select_one("a[rel='next']")
                if not next_page_link:
                    break

                page_num += 1
                time.sleep(self.DELAY)

        # 総件数設定
        self.total_items = item_count
        self.logger.info(f"Total items scraped: {item_count}")

    def _parse_article(self, article, category_url: str) -> dict | None:
        """
        個別の求人投稿（article）をパース
        """
        try:
            # 店舗名（name）
            shop_name_elem = article.select_one("h2.shop_name p")
            shop_name_text = shop_name_elem.get_text(strip=True) if shop_name_elem else ""

            # 応募先の企業/店舗名
            apply_box_name = article.select_one("ul#apply_box li.name span.arrow_box")
            company_name = None
            if apply_box_name:
                name_elem = apply_box_name.find_next()
                if name_elem:
                    company_name = name_elem.get_text(strip=True).replace("応募先", "").strip()

            if not company_name:
                company_name = shop_name_text

            # 概要セクション（dl.overview）から情報抽出
            overview_dl = article.select_one("dl.overview")
            if not overview_dl:
                return None

            # 業種
            job_category_text = self._extract_dt_dd(overview_dl, "業種")

            # 職種と給与
            jobtype_salary_elem = self._extract_dt_dd_element(overview_dl, "募集職種・給与")
            position = ""
            salary = ""

            if jobtype_salary_elem:
                jobtype_span = jobtype_salary_elem.select_one("span.jobtype")
                salary_span = jobtype_salary_elem.select_one("span.salary")

                if jobtype_span:
                    position = jobtype_span.get_text(strip=True)
                if salary_span:
                    salary = salary_span.get_text(strip=True)

            # 勤務地（address）
            address = self._extract_dt_dd(overview_dl, "勤務地")

            # 電話番号
            phone = ""
            phone_link = article.select_one("ul#apply_box li.tel a[href^='tel:']")
            if phone_link:
                phone = phone_link.get_text(strip=True)

            # メールアドレス
            email = ""
            email_link = article.select_one("ul#apply_box li.btns a[href^='mailto:']")
            if email_link:
                href = email_link.get("href", "")
                match = re.search(r"mailto:([^?&]+)", href)
                if match:
                    email = match.group(1)

            # URL（詳細ページへのリンク）
            url = ""
            detail_link = article.select_one("ul#apply_box li.btns a.apply")
            if detail_link:
                url = detail_link.get("href", "")

            # 記事IDをキーとして使用
            article_id = article.get("id", "").replace("post-", "")

            return {
                Schema.URL: url,
                Schema.NAME: company_name,
                Schema.ADDR: address,
                Schema.PHONE: phone,
                Schema.EMAIL: email,
                "job_category": job_category_text,
                "position": position,
                "salary": salary,
                "location": shop_name_text,
                "article_id": article_id,
            }

        except Exception as e:
            self.logger.error(f"Error parsing article: {e}")
            return None

    @staticmethod
    def _extract_dt_dd(dl_elem, dt_text: str) -> str:
        """dt テキストに対応する dd の内容を抽出"""
        for dt in dl_elem.find_all("dt"):
            if dt.get_text(strip=True) == dt_text:
                dd = dt.find_next("dd")
                if dd:
                    return dd.get_text(strip=True)
        return ""

    @staticmethod
    def _extract_dt_dd_element(dl_elem, dt_text: str):
        """dt テキストに対応する dd 要素を返す"""
        for dt in dl_elem.find_all("dt"):
            if dt.get_text(strip=True) == dt_text:
                dd = dt.find_next("dd")
                if dd:
                    return dd
        return None


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    crawler = YashokuGorakubuCrawler()
    crawler.execute(YashokuGorakubuCrawler.START_URL)
