import sys
from pathlib import Path
import time
from typing import Generator

# sys.path を調整（4階層上へ）
base_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(base_dir))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class NightlyJobCrawler(StaticCrawler):
    """
    NIGHTLY（ナイトリー）- https://nightly.jp/job/
    ナイトワーク求人サイト（スナック、ガールズバー、キャバクラ等）
    """

    SITE_ID = "nightly_job"
    BASE_URL = "https://nightly.jp"
    START_URL = "https://nightly.jp/job/"
    DELAY = 1.5
    EXTRA_COLUMNS = ["position", "salary", "station", "shop_type", "pr_text", "salary_regular", "business_hours", "min_attendance", "job_id"]

    def prepare(self):
        pass

    def parse(self, url: str) -> Generator[dict, None, None]:
        """
        ページネーション対応で全ページの求人を取得
        """
        page = 1
        item_count = 0
        has_items = True

        while has_items:
            if page == 1:
                current_url = url
            else:
                current_url = f"{self.BASE_URL}/job/page/{page}/"

            self.logger.info(f"Processing page {page}: {current_url}")

            soup = self.get_soup(current_url)
            if not soup:
                self.logger.warning(f"Failed to fetch {current_url}")
                break

            # article.shop_flame を全て抽出
            articles = soup.select("article.shop_flame")

            if not articles:
                self.logger.info(f"No articles found on page {page}, stopping pagination")
                break

            self.logger.info(f"Found {len(articles)} articles on page {page}")

            for article in articles:
                try:
                    item = self._parse_article(article)
                    if item:
                        yield item
                        item_count += 1
                except Exception as e:
                    self.logger.warning(f"Error parsing article: {e}")
                    continue

            page += 1
            time.sleep(self.DELAY)

        # 総件数を設定
        self.total_items = item_count
        self.logger.info(f"Total items scraped: {item_count}")

    def _parse_article(self, article) -> dict | None:
        """
        個別の求人アイテム（article.shop_flame）をパース
        """
        try:
            # 店舗名（NAME）
            shop_name_elem = article.select_one("h3")
            shop_name = shop_name_elem.get_text(strip=True) if shop_name_elem else ""

            if not shop_name:
                return None

            # 地域（ADDRESS）
            area_elem = article.select_one("p.area")
            area = ""
            if area_elem:
                # <i> タグを除去して、テキストのみを抽出
                for i in area_elem.find_all("i"):
                    i.decompose()
                area = area_elem.get_text(strip=True)

            # 駅（EXTRA: station）
            station_elem = article.select_one("p.station")
            station = ""
            if station_elem:
                for i in station_elem.find_all("i"):
                    i.decompose()
                station = station_elem.get_text(strip=True)

            # 店舗タイプ（EXTRA: shop_type）
            shop_type = ""
            shop_type_elem = article.select_one("p.type")
            if shop_type_elem:
                shop_type = shop_type_elem.get_text(strip=True)

            # PR文（EXTRA: pr_text）
            pr_text = ""
            pr_elem = article.select_one("p.pr")
            if pr_elem:
                pr_text = pr_elem.get_text(strip=True)

            # 職種（POSITION）
            position = ""
            # dl.jobtype を探す
            jobtype_dls = article.select("div.set2 dl.jobtype")
            if jobtype_dls:
                dd = jobtype_dls[0].select_one("dd")
                if dd:
                    position = dd.get_text(strip=True)

            # 体験入店時給（SALARY）
            salary = ""
            # 最初のdl.jobpay を探す（体験入店）
            jobpay_dls = article.select("div.set2 dl.jobpay")
            if jobpay_dls:
                dd = jobpay_dls[0].select_one("dd")
                if dd:
                    salary = dd.get_text(strip=True)

            # 本入店時給（EXTRA: salary_regular）
            salary_regular = ""
            if len(jobpay_dls) >= 2:
                dd = jobpay_dls[1].select_one("dd")
                if dd:
                    salary_regular = dd.get_text(strip=True)

            # 営業時間（EXTRA: business_hours）
            business_hours = ""
            open_dl = article.select_one("div.set1 dl.open")
            if open_dl:
                dd = open_dl.select_one("dd")
                if dd:
                    business_hours = dd.get_text(strip=True)

            # 最低出勤（EXTRA: min_attendance）
            min_attendance = ""
            pay_dl = article.select_one("div.set1 dl.pay")
            if pay_dl:
                dd = pay_dl.select_one("dd")
                if dd:
                    min_attendance = dd.get_text(strip=True)

            # URL
            url = ""
            url_link = article.select_one("a[href]")
            if url_link:
                href = url_link.get("href", "")
                if href:
                    if not href.startswith("http"):
                        href = self.BASE_URL + href
                    url = href

            # 記事ID（URLから抽出）
            job_id = ""
            if url:
                import re
                match = re.search(r'/shop(\d+)/', url)
                if match:
                    job_id = match.group(1)

            if not job_id:
                job_id = shop_name  # フォールバック

            return {
                Schema.URL: url,
                Schema.NAME: shop_name,
                Schema.ADDR: area,
                Schema.PHONE: "",
                Schema.EMAIL: "",
                "position": position,
                "salary": salary,
                "station": station,
                "shop_type": shop_type,
                "pr_text": pr_text,
                "salary_regular": salary_regular,
                "business_hours": business_hours,
                "min_attendance": min_attendance,
                "job_id": job_id,
            }

        except Exception as e:
            self.logger.error(f"Error parsing article: {e}")
            return None


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    crawler = NightlyJobCrawler()
    crawler.execute(NightlyJobCrawler.START_URL)
