import sys
from pathlib import Path
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from typing import Optional
import time

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "bin"))
from crawler_base import StaticCrawler, Schema, register_crawler

class SnackNaviCrawler(StaticCrawler):
    """
    スナックナビ クローラー
    東京のスナック店舗求人情報を取得
    """

    def __init__(self):
        super().__init__(site_id="snack_navi")
        self.base_url = "https://snacknavi.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.DELAY = 1.5

    def parse(self):
        """一覧ページと詳細ページから店舗情報を取得"""
        page = 1
        total_pages_detected = False

        while True:
            list_url = f"{self.base_url}/girl_top.php?page={page}"
            self.logger.info(f"Fetching page {page}: {list_url}")

            try:
                response = self.session.get(list_url, timeout=10)
                response.raise_for_status()
            except requests.RequestException as e:
                self.logger.error(f"Failed to fetch {list_url}: {e}")
                break

            soup = BeautifulSoup(response.content, 'html.parser')

            # 初回ページで総件数を設定
            if not total_pages_detected:
                # 総件数を取得（例："東京7,397件のスナック" から抽出）
                text_elements = soup.find_all(['p', 'div', 'span'])
                total_count = None
                for elem in text_elements:
                    text = elem.get_text()
                    if '件のスナック' in text or '件の' in text:
                        import re
                        match = re.search(r'(\d+)件', text)
                        if match:
                            total_count = int(match.group(1).replace(',', ''))
                            break

                if total_count:
                    self.total_items = total_count
                    self.logger.info(f"Total items estimated: {total_count}")
                total_pages_detected = True

            # 一覧ページから店舗リンクを抽出
            shop_links = []
            for link in soup.find_all('a', href=True):
                href = link.get('href')
                # /rec/0/{shop_id}/ パターンを抽出
                if href.startswith('/rec/0/') and href.endswith('/'):
                    if href not in [s['href'] for s in shop_links]:
                        shop_links.append({'href': href, 'text': link.get_text(strip=True)})

            if not shop_links:
                self.logger.info(f"No shop links found on page {page}. Assuming end of pages.")
                break

            self.logger.info(f"Found {len(shop_links)} shops on page {page}")

            # 各店舗の詳細ページを取得
            for shop_link in shop_links:
                try:
                    detail_url = self.base_url + shop_link['href']
                    self._scrape_detail(detail_url)
                    time.sleep(self.DELAY)
                except Exception as e:
                    self.logger.warning(f"Error scraping {detail_url}: {e}")
                    continue

            page += 1

            # 安全装置：1000ページ以上の場合は中断
            if page > 1000:
                self.logger.warning("Reached 1000 pages, stopping crawl")
                break

    def _scrape_detail(self, detail_url: str):
        """店舗詳細ページから情報を抽出"""
        try:
            response = self.session.get(detail_url, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch {detail_url}: {e}")
            return

        soup = BeautifulSoup(response.content, 'html.parser')

        record = {}

        # 店舗名
        name_elem = soup.find('h1') or soup.find(['h2', 'title'])
        shop_name = name_elem.get_text(strip=True) if name_elem else None
        record[Schema.NAME] = shop_name

        # 住所
        address = None
        # 郵便番号と住所を含むテキストを探す
        for elem in soup.find_all(['p', 'div', 'dd']):
            text = elem.get_text(strip=True)
            if '〒' in text or '東京都' in text:
                # 最初の行のみを住所とする
                address_text = text.split('\n')[0] if '\n' in text else text
                if '営業時間' not in address_text:
                    address = address_text
                    break
        record[Schema.ADDRESS] = address

        # 電話番号
        phone = None
        for elem in soup.find_all(['p', 'div', 'dd', 'a']):
            text = elem.get_text(strip=True)
            if text and text.replace('-', '').replace('(', '').replace(')', '').replace(' ', '').isdigit():
                # 電話番号らしい形式
                if len(text) >= 10:
                    phone = text
                    break
            # or check href for tel:
            href = elem.get('href', '')
            if href.startswith('tel:'):
                phone = href.replace('tel:', '')
                break
        record[Schema.PHONE] = phone

        # 最寄駅（複数）
        stations = []
        station_text = None
        for elem in soup.find_all(['p', 'div', 'li']):
            text = elem.get_text(strip=True)
            if '駅' in text and len(text) < 100:
                station_text = text
                # 複数駅を抽出
                import re
                station_names = re.findall(r'([ぁ-ん\w一-龥]+駅)', text)
                stations.extend(station_names)

        record['最寄駅'] = ', '.join(set(stations)) if stations else station_text

        # 営業時間
        hours = None
        for elem in soup.find_all(['p', 'div', 'dd']):
            text = elem.get_text(strip=True)
            if any(x in text for x in ['営業時間', '営業', '時間']) or ('-' in text and ('時' in text or ':' in text)):
                # 最初の行
                hours = text.split('\n')[0] if '\n' in text else text
                break
        record['営業時間'] = hours

        # 定休日
        closed_days = None
        for elem in soup.find_all(['p', 'div', 'dd']):
            text = elem.get_text(strip=True)
            if '定休' in text or '休み' in text or '休業' in text:
                closed_days = text.split('\n')[0] if '\n' in text else text
                break
        record['定休日'] = closed_days

        # 給与（時給）
        wage = None
        for elem in soup.find_all(['p', 'div', 'dd', 'span']):
            text = elem.get_text(strip=True)
            if '円' in text and ('時' in text or '時間' in text or '給' in text):
                wage = text.split('\n')[0] if '\n' in text else text
                break
        record['給与'] = wage

        # 仕事内容
        job_type = None
        for elem in soup.find_all(['p', 'div', 'dd']):
            text = elem.get_text(strip=True)
            if 'スタッフ' in text or '職種' in text or '女性' in text:
                job_type = text.split('\n')[0] if '\n' in text else text
                if len(job_type) < 100:
                    break
        record['仕事内容'] = job_type

        # 年齢要件
        age_req = None
        for elem in soup.find_all(['p', 'div', 'dd']):
            text = elem.get_text(strip=True)
            if '才' in text and ('以上' in text or 'OK' in text):
                age_req = text.split('\n')[0] if '\n' in text else text
                break
        record['年齢要件'] = age_req

        # 経験要件
        exp_req = None
        for elem in soup.find_all(['p', 'div', 'dd']):
            text = elem.get_text(strip=True)
            if '未経験' in text or '経験' in text or 'OK' in text:
                if any(x in text for x in ['未経験', '経験者', '経験OK']):
                    exp_req = text.split('\n')[0] if '\n' in text else text
                    break
        record['経験要件'] = exp_req

        # シフト情報
        shift = None
        for elem in soup.find_all(['p', 'div', 'dd']):
            text = elem.get_text(strip=True)
            if '週' in text and '日' in text and ('OK' in text or 'ok' in text):
                shift = text.split('\n')[0] if '\n' in text else text
                break
        record['シフト'] = shift

        # 福利厚生
        benefits = None
        for elem in soup.find_all(['p', 'div', 'dd', 'li']):
            text = elem.get_text(strip=True)
            if '日払い' in text or 'ボーナス' in text or '福利' in text:
                benefits = text
                break
        record['福利厚生'] = benefits

        # Instagram
        instagram_url = None
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if 'instagram.com' in href:
                instagram_url = href
                break
        record['Instagram'] = instagram_url

        # 記録を保存
        self.save_record(record)
        self.logger.info(f"Saved: {shop_name or 'Unknown'}")

    def prepare(self):
        """初期化処理"""
        self.logger.info(f"Starting {self.site_id} crawler")

    def finalize(self):
        """終了処理"""
        self.session.close()
        self.logger.info(f"Finished {self.site_id} crawler")

# クローラー登録
register_crawler(SnackNaviCrawler)

if __name__ == "__main__":
    crawler = SnackNaviCrawler()
    crawler.run()
