# ===== 【pip install -e . を実行していない場合のみ必要】===========
import sys
from pathlib import Path
_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
# ================================================================

import json
import re
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class HospitaScraper(StaticCrawler):
    """
    ホスピタ（hospita.jp）スクレイパー

    - 一覧1ページ取得 → 詳細20件を5並列で即取得 を繰り返す
    - 実行開始直後からCSVに書き出し（リアルタイム出力）
    """

    DELAY     = 0.5   # 一覧ページ間のウェイト（詳細は並列なのでウェイトなし）
    WORKERS   = 10    # 詳細ページの並列数
    EXTRA_COLUMNS = [
        "アクセス",
    ]

    def parse(self, url: str):

        # ===== 1ページ目から総件数取得 =====
        soup = self.get_soup(url)

        count_el = soup.select_one('.list-result-count')
        if not count_el:
            raise ValueError("件数が取得できませんでした: .list-result-count が見つかりません")

        total_count = int(re.sub(r'[^\d]', '', count_el.get_text()))
        total_pages = math.ceil(total_count / 20)
        self.total_items = total_count
        self.logger.info("総件数: %s / 総ページ数: %s", total_count, total_pages)

        # ===== 1ページごとに一覧取得 → 詳細を並列スクレイプ =====
        for page_num in range(1, total_pages + 1):
            page_url = f"{url}?page={page_num}"
            self.logger.info("一覧ページ取得中 [%d/%d]: %s", page_num, total_pages, page_url)

            try:
                page_soup = self.get_soup(page_url)
            except Exception as e:
                self.logger.warning("一覧ページ取得に失敗しました: %s / %s", page_url, e)
                continue

            cards = page_soup.select('.list-result-item.list-item-hospital')
            if not cards:
                self.logger.warning("カードが取得できませんでした: %s", page_url)
                continue

            # このページの詳細URLを重複なく収集
            seen = set()
            detail_urls = []
            for card in cards:
                link = card.select_one('a[href*="/hospital/"]')
                if not link:
                    continue
                detail_url = urljoin(url, link['href'])
                if detail_url not in seen:
                    seen.add(detail_url)
                    detail_urls.append(detail_url)

            # 詳細ページを並列取得して順次 yield
            with ThreadPoolExecutor(max_workers=self.WORKERS) as executor:
                futures = {executor.submit(self._fetch_detail, u): u for u in detail_urls}
                for future in as_completed(futures):
                    item = future.result()
                    if item:
                        yield item

            time.sleep(self.DELAY)

    def _fetch_detail(self, url: str) -> dict | None:
        """
        詳細ページを取得してdictで返す（スレッドセーフ・yield不使用）
        名称・住所・業種は JSON-LD 構造化データから取得（ページ差異に依存しない）
        """
        try:
            self.logger.info("詳細ページ取得中: %s", url)
            resp = requests.get(
                url,
                headers={"User-Agent": self.USER_AGENT},
                timeout=self.TIMEOUT,
            )
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            # ===== JSON-LD から各フィールドを取得 =====
            name, addr, genre, tel, post_code = '', '', '', '', ''
            for script in soup.find_all('script', type='application/ld+json'):
                try:
                    ld = json.loads(script.string or '')
                    if ld.get('@type') == 'MedicalOrganization':
                        name = ld.get('name', '')

                        addr_obj = ld.get('address', {})
                        post_code = addr_obj.get('postalCode', '')
                        addr = (
                            addr_obj.get('addressRegion', '') +
                            addr_obj.get('addressLocality', '') +
                            addr_obj.get('streetAddress', '')
                        )

                        specialties = ld.get('medicalSpecialty', [])
                        genre = '、'.join(specialties) if isinstance(specialties, list) else str(specialties)

                        tel = ld.get('telephone', '')
                        break
                except Exception:
                    continue

            # ===== 診療時間（.hour-table）=====
            hour_el = soup.select_one('.hour-table')
            business_time = hour_el.get_text(' ', strip=True) if hour_el else ''

            # ===== アクセス（「【アクセス方法】」ラベルを除去）=====
            access_el = soup.select_one('div.access')
            access = ''
            if access_el:
                access = re.sub(r'【アクセス方法】\s*', '', access_el.get_text(' ', strip=True)).strip()

            return {
                Schema.URL:       url,
                Schema.NAME:      name,
                Schema.POST_CODE: post_code,
                Schema.ADDR:      addr,
                Schema.TEL:       tel,
                Schema.TIME:      business_time,
                Schema.CAT_SITE:  genre,
                'アクセス':          access,
            }

        except Exception as e:
            self.logger.warning("詳細取得に失敗しました: %s / %s", url, e)
            return None


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)

    HospitaScraper().execute(
        'https://www.hospita.jp/list/hospital/'
    )
