# ===== 【pip install -e . を実行していない場合のみ必要】===========
import sys
from pathlib import Path
_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
# ================================================================

import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class AnokonoEhimeScraper(StaticCrawler):
    """
    あのこの愛媛（ano-kono.ehime.jp）スクレイパー

    - 一覧1ページ取得 → 詳細10件を10並列で即取得 を繰り返す
    - 実行開始直後からCSVに書き出し（リアルタイム出力）
    """

    DELAY   = 0.5   # 一覧ページ間のウェイト
    WORKERS = 10    # 詳細ページの並列数

    EXTRA_COLUMNS = [
        "職種",
        "アクセス",
        "受動喫煙対策",
        "雇用期間",
        "加入保険",
    ]

    BASE_URL = "https://ano-kono.ehime.jp"

    def parse(self, url: str):

        # ===== 総件数はJSレンダリングのため取得不可 → カードがなくなるまでページを巡回 =====
        self.logger.info("ページ巡回開始（件数はJS描画のため未取得）")

        page_num = 1
        while True:
            page_url = f"{self.BASE_URL}/search?contentType=ALL&page={page_num}"
            self.logger.info("一覧ページ取得中 [page=%d]: %s", page_num, page_url)

            try:
                page_soup = self.get_soup(page_url)
            except Exception as e:
                self.logger.warning("一覧ページ取得に失敗しました: %s / %s", page_url, e)
                page_num += 1
                continue

            cards = page_soup.select('.list-search-result-item')
            if not cards:
                self.logger.info("カードが0件のため巡回終了 (最終ページ: %d)", page_num)
                break

            # このページの詳細URLを重複なく収集
            seen = set()
            detail_urls = []
            for card in cards:
                link = card.select_one('a[href*="/job/"]')
                if not link:
                    continue
                detail_url = urljoin(self.BASE_URL, link['href'])
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

            page_num += 1
            time.sleep(self.DELAY)

    def _fetch_detail(self, url: str) -> dict | None:
        """
        詳細ページを取得してdictで返す（スレッドセーフ・yield不使用）
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

            # ===== 名称 =====
            name_el = soup.select_one('div.job-title')
            name = name_el.get_text(strip=True) if name_el else ''

            # ===== 住所 =====
            addr_el = soup.select_one('span.job-ttl-address')
            addr = addr_el.get_text(strip=True) if addr_el else ''

            # ===== 電話番号 =====
            tel_el = soup.select_one('a[href^="tel:"]')
            tel = tel_el.get_text(strip=True) if tel_el else ''

            # ===== 職種 =====
            shokushu_el = soup.select_one('h1.job-ttl-sub')
            shokushu = shokushu_el.get_text(strip=True) if shokushu_el else ''

            # ===== dt/dd マップ =====
            dt_map = {}
            for dt in soup.select('dt'):
                dd = dt.find_next_sibling('dd')
                if dd:
                    dt_map[dt.get_text(strip=True)] = dd.get_text(' ', strip=True)

            return {
                Schema.URL:      url,
                Schema.NAME:     name,
                Schema.ADDR:     addr,
                Schema.TEL:      tel,
                Schema.CAT_SITE: dt_map.get('業種・業態', ''),
                '職種':           shokushu,
                'アクセス':        dt_map.get('アクセス', ''),
                '受動喫煙対策':     dt_map.get('受動喫煙対策', ''),
                '雇用期間':        dt_map.get('雇用期間', ''),
                '加入保険':        dt_map.get('加入保険', ''),
            }

        except Exception as e:
            self.logger.warning("詳細取得に失敗しました: %s / %s", url, e)
            return None


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)

    AnokonoEhimeScraper().execute(
        'https://ano-kono.ehime.jp/search?searchWords.value=&locationWords.value='
    )
