"""
ハウジングバザール — 工務店・ハウスメーカー情報スクレイパー

取得対象:
    - 全国の工務店・ハウスメーカー（約80社）の企業情報

取得フロー:
    一覧ページ(8ページ) -> 各企業の詳細ページ

実行方法:
    # ローカルテスト
    python scripts/sites/construction/housing_bazar.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id housing_bazar
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_BASE_URL = "https://www.housingbazar.jp"
_POSTCODE_RE = re.compile(r"〒(\d{3}-\d{4})\s*")
_PREF_RE = re.compile(r"(北海道|東京都|大阪府|京都府|.{2,3}[都道府県])")
_TOTAL_PAGES = 8


class HousingBazarScraper(StaticCrawler):
    """ハウジングバザール スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["施工エリア", "特徴タグ"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_ids: set[str] = set()
        last_page_idx = _TOTAL_PAGES - 1  # 初期値; 1ページ目の pager から上書き
        page_idx = 0

        # ページインデックスは0始まり: 1ページ目=インデックスなし, 2ページ目=page=1, ...
        while page_idx <= last_page_idx:
            page_url = f"{url}&page={page_idx}" if page_idx > 0 else url
            self.logger.info(f"一覧ページ取得: {page_url}")
            soup = self.get_soup(page_url)
            if not soup:
                break

            # 初回ページで最終ページインデックスを pager から動的取得
            if page_idx == 0:
                pager_links = soup.select('.pager a[data-d]')
                if pager_links:
                    last_data = pager_links[-1].get("data-d", "")
                    m = re.search(r"page=(\d+)", last_data)
                    if m:
                        last_page_idx = int(m.group(1))
                self.total_items = (last_page_idx + 1) * 10  # 推定

            links = soup.select('a[href*="/vendors/feature.php?id="]')
            if not links:
                self.logger.warning(f"リンクなし: {page_url}")
                break

            detail_urls = []
            for link in links:
                href = link.get("href", "")
                m = re.search(r"id=(\d+)", href)
                if m:
                    vid = m.group(1)
                    if vid not in seen_ids:
                        seen_ids.add(vid)
                        detail_urls.append(urljoin(_BASE_URL, href))

            for detail_url in detail_urls:
                time.sleep(self.DELAY)
                item = self._scrape_detail(detail_url)
                if item:
                    yield item

            page_idx += 1
            time.sleep(self.DELAY)

    def _scrape_detail(self, url: str) -> dict | None:
        try:
            soup = self.get_soup(url)
            if not soup:
                return None

            data: dict = {Schema.URL: url}

            # 会社名: h1から「〇〇の評判、評価...」パターンを除去して抽出
            h1 = soup.select_one("h1")
            if h1:
                name_text = h1.get_text(strip=True)
                name = re.sub(r"の評判.*$|の特徴.*$", "", name_text).strip()
                if name:
                    data[Schema.NAME] = name

            # th/td テーブル形式から基本情報を取得
            for tr in soup.select("tr"):
                th = tr.select_one("th")
                td = tr.select_one("td")
                if th and td:
                    key = th.get_text(strip=True)
                    val = " ".join(td.get_text(strip=True).split())
                    self._map_field(data, key, val)

            # dt/dd 定義リスト形式からも取得（th/td で取れなかった場合）
            for dt in soup.select("dt"):
                key = dt.get_text(strip=True)
                dd = dt.find_next_sibling("dd")
                if dd:
                    val = " ".join(dd.get_text(strip=True).split())
                    self._map_field(data, key, val)

            # 住所から郵便番号・都道府県・番地を分離
            if Schema.ADDR in data:
                addr_raw = data[Schema.ADDR]
                mp = _POSTCODE_RE.match(addr_raw)
                if mp:
                    data[Schema.POST_CODE] = mp.group(1)
                    addr_raw = addr_raw[mp.end():].strip()
                mp2 = _PREF_RE.match(addr_raw)
                if mp2:
                    data[Schema.PREF] = mp2.group(1)
                    data[Schema.ADDR] = addr_raw[mp2.end():].strip()
                else:
                    data[Schema.ADDR] = addr_raw

            # 特徴タグ: テーマアイコン画像のalt属性から収集
            theme_imgs = soup.select(
                '[class*="theme"] img[alt], [class*="feature"] img[alt], '
                '[class*="tag"] img[alt], [class*="point"] img[alt]'
            )
            if theme_imgs:
                alts = [img.get("alt", "").strip() for img in theme_imgs if img.get("alt", "").strip()]
                if alts:
                    data["特徴タグ"] = ", ".join(alts)

            return data if data.get(Schema.NAME) else None

        except Exception as e:
            self.logger.warning(f"詳細ページスキップ: {url} ({e})")
            return None

    def _map_field(self, data: dict, key: str, val: str) -> None:
        if not val:
            return
        if Schema.NAME not in data and ("企業名" in key or "会社名" in key):
            data[Schema.NAME] = val
        elif Schema.ADDR not in data and "住所" in key:
            data[Schema.ADDR] = val
        elif "施工エリア" in key or "対応エリア" in key:
            data["施工エリア"] = val
        elif "設立" in key:
            data[Schema.OPEN_DATE] = val
        elif "資本金" in key:
            data[Schema.CAP] = val
        elif "スタッフ" in key or "従業員" in key:
            data[Schema.EMP_NUM] = val
        elif "営業時間" in key:
            data[Schema.TIME] = val
        elif "定休日" in key:
            data[Schema.HOLIDAY] = val
        elif "事業内容" in key or "業種" in key:
            data[Schema.LOB] = val
        elif "電話" in key or "TEL" in key.upper():
            data[Schema.TEL] = val
        elif "HP" in key or "ホームページ" in key:
            data[Schema.HP] = val
        elif "代表" in key:
            data[Schema.REP_NM] = val


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = HousingBazarScraper()
    scraper.execute("https://www.housingbazar.jp/vendors/search.php?p[]=")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
