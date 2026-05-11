"""
スナックナビ — 全国スナック店舗求人スクレイパー

取得フロー:
    東京: /rec/{areaID}/ → /rec/{areaID}&p=N/ → /rec/{areaID}/{shopID}/
    全国: /{region}/girl_top.php → /{region}/rec.php?a=1&i={prefID}
          → /{region}/rec.html?p=N&a=1&i={prefID} → /{region}/recs{shopID}.html

実行方法:
    python scripts/sites/nightlife/snack_navi.py
    python bin/run_flow.py --site-id snack_navi
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Generator

from bs4 import BeautifulSoup

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler


class SnackNaviCrawler(StaticCrawler):
    """スナックナビ クローラー — 全国スナック店舗求人情報を取得"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["最寄駅", "給与", "仕事内容", "勤務時間", "資格", "待遇", "雇用形態"]

    BASE_URL = "https://snacknavi.com"

    # 東京 23エリアID（girl_top.php の onclick="location.href='/rec/NNNN/'" より）
    TOKYO_AREAS = [
        992, 1004, 1005, 1006, 1009, 1032, 1033, 1034, 1035, 1036,
        1037, 1038, 1039, 1040, 1041, 1042, 1043, 1044, 1045, 1046,
        1047, 1048, 1066,
    ]

    # 全国地方スラグ
    REGIONS = [
        "hokkaidou", "tohoku", "hokuriku", "kantou",
        "tokai", "kansai", "shikoku", "kyushu", "okinawa",
    ]

    # info-list-ttl ラベル → Schema / EXTRA_COLUMNS キー のマッピング
    _LABEL_MAP = {
        "店舗情報": Schema.ADDR,
        "最寄り駅": "最寄駅",
        "給与":     "給与",
        "仕事内容": "仕事内容",
        "勤務時間": "勤務時間",
        "資格":     "資格",
        "待遇":     "待遇",
        "雇用形態": "雇用形態",
    }

    def parse(self, url: str) -> Generator:
        seen: set[str] = set()

        # 1. 東京 23エリア
        for area_id in self.TOKYO_AREAS:
            yield from self._crawl_tokyo_area(area_id, seen)

        # 2. 全国9地方
        for region in self.REGIONS:
            yield from self._crawl_region(region, seen)

    # ------------------------------------------------------------------
    # 東京エリア
    # ------------------------------------------------------------------

    def _crawl_tokyo_area(self, area_id: int, seen: set) -> Generator:
        page = 1
        while True:
            list_url = (
                f"{self.BASE_URL}/rec/{area_id}/"
                if page == 1
                else f"{self.BASE_URL}/rec/{area_id}&p={page}/"
            )
            soup = self._fetch(list_url)
            if soup is None:
                break

            pattern = re.compile(rf"^/rec/{area_id}/\d+/$")
            shop_urls = [
                self.BASE_URL + a["href"]
                for a in soup.find_all("a", href=pattern)
            ]

            if not shop_urls:
                break

            for shop_url in shop_urls:
                if shop_url not in seen:
                    seen.add(shop_url)
                    record = self._scrape_detail(shop_url)
                    if record:
                        yield record

            if not self._has_next_page(soup):
                break
            page += 1

    # ------------------------------------------------------------------
    # 全国地方
    # ------------------------------------------------------------------

    def _crawl_region(self, region: str, seen: set) -> Generator:
        top_url = f"{self.BASE_URL}/{region}/girl_top.php"
        soup = self._fetch(top_url)
        if soup is None:
            return

        # 都道府県リンクを抽出（href と onclick の両方に対応）
        pref_pattern = re.compile(rf"/{region}/rec\.php\?a=1&i=(\d+)")
        pref_ids: list[int] = []
        seen_pref: set[int] = set()

        for a in soup.find_all("a", href=pref_pattern):
            m = pref_pattern.search(a["href"])
            if m:
                pid = int(m.group(1))
                if pid not in seen_pref:
                    seen_pref.add(pid)
                    pref_ids.append(pid)

        for elem in soup.find_all(onclick=True):
            m = pref_pattern.search(elem["onclick"])
            if m:
                pid = int(m.group(1))
                if pid not in seen_pref:
                    seen_pref.add(pid)
                    pref_ids.append(pid)

        for pref_id in pref_ids:
            yield from self._crawl_regional_pref(region, pref_id, seen)

    def _crawl_regional_pref(self, region: str, pref_id: int, seen: set) -> Generator:
        page = 1
        while True:
            list_url = (
                f"{self.BASE_URL}/{region}/rec.php?a=1&i={pref_id}"
                if page == 1
                else f"{self.BASE_URL}/{region}/rec.html?p={page}&a=1&i={pref_id}"
            )
            soup = self._fetch(list_url)
            if soup is None:
                break

            shop_pattern = re.compile(r"^recs\d+\.html$")
            shop_urls = [
                f"{self.BASE_URL}/{region}/{a['href']}"
                for a in soup.find_all("a", href=shop_pattern)
            ]

            if not shop_urls:
                break

            for shop_url in shop_urls:
                if shop_url not in seen:
                    seen.add(shop_url)
                    record = self._scrape_detail(shop_url)
                    if record:
                        yield record

            if not self._has_next_page(soup):
                break
            page += 1

    # ------------------------------------------------------------------
    # 共通ユーティリティ
    # ------------------------------------------------------------------

    def _fetch(self, url: str) -> BeautifulSoup | None:
        try:
            resp = self.session.get(url, timeout=15)
            resp.raise_for_status()
            return BeautifulSoup(resp.content, "html.parser")
        except Exception as e:
            self.logger.warning(f"Fetch failed: {url} — {e}")
            return None

    def _has_next_page(self, soup: BeautifulSoup) -> bool:
        return bool(soup.find("a", string=re.compile("次のページ")))

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self._fetch(detail_url)
        if soup is None:
            return None

        record: dict = {Schema.URL: detail_url}

        # 店舗名
        h2 = soup.select_one("div.ttl-shop-name h2")
        record[Schema.NAME] = h2.get_text(strip=True) if h2 else None

        # 電話番号
        tel_a = soup.select_one("p.ttl-shop-tel a[href^='tel:']")
        if tel_a:
            record[Schema.TEL] = tel_a.get_text(strip=True)

        # 郵便番号（住所テキストの先頭 〒NNN-NNNN から抽出）
        addr_header = soup.select_one("div.ttl-shop-info")
        if addr_header:
            raw = addr_header.get_text(" ", strip=True)
            m = re.search(r"〒(\d{3}-\d{4})", raw)
            if m:
                record[Schema.POST_CODE] = m.group(1)

        # info-list-ttl / info-list-txt ペアから各フィールドを取得
        for li in soup.find_all("li"):
            ttl_span = li.find("span", class_="info-list-ttl")
            txt_div = li.find("div", class_="info-list-txt")
            if not ttl_span or not txt_div:
                continue
            label = ttl_span.get_text(strip=True)
            value = txt_div.get_text(" ", strip=True)
            key = self._LABEL_MAP.get(label)
            if key:
                record[key] = value

        self.logger.info(f"Saved: {record.get(Schema.NAME) or detail_url}")
        return record


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    scraper = SnackNaviCrawler()
    scraper.execute("https://snacknavi.com/girl_top.php")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
