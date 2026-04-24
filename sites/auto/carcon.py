"""
カーコンビニ倶楽部 — 全国加盟店スクレイパー

取得対象:
    - sitemap.xml に列挙された /all/shop-XXXXXXX/ 形式の加盟店詳細ページ (約 519 件)

取得フロー:
    1. https://www.carcon.co.jp/sitemap.xml を取得し、shop URL を抽出
    2. 各詳細ページから店舗情報 (名称・住所・TEL・FAX・営業時間・休業日・対応サービス) を抽出

実行方法:
    python scripts/sites/auto/carcon.py
    python bin/run_flow.py --site-id carcon
"""

import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_SHOP_URL_RE = re.compile(r"https?://www\.carcon\.co\.jp/all/shop-(\d+)/?")
_SHOP_ID_RE = re.compile(r"/all/shop-(\d+)/?")
_PREF_RE = re.compile(r"^(北海道|東京都|大阪府|京都府|.{2,3}県)")
_TIME_RE = re.compile(r"(平日:\s*\d{1,2}:\d{2}\s*～\s*\d{1,2}:\d{2})\s*(平日以外:\s*\d{1,2}:\d{2}\s*～\s*\d{1,2}:\d{2})?")


class CarconScraper(StaticCrawler):
    """カーコンビニ倶楽部 加盟店スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["FAX", "対応サービス", "店舗ID"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        shop_urls = self._collect_shop_urls(url)
        self.total_items = len(shop_urls)
        self.logger.info("加盟店URL収集完了: %d 件", len(shop_urls))

        for shop_url in shop_urls:
            try:
                item = self._scrape_detail(shop_url)
            except Exception as e:
                self.logger.warning("詳細取得エラー %s: %s", shop_url, e)
                continue
            if item:
                yield item

    def _collect_shop_urls(self, sitemap_url: str) -> list[str]:
        try:
            r = self.session.get(sitemap_url, timeout=self.TIMEOUT)
            r.raise_for_status()
        except Exception as e:
            self.logger.error("サイトマップ取得失敗 %s: %s", sitemap_url, e)
            return []

        urls: list[str] = []
        try:
            root = ET.fromstring(r.content)
            for el in root.iter():
                if el.tag.endswith("loc") and el.text:
                    u = el.text.strip()
                    if _SHOP_URL_RE.match(u):
                        urls.append(u.rstrip("/") + "/")
        except ET.ParseError as e:
            self.logger.error("サイトマップ解析失敗: %s", e)
            return []

        return sorted(set(urls))

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}

        m = _SHOP_ID_RE.search(url)
        if m:
            data["店舗ID"] = m.group(1)

        h1 = soup.find("h1")
        if h1:
            raw = h1.get_text(strip=True)
            data[Schema.NAME] = raw.split("｜")[0].strip()

        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                th = row.find("th")
                td = row.find("td")
                if not th or not td:
                    continue
                label = th.get_text(strip=True)
                val = td.get_text(separator=" ", strip=True)
                if not val:
                    continue
                if "所在地" in label and Schema.ADDR not in data:
                    data[Schema.ADDR] = val
                    pm = _PREF_RE.match(val)
                    if pm:
                        data[Schema.PREF] = pm.group(1)
                elif label == "TEL" and Schema.TEL not in data:
                    data[Schema.TEL] = val
                elif "FAX" in label and "FAX" not in data:
                    data["FAX"] = val
                elif "営業時間" in label and Schema.TIME not in data:
                    data[Schema.TIME] = self._normalize_time(val)
                elif ("休業日" in label or "定休日" in label) and Schema.HOLIDAY not in data:
                    data[Schema.HOLIDAY] = val

        services = [dt.get_text(strip=True) for dt in soup.select("dl.tp-shop-accessory dt")]
        services = [s for s in services if s]
        if services:
            data["対応サービス"] = ", ".join(dict.fromkeys(services))

        data[Schema.LOB] = "自動車修理・板金塗装"
        data[Schema.CAT_SITE] = "カーコンビニ倶楽部"

        if not data.get(Schema.NAME):
            return None
        return data

    @staticmethod
    def _normalize_time(text: str) -> str:
        t = re.sub(r"\s+", " ", text).strip()
        m = _TIME_RE.search(t)
        if not m:
            return t
        weekday = m.group(1).strip()
        weekend = (m.group(2) or "").strip()
        return f"{weekday} / {weekend}" if weekend else weekday


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = CarconScraper()
    scraper.execute("https://www.carcon.co.jp/sitemap.xml")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
