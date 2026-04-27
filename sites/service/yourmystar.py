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

_SUPPLIER_PATTERN = re.compile(r"^https://yourmystar\.jp/suppliers/[^/]+/$")


class YourmystarScraper(StaticCrawler):
    """ユアマイスター 業者情報スクレイパー"""

    DELAY = 0.5
    EXTRA_COLUMNS = ["評価", "口コミ件数", "対応地域"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        supplier_urls = self._collect_supplier_urls(url)
        self.total_items = len(supplier_urls)
        self.logger.info("業者URL収集完了: %d 件", len(supplier_urls))
        for su in supplier_urls:
            item = self._scrape_detail(su)
            if item:
                yield item

    def _collect_supplier_urls(self, sitemap_url: str) -> list[str]:
        urls: list[str] = []
        try:
            r = self.session.get(sitemap_url, timeout=self.TIMEOUT)
            r.raise_for_status()
            root = ET.fromstring(r.content)
            locs = [el.text.strip() for el in root.iter() if el.tag.endswith("loc") and el.text]
            if root.tag.lower().endswith("sitemapindex"):
                for child_url in locs:
                    try:
                        cr = self.session.get(child_url, timeout=self.TIMEOUT)
                        cr.raise_for_status()
                        child_root = ET.fromstring(cr.content)
                        for loc in child_root.iter():
                            if loc.tag.endswith("loc") and loc.text:
                                u = loc.text.strip()
                                if _SUPPLIER_PATTERN.match(u):
                                    urls.append(u)
                    except Exception:
                        pass
            else:
                for u in locs:
                    if _SUPPLIER_PATTERN.match(u):
                        urls.append(u)
        except Exception as e:
            self.logger.warning("サイトマップ取得エラー: %s", e)
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        name_el = soup.select_one("a.supplier-name-block_link")
        if name_el:
            data[Schema.NAME] = name_el.get_text(strip=True)

        rep_el = soup.select_one(".supervisor-detail-block_name")
        if rep_el:
            data[Schema.REP_NM] = rep_el.get_text(strip=True)

        # 郵便番号・住所
        loc_el = soup.select_one(".address-block_text")
        if loc_el:
            raw = loc_el.get_text(" ", strip=True).replace("\xa0", " ").strip()
            m = re.search(r"〒?\s*(\d{3})-?(\d{4})", raw)
            if m:
                data[Schema.POST_CODE] = f"〒{m.group(1)}-{m.group(2)}"
                data[Schema.ADDR] = raw[m.end():].strip()
            else:
                data[Schema.ADDR] = raw

        time_el = soup.select_one(".business-hour-block_text")
        if time_el:
            data[Schema.TIME] = time_el.get_text(" ", strip=True)

        holiday_el = soup.select_one(".holiday-block_text")
        if holiday_el:
            data[Schema.HOLIDAY] = holiday_el.get_text(" ", strip=True)

        # 評価・口コミ件数
        score_el = soup.select_one('[data-element-id="element-review-score"]')
        if score_el:
            data["評価"] = score_el.get_text(strip=True)
        count_el = soup.select_one('[data-element-id="element-review-count"]')
        if count_el:
            txt = count_el.get_text(strip=True)
            m = re.search(r"(\d+)\s*件", txt)
            data["口コミ件数"] = f"{m.group(1)}件" if m else txt

        # 対応地域（都道府県を連結）
        covered = soup.select_one(".supplier-detail-block_covered-areas")
        if covered:
            prefs = [
                el.get_text(" ", strip=True)
                for el in covered.select(".prefecture-and-city-block_prefecture-name")
            ]
            if prefs:
                data["対応地域"] = " / ".join(prefs)

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    YourmystarScraper().execute("https://yourmystar.jp/sitemap.xml")
