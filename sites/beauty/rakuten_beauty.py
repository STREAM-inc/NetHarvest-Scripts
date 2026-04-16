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


class RakutenBeautyScraper(StaticCrawler):
    """楽天ビューティ 店舗情報スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["掲載台数"]

    _SHOP_PATTERN = re.compile(r"^https://beauty\.rakuten\.co\.jp/s\d+/$")

    def parse(self, url: str) -> Generator[dict, None, None]:
        """サイトマップインデックスから店舗URL(s数字)を収集してスクレイプ"""
        shop_urls = self._collect_shop_urls(url)
        self.total_items = len(shop_urls)
        self.logger.info("店舗URL収集完了: %d 件", len(shop_urls))

        for shop_url in shop_urls:
            item = self._scrape_detail(shop_url)
            if item:
                yield item

    def _collect_shop_urls(self, index_url: str) -> list[str]:
        """サイトマップインデックス → 子サイトマップ → 店舗URL(s数字)"""
        urls = []
        try:
            resp = self.session.get(index_url, timeout=self.TIMEOUT)
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
            child_locs = [el.text.strip() for el in root.iter() if el.tag.endswith("loc")]

            for child_url in child_locs:
                try:
                    r = self.session.get(child_url, timeout=self.TIMEOUT)
                    r.raise_for_status()
                    child_root = ET.fromstring(r.content)
                    for loc_el in child_root.iter():
                        if loc_el.tag.endswith("loc") and loc_el.text:
                            u = loc_el.text.strip()
                            if self._SHOP_PATTERN.match(u):
                                urls.append(u)
                except Exception as e:
                    self.logger.warning("子サイトマップ取得エラー %s: %s", child_url, e)
        except Exception as e:
            self.logger.warning("サイトマップインデックス取得エラー: %s", e)
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        name_tag = soup.select_one("td.c-table__body")
        if name_tag:
            data[Schema.NAME] = name_tag.get_text(strip=True).replace("\n", "")

        cat_tag = soup.select_one(
            "a.c-breadcrumbs__link[href*='/relax/'], "
            "a.c-breadcrumbs__link[href*='/nail/'], "
            "a.c-breadcrumbs__link[href*='/hair/']"
        )
        if cat_tag:
            data[Schema.CAT_SITE] = cat_tag.get_text(strip=True)

        tel_tag = soup.select_one("span.m-shopDetailInfo__tel")
        if tel_tag:
            data[Schema.TEL] = tel_tag.get_text(strip=True)

        for th in soup.select("th.c-table__head"):
            label = th.get_text(strip=True)
            td = th.find_next("td")
            if not td:
                continue
            if "定休日" in label:
                data[Schema.HOLIDAY] = td.get_text(strip=True)
            elif "営業時間" in label:
                data[Schema.TIME] = td.get_text(strip=True)
            elif "住所" in label:
                data[Schema.ADDR] = td.get_text(" ", strip=True)

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    RakutenBeautyScraper().execute("https://beauty.rakuten.co.jp/sitemap.xml")
