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

_SHOP_PATTERN = re.compile(r"^https://bilax\.net/\d+/?$")


class BilaxNetScraper(StaticCrawler):
    """びらくネット 店舗情報スクレイパー"""

    DELAY = 0.5
    EXTRA_COLUMNS = ["業種"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        shop_urls = self._collect_shop_urls(url)
        self.total_items = len(shop_urls)
        self.logger.info("店舗URL収集完了: %d 件", len(shop_urls))
        for shop_url in shop_urls:
            item = self._scrape_detail(shop_url)
            if item:
                yield item

    def _collect_shop_urls(self, sitemap_url: str) -> list[str]:
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
                                if _SHOP_PATTERN.match(u):
                                    urls.append(u)
                    except Exception:
                        pass
            else:
                for u in locs:
                    if _SHOP_PATTERN.match(u):
                        urls.append(u)
        except Exception as e:
            self.logger.warning("サイトマップ取得エラー: %s", e)
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        # 店名
        name_tag = soup.find("h1", class_="tenpo_name")
        if name_tag:
            data[Schema.NAME] = name_tag.get_text(strip=True)

        # 業種（パンくずリストの最後）
        bread_tags = soup.select("nav ol li span[itemprop='name']")
        if bread_tags:
            data["業種"] = bread_tags[-1].get_text(strip=True)

        # テーブルカラム解析
        for li in soup.select("div.table-column_g li.table-column"):
            img = li.find("img")
            if img:
                src = img.get("src", "")
                text = li.get_text(strip=True)
                if "todohuken.png" in src:
                    # 住所（都道府県含む）
                    data[Schema.ADDR] = text
                elif "denwa.png" in src:
                    # 電話番号（注記除去）
                    data[Schema.TEL] = text.replace("(びらくネットを見たとお伝えください)", "").strip()
                    continue
            # 定休日テキスト
            text = li.get_text(strip=True)
            if text.startswith("定休日："):
                data[Schema.HOLIDAY] = text.replace("定休日：", "").strip()
            # 営業時間 (p.m)
            p_m = li.select_one("p.m")
            if p_m and Schema.TIME not in data:
                data[Schema.TIME] = p_m.get_text(separator=" ", strip=True)

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    BilaxNetScraper().execute("https://bilax.net/sitemap.xml")
