import gzip
import io
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

_SHOP_PATTERN = re.compile(r"https://www\.goo-net\.com/pit/shop/.+/top$")


class GoonetPitScraper(StaticCrawler):
    """グーネットピット 自動車整備店情報スクレイパー"""

    DELAY = 0.5
    EXTRA_COLUMNS = ["法人名", "担当者名", "加盟団体"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        shop_urls = self._collect_shop_urls(url)
        self.total_items = len(shop_urls)
        self.logger.info("店舗URL収集完了: %d 件", len(shop_urls))
        for shop_url in shop_urls:
            item = self._scrape_detail(shop_url)
            if item:
                yield item

    def _fetch_xml(self, url: str) -> bytes | None:
        try:
            r = self.session.get(url, timeout=self.TIMEOUT)
            r.raise_for_status()
            data = r.content
            if url.endswith(".gz"):
                with gzip.GzipFile(fileobj=io.BytesIO(data)) as f:
                    data = f.read()
            return data
        except Exception as e:
            self.logger.warning("サイトマップ取得エラー %s: %s", url, e)
            return None

    def _collect_shop_urls(self, index_url: str) -> list[str]:
        urls = []
        data = self._fetch_xml(index_url)
        if not data:
            return urls
        root = ET.fromstring(data)
        child_locs = [el.text.strip() for el in root.iter() if el.tag.endswith("loc") and el.text]
        for child_url in child_locs:
            child_data = self._fetch_xml(child_url)
            if not child_data:
                continue
            try:
                child_root = ET.fromstring(child_data)
                for loc in child_root.iter():
                    if loc.tag.endswith("loc") and loc.text:
                        u = loc.text.strip()
                        if _SHOP_PATTERN.match(u):
                            urls.append(u)
            except Exception:
                pass
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        # 固定フィールド: dl.top_info
        for dl in soup.select("dl.top_info"):
            dt = dl.find("dt")
            dd = dl.find("dd")
            if not dt or not dd:
                continue
            key = dt.get_text(strip=True)
            val = dd.get_text(separator=" ", strip=True)
            if "住所" in key:
                m = re.match(r"(〒\d{3}-\d{4})\s*(.+)", val)
                if m:
                    data[Schema.POST_CODE] = m.group(1)
                    data[Schema.ADDR] = m.group(2)
                else:
                    data[Schema.ADDR] = val
            elif "営業時間" in key:
                data[Schema.TIME] = val
            elif "定休日" in key:
                data[Schema.HOLIDAY] = val

        # 動的フィールド: div.info_bottom table td (key:value with ：)
        info = soup.select_one("div.info_bottom")
        if info:
            for td in info.select("table tbody tr td"):
                text = re.sub(r"\s+", " ", td.get_text(separator=" ", strip=True)).strip()
                if "：" not in text:
                    continue
                key, val = text.split("：", 1)
                key = key.strip()
                val = val.strip()
                a = td.find("a", href=True)
                if a and a["href"].strip():
                    val = a["href"].strip()
                if "店名" in key or "名称" in key:
                    data.setdefault(Schema.NAME, val)
                elif "法人名" in key:
                    data.setdefault("法人名", val)
                elif "担当者" in key:
                    data.setdefault("担当者名", val)
                elif "TEL" in key or "電話" in key:
                    data.setdefault(Schema.TEL, val)
                elif "FAX" in key:
                    pass
                elif "加盟" in key:
                    data.setdefault("加盟団体", val)
                elif "ホームページ" in key or "HP" in key:
                    data.setdefault(Schema.HP, val)

        # 店名が div.info_bottom に無い場合は h1 等から
        if not data.get(Schema.NAME):
            h1 = soup.select_one("h1.shopName, h1.shop-name, h1")
            if h1:
                data[Schema.NAME] = h1.get_text(strip=True)

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    GoonetPitScraper().execute("https://www.goo-net.com/pit/sitemap_index.xml")
