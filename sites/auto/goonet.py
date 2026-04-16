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

_SHOP_PATTERN = re.compile(r"https://www\.goo-net\.com/usedcar/spread/goo/\d+/\d+0A\d+\.html")


class GoonetScraper(StaticCrawler):
    """グーネット 中古車販売店情報スクレイパー"""

    DELAY = 0.5
    EXTRA_COLUMNS = ["投稿数", "掲載台数", "FAX", "古物商許可番号", "所属グループ", "住所（代表）", "TEL（代表）"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        """サイトマップから販売店URLを収集してスクレイプ"""
        shop_urls = self._collect_shop_urls(url)
        self.total_items = len(shop_urls)
        self.logger.info("販売店URL収集完了: %d 件", len(shop_urls))
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

        name_tag = soup.find("h1", class_="item")
        if name_tag:
            data[Schema.NAME] = name_tag.get_text(strip=True)

        post_tag = soup.find("a", href=lambda x: x and "user_review" in x)
        if post_tag:
            ct = post_tag.find_next("em", class_="count")
            if ct:
                data["投稿数"] = ct.get_text(strip=True)

        stock_tag = soup.find("a", href=lambda x: x and "stock.html" in x)
        if stock_tag:
            em = stock_tag.find_next("em")
            if em:
                data["掲載台数"] = em.get_text(strip=True)

        ZENKAKU = str.maketrans("０１２３４５６７８９", "0123456789")
        table = soup.find("table", class_="tbl_type01")
        if table:
            for row in table.find_all("tr"):
                th = row.find("th")
                td = row.find("td")
                if not th or not td:
                    continue
                label = th.get_text(strip=True)
                val = td.get_text(strip=True).translate(ZENKAKU)

                if "住所" in label:
                    m = re.match(r"(〒\d{3}-\d{4})\s*(.+)", val)
                    if m:
                        data[Schema.POST_CODE] = m.group(1)
                        data[Schema.ADDR] = m.group(2)
                    else:
                        data[Schema.ADDR] = val
                elif label == "TEL":
                    data[Schema.TEL] = val
                elif "FAX" in label:
                    data["FAX"] = val
                elif "営業時間" in label:
                    data[Schema.TIME] = val
                elif "定休日" in label:
                    data[Schema.HOLIDAY] = val
                elif "事業内容" in label:
                    data[Schema.LOB] = val
                elif "従業員数" in label:
                    data[Schema.EMP_NUM] = val
                elif "古物商" in label:
                    data["古物商許可番号"] = val
                elif "グループ" in label:
                    data["所属グループ"] = val

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    GoonetScraper().execute("https://www.goo-net.com/sitemap_index.xml.gz")
