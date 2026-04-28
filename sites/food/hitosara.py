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

_SHOP_PATTERN = re.compile(r"^https://hitosara\.com/\d{4,}/$")


class HitosaraScraper(StaticCrawler):
    """ヒトサラ レストラン・飲食店情報スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["名称_フリガナ", "最寄駅", "平均予算", "お支払い情報", "キャパシティ", "駐車場"]

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
            if root.tag.lower().endswith("sitemapindex"):
                for el in root.iter():
                    if el.tag.endswith("loc") and el.text:
                        child_url = el.text.strip()
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
                for el in root.iter():
                    if el.tag.endswith("loc") and el.text:
                        u = el.text.strip()
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

        # 名称・フリガナ
        name_block = soup.select_one("div.shop-name")
        if name_block:
            h2 = name_block.find("h2")
            if h2:
                data[Schema.NAME] = h2.get_text(strip=True)
            rubi = name_block.find("p", class_="rubi")
            if rubi:
                data["名称_フリガナ"] = rubi.get_text(strip=True)

        # TEL (th="TEL" → td 内の p.phone_num)
        tel_th = soup.find("th", string=re.compile(r"^TEL"))
        if tel_th:
            tel_td = tel_th.find_next("td")
            if tel_td:
                phone_el = tel_td.select_one("p.phone_num.numinq") or tel_td.select_one("p.phone_num")
                if phone_el:
                    data[Schema.TEL] = phone_el.get_text(strip=True)

        # th テキストマッチで各フィールド
        field_map = {
            "最寄駅": "最寄駅",
            "住所": Schema.ADDR,
            "営業時間": Schema.TIME,
            "定休日": Schema.HOLIDAY,
            "平均予算": "平均予算",
            "お支払い情報": "お支払い情報",
            "キャパシティ": "キャパシティ",
            "駐車場": "駐車場",
        }
        for th in soup.find_all("th"):
            th_text = th.get_text(strip=True)
            for key, schema_key in field_map.items():
                if key in th_text and schema_key not in data:
                    td = th.find_next("td")
                    if td:
                        val = td.get_text(" ", strip=True).replace("地図を見る", "").strip()
                        data[schema_key] = val
                    break

        # HP
        hp_th = soup.find("th", string=re.compile(r"ホームページ"))
        if hp_th:
            hp_td = hp_th.find_next("td")
            if hp_td:
                a = hp_td.find("a", href=True)
                if a:
                    data[Schema.HP] = a["href"].strip()

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    HitosaraScraper().execute("https://hitosara.com/sitemap_index.xml")
