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

_SHOP_PATTERN = re.compile(r"^https://chocolat\.work/[^/]+/[^/]+/shop/\d+/$")
NAME_FURI_RE = re.compile(r"^(.+?)[（(]([^）)]+)[）)]\s*$")


class TainyuChocolatScraper(StaticCrawler):
    """体入ショコラ（chocolat.work）ナイト系店舗情報スクレイパー"""

    DELAY = 5.0
    EXTRA_COLUMNS = ["名称_フリガナ", "業種", "職種", "最寄り駅"]

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
        seen: set[str] = set()
        queue = [sitemap_url]
        visited: set[str] = set()
        while queue:
            sm_url = queue.pop(0)
            if sm_url in visited:
                continue
            visited.add(sm_url)
            try:
                r = self.session.get(sm_url, timeout=self.TIMEOUT)
                if r.status_code == 404:
                    continue
                r.raise_for_status()
                root = ET.fromstring(r.content)
                locs = [el.text.strip() for el in root.iter() if el.tag.endswith("loc") and el.text]
                if root.tag.lower().endswith("sitemapindex"):
                    queue.extend(locs)
                else:
                    for u in locs:
                        if _SHOP_PATTERN.match(u) and u not in seen:
                            seen.add(u)
                            urls.append(u)
            except Exception as e:
                self.logger.debug("サイトマップスキップ %s: %s", sm_url, e)
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        # 名称・フリガナ
        name_td = soup.select_one("td.shop_name")
        if name_td:
            text = name_td.get_text(strip=True)
            m = NAME_FURI_RE.match(text)
            if m:
                data[Schema.NAME] = m.group(1).strip()
                data["名称_フリガナ"] = m.group(2).strip()
            else:
                data[Schema.NAME] = text

        # th→next td パターンでフィールド取得
        def get_th_td(label: str) -> str:
            for th in soup.find_all("th"):
                if label in th.get_text(strip=True):
                    td = th.find_next("td")
                    if td:
                        return td.get_text(strip=True)
            return ""

        gyoshu_a = soup.select_one("th:-soup-contains('業 種') + td a") if hasattr(soup, "select_one") else None
        if not gyoshu_a:
            # BS4 -soup-contains is not universal; use find instead
            for th in soup.find_all("th"):
                if "業" in th.get_text() and "種" in th.get_text():
                    td = th.find_next("td")
                    if td:
                        a = td.find("a")
                        data["業種"] = a.get_text(strip=True) if a else td.get_text(strip=True)
                    break

        shokusyu = get_th_td("職種")
        if shokusyu:
            data["職種"] = shokusyu

        hp_th = soup.find("th", string=lambda x: x and "WEBサイト" in x)
        if hp_th:
            hp_td = hp_th.find_next("td")
            if hp_td:
                a = hp_td.find("a", href=True)
                if a:
                    data[Schema.HP] = a["href"].strip()

        # TEL
        tel_p = soup.select_one("p.number")
        if tel_p:
            data[Schema.TEL] = tel_p.get_text(strip=True)

        # 最寄り駅
        eki = get_th_td("最寄り駅")
        if eki:
            data["最寄り駅"] = eki

        # 住所
        addr_th = soup.find("th", string=lambda x: x and "住" in x and "所" in x)
        if addr_th:
            addr_td = addr_th.find_next("td")
            if addr_td:
                for tag in addr_td.select("span.btn-text, a.map-btn"):
                    tag.decompose()
                addr = " ".join(addr_td.stripped_strings)
                addr = addr.replace("GoogleMAPを開く", "").replace("Google MAPを開く", "").strip()
                data[Schema.ADDR] = addr

        # 定休日
        teikyu = get_th_td("定休日")
        if teikyu:
            data[Schema.HOLIDAY] = teikyu.split("\n")[0].strip()

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    TainyuChocolatScraper().execute("https://chocolat.work/sitemap_index.xml")
