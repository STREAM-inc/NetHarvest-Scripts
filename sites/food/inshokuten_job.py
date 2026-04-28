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

_SHOP_PATTERN = re.compile(r"^https://job\.inshokuten\.com/[^/]+/work/detail/\d+$")


class InshokutenJobScraper(StaticCrawler):
    """求人飲食店ドットコム（job.inshokuten.com）飲食店情報スクレイパー"""

    DELAY = 0.5
    EXTRA_COLUMNS = ["オープン日", "公開日", "客単価", "席数", "喫煙", "最寄駅", "運営", "特徴"]

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

        h1 = soup.select_one("div.shop-detail-area h1.title")
        if h1:
            name = h1.get_text(" ", strip=True)
            name = re.sub(r"\s*の求人情報.*$", "", name).strip()
            data[Schema.NAME] = name

        open_time = soup.select_one("time.shop-open-date")
        if open_time:
            text = open_time.get_text(" ", strip=True)
            m = re.search(r"オープン日：\s*([0-9]{4}年[0-9]{1,2}月[0-9]{1,2}日)", text)
            if m:
                data["オープン日"] = m.group(1)

        pub_time = soup.select_one("time.open-date")
        if pub_time:
            text = pub_time.get_text(" ", strip=True)
            m = re.search(r"公開日：\s*([0-9]{4}年[0-9]{1,2}月[0-9]{1,2}日)", text)
            if m:
                data["公開日"] = m.group(1)

        def get_by_th(label: str) -> str:
            th = soup.find("th", string=lambda x: x and x.strip() == label)
            if not th:
                return ""
            td = th.find_next("td")
            return td.get_text(" ", strip=True) if td else ""

        addr_th = soup.find("th", string=lambda x: x and x.strip() == "勤務地")
        if addr_th:
            td = addr_th.find_next("td")
            if td:
                for a in td.select("a.map-page"):
                    a.decompose()
                data[Schema.ADDR] = td.get_text(" ", strip=True).replace("[地図]", "").strip()

        data["客単価"] = get_by_th("客単価")
        data["席数"] = get_by_th("席数")
        data["喫煙"] = get_by_th("喫煙")
        data["最寄駅"] = get_by_th("最寄駅")
        data[Schema.HOLIDAY] = get_by_th("定休日")
        data["運営"] = get_by_th("運営")

        feats = [sp.get_text(strip=True) for sp in soup.select("span.ji-tag__characteristics")]
        feats = [f for f in feats if f]
        if feats:
            data["特徴"] = "|".join(feats)

        hp_a = soup.select_one("span.store-url a.accesslog[href]")
        if hp_a:
            data[Schema.HP] = hp_a.get("href", "").strip()

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    InshokutenJobScraper().execute("https://job.inshokuten.com/sitemapindex.xml")
