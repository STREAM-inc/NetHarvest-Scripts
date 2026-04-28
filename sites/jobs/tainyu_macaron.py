import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Generator
from urllib.parse import urlparse, parse_qs

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

NAME_FURI_RE = re.compile(r"^(.+?)[（(]([^）)]+)[）)]\s*$")
PHONE_RE = re.compile(r"(?:0\d{1,4}-\d{1,4}-\d{3,4}|0\d{9,10})")


def _is_target_url(u: str) -> bool:
    p = urlparse(u)
    if p.netloc not in ("picsastock.com", "www.picsastock.com"):
        return False
    if p.path.rstrip("/") != "/kw":
        return False
    qs = parse_qs(p.query)
    return "q" in qs and len(qs["q"]) == 1 and re.fullmatch(r"\d+", qs["q"][0]) is not None


class TainyuMacaronScraper(StaticCrawler):
    """体入マカロン（picsastock.com）ナイト系店舗情報スクレイパー"""

    DELAY = 0.5
    EXTRA_COLUMNS = ["名称_フリガナ", "業種", "エリア", "最寄り駅", "掲載開始日", "掲載終了日", "応募方法", "応募電話"]

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
                        if _is_target_url(u) and u not in seen:
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
        block = soup.select_one("div.shop_detail") or soup
        table = block.select_one("table.shop_detail__table") or block

        def get_label(label: str) -> str:
            for th in table.find_all("th", class_="shop_detail__label"):
                if re.sub(r"\s+", " ", th.get_text(strip=True)) == label:
                    td = th.find_next("td")
                    if td:
                        return " ".join(td.stripped_strings)
            return ""

        name_raw = get_label("店名")
        if name_raw:
            m = NAME_FURI_RE.match(name_raw)
            if m:
                data[Schema.NAME] = m.group(1).strip()
                data["名称_フリガナ"] = m.group(2).strip()
            else:
                data[Schema.NAME] = name_raw

        data["業種"] = get_label("業種")
        data["エリア"] = get_label("エリア")
        data[Schema.ADDR] = get_label("住所")
        data["最寄り駅"] = get_label("最寄り駅")
        data[Schema.TIME] = get_label("営業時間")
        data[Schema.HOLIDAY] = get_label("定休日")

        # 掲載期間
        term = get_label("掲載期間")
        if term:
            parts = re.split(r"\s*[~〜]\s*", term)
            if len(parts) >= 1:
                data["掲載開始日"] = parts[0].strip()
            if len(parts) >= 2:
                right = parts[1].strip()
                m2 = re.search(r"(.+?)\s+(\d{1,2}:\d{2})$", right)
                data["掲載終了日"] = m2.group(1).strip() if m2 else right

        apply_val = get_label("応募方法")
        if apply_val:
            data["応募方法"] = apply_val
            m3 = PHONE_RE.search(apply_val)
            if m3:
                data["応募電話"] = m3.group(0)

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    TainyuMacaronScraper().execute("https://picsastock.com/sitemap.xml")
