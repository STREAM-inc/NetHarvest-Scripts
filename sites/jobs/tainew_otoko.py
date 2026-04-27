import re
import sys
import unicodedata
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://tainew-otoko.com"
SEARCH_URL = "https://tainew-otoko.com/shoplist/search/?salary_type=0_1&word=&page={}"
MAX_PAGES = 50

KEY_ALIAS = {
    "店舗URL": Schema.HP, "ホームページ": Schema.HP, "URL": Schema.HP,
    "公式サイト": Schema.HP, "公式HP": Schema.HP, "HP": Schema.HP,
}


def _norm(text) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", text)
    return re.sub(r"\s+", " ", text).strip()


class TainewOtokoScraper(StaticCrawler):
    """メンズ体入（たいニュー）スクレイパー（tainew-otoko.com）"""

    DELAY = 0.5
    EXTRA_COLUMNS = ["業種", "エリア", "SNS"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls = self._collect_detail_urls()
        self.total_items = len(detail_urls)
        self.logger.info("店舗URL収集完了: %d 件", len(detail_urls))
        for detail_url in detail_urls:
            item = self._scrape_detail(detail_url)
            if item:
                yield item

    def _collect_detail_urls(self) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        for page in range(1, MAX_PAGES + 1):
            soup = self.get_soup(SEARCH_URL.format(page))
            if soup is None:
                break

            found = 0
            for a in soup.select("a[href]"):
                href = a.get("href", "")
                if "/shopdetail/" in href:
                    full = href if href.startswith("http") else urljoin(BASE_URL, href)
                    if full not in seen:
                        seen.add(full)
                        urls.append(full)
                        found += 1

            if found == 0:
                break

        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        # Name from h1 or heading
        h1 = soup.select_one("h1.shopname, h1.shop-name, h1.title, h1")
        if h1:
            data[Schema.NAME] = _norm(h1.get_text())

        # Tags (業種/エリア/店舗規模)
        tags = [_norm(sp.get_text()) for sp in soup.select("span.tagWrap, div.tagWrap span")]
        if tags:
            data["業種"] = "|".join(tags)

        # table info
        for tr in soup.select("table tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            key = _norm(th.get_text())
            val = _norm(td.get_text(" "))
            if "住所" in key or "所在地" in key:
                data[Schema.ADDR] = val
            elif key in ("TEL", "電話番号", "ＴＥＬ"):
                data[Schema.TEL] = val
            elif "営業時間" in key:
                data[Schema.TIME] = val
            elif "定休日" in key:
                data[Schema.HOLIDAY] = val
            elif Schema.HP not in data:
                mapped = KEY_ALIAS.get(key)
                if mapped == Schema.HP:
                    a = td.find("a", href=True)
                    data[Schema.HP] = a["href"] if a else val
            elif "SNS" in key or "ＳＮＳ" in key:
                data["SNS"] = val
            elif "エリア" in key:
                data["エリア"] = val

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    TainewOtokoScraper().execute("https://tainew-otoko.com/shoplist/search/?salary_type=0_1&word=")
