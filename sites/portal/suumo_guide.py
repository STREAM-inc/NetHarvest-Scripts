import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://suumo.jp"

PREF_SLUGS = [
    "hokkaido_", "aomori", "iwate", "miyagi", "akita", "yamagata", "fukushima",
    "ibaraki", "tochigi", "gumma", "saitama", "chiba", "tokyo", "kanagawa",
    "niigata", "toyama", "ishikawa", "fukui", "yamanashi", "nagano", "gifu",
    "shizuoka", "aichi", "mie", "shiga", "kyoto", "osaka", "hyogo", "nara",
    "wakayama", "tottori", "shimane", "okayama", "hiroshima", "yamaguchi",
    "tokushima", "kagawa", "ehime", "kochi", "fukuoka", "saga", "nagasaki",
    "kumamoto", "oita", "miyazaki", "kagoshima", "okinawa",
]


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class SuumoGuideScraper(StaticCrawler):
    """SUUMO 不動産会社ガイド スクレイパー（suumo.jp/jj/guide/）"""

    DELAY = 0.5
    EXTRA_COLUMNS = ["設立", "資本金", "売上高", "従業員数", "事業内容", "支店", "関連会社", "ブランド名"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls: list[str] = []
        seen: set[str] = set()
        for pref_slug in PREF_SLUGS:
            list_url = f"{BASE_URL}/jj/guide/{pref_slug}/city/"
            self.logger.info("都道府県: %s", pref_slug)
            urls = self._collect_detail_urls(list_url)
            for u in urls:
                if u not in seen:
                    seen.add(u)
                    detail_urls.append(u)
        self.total_items = len(detail_urls)
        self.logger.info("詳細URL収集完了: %d 件", len(detail_urls))
        for detail_url in detail_urls:
            item = self._scrape_detail(detail_url)
            if item:
                yield item

    def _collect_detail_urls(self, list_url: str) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        current = list_url
        while current:
            soup = self.get_soup(current)
            if soup is None:
                break
            for a in soup.select("div.mT10.l.icOrangeArrow14 a[href], a[href*='/jj/guide/shosai/']"):
                href = a.get("href", "").strip()
                full = urljoin(BASE_URL, href)
                if "/jj/guide/shosai/" in full and full not in seen:
                    seen.add(full)
                    urls.append(full)
            next_a = soup.select_one("a[rel='next'], li.pagination-next a")
            if next_a:
                href = next_a.get("href", "")
                next_url = urljoin(BASE_URL, href)
                current = next_url if next_url != current else None
            else:
                current = None
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        h3 = soup.select_one("h3.mT35.h3Ttl")
        if h3:
            data[Schema.NAME] = _clean(h3.get_text())

        table = soup.find("table", class_=lambda c: c and "data_table" in c)
        if table:
            for tr in table.find_all("tr"):
                ths = tr.find_all("th")
                tds = tr.find_all("td")
                for i in range(min(len(ths), len(tds))):
                    key = _clean(ths[i].get_text(" ", strip=True))
                    val = _clean(tds[i].get_text("\n", strip=True))
                    if key == "設立":
                        data["設立"] = val
                    elif key == "資本金":
                        data["資本金"] = val
                    elif key == "売上高":
                        data["売上高"] = val
                    elif key == "代表者名":
                        data[Schema.REP_NM] = val
                    elif key == "従業員数":
                        data["従業員数"] = val
                    elif key == "事業内容":
                        data["事業内容"] = val
                    elif key == "支店":
                        data["支店"] = val
                    elif key == "関連会社":
                        data["関連会社"] = val
                    elif key == "ブランド名":
                        data["ブランド名"] = val

        # 住所
        box = soup.select_one("div.w888.pT13.pH15.pB15.fs14.bdGuideDGray")
        if box:
            for p in box.find_all("p"):
                raw = _clean(p.get_text("\n", strip=True))
                if "[所在地]" in raw:
                    data[Schema.ADDR] = raw.replace("[所在地]", "").strip()
                    break

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    SuumoGuideScraper().execute("https://suumo.jp/jj/guide/")
