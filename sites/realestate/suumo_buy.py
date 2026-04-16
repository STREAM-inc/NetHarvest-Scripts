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


class SuumoBuyScraper(StaticCrawler):
    """SUUMO 新築・中古物件掲載不動産会社情報スクレイパー（suumo.jp/ms/）"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["免許番号", "取引形態"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_companies: set[str] = set()
        for pref_slug in PREF_SLUGS:
            self.logger.info("都道府県: %s", pref_slug)
            # 新築マンション
            city_url = f"{BASE_URL}/ms/shinchiku/{pref_slug}/city/"
            yield from self._scrape_area(city_url, pref_slug, seen_companies)

    def _scrape_area(self, city_url: str, pref_slug: str, seen: set) -> Generator[dict, None, None]:
        soup = self.get_soup(city_url)
        if soup is None:
            return

        sc_urls: list[str] = []
        needle = f"/ms/shinchiku/{pref_slug}/sc_"
        for a in soup.select("a[href]"):
            href = a.get("href", "").strip()
            if href.startswith(needle):
                full = urljoin(BASE_URL, href)
                if full not in sc_urls:
                    sc_urls.append(full)

        for sc_url in sc_urls:
            yield from self._scrape_listing(sc_url, seen)

    def _scrape_listing(self, list_url: str, seen: set) -> Generator[dict, None, None]:
        current = list_url
        while current:
            soup = self.get_soup(current)
            if soup is None:
                break
            for item_div in soup.select("div.property_unit, li.property_unit"):
                item = self._extract_contact(item_div, current)
                if item and item.get(Schema.NAME):
                    key = item[Schema.NAME]
                    if key not in seen:
                        seen.add(key)
                        yield item
            next_a = soup.select_one("a[rel='next'], a.pagination-parts:contains('次へ')")
            if next_a:
                href = next_a.get("href", "")
                next_url = urljoin(BASE_URL, href)
                current = next_url if next_url != current else None
            else:
                current = None

    def _extract_contact(self, item_div, page_url: str) -> dict | None:
        out = {Schema.URL: page_url}
        # お問い合せ先ブロックを探す
        for th in item_div.select("th"):
            t = _clean(th.get_text(" ", strip=True))
            if t not in ("お問い合せ先", "お問い合わせ先"):
                continue
            tr = th.find_parent("tr")
            if not tr:
                continue
            tds = tr.find_all("td")
            if not tds:
                continue
            left_td = tds[0]
            left_text = _clean(left_td.get_text(" ", strip=True))
            p0 = left_td.find("p")
            if p0:
                out[Schema.NAME] = _clean(p0.get_text(" ", strip=True))
            else:
                out[Schema.NAME] = left_text.split("TEL")[0].strip()
            m = re.search(r"TEL[:：]?\s*([0-9\-]+)", left_text)
            if m:
                out[Schema.TEL] = m.group(1)
            right_td = tds[-1]
            right_text = _clean(right_td.get_text(" ", strip=True))
            m2 = re.search(r"免許番号[:：]?\s*([^\s]+.*?号)", right_text)
            if m2:
                out["免許番号"] = _clean(m2.group(1))
            m3 = re.search(r"取引態様[:：]?\s*＜([^＞]+)＞", right_text)
            if m3:
                out["取引形態"] = _clean(m3.group(1))
            m4 = re.search(r"営業時間[:：]?\s*([^／]+)", right_text)
            if m4:
                out[Schema.TIME] = _clean(m4.group(1))
            m5 = re.search(r"定休日[:：]?\s*(.+)$", right_text)
            if m5:
                out[Schema.HOLIDAY] = _clean(m5.group(1))
            return out
        return None


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    SuumoBuyScraper().execute("https://suumo.jp/ms/shinchiku/")
