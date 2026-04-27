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

PREF_MAP = {
    "hokkaido_": "北海道", "aomori": "青森県", "iwate": "岩手県", "miyagi": "宮城県",
    "akita": "秋田県", "yamagata": "山形県", "fukushima": "福島県", "ibaraki": "茨城県",
    "tochigi": "栃木県", "gumma": "群馬県", "saitama": "埼玉県", "chiba": "千葉県",
    "tokyo": "東京都", "kanagawa": "神奈川県", "niigata": "新潟県", "toyama": "富山県",
    "ishikawa": "石川県", "fukui": "福井県", "yamanashi": "山梨県", "nagano": "長野県",
    "gifu": "岐阜県", "shizuoka": "静岡県", "aichi": "愛知県", "mie": "三重県",
    "shiga": "滋賀県", "kyoto": "京都府", "osaka": "大阪府", "hyogo": "兵庫県",
    "nara": "奈良県", "wakayama": "和歌山県", "tottori": "鳥取県", "shimane": "島根県",
    "okayama": "岡山県", "hiroshima": "広島県", "yamaguchi": "山口県", "tokushima": "徳島県",
    "kagawa": "香川県", "ehime": "愛媛県", "kochi": "高知県", "fukuoka": "福岡県",
    "saga": "佐賀県", "nagasaki": "長崎県", "kumamoto": "熊本県", "oita": "大分県",
    "miyazaki": "宮崎県", "kagoshima": "鹿児島県", "okinawa": "沖縄県",
}


class SuumoScraper(StaticCrawler):
    """SUUMO 不動産会社情報スクレイパー（suumo.jp/kaisha/）"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["FAX", "関連サイト", "免許番号"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls: list[tuple[str, str]] = []
        for slug in PREF_SLUGS:
            pref_jp = PREF_MAP.get(slug, "")
            city_url = f"{BASE_URL}/kaisha/{slug}/city/"
            self.logger.info("都道府県: %s", pref_jp)
            city_urls = self._collect_city_urls(city_url)
            for city_url in city_urls:
                companies = self._collect_company_urls(city_url)
                for comp_url in companies:
                    detail_urls.append((pref_jp, comp_url))
        self.total_items = len(detail_urls)
        self.logger.info("会社URL収集完了: %d 件", len(detail_urls))
        seen: set[str] = set()
        for pref_jp, comp_url in detail_urls:
            if comp_url in seen:
                continue
            seen.add(comp_url)
            item = self._scrape_detail(comp_url, pref_jp)
            if item:
                yield item

    def _collect_city_urls(self, city_url: str) -> list[str]:
        soup = self.get_soup(city_url)
        if soup is None:
            return []
        urls = []
        seen: set[str] = set()
        for div in soup.find_all("div", class_="stripe_lists"):
            for a in div.find_all("a", href=True):
                href = a["href"]
                full = href if href.startswith("http") else f"{BASE_URL}{href}"
                if "/kaisha/" in full and full not in seen:
                    seen.add(full)
                    urls.append(full)
        return urls

    def _collect_company_urls(self, list_url: str) -> list[str]:
        urls = []
        seen: set[str] = set()
        current = list_url
        while current:
            soup = self.get_soup(current)
            if soup is None:
                break
            for a in soup.select("div.stripe_lists a[href], ul.l-results a[href]"):
                href = a["href"]
                full = href if href.startswith("http") else f"{BASE_URL}{href}"
                if "/kaisha/" in full and full not in seen:
                    seen.add(full)
                    urls.append(full)
            next_a = soup.select_one("a.pagination-parts:contains('次へ'), li.pagination-next a")
            if next_a:
                href = next_a.get("href", "")
                next_url = href if href.startswith("http") else urljoin(BASE_URL, href)
                current = next_url if next_url != current else None
            else:
                current = None
        return urls

    def _scrape_detail(self, url: str, pref_jp: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url, Schema.PREF: pref_jp}

        title = soup.find("h1")
        if title:
            data[Schema.NAME] = title.get_text(separator=" ", strip=True)

        table = soup.find("table", class_=lambda c: c and "data_table" in c)
        if table:
            for tr in table.find_all("tr"):
                ths = tr.find_all("th")
                tds = tr.find_all("td")
                for i in range(min(len(ths), len(tds))):
                    key = ths[i].get_text(separator=" ", strip=True)
                    td = tds[i]
                    if "所在地" in key:
                        data[Schema.ADDR] = td.get_text(separator=" ", strip=True)
                    elif "TEL" in key:
                        em = td.find("em")
                        data[Schema.TEL] = em.get_text(strip=True) if em else td.get_text(separator=" ", strip=True)
                    elif "FAX" in key:
                        data["FAX"] = td.get_text(separator=" ", strip=True)
                    elif "営業時間" in key:
                        data[Schema.TIME] = td.get_text(separator=" ", strip=True)
                    elif "定休日" in key:
                        data[Schema.HOLIDAY] = td.get_text(separator=" ", strip=True)
                    elif "免許番号" in key:
                        data["免許番号"] = td.get_text(separator=" ", strip=True)
                    elif "関連サイト" in key:
                        links = []
                        for a in td.find_all("a"):
                            href = a.get("href", "")
                            if href.startswith("http"):
                                links.append(href)
                        data["関連サイト"] = " | ".join(links) if links else td.get_text(separator=" ", strip=True)
        else:
            addr_text = soup.find(string=re.compile("所在地"))
            if addr_text and addr_text.parent:
                sibling = addr_text.parent.find_next_sibling()
                if sibling:
                    data[Schema.ADDR] = sibling.get_text(strip=True)

        if not data.get(Schema.NAME) and not data.get(Schema.ADDR):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    SuumoScraper().execute("https://suumo.jp/kaisha/")
