import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://www.hanacupid.or.jp"

PREF_CODES = [f"{i:02d}" for i in range(1, 48)]

PREF_MAP = {
    "01": "北海道", "02": "青森県", "03": "岩手県", "04": "宮城県", "05": "秋田県",
    "06": "山形県", "07": "福島県", "08": "茨城県", "09": "栃木県", "10": "群馬県",
    "11": "埼玉県", "12": "千葉県", "13": "東京都", "14": "神奈川県", "15": "新潟県",
    "16": "富山県", "17": "石川県", "18": "福井県", "19": "山梨県", "20": "長野県",
    "21": "岐阜県", "22": "静岡県", "23": "愛知県", "24": "三重県", "25": "滋賀県",
    "26": "京都府", "27": "大阪府", "28": "兵庫県", "29": "奈良県", "30": "和歌山県",
    "31": "鳥取県", "32": "島根県", "33": "岡山県", "34": "広島県", "35": "山口県",
    "36": "徳島県", "37": "香川県", "38": "愛媛県", "39": "高知県",
    "40": "福岡県", "41": "佐賀県", "42": "長崎県", "43": "熊本県", "44": "大分県",
    "45": "宮崎県", "46": "鹿児島県", "47": "沖縄県",
}


class HanacupidScraper(StaticCrawler):
    """花キューピット 加盟店舗スクレイパー（hanacupid.or.jp）"""

    DELAY = 0.5
    EXTRA_COLUMNS = ["名称_フリガナ", "FAX番号", "営業時間", "配達エリア"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        for pref_code in PREF_CODES:
            pref_ja = PREF_MAP.get(pref_code, pref_code)
            self.logger.info("都道府県: %s", pref_ja)
            area_url = f"{BASE_URL}/stores/area/{pref_code}"
            city_links = self._get_city_links(area_url)
            for city_url in city_links:
                store_links = self._get_store_links(city_url)
                for store_url in store_links:
                    if store_url in seen:
                        continue
                    seen.add(store_url)
                    item = self._scrape_detail(store_url, pref_code)
                    if item:
                        yield item

    def _get_city_links(self, area_url: str) -> list[str]:
        soup = self.get_soup(area_url)
        if soup is None:
            return []
        links = []
        for a in soup.select("#wrap > main > article > div > dl a[href]"):
            href = a.get("href", "")
            if href and "/stores/results" in href:
                links.append(BASE_URL + href if href.startswith("/") else href)
        return links

    def _get_store_links(self, city_url: str) -> list[str]:
        soup = self.get_soup(city_url)
        if soup is None:
            return []
        return [
            BASE_URL + a.get("href")
            for a in soup.select(
                "#wrap > main > article > div > div.list a[href^='/stores/details/']"
            )
            if a.get("href")
        ]

    def _scrape_detail(self, url: str, pref_code: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url, Schema.PREF: PREF_MAP.get(pref_code, "")}

        h2 = soup.select_one("h2.storeName")
        if not h2:
            return None

        yomi = h2.select_one("span.yomi")
        if yomi:
            data["名称_フリガナ"] = yomi.get_text(strip=True)
        spans = h2.find_all("span")
        if spans:
            data[Schema.NAME] = spans[-1].get_text(strip=True)

        data_dl = soup.select_one("#wrap > main > article > div > div > div.data > dl")
        if data_dl:
            for dt, dd in zip(data_dl.find_all("dt"), data_dl.find_all("dd")):
                key = dt.get_text(strip=True)
                val = dd.get_text(" ", strip=True)
                if "住所" in key:
                    data[Schema.ADDR] = val
                elif "電話番号" in key:
                    data[Schema.TEL] = val
                elif "FAX" in key:
                    data["FAX番号"] = val
                elif "営業時間" in key:
                    data[Schema.TIME] = val
                elif "定休日" in key:
                    data[Schema.HOLIDAY] = val
                elif "配達エリア" in key:
                    data["配達エリア"] = val

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    HanacupidScraper().execute("https://www.hanacupid.or.jp/stores/")
