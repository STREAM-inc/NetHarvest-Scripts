import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

BASE_URL = "https://www.qlife.jp"
LIST_URL_TEMPLATE = "https://www.qlife.jp/search_hospital11b_0_{pref_code}_0_{page_num}"
DETAIL_PREFIX = "/hospital_detail_"
PREF_MAP = {
    1: "北海道", 2: "青森県", 3: "岩手県", 4: "宮城県", 5: "秋田県",
    6: "山形県", 7: "福島県", 8: "茨城県", 9: "栃木県", 10: "群馬県",
    11: "埼玉県", 12: "千葉県", 13: "東京都", 14: "神奈川県", 15: "新潟県",
    16: "富山県", 17: "石川県", 18: "福井県", 19: "山梨県", 20: "長野県",
    21: "岐阜県", 22: "静岡県", 23: "愛知県", 24: "三重県", 25: "滋賀県",
    26: "京都府", 27: "大阪府", 28: "兵庫県", 29: "奈良県", 30: "和歌山県",
    31: "鳥取県", 32: "島根県", 33: "岡山県", 34: "広島県", 35: "山口県",
    36: "徳島県", 37: "香川県", 38: "愛媛県", 39: "高知県", 40: "福岡県",
    41: "佐賀県", 42: "長崎県", 43: "熊本県", 44: "大分県", 45: "宮崎県",
    46: "鹿児島県", 47: "沖縄県",
}


class QlifeScraper(StaticCrawler):
    """QLife病院検索の医療機関情報を取得する静的クローラー。"""

    DELAY = 0.3

    EXTRA_COLUMNS = ["駐車場", "人間ドック", "カード", "院内処方"]

    def prepare(self):
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/139.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
                "Referer": BASE_URL + "/",
            }
        )

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_urls: set[str] = set()

        for pref_code, pref_name in self._resolve_prefectures(url):
            page_num = 1
            empty_count = 0

            while empty_count < 3:
                list_url = LIST_URL_TEMPLATE.format(pref_code=pref_code, page_num=page_num)
                self.logger.info("一覧ページ取得: %s", list_url)
                soup = self.get_soup(list_url)

                detail_urls = []
                main_wrap = soup.select_one("#contents > div.clearfix > div.main_wrap")
                if main_wrap:
                    for link in main_wrap.select("a[href^='/hospital_detail_']"):
                        href = link.get("href")
                        if not href:
                            continue
                        detail_url = urljoin(BASE_URL, href)
                        if detail_url in seen_urls:
                            continue
                        seen_urls.add(detail_url)
                        detail_urls.append(detail_url)

                if not detail_urls:
                    empty_count += 1
                    page_num += 1
                    continue

                empty_count = 0
                for detail_url in detail_urls:
                    if self.DELAY > 0:
                        time.sleep(self.DELAY)

                    item = self._scrape_detail(detail_url, pref_name)
                    if item:
                        yield item

                page_num += 1

    def _resolve_prefectures(self, url: str) -> list[tuple[int, str]]:
        match = re.search(r"search_hospital11b_0_(\d+)_0_\d+", url)
        if match:
            pref_code = int(match.group(1))
            pref_name = PREF_MAP.get(pref_code)
            if pref_name:
                return [(pref_code, pref_name)]
        return list(PREF_MAP.items())

    def _scrape_detail(self, url: str, pref_name: str) -> dict | None:
        self.logger.info("詳細ページ取得: %s", url)
        soup = self.get_soup(url)

        item: dict[str, str] = {
            Schema.URL: url,
            Schema.PREF: pref_name,
        }

        name_tag = soup.select_one("h1.page_title a, h1.page_title, h1")
        if name_tag:
            raw_name = self._normalize_text(name_tag.get_text(" ", strip=True))
            item[Schema.NAME] = re.sub(r"\s*（.*?）$", "", raw_name).strip()

        for row in soup.select("div.data div.detail table tr"):
            th = row.select_one("th")
            td = row.select_one("td")
            if not th or not td:
                continue

            label = self._normalize_text(th.get_text(" ", strip=True))
            value = self._normalize_text(td.get_text(" ", strip=True))

            if "住所" in label:
                value = value.split("地図を見る")[0].strip()
                item[Schema.ADDR] = value
                post_match = re.search(r"〒?\s*(\d{3}-\d{4})", value)
                if post_match:
                    item[Schema.POST_CODE] = post_match.group(1)
            elif "電話番号" in label:
                item[Schema.TEL] = value
            elif "最寄駅" in label:
                item["最寄駅"] = value

        subjects = [self._normalize_text(a.get_text(" ", strip=True)) for a in soup.select("div.course_block p a")]
        if subjects:
            item[Schema.CAT_SITE] = "／".join(subjects)

        site_tag = soup.select_one("td.site_address a[href^='http']")
        if site_tag:
            item[Schema.HP] = site_tag.get("href", "").strip()

        facility_headers = [self._normalize_text(th.get_text(" ", strip=True)) for th in soup.select("div.facility_info_block table thead th")]
        facility_values = [self._normalize_text(td.get_text(" ", strip=True)) for td in soup.select("div.facility_info_block table tbody tr td")]
        for header, value in zip(facility_headers, facility_values):
            if header:
                item[header] = value

        if Schema.NAME not in item:
            return None

        return item

    @staticmethod
    def _normalize_text(text: str) -> str:
        return " ".join(str(text).split())


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    QlifeScraper().execute("https://www.qlife.jp/search_hospital11b_0_13_0_1")
