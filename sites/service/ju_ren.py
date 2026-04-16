import sys
from pathlib import Path
from typing import Generator
from urllib.parse import quote

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

PREFS = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]

BASE_URL = "https://ju-ren.jp/location/stands/?page={page}&pref={pref}"


class JuRenScraper(DynamicCrawler):
    """充レン EV充電スタンド情報スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["設置場所", "利用可能時間"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        for pref in PREFS:
            self.logger.info("都道府県: %s", pref)
            pref_encoded = quote(pref)
            page = 0
            while True:
                page_url = BASE_URL.format(page=page, pref=pref_encoded)
                soup = self.get_soup(page_url, wait_until="networkidle")
                if soup is None:
                    break

                rows = soup.select("tr.tbody")
                if not rows:
                    break

                for tr in rows:
                    tds = tr.find_all("td")
                    place = tds[1].get_text(strip=True) if len(tds) > 1 else ""
                    addr = tds[2].get_text(strip=True) if len(tds) > 2 else ""
                    available = tds[3].get_text(separator=" ", strip=True) if len(tds) > 3 else ""

                    if not place and not addr:
                        continue

                    yield {
                        Schema.URL: page_url,
                        Schema.PREF: pref,
                        Schema.NAME: place,
                        Schema.ADDR: addr,
                        "設置場所": place,
                        "利用可能時間": available,
                    }

                page += 1


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    JuRenScraper().execute("https://ju-ren.jp/location/stands/")
