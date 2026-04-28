import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

BASE_URL = "https://job-chocolat.jp"

PREF_MAP = {
    "hokkaido": "北海道", "aomori": "青森県", "iwate": "岩手県", "miyagi": "宮城県",
    "akita": "秋田県", "yamagata": "山形県", "fukushima": "福島県", "ibaraki": "茨城県",
    "tochigi": "栃木県", "gunma": "群馬県", "saitama": "埼玉県", "chiba": "千葉県",
    "tokyo": "東京都", "kanagawa": "神奈川県", "niigata": "新潟県", "toyama": "富山県",
    "ishikawa": "石川県", "fukui": "福井県", "yamanashi": "山梨県", "nagano": "長野県",
    "gifu": "岐阜県", "shizuoka": "静岡県", "aichi": "愛知県", "mie": "三重県",
    "shiga": "滋賀県", "kyoto": "京都府", "osaka": "大阪府", "hyogo": "兵庫県",
    "nara": "奈良県", "wakayama": "和歌山県", "tottori": "鳥取県", "shimane": "島根県",
    "okayama": "岡山県", "hiroshima": "広島県", "yamaguchi": "山口県",
    "tokushima": "徳島県", "kagawa": "香川県", "ehime": "愛媛県", "kochi": "高知県",
    "fukuoka": "福岡県", "saga": "佐賀県", "nagasaki": "長崎県", "kumamoto": "熊本県",
    "oita": "大分県", "miyazaki": "宮崎県", "kagoshima": "鹿児島県", "okinawa": "沖縄県",
}


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"[\r\n\t]+", " ", re.sub(r"\s{2,}", " ", str(s))).strip()


class JobChocolatScraper(DynamicCrawler):
    """ジョブショコラ ナイト求人スクレイパー（job-chocolat.jp）"""

    DELAY = 0.6
    EXTRA_COLUMNS = ["業種", "LINE公式", "アクセス"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        for pref_code, pref_ja in PREF_MAP.items():
            list_url = f"{BASE_URL}/{pref_code}/shoplist/"
            self.logger.info("都道府県: %s", pref_ja)
            yield from self._scrape_pref(list_url, pref_ja, seen)

    def _scrape_pref(self, list_url: str, pref_ja: str, seen: set) -> Generator[dict, None, None]:
        current = list_url
        while current:
            soup = self.get_soup(current, wait_until="networkidle")
            if soup is None:
                break

            for a in soup.select("a[href*='/shopdetail/'], a[href*='/detail/']"):
                href = a.get("href", "").strip()
                full = href if href.startswith("http") else BASE_URL + href
                if full not in seen:
                    seen.add(full)
                    item = self._scrape_detail(full, pref_ja)
                    if item:
                        yield item

            next_a = soup.select_one("a[rel='next'], a.next, li.next a")
            if next_a and next_a.get("href"):
                next_url = urljoin(current, next_a["href"])
                current = next_url if next_url != current else None
            else:
                current = None

    def _scrape_detail(self, url: str, pref_ja: str) -> dict | None:
        soup = self.get_soup(url, wait_until="networkidle")
        if soup is None:
            return None

        data = {Schema.URL: url, Schema.PREF: pref_ja}

        h1 = soup.select_one("h1")
        if h1:
            data[Schema.NAME] = _clean(h1.get_text())

        # table info
        for tr in soup.select("table tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            key = th.get_text(strip=True)
            val = _clean(td.get_text(" "))
            if "住所" in key or "所在地" in key:
                data[Schema.ADDR] = val
            elif key in ("TEL", "電話番号"):
                data[Schema.TEL] = val
            elif "業種" in key:
                data["業種"] = val
            elif "営業時間" in key:
                data[Schema.TIME] = val
            elif "定休日" in key:
                data[Schema.HOLIDAY] = val
            elif "LINE" in key:
                data["LINE公式"] = val
            elif "アクセス" in key:
                data["アクセス"] = val

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    JobChocolatScraper().execute("https://job-chocolat.jp/")
