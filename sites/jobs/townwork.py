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

BASE_URL = "https://townwork.net"

PREFS = [
    "hokkaidou", "aomori", "iwate", "miyagi", "akita", "yamagata", "fukushima",
    "ibaraki", "tochigi", "gunma", "saitama", "chiba", "tokyo", "kanagawa",
    "niigata", "toyama", "ishikawa", "fukui", "yamanashi", "nagano", "gifu",
    "shizuoka", "aichi", "mie", "shiga", "kyoto", "osaka", "hyogo", "nara",
    "wakayama", "tottori", "shimane", "okayama", "hiroshima", "yamaguchi",
    "tokushima", "kagawa", "ehime", "kochi", "fukuoka", "saga", "nagasaki",
    "kumamoto", "oita", "miyazaki", "kagoshima", "okinawa",
]

PREF_JA = {
    "hokkaidou": "北海道", "aomori": "青森県", "iwate": "岩手県", "miyagi": "宮城県",
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
    return re.sub(r"\s+", " ", str(s)).strip()


class TownworkScraper(DynamicCrawler):
    """タウンワーク 求人企業情報スクレイパー（townwork.net）"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["業種", "代表者", "採用人数"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_companies: set[str] = set()
        for pref in PREFS:
            pref_ja = PREF_JA.get(pref, pref)
            list_url = f"{BASE_URL}/{pref}/"
            self.logger.info("都道府県: %s", pref_ja)
            yield from self._scrape_pref(list_url, pref_ja, seen_companies)

    def _scrape_pref(self, list_url: str, pref_ja: str, seen: set) -> Generator[dict, None, None]:
        current = list_url
        while current:
            soup = self.get_soup(current, wait_until="networkidle")
            if soup is None:
                break

            for a in soup.select("a[href*='/detail/']"):
                href = a.get("href", "").strip()
                full = href if href.startswith("http") else urljoin(BASE_URL, href)
                if full not in seen:
                    seen.add(full)
                    item = self._scrape_detail(full, pref_ja)
                    if item and item.get(Schema.NAME):
                        yield item

            next_a = soup.select_one("a[rel='next'], a.next, a.pager-next")
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

        def extract(label: str) -> str:
            el = soup.find(string=lambda t: t and label in t)
            if el:
                parent = el.find_parent()
                sibling = parent.find_next_sibling() if parent else None
                if sibling:
                    return _clean(sibling.get_text(" "))
            return ""

        # try structured table approach
        for tr in soup.select("table tr, dl"):
            th = tr.find("th") or tr.find("dt")
            td = tr.find("td") or tr.find("dd")
            if not th or not td:
                continue
            key = th.get_text(strip=True)
            val = _clean(td.get_text(" "))
            if "会社名" in key or "企業名" in key:
                data[Schema.NAME] = val
            elif "所在住所" in key or "住所" in key or "所在地" in key:
                data[Schema.ADDR] = val
            elif "代表電話番号" in key or "電話番号" in key:
                data[Schema.TEL] = val
            elif "代表者" in key:
                data[Schema.REP_NM] = val
            elif "事業内容" in key or "業種" in key:
                data["業種"] = val
            elif "ホームページ" in key or "URL" in key:
                a = td.find("a", href=True)
                data[Schema.HP] = a["href"] if a else val
            elif "採用" in key and "人数" in key:
                data["採用人数"] = val

        # fallback
        if not data.get(Schema.NAME):
            name_val = extract("会社名") or extract("企業名")
            if name_val:
                data[Schema.NAME] = name_val

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    TownworkScraper().execute("https://townwork.net/")
