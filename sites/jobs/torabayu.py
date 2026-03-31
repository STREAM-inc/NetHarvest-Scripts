import re
import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema


PREFS = [
    "hokkaido", "aomori", "iwate", "miyagi", "akita", "yamagata", "fukushima",
    "ibaraki", "tochigi", "gunma", "saitama", "chiba", "tokyo", "kanagawa",
    "niigata", "toyama", "ishikawa", "fukui", "yamanashi", "nagano", "gifu",
    "shizuoka", "aichi", "mie", "shiga", "kyoto", "osaka", "hyogo", "nara",
    "wakayama", "tottori", "shimane", "okayama", "hiroshima", "yamaguchi",
    "tokushima", "kagawa", "ehime", "kochi", "fukuoka", "saga", "nagasaki",
    "kumamoto", "oita", "miyazaki", "kagoshima", "okinawa",
]

PREF_JA = {
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


class TorabayuScraper(DynamicCrawler):
    """とらばーゆ(toranet.jp) 企業情報スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["事業内容"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        """全都道府県の求人一覧 → 企業詳細ページをスクレイプ"""
        base = url.rstrip("/")
        for pref in PREFS:
            pref_url = f"{base}/{pref}/job_search/?emp=04&emp=03&emp=01&emp=02&emp=05&sc=2"
            self.logger.info("都道府県取得: %s", pref)
            yield from self._scrape_pref(pref_url, PREF_JA.get(pref, pref))

    def _scrape_pref(self, list_url: str, pref_ja: str) -> Generator[dict, None, None]:
        current_url = list_url
        while current_url:
            soup = self.get_soup(current_url, wait_until="networkidle")
            if soup is None:
                break

            links = soup.select("a.styles_bigCard__pKdMA.styles_resultBgCard__5ZYNQ")
            for link in links:
                detail_url = link.get("href", "")
                if not detail_url:
                    continue
                if not detail_url.startswith("http"):
                    detail_url = "https://toranet.jp" + detail_url
                item = self._scrape_detail(detail_url, pref_ja)
                if item:
                    yield item

            next_btn = soup.select_one("a.styles_arrowButton__K2TR1.styles_next__5kh7k")
            if next_btn and next_btn.get("aria-disabled") != "true":
                href = next_btn.get("href", "")
                current_url = href if href.startswith("http") else "https://toranet.jp" + href
            else:
                current_url = None

    def _scrape_detail(self, url: str, pref_ja: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url, Schema.PREF: pref_ja}

        for block in soup.select("div.styles_companyInfo__sPviw"):
            label_el = block.find("p")
            value_el = block.select_one("p.styles_content__HWIR6, a")
            if not label_el or not value_el:
                continue
            label = label_el.get_text(strip=True)
            value = value_el.get_text(strip=True)
            href = value_el.get("href", "") if value_el.name == "a" else ""

            if "会社名" in label:
                data.setdefault(Schema.NAME, value)
            elif "代表者" in label:
                data.setdefault(Schema.REP_NM, value)
            elif "所在住所" in label or "住所" in label:
                m = re.match(r"^〒?\s*(\d{3})[-]?(\d{4})", value)
                if m:
                    data.setdefault(Schema.POST_CODE, f"{m.group(1)}-{m.group(2)}")
                    value = value[m.end():].strip()
                data.setdefault(Schema.ADDR, value)
            elif "事業内容" in label:
                data.setdefault("事業内容", value)
            elif any(k in label for k in ("ホームページ", "HP")):
                data.setdefault(Schema.HP, href or value)
            elif any(k in label for k in ("電話番号", "TEL", "代表電話番号")):
                digits = re.sub(r"\D", "", href if href.startswith("tel:") else value)
                if digits and Schema.TEL not in data:
                    data[Schema.TEL] = digits

        if Schema.NAME not in data:
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    TorabayuScraper().execute("https://toranet.jp/prefectures/")
