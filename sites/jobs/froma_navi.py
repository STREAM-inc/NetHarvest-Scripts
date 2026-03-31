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


class FromaNaviScraper(DynamicCrawler):
    """フロム・エー ナビ 企業情報スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["事業内容"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        """全都道府県の求人一覧 → 企業詳細ページをスクレイプ"""
        base = url.rstrip("/")
        for pref in PREFS:
            pref_url = f"{base}/{pref}/job_search/?emp=08&emp=04&emp=03&emp=02&sc=2"
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
                    detail_url = "https://www.froma.com" + detail_url
                item = self._scrape_detail(detail_url, pref_ja)
                if item:
                    yield item

            # 次ページ
            next_btn = soup.select_one("a.styles_arrowButton__K2TR1.styles_next__5kh7k")
            if next_btn and next_btn.get("aria-disabled") != "true":
                href = next_btn.get("href", "")
                current_url = href if href.startswith("http") else "https://www.froma.com" + href
            else:
                current_url = None

    def _scrape_detail(self, url: str, pref_ja: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url, Schema.PREF: pref_ja}

        root = soup.select_one("div[class*='styles_listCompanyInfo']")
        if not root:
            return None

        for block in root.select("div[class*='styles_companyInfo']"):
            label_el = (
                block.select_one("h3 p") or
                block.select_one("p[class*='title']") or
                block.find("p")
            )
            value_el = (
                block.select_one("p[class*='content']") or
                block.find("a") or
                block.find("p")
            )
            if not label_el or not value_el:
                continue

            label = label_el.get_text(strip=True)
            value_text = value_el.get_text(" ", strip=True)
            value_href = value_el.get("href", "") if value_el.name == "a" else ""

            if any(k in label for k in ("会社名", "企業名", "社名")):
                data.setdefault(Schema.NAME, value_text)
            elif "代表者" in label:
                data.setdefault(Schema.REP_NM, value_text)
            elif any(k in label for k in ("所在住所", "所在地", "本社所在地", "住所")):
                m = re.match(r"^\s*〒?\s*(\d{3})[-]?(\d{4})", value_text)
                if m:
                    data.setdefault(Schema.POST_CODE, f"{m.group(1)}-{m.group(2)}")
                    value_text = value_text[m.end():].strip(" ,，、：:　")
                data.setdefault(Schema.ADDR, value_text)
            elif any(k in label for k in ("事業内容", "仕事内容")):
                data.setdefault("事業内容", value_text)
            elif any(k in label for k in ("ホームページ", "HP", "ＨＰ")):
                data.setdefault(Schema.HP, value_href or value_text)
            elif any(k in label for k in ("お問い合わせ", "代表電話番号", "電話番号", "TEL")):
                raw = value_href if value_href.startswith("tel:") else value_text
                m = re.search(r"0\d{9,10}", re.sub(r"\D", "", raw))
                if m and Schema.TEL not in data:
                    data[Schema.TEL] = m.group(0)

        if Schema.NAME not in data:
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    FromaNaviScraper().execute("https://www.froma.com/prefectures/")
