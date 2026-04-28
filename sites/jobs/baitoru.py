import re
import sys
from pathlib import Path
from typing import Generator

from bs4 import BeautifulSoup

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

BASE_URL = "https://baitoru.com"

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


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class BaitoruScraper(DynamicCrawler):
    """バイトル 求人企業情報スクレイパー（baitoru.com）"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["業種", "代表者", "採用人数"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_companies: set[str] = set()
        for pref in PREFS:
            pref_ja = PREF_JA.get(pref, pref)
            list_url = f"{BASE_URL}/search/list/?searchWord=&searchArea={pref}"
            self.logger.info("都道府県: %s", pref_ja)
            yield from self._scrape_pref(list_url, pref_ja, seen_companies)

    def _scrape_pref(self, list_url: str, pref_ja: str, seen: set) -> Generator[dict, None, None]:
        try:
            self.page.goto(list_url, wait_until="domcontentloaded")
            self.page.wait_for_selector("article", timeout=8000)
        except Exception:
            return
        soup = BeautifulSoup(self.page.content(), "html.parser")

        visited_jobs: set[str] = set()
        for article in soup.select("article"):
            a = article.select_one("a[href*='/job']")
            if not a:
                continue
            href = re.sub(r"\?.*$", "", a.get("href", "").strip())
            job_url = href if href.startswith("http") else "https://www.baitoru.com" + href
            if job_url in visited_jobs:
                continue
            visited_jobs.add(job_url)

            item = self._scrape_detail(job_url, pref_ja)
            if not item or not item.get(Schema.NAME):
                continue

            company_url = item.get(Schema.URL, job_url)
            if company_url in seen:
                continue
            seen.add(company_url)
            yield item

    def _scrape_detail(self, url: str, pref_ja: str) -> dict | None:
        try:
            self.page.goto(url, wait_until="domcontentloaded")
            self.page.wait_for_selector("div.detail-companyInfo", timeout=10000)
        except Exception:
            return None
        soup = BeautifulSoup(self.page.content(), "html.parser")

        data = {Schema.URL: url, Schema.PREF: pref_ja}

        company_info = soup.find("div", class_="detail-companyInfo")
        if company_info:
            # 企業ページURL（/cjlist:id/）を取得して重複排除に使う
            link01 = company_info.find("a", class_="link01")
            if link01:
                cj_href = link01.get("href", "").split("#")[0].rstrip("/")
                if cj_href:
                    data[Schema.URL] = (cj_href if cj_href.startswith("http")
                                        else "https://www.baitoru.com" + cj_href)

            pt02 = company_info.find("div", class_="pt02")
            if pt02:
                p = pt02.find("p")
                if p:
                    a = p.find("a")
                    data[Schema.NAME] = _clean(a.get_text() if a else p.get_text())

            pt03 = company_info.find("div", class_="pt03")
            if pt03:
                for dl in pt03.find_all("dl"):
                    dt = dl.find("dt")
                    dd = dl.find("dd")
                    if not dt or not dd:
                        continue
                    key = dt.get_text(strip=True)
                    val = _clean(dd.get_text(" "))
                    if "所在地" in key:
                        data[Schema.ADDR] = val
                    elif "代表電話番号" in key or "電話番号" in key:
                        data[Schema.TEL] = val
                    elif "代表者" in key:
                        data[Schema.REP_NM] = val
                    elif "事業内容" in key or "業種" in key:
                        data["業種"] = val
                    elif "ホームページ" in key or "URL" in key:
                        a = dd.find("a", href=True)
                        data[Schema.HP] = a["href"] if a else val
                    elif "採用予定人数" in key:
                        data["採用人数"] = val

        if not data.get(Schema.NAME):
            h1 = soup.select_one("h1")
            if h1:
                data[Schema.NAME] = _clean(h1.get_text())

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    BaitoruScraper().execute("https://baitoru.com/search/list/")
