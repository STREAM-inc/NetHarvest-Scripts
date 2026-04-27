import re
import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
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

CATEGORY_URLS = [
    "https://dairitenfc.com/search/beauty_health.html",
    "https://dairitenfc.com/search/internet.html",
    "https://dairitenfc.com/search/communication.html",
    "https://dairitenfc.com/search/eat_drink.html",
    "https://dairitenfc.com/search/education.html",
    "https://dairitenfc.com/search/estate.html",
    "https://dairitenfc.com/search/environment.html",
    "https://dairitenfc.com/search/retail.html",
    "https://dairitenfc.com/search/service.html",
    "https://dairitenfc.com/search/other.html",
]


class DairitenfcScraper(StaticCrawler):
    """代理店FC フランチャイズ代理店情報スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["エリア", "設立", "従業員"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls = self._collect_detail_urls()
        self.total_items = len(detail_urls)
        self.logger.info("詳細URL収集完了: %d 件", len(detail_urls))
        for detail_url in detail_urls:
            item = self._scrape_detail(detail_url)
            if item:
                yield item

    def _collect_detail_urls(self) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        for cat_url in CATEGORY_URLS:
            page = 1
            while True:
                page_url = f"{cat_url}?page={page}"
                soup = self.get_soup(page_url)
                if soup is None:
                    break
                found = False
                for btn in soup.select("div.button a[href]"):
                    href = btn.get("href", "")
                    if "details/" in href:
                        full = "https://dairitenfc.com/search/" + href.replace("../", "").lstrip("/")
                        if full not in seen:
                            seen.add(full)
                            urls.append(full)
                            found = True
                if not found:
                    break
                page += 1
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        for table in soup.select("div.ess_details table"):
            for row in table.select("tr"):
                th = row.find("th")
                td = row.find("td")
                if not th or not td:
                    continue
                key = th.get_text(strip=True)
                val = td.get_text(" ", strip=True)
                if val in ["-", "‐", "ー"]:
                    val = ""

                if "会社名" in key:
                    data[Schema.NAME] = val
                elif "エリア" in key:
                    data["エリア"] = val
                elif "所在地" in key:
                    m = re.search(r"(〒?\s*\d{3}-?\d{4})", val)
                    if m:
                        digits = re.sub(r"\D", "", m.group(1))
                        if len(digits) == 7:
                            data[Schema.POST_CODE] = f"〒{digits[:3]}-{digits[3:]}"
                        data[Schema.ADDR] = val[m.end():].strip()
                    else:
                        data[Schema.ADDR] = val
                    pref = next((p for p in PREFS if p in val), "")
                    if pref:
                        data[Schema.PREF] = pref
                elif "設立" in key:
                    data["設立"] = val
                elif "代表" in key:
                    data[Schema.REP_NM] = val
                elif "資本金" in key:
                    data[Schema.CAP] = val
                elif "売上" in key:
                    data[Schema.SALES] = val
                elif "従業員" in key:
                    data["従業員"] = val
                elif "事業内容" in key:
                    data[Schema.LOB] = val

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    DairitenfcScraper().execute("https://dairitenfc.com/search/")
