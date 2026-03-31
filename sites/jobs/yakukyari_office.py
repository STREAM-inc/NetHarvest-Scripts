import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://pcareer.m3.com"
LIST_URL = "https://pcareer.m3.com/shokubanavi/offices"


class YakukyariOfficeScraper(StaticCrawler):
    """薬キャリ 事業所情報スクレイパー（pcareer.m3.com/shokubanavi/offices）"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["法人名", "所在地", "処方科目", "処方枚数", "薬剤師数", "医療事務人数", "薬歴", "募集職種", "雇用形態", "勤務時間"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        office_urls = self._collect_office_urls(url)
        self.total_items = len(office_urls)
        self.logger.info("事業所URL収集完了: %d 件", len(office_urls))
        for office_url in office_urls:
            item = self._scrape_detail(office_url)
            if item:
                yield item

    def _collect_office_urls(self, base_url: str) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        page = 1
        consecutive_empty = 0
        while consecutive_empty < 3:
            page_url = f"{base_url}?page={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                consecutive_empty += 1
                page += 1
                continue
            h3s = soup.find_all("h3", class_="m3p_jobnavi-box-wrap-cont-company-list-h3")
            if not h3s:
                consecutive_empty += 1
                page += 1
                continue
            consecutive_empty = 0
            for h3 in h3s:
                link = h3.find("a")
                if link and link.get("href"):
                    full = urljoin(BASE_URL, link["href"].strip())
                    if full not in seen:
                        seen.add(full)
                        urls.append(full)
            page += 1
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        h2 = soup.find("h2", class_=lambda c: c and "m3-heading1--m3p_jobnavi-icon" in c)
        if h2:
            name = h2.get_text(strip=True).replace("募集要項", "").strip()
            if name:
                data[Schema.NAME] = name

        # 複雑なテーブル（m3-table--m3p_jobnavi-complex）
        for table in soup.find_all("table", class_=lambda c: c and "m3-table--m3p_jobnavi-complex" in c):
            for row in table.find_all("tr"):
                ths = row.find_all("th")
                tds = row.find_all("td")
                if ths and tds:
                    key = ths[-1].get_text(strip=True)
                    val = tds[0].get_text(separator=" ", strip=True)
                    if key == "所在地":
                        data["所在地"] = val
                    elif key == "営業時間":
                        if not data.get(Schema.TIME):
                            data[Schema.TIME] = val
                    elif key == "定休日":
                        if not data.get(Schema.HOLIDAY):
                            data[Schema.HOLIDAY] = val
                    elif key == "休憩":
                        data["勤務時間"] = val
                    elif key == "処方科目":
                        data["処方科目"] = val
                    elif key == "処方枚数":
                        data["処方枚数"] = val
                    elif key == "薬剤師数":
                        data["薬剤師数"] = val
                    elif key == "医療事務人数":
                        data["医療事務人数"] = val
                    elif key == "薬歴":
                        data["薬歴"] = val

        # 通常テーブル
        for table in soup.find_all("table", class_=lambda c: c and "m3-table" in c and "m3-table--m3p_jobnavi-complex" not in c):
            for row in table.find_all("tr"):
                th = row.find("th")
                td = row.find("td")
                if th and td:
                    key = th.get_text(strip=True)
                    val = td.get_text(separator=" ", strip=True)
                    if key == "法人名":
                        data["法人名"] = val
                    elif key == "住所":
                        data[Schema.ADDR] = val
                    elif key == "営業時間" and not data.get(Schema.TIME):
                        data[Schema.TIME] = val
                    elif key == "定休日" and not data.get(Schema.HOLIDAY):
                        data[Schema.HOLIDAY] = val
                    elif key == "募集職種":
                        data["募集職種"] = val
                    elif key == "雇用形態":
                        data["雇用形態"] = val
                    elif key == "勤務時間" and not data.get("勤務時間"):
                        data["勤務時間"] = val

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    YakukyariOfficeScraper().execute(LIST_URL)
