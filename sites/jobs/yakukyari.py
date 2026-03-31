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
LIST_URL = "https://pcareer.m3.com/shokubanavi/companies"


class YakukyariScraper(StaticCrawler):
    """薬キャリ 法人情報スクレイパー（pcareer.m3.com/shokubanavi/companies）"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["業種", "事業所数", "設立", "薬剤師数", "施設エリア"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        company_urls = self._collect_company_urls(url)
        self.total_items = len(company_urls)
        self.logger.info("法人URL収集完了: %d 件", len(company_urls))
        for company_url in company_urls:
            item = self._scrape_detail(company_url)
            if item:
                yield item

    def _collect_company_urls(self, base_url: str) -> list[str]:
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
            divs = soup.find_all("div", class_="m3p_jobnavi-company-list-header-logo")
            if not divs:
                consecutive_empty += 1
                page += 1
                continue
            consecutive_empty = 0
            for div in divs:
                link = div.find("a")
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

        def parse_table(table):
            for row in table.find_all("tr"):
                th = row.find("th")
                td = row.find("td")
                if th and td:
                    key = th.get_text(strip=True)
                    val = td.get_text(separator="\n", strip=True)
                    if key == "法人名称":
                        data[Schema.NAME] = val
                    elif key == "事業内容":
                        data[Schema.LOB] = val
                    elif key == "所在地":
                        data[Schema.ADDR] = val
                    elif key == "業種":
                        data["業種"] = val
                    elif key == "事業所数":
                        data["事業所数"] = val
                    elif key == "設立":
                        data["設立"] = val
                    elif key == "売上高":
                        data[Schema.SALES] = val
                    elif key == "資本金":
                        data[Schema.CAP] = val
                    elif key == "従業員数":
                        data[Schema.EMP_NUM] = val
                    elif key == "薬剤師数":
                        data["薬剤師数"] = val
                    elif key == "施設エリア":
                        data["施設エリア"] = val

        main_table = soup.find("table", class_=lambda c: c and "m3-table--m3p_jobnavi" in c and "free-basic-info" not in c)
        if main_table:
            parse_table(main_table)

        free_table = soup.find("table", class_=lambda c: c and "free-basic-info" in c)
        if free_table:
            parse_table(free_table)

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    YakukyariScraper().execute(LIST_URL)
