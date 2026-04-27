import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://www.kenkou-job.com"


class BeproScraper(StaticCrawler):
    """美プロ（kenkou-job.com）求人企業情報スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["業種", "資本金"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        area_urls = self._collect_area_urls(url)
        self.logger.info("エリアURL収集完了: %d 件", len(area_urls))
        detail_urls = self._collect_detail_urls(area_urls)
        self.total_items = len(detail_urls)
        self.logger.info("詳細URL収集完了: %d 件", len(detail_urls))
        for detail_url in detail_urls:
            item = self._scrape_detail(detail_url)
            if item:
                yield item

    def _collect_area_urls(self, top_url: str) -> list[str]:
        soup = self.get_soup(top_url)
        if soup is None:
            return []
        seen: set[str] = set()
        urls: list[str] = []
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            full = urljoin(BASE_URL, href)
            if "/area/" in full and full not in seen:
                seen.add(full)
                urls.append(full)
        return urls if urls else [top_url]

    def _collect_detail_urls(self, area_urls: list[str]) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        for area_url in area_urls:
            current = area_url
            while current:
                soup = self.get_soup(current)
                if soup is None:
                    break
                for action_div in soup.find_all("div", class_="action"):
                    a = action_div.find("a", class_="btn-manu-link")
                    if a and a.get("href"):
                        full = urljoin(BASE_URL, a["href"].strip())
                        if full not in seen:
                            seen.add(full)
                            urls.append(full)
                next_li = soup.find("li", class_="next")
                if next_li and next_li.find("a"):
                    next_href = next_li.find("a")["href"]
                    next_url = urljoin(BASE_URL, next_href)
                    current = next_url if next_url != current else None
                else:
                    current = None
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        company_div = soup.find("div", class_="manu-company")
        if company_div:
            table = company_div.find("table")
            if table:
                for row in table.find_all("tr"):
                    th = row.find("th")
                    td = row.find("td")
                    if th and td:
                        key = th.get_text(strip=True)
                        val = td.get_text(strip=True)
                        if key == "会社名":
                            data[Schema.NAME] = val
                        elif key == "事業内容":
                            data["業種"] = val
                        elif key == "本社所在地":
                            data[Schema.ADDR] = val
                        elif key == "資本金":
                            data["資本金"] = val

        tel_div = soup.find("div", class_="tel")
        if tel_div:
            phone_p = tel_div.find("p", class_="phone-number")
            if phone_p:
                data[Schema.TEL] = phone_p.get_text(strip=True).replace("📞", "").strip()

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    BeproScraper().execute("https://www.kenkou-job.com")
