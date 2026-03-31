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

BASE_URL = "https://next.rikunabi.com"
# 社用車制度 keyword search
START_URL = "https://next.rikunabi.com/job_search/kw/%E7%A4%BE%E7%94%A8%E8%BB%8A%E5%88%B6%E5%BA%A6/"


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class RikunabiNextSyayoshaScraper(DynamicCrawler):
    """リクナビNEXT 社用車あり求人 企業情報スクレイパー（next.rikunabi.com）"""

    DELAY = 2.0
    EXTRA_COLUMNS = ["業種", "事業内容"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls = self._collect_detail_urls(url)
        self.total_items = len(detail_urls)
        self.logger.info("詳細URL収集完了: %d 件", len(detail_urls))
        seen_companies: set[str] = set()
        for detail_url in detail_urls:
            item = self._scrape_detail(detail_url)
            if item and item.get(Schema.NAME):
                key = item[Schema.NAME]
                if key not in seen_companies:
                    seen_companies.add(key)
                    yield item

    def _collect_detail_urls(self, start_url: str) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        current = start_url
        while current:
            soup = self.get_soup(current, wait_until="networkidle")
            if soup is None:
                break

            for a in soup.select("a[href*='/viewjob/']"):
                href = a.get("href", "").strip()
                full = href if href.startswith("http") else BASE_URL + href
                if full not in seen:
                    seen.add(full)
                    urls.append(full)

            next_a = soup.select_one("a[rel='next'], a[aria-label='次へ']")
            if next_a and next_a.get("href"):
                next_url = urljoin(BASE_URL, next_a["href"])
                current = next_url if next_url != current else None
            else:
                current = None

        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url, wait_until="networkidle")
        if soup is None:
            return None

        data = {Schema.URL: url}

        company_section = soup.select_one("#company-information, div[id*='company']")
        if company_section is None:
            company_section = soup

        for tr in company_section.select("tr, dl"):
            th = tr.find("th") or tr.find("dt")
            td = tr.find("td") or tr.find("dd")
            if not th or not td:
                continue
            key = th.get_text(strip=True)
            val = _clean(td.get_text(" "))
            if "社名" in key or "会社名" in key:
                data[Schema.NAME] = val
            elif "本社所在地" in key or "所在地" in key or "住所" in key:
                m = re.match(r"^\s*〒?\s*(\d{3})-?(\d{4})", val)
                if m:
                    data[Schema.POST_CODE] = f"{m.group(1)}-{m.group(2)}"
                    val = val[m.end():].strip(" ,，、：:　")
                data[Schema.ADDR] = val
            elif "代表番号" in key or "企業代表番号" in key or "電話番号" in key:
                data[Schema.TEL] = val
            elif "企業ホームページ" in key or "HP" in key:
                a = td.find("a", href=True)
                data[Schema.HP] = a["href"] if a else val
            elif "事業内容" in key:
                data["事業内容"] = val
            elif "業種" in key:
                data["業種"] = val
            elif "代表者" in key:
                data[Schema.REP_NM] = val

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    RikunabiNextSyayoshaScraper().execute(START_URL)
