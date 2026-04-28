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

BASE_URL = "https://job-con.jp"


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class JobconSecurityScraper(DynamicCrawler):
    """ジョブコンプラスS 警備求人スクレイパー（job-con.jp/security）"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["業種", "募集職種"]

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

            for a in soup.select("a.job-card__element.--title, a[href*='/security/detail/']"):
                href = a.get("href", "").strip()
                if not href:
                    company_id = a.get("data-company-id", "")
                    offer_id = a.get("data-offer-id", "")
                    if company_id and offer_id:
                        href = f"/security/detail/{company_id}/{offer_id}"
                full = href if href.startswith("http") else BASE_URL + href
                if full not in seen:
                    seen.add(full)
                    urls.append(full)

            next_a = soup.select_one("a.c-pagination__link.--next")
            if next_a and next_a.get("aria-disabled") != "true" and next_a.get("href"):
                href = next_a["href"]
                next_url = href if href.startswith("http") else BASE_URL + href
                current = next_url if next_url != current else None
            else:
                current = None

        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url, wait_until="networkidle")
        if soup is None:
            return None

        data = {Schema.URL: url}

        for p in soup.select("div.detail-body__text p"):
            text = p.get_text(" ", strip=True)
            if "業種" in text and "職種" in text:
                parts = text.split("職種：")
                if "業種：" in parts[0]:
                    data["業種"] = _clean(parts[0].replace("業種：", ""))
                if len(parts) > 1:
                    data["募集職種"] = _clean(parts[1])
                break

        for tr in soup.select("table tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            key = th.get_text(strip=True)
            val = _clean(td.get_text(" "))
            if "企業名" in key:
                data[Schema.NAME] = val
            elif "電話番号" in key:
                data[Schema.TEL] = val
            elif "住所" in key:
                data[Schema.ADDR] = re.sub(r"〒", "", val).strip()

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    JobconSecurityScraper().execute("https://job-con.jp/security/search")
