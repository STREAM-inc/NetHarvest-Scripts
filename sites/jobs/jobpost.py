import json
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse, urldefrag

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://jobpost.jp"
START_URL = "https://jobpost.jp/search/g:1/"

MAX_G_ID = 500  # 職種カテゴリIDの上限（404 が続いたら終了）


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("\u3000", " ")).strip()


class JobpostScraper(StaticCrawler):
    """ジョブポスト 求人企業情報スクレイパー（jobpost.jp）"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["業種", "募集職種"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_urls: set[str] = set()
        seen_companies: set[str] = set()
        consecutive_empty = 0

        for g_id in range(1, MAX_G_ID + 1):
            g_url = f"{BASE_URL}/search/g:{g_id}/"
            detail_urls = self._collect_g_page(g_url)
            if not detail_urls:
                consecutive_empty += 1
                if consecutive_empty >= 10:
                    self.logger.info("連続 %d 回空のため終了", consecutive_empty)
                    break
                continue
            consecutive_empty = 0

            for detail_url in detail_urls:
                if detail_url in seen_urls:
                    continue
                seen_urls.add(detail_url)
                item = self._scrape_detail(detail_url)
                if item and item.get(Schema.NAME):
                    key = item[Schema.NAME]
                    if key not in seen_companies:
                        seen_companies.add(key)
                        yield item

    def _collect_g_page(self, start_url: str) -> list[str]:
        urls: list[str] = []
        current = start_url
        visited: set[str] = set()

        while current:
            if current in visited:
                break
            visited.add(current)

            soup = self.get_soup(current)
            if soup is None:
                break

            for a in soup.select(".apply a[href]"):
                href = a.get("href", "")
                full = urljoin(current, href)
                full, _ = urldefrag(full)
                if urlparse(full).netloc == "jobpost.jp":
                    path = urlparse(full).path
                    if re.fullmatch(r"/j\d+/?", path) and full not in urls:
                        urls.append(full)

            # 次ページ
            next_href = None
            link_next = soup.select_one("link[rel='next'][href]")
            if link_next:
                next_href = link_next.get("href")
            if not next_href:
                a_next = soup.select_one("a.next[href]")
                if a_next:
                    next_href = a_next.get("href")

            current = urljoin(current, next_href) if next_href else None

        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        # JSON-LD
        script = soup.find("script", type="application/ld+json")
        if script:
            try:
                obj = json.loads(script.string or "")
                if isinstance(obj, list):
                    job = next((j for j in obj if j.get("@type") == "JobPosting"), None)
                elif isinstance(obj, dict) and obj.get("@type") == "JobPosting":
                    job = obj
                else:
                    job = None
                if job:
                    org = job.get("hiringOrganization", {})
                    if isinstance(org, dict):
                        data[Schema.NAME] = _clean(org.get("name", ""))
                    addr = job.get("jobLocation", {}).get("address", {})
                    if isinstance(addr, dict):
                        data[Schema.PREF] = _clean(addr.get("addressRegion", ""))
            except Exception:
                pass

        # HTML セクション section.detail151009
        for sec in soup.find_all("section", class_="detail151009"):
            for item in sec.find_all("li"):
                h3 = item.find("h3")
                p = item.find("p", class_="readMore")
                if not (h3 and p):
                    continue
                label = h3.get_text(strip=True)
                val = _clean(p.get_text())
                if label in ("社名", "企業名") and not data.get(Schema.NAME):
                    data[Schema.NAME] = val
                elif label == "住所" and not data.get(Schema.ADDR):
                    data[Schema.ADDR] = re.sub(r"〒\d{3}-\d{4}\s*", "", val).strip()
                elif label == "事業内容" and not data.get("業種"):
                    data["業種"] = val
                elif label == "職種" and not data.get("募集職種"):
                    data["募集職種"] = val.split("/")[0].strip() if "/" in val else val

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    JobpostScraper().execute(START_URL)
