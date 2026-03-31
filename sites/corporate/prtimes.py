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

BASE_URL = "https://prtimes.jp"
START_URL = "https://prtimes.jp/technology/"

CATEGORY_PATHS = [
    "/technology/",
    "/mobile/",
    "/app/",
    "/entertainment/",
    "/beauty/",
    "/fashion/",
    "/lifestyle/",
    "/business/",
    "/gourmet/",
    "/sports/",
]


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class PrtimesScraper(DynamicCrawler):
    """PR TIMES 企業情報スクレイパー（prtimes.jp）"""

    DELAY = 2.0
    EXTRA_COLUMNS = ["業種", "資本金", "上場", "ビジネスカテゴリ"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_urls: set[str] = set()
        seen_companies: set[str] = set()

        for path in CATEGORY_PATHS:
            cat_url = BASE_URL + path
            self.logger.info("カテゴリ: %s", path)
            article_urls = self._collect_articles(cat_url)
            for article_url in article_urls:
                if article_url in seen_urls:
                    continue
                seen_urls.add(article_url)
                item = self._scrape_detail(article_url)
                if item and item.get(Schema.NAME):
                    key = item[Schema.NAME]
                    if key not in seen_companies:
                        seen_companies.add(key)
                        yield item

    def _collect_articles(self, cat_url: str) -> list[str]:
        urls: list[str] = []
        soup = self.get_soup(cat_url, wait_until="networkidle")
        if soup is None:
            return urls
        for article in soup.find_all("article", class_="list-article"):
            a = article.find("a")
            if a and a.get("href"):
                full = BASE_URL + a["href"] if not a["href"].startswith("http") else a["href"]
                urls.append(full)
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url, wait_until="networkidle")
        if soup is None:
            return None

        data = {Schema.URL: url}

        # 企業名
        name_tag = soup.find("a", class_=lambda c: c and "company-name_companyName" in c)
        if not name_tag:
            name_tag = soup.select_one("a.company-name_companyName__xKkFh")
        if name_tag:
            data[Schema.NAME] = _clean(name_tag.get_text())

        if not data.get(Schema.NAME):
            return None

        # 企業情報テーブル dl
        dl = soup.find("dl", class_=lambda c: c and "table_container" in c)
        if dl:
            for dt in dl.find_all("dt"):
                key = dt.get_text(strip=True)
                dd = dt.find_next_sibling("dd")
                if not dd:
                    continue
                val = _clean(dd.get_text(" "))
                if key == "本社所在地":
                    data[Schema.ADDR] = val
                elif key == "電話番号":
                    data[Schema.TEL] = val
                elif key == "業種":
                    data["業種"] = val
                elif key == "代表者名":
                    data[Schema.REP_NM] = val
                elif key == "資本金":
                    data["資本金"] = val
                elif key == "上場":
                    data["上場"] = val
                elif key in ("URL", "ホームページ"):
                    a = dd.find("a", href=True)
                    data[Schema.HP] = a["href"] if a else val

        # ビジネスカテゴリ
        try:
            bc_dt = soup.find("dt", string="ビジネスカテゴリ")
            if bc_dt:
                bc_dd = bc_dt.find_next_sibling("dd")
                if bc_dd:
                    spans = [s.get_text(strip=True) for s in bc_dd.find_all("span")]
                    data["ビジネスカテゴリ"] = "/".join(spans)
        except Exception:
            pass

        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    PrtimesScraper().execute(START_URL)
