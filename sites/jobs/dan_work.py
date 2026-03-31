import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://www.dan-work.com"

REGION_URLS = [
    "https://www.dan-work.com/hokkaido/src_brea.html",
    "https://www.dan-work.com/touhoku/src_jdlq.html",
    "https://www.dan-work.com/hokuriku/src_koxs.html",
    "https://www.dan-work.com/kantou/src_qieo.html",
    "https://www.dan-work.com/toukai/src_culd.html",
    "https://www.dan-work.com/kansai/src_dbik.html",
    "https://www.dan-work.com/chuugoku/src_ranq.html",
    "https://www.dan-work.com/shikoku/src_bpvr.html",
    "https://www.dan-work.com/kyuusyuu/src_bebs.html",
]


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class DanWorkScraper(StaticCrawler):
    """男ワーク 求人情報スクレイパー（dan-work.com）"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["業種", "勤務地", "募集職種", "アクセス"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls: list[str] = []
        seen: set[str] = set()
        for region_url in REGION_URLS:
            self.logger.info("地域取得: %s", region_url)
            urls = self._collect_region_urls(region_url)
            for u in urls:
                if u not in seen:
                    seen.add(u)
                    detail_urls.append(u)
        self.total_items = len(detail_urls)
        self.logger.info("詳細URL収集完了: %d 件", len(detail_urls))
        for detail_url in detail_urls:
            item = self._scrape_detail(detail_url)
            if item:
                yield item

    def _collect_region_urls(self, start_url: str) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        current = start_url
        while current:
            soup = self.get_soup(current)
            if soup is None:
                break

            main = soup.select_one("#container > main")
            if not main:
                break

            for a in main.select("a.shoplinkbtn[href]"):
                if "詳細を見る" in a.get_text(strip=True):
                    href = a["href"].strip()
                    full = urljoin(current, href)
                    if full not in seen:
                        seen.add(full)
                        urls.append(full)

            next_a = main.find("a", string=lambda t: t and "次のページ" in t)
            if next_a and next_a.get("href"):
                next_url = urljoin(current, next_a["href"].strip())
                current = next_url if next_url != current else None
            else:
                current = None

        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        name_tag = soup.select_one(".shopname")
        if name_tag:
            data[Schema.NAME] = _clean(name_tag.get_text())

        for dl in soup.select("#recdata > div > div.data dl"):
            dt = dl.find("dt")
            dd = dl.find("dd")
            if not dt or not dd:
                continue
            key = dt.get_text(strip=True)
            val = _clean(" ".join(dd.stripped_strings))
            if "勤務地" in key:
                data["勤務地"] = val
            elif "業種" in key:
                data["業種"] = val
            elif "住所" in key:
                data[Schema.ADDR] = val
            elif key in ("TEL", "電話番号"):
                data[Schema.TEL] = val
            elif key in ("HP", "ホームページ", "URL"):
                data[Schema.HP] = val
            elif "定休日" in key:
                data[Schema.HOLIDAY] = val
            elif "アクセス" in key:
                data["アクセス"] = val
            elif "募集職種" in key:
                data["募集職種"] = val

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    DanWorkScraper().execute("https://www.dan-work.com")
