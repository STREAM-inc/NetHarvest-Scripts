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

BASE_URL = "https://ai-med.jp"
LIST_URL = "https://ai-med.jp/hospitals?page={}"
MAX_CONSECUTIVE_EMPTY = 5


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class AiMedScraper(StaticCrawler):
    """ai-med.jp 病院情報スクレイパー"""

    DELAY = 0.5
    EXTRA_COLUMNS = ["業種", "診療時間", "休診日", "最寄り駅"]

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
        consecutive_empty = 0
        page = 1

        while consecutive_empty < MAX_CONSECUTIVE_EMPTY:
            soup = self.get_soup(LIST_URL.format(page))
            if soup is None:
                consecutive_empty += 1
                page += 1
                continue

            main = soup.select_one(
                "body > div.wrapper.search > div.search_wrap.container > main"
            )
            if main is None:
                consecutive_empty += 1
                page += 1
                continue

            links = [
                BASE_URL + a.get("href")
                for a in main.select("a[href^='/hospitals/']")
                if a.get("href")
            ]
            new_links = [u for u in links if u not in seen]
            for u in new_links:
                seen.add(u)
                urls.append(u)

            if not new_links:
                consecutive_empty += 1
            else:
                consecutive_empty = 0

            # end-of-list hint
            if soup.select_one('link[rel="next"]') is None and not new_links:
                break

            page += 1

        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        name_tag = soup.select_one("h2.clinic_name")
        if name_tag:
            data[Schema.NAME] = _clean(name_tag.get_text())

        labels = [a.get_text(strip=True) for a in soup.select("ul.clinic_label li a")]
        if labels:
            data["業種"] = "|".join(labels)

        KEY_MAP = {
            "住所": Schema.ADDR,
            "電話番号": Schema.TEL,
            "診療時間": "診療時間",
            "休診日": "休診日",
            "最寄り駅": "最寄り駅",
        }

        for dl in soup.select("div.details_wrap dl.info_details"):
            dt = dl.find("dt")
            dd = dl.find("dd")
            if not dt or not dd:
                continue
            key_raw = _clean(dt.get_text(" ", strip=True))
            # normalize key
            img = dt.find("img", alt=True)
            if img and img.get("alt", "").strip():
                key_raw = re.sub(r"\s+", "", img["alt"].strip())
            key_norm = {
                "診療時間": "診療時間",
                "休診日": "休診日",
                "住所": "住所",
                "最寄り駅：": "最寄り駅",
                "最寄り駅": "最寄り駅",
                "連絡先": "電話番号",
            }.get(key_raw, key_raw)
            val = _clean(dd.get_text(" ", strip=True))
            target = KEY_MAP.get(key_norm)
            if target:
                data[target] = val

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    AiMedScraper().execute("https://ai-med.jp/hospitals")
