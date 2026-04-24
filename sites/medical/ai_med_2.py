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
LIST_URL = "https://ai-med.jp/sika?page={}"
MAX_CONSECUTIVE_EMPTY = 5

_POST_CODE_RE = re.compile(r"〒\s*(\d{3})-?(\d{4})")
_SCORE_RE = re.compile(r"評価\s*([\d.]+)")
_REVIEW_RE = re.compile(r"口コミ\s*(\d+)\s*件")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class AiMed2Scraper(StaticCrawler):
    """ai-med.jp 歯科系クリニック情報スクレイパー (site_id=ai_med_2)"""

    DELAY = 0.5
    EXTRA_COLUMNS = ["最寄り駅", "診療備考", "こだわり条件"]

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

            main = soup.select_one("main")
            if main is None:
                consecutive_empty += 1
                page += 1
                continue

            links = [
                urljoin(BASE_URL, a.get("href"))
                for a in main.select("div.search_result.clinic_info a[href*='/hospitals/']")
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

            if soup.select_one('link[rel="next"]') is None and not new_links:
                break

            page += 1

        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}

        name_tag = soup.select_one("h2.clinic_name")
        if name_tag:
            data[Schema.NAME] = _clean(name_tag.get_text())

        labels = [a.get_text(strip=True) for a in soup.select("ul.clinic_label li a")]
        if labels:
            data[Schema.CAT_SITE] = "|".join(labels)

        KEY_MAP = {
            "診療時間": Schema.TIME,
            "休診日": Schema.HOLIDAY,
            "住所": Schema.ADDR,
            "連絡先": Schema.TEL,
            "HP": Schema.HP,
            "最寄り駅": "最寄り駅",
            "診療・診察時間の備考": "診療備考",
            "こだわり条件": "こだわり条件",
        }

        for dl in soup.select("div.details_wrap dl.info_details"):
            dt = dl.find("dt")
            dd = dl.find("dd")
            if not dt or not dd:
                continue
            img = dt.find("img", alt=True) if hasattr(dt, "find") else None
            if img and img.get("alt", "").strip():
                key_raw = re.sub(r"\s+", "", img["alt"].strip())
            else:
                key_raw = _clean(dt.get_text(" ", strip=True))
            key_norm = {
                "最寄り駅：": "最寄り駅",
                "最寄り駅": "最寄り駅",
            }.get(key_raw, key_raw)
            val = _clean(dd.get_text(" ", strip=True))
            target = KEY_MAP.get(key_norm)
            if target:
                data[target] = val

        raw_addr = data.get(Schema.ADDR, "")
        if raw_addr:
            m = _POST_CODE_RE.search(raw_addr)
            if m:
                data[Schema.POST_CODE] = f"{m.group(1)}-{m.group(2)}"
                raw_addr = _POST_CODE_RE.sub("", raw_addr).strip()
            data[Schema.ADDR] = _clean(raw_addr)

        main_text = soup.select_one("main").get_text(" ", strip=True) if soup.select_one("main") else ""
        m_score = _SCORE_RE.search(main_text)
        if m_score:
            score = m_score.group(1)
            if score not in ("0", "0.0"):
                data[Schema.SCORES] = score
        m_rev = _REVIEW_RE.search(main_text)
        if m_rev:
            count = m_rev.group(1)
            if count != "0":
                data[Schema.REV_SCR] = count

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    AiMed2Scraper().execute("https://ai-med.jp/sika")
