import json
import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

BASE_URL = "https://www.ozmall.co.jp"
DETAIL_RE = re.compile(r"^https://www\.ozmall\.co\.jp/hairsalon/\d+/$")


class OzmallHairSalonScraper(StaticCrawler):
    """OZmallヘアサロンの店舗情報を取得する静的クローラー。"""

    DELAY = 0.3

    def prepare(self):
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/139.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
                "Referer": BASE_URL + "/",
            }
        )

    def parse(self, url: str) -> Generator[dict, None, None]:
        current_url = url
        seen_detail_urls: set[str] = set()

        while current_url:
            self.logger.info("一覧ページ取得: %s", current_url)
            soup = self.get_soup(current_url)

            detail_urls: list[str] = []
            for link in soup.select("a[href]"):
                href = link.get("href")
                if not href:
                    continue

                detail_url = urljoin(BASE_URL, href)
                if not DETAIL_RE.match(detail_url):
                    continue
                if detail_url in seen_detail_urls:
                    continue

                seen_detail_urls.add(detail_url)
                detail_urls.append(detail_url)

            for detail_url in detail_urls:
                if self.DELAY > 0:
                    time.sleep(self.DELAY)

                item = self._scrape_detail(detail_url)
                if item:
                    yield item

            next_url = None
            for link in soup.select("a[href]"):
                href = link.get("href")
                if not href:
                    continue
                next_candidate = urljoin(current_url, href)
                if self._is_next_list_page(current_url, next_candidate):
                    next_url = next_candidate
                    break

            current_url = next_url

    def _scrape_detail(self, url: str) -> dict | None:
        self.logger.info("詳細ページ取得: %s", url)
        soup = self.get_soup(url)

        item: dict[str, str] = {
            Schema.URL: url,
            Schema.CAT_SITE: "ヘアサロン",
        }

        table = soup.select_one("table.common-table")
        if table:
            for row in table.select("tr"):
                th = row.select_one("th")
                td = row.select_one("td")
                if not th or not td:
                    continue

                label = self._normalize_text(th.get_text(" ", strip=True))
                value = self._normalize_text(td.get_text(" ", strip=True))

                if label == "サロン名":
                    name, kana = self._split_name_and_kana(value)
                    if name:
                        item[Schema.NAME] = name
                    if kana:
                        item[Schema.NAME_KANA] = kana
                elif label == "住所":
                    item[Schema.ADDR] = value.replace("地図で見る", "").strip()
                elif label == "電話番号":
                    item[Schema.TEL] = value.split("※")[0].strip()
                elif label == "付近の駅":
                    item["最寄駅"] = value
                elif label == "アクセス":
                    item["アクセス"] = value
                elif label == "道順":
                    item["道順"] = value
                elif label == "営業時間":
                    item[Schema.TIME] = value
                elif label == "定休日":
                    item[Schema.HOLIDAY] = value
                elif label == "支払方法":
                    item[Schema.PAYMENTS] = value
                elif label == "代表者名":
                    item[Schema.REP_NM] = value

        self._merge_json_ld(soup, item)

        if Schema.NAME not in item:
            return None

        return item

    def _merge_json_ld(self, soup, item: dict[str, str]) -> None:
        for script in soup.select('script[type="application/ld+json"]'):
            raw = script.get_text(strip=True)
            if not raw:
                continue

            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue

            if not isinstance(payload, dict):
                continue
            if payload.get("@type") != "Hairsalon":
                continue

            if Schema.NAME not in item and payload.get("name"):
                item[Schema.NAME] = self._normalize_text(payload["name"])
            if Schema.ADDR not in item and payload.get("address"):
                item[Schema.ADDR] = self._normalize_text(payload["address"])
            if Schema.TEL not in item and payload.get("telephone"):
                item[Schema.TEL] = self._normalize_text(payload["telephone"])
            return

    @staticmethod
    def _split_name_and_kana(value: str) -> tuple[str, str]:
        match = re.match(r"^(.*?)（(.*?)）$", value)
        if match:
            return match.group(1).strip(), match.group(2).strip()
        return value.strip(), ""

    @staticmethod
    def _normalize_text(text: str) -> str:
        return " ".join(str(text).split())

    @staticmethod
    def _is_next_list_page(current_url: str, next_url: str) -> bool:
        current = urlparse(current_url)
        target = urlparse(next_url)
        if current.path != target.path:
            return False
        if "pageNo=" not in target.query:
            return False

        current_page_match = re.search(r"(?:^|&)pageNo=(\d+)", current.query)
        next_page_match = re.search(r"(?:^|&)pageNo=(\d+)", target.query)
        current_page = int(current_page_match.group(1)) if current_page_match else 1
        next_page = int(next_page_match.group(1)) if next_page_match else 1
        return next_page == current_page + 1


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    OzmallHairSalonScraper().execute("https://www.ozmall.co.jp/hairsalon/list/")
