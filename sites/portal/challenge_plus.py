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

BASE_URL = "https://www.challenge-plus.jp"


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class ChallengePlusScraper(StaticCrawler):
    """チャレンジプラス 企業情報スクレイパー（challenge-plus.jp）"""

    DELAY = 0.5
    EXTRA_COLUMNS = ["代表者", "資本金", "事業内容"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls = self._collect_detail_urls(url)
        self.total_items = len(detail_urls)
        self.logger.info("詳細URL収集完了: %d 件", len(detail_urls))
        for detail_url in detail_urls:
            item = self._scrape_detail(detail_url)
            if item:
                yield item

    def _collect_detail_urls(self, list_url: str) -> list[str]:
        soup = self.get_soup(list_url)
        if soup is None:
            return []
        urls: list[str] = []
        seen: set[str] = set()
        main_block = soup.select_one("div.contents_wrap_main")
        if not main_block:
            return []
        for a in main_block.select("div.contents_wrap_main_innerA ul.interview_list_row li a"):
            href = a.get("href", "").strip()
            if not href:
                continue
            full = href if href.startswith("http") else BASE_URL + href
            if full not in seen:
                seen.add(full)
                urls.append(full)
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        # Name from breadcrumb
        crumb = soup.select_one("nav.topic_path ol li:nth-child(3) span[itemprop='name']")
        if crumb:
            full_text = crumb.get_text(strip=True)
            if "代表" in full_text:
                parts = full_text.split("代表", 1)
                data[Schema.NAME] = _clean(parts[0])
                data["代表者"] = _clean("代表" + parts[1])
            else:
                data[Schema.NAME] = _clean(full_text)

        # Company data block (dt/dd)
        block = (
            soup.select_one("div.companydata_new div.company_data_inner div.companydata_txt") or
            soup.select_one("div.company_data_inner div.companydata_txt") or
            soup.select_one("div.companydata_txt")
        )
        table_block = soup.select_one("div.companydata table.inter-detail-companytable")

        info: dict[str, str] = {}
        if block:
            dts = block.select("dt")
            dds = block.select("dd")
            for dt, dd in zip(dts, dds):
                k = _clean(dt.get_text(strip=True))
                a = dd.select_one("a")
                v = a.get("href", "") if a else _clean(dd.get_text("\n", strip=True))
                info[k] = v
        if table_block:
            for tr in table_block.select("tr"):
                cols = tr.find_all(["td", "th"])
                if len(cols) == 2:
                    k = _clean(cols[0].get_text(strip=True))
                    a = cols[1].select_one("a")
                    v = a.get("href", "") if a else _clean(cols[1].get_text("\n", strip=True))
                    if k:
                        info[k] = v

        for k in ("住所", "所在地"):
            if k in info:
                data[Schema.ADDR] = info[k].replace("\n", " ").strip()
                break
        for k in ("代表者", "代表", "代表取締役"):
            if k in info and Schema.REP_NM not in data and "代表者" not in data:
                data["代表者"] = info[k]
                break
        for k in ("URL", "ホームページ", "HP"):
            if k in info:
                data[Schema.HP] = info[k]
                break
        if "資本金" in info:
            data["資本金"] = info["資本金"]
        if "事業内容" in info:
            data["事業内容"] = info["事業内容"]

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    ChallengePlusScraper().execute("https://www.challenge-plus.jp/list/")
