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

BASE_URL = "https://caloo.jp"
START_URL = "https://caloo.jp/hospitals/search/01/all"

PREF_CODES = [str(i).zfill(2) for i in range(1, 48)]


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class CalooScraper(StaticCrawler):
    """Caloo 病院・クリニック情報スクレイパー（caloo.jp）"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["診療科目"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        for code in PREF_CODES:
            list_url = f"{BASE_URL}/hospitals/search/{code}/all"
            self.logger.info("都道府県コード: %s", code)
            yield from self._scrape_pref(list_url, seen)

    def _scrape_pref(self, list_url: str, seen: set) -> Generator[dict, None, None]:
        current = list_url
        page = 1
        while current:
            soup = self.get_soup(current)
            if soup is None:
                break

            links = soup.select("a[href^='/hospitals/detail/']")
            if not links:
                break

            for a in links:
                href = a.get("href", "").strip()
                if not re.search(r"/detail/\d+$", href):
                    continue
                full = BASE_URL + href
                if full not in seen:
                    seen.add(full)
                    item = self._scrape_detail(full)
                    if item and item.get(Schema.NAME):
                        yield item

            next_a = soup.find("a", string="次 »")
            if next_a and next_a.get("href"):
                page += 1
                current = f"{list_url}?page={page}"
            else:
                break

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        # 院長名
        doctor_box = soup.find("div", class_="hosp-top-doctor")
        if doctor_box:
            name_div = doctor_box.find("div", class_="name")
            if name_div:
                data[Schema.REP_NM] = _clean(name_div.get_text()).replace("院長", "").strip()

        # 基本情報テーブル
        for section in soup.find_all("section", class_="hospbox"):
            h2 = section.find("h2")
            if not h2 or "基本情報" not in h2.get_text():
                continue
            tbl = section.find("table", class_="hosp-tbl")
            if not tbl:
                continue
            for tr in tbl.find_all("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                label = th.get_text(strip=True)
                val = _clean(td.get_text(" "))
                if label == "医療機関名称":
                    data[Schema.NAME] = val
                elif "所在地" in label:
                    m = re.search(r"〒?(\d{3})-?(\d{4})", val)
                    if m:
                        data[Schema.POST_CODE] = f"{m.group(1)}-{m.group(2)}"
                        val = re.sub(r"〒?\d{3}-\d{4}\s*", "", val).strip()
                    data[Schema.ADDR] = val.replace("【地図】", "").strip()
                elif "電話番号" in label:
                    data[Schema.TEL] = val
                elif "公式サイト" in label:
                    a = td.find("a", href=True)
                    data[Schema.HP] = a["href"] if a else val
                elif "管理医師" in label and not data.get(Schema.REP_NM):
                    data[Schema.REP_NM] = val.replace("院長", "").strip()

        # 診療科目
        kamoks = soup.find("section", id="kamoksall")
        if kamoks:
            tbl = kamoks.find("table", class_="hosp-tbl")
            if tbl:
                for tr in tbl.find_all("tr"):
                    th = tr.find("th")
                    td = tr.find("td")
                    if th and "診療科目" in th.get_text() and td:
                        items = [_clean(dd.get_text()) for dd in td.find_all("dd")]
                        data["診療科目"] = "・".join(items)

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    CalooScraper().execute(START_URL)
