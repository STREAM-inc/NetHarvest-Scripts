import re
import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

PREF_RE = re.compile(r"(北海道|東京都|大阪府|京都府|.{2,3}県)")


class DokenchikuScraper(StaticCrawler):
    """ドケンチク 建設・建築会社情報スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["メール", "職種"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        """連番URL https://dokenchiku.com/company/{page} を順に巡回"""
        base = url.rstrip("/")
        page = 1
        empty_streak = 0
        while empty_streak < 10:
            page_url = f"{base}/{page}"
            soup = self.get_soup(page_url)
            if soup is None:
                empty_streak += 1
                page += 1
                continue

            name_tag = soup.select_one("h3.company02-content-companyname")
            if not name_tag or not name_tag.get_text(strip=True):
                empty_streak += 1
                page += 1
                continue

            empty_streak = 0
            data = {Schema.URL: page_url}
            data[Schema.NAME] = name_tag.get_text(strip=True)

            # .company-information dt/dd pairs
            info_items = soup.select(".company-information dt, .company-information dd")
            for i in range(0, len(info_items) - 1, 2):
                label = info_items[i].get_text(strip=True)
                value = info_items[i + 1].get_text(strip=True)
                if "住所" in label:
                    addr = value.replace("ー", "-")
                    m = PREF_RE.search(addr)
                    if m:
                        data[Schema.PREF] = m.group(1)
                    data[Schema.ADDR] = addr
                elif "電話番号" in label:
                    data[Schema.TEL] = value.replace("-", "")
                elif "メール" in label:
                    data["メール"] = value
                elif "ホームページ" in label:
                    data[Schema.HP] = value

            # 職種
            recruit_div = soup.find("div", class_="recruit-information")
            if recruit_div:
                text = recruit_div.get_text(separator="\n", strip=True)
                job_titles = re.findall(r"●職種：(.+)", text)
                if job_titles:
                    data["職種"] = ", ".join(job_titles)

            yield data
            page += 1


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    DokenchikuScraper().execute("https://dokenchiku.com/company")
