import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

LIST_URLS = [
    "http://www.ksci.or.jp/web.html",
    "http://www.ksci.or.jp/web2.html",
]

TABLE_CSS = "body > div > div.contents > div > table"


class KawachinaganoShokokaiScraper(StaticCrawler):
    """河内長野市商工会 会員情報スクレイパー（ksci.or.jp）"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["業種"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        for list_url in LIST_URLS:
            self.logger.info("ページ取得: %s", list_url)
            soup = self.get_soup(list_url)
            if soup is None:
                continue

            table = soup.select_one(TABLE_CSS)
            if not table:
                for t in soup.find_all("table"):
                    score = len(t.select("tr th a[href]"))
                    if score > 0:
                        table = t
                        break

            if not table:
                continue

            for tr in table.find_all("tr"):
                ths = tr.find_all("th")
                tds = tr.find_all("td")
                if not ths and not tds:
                    continue

                # Each row may have company name + HP link in th/td or th only
                for cell in ths + tds:
                    a = cell.find("a", href=True)
                    name = cell.get_text(" ", strip=True)
                    if not name or name in seen:
                        continue
                    if a and a.get("href", "").startswith("http"):
                        seen.add(name)
                        yield {
                            Schema.URL: list_url,
                            Schema.NAME: name,
                            Schema.HP: a["href"],
                        }
                    elif name and len(name) > 1:
                        seen.add(name)
                        yield {
                            Schema.URL: list_url,
                            Schema.NAME: name,
                        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    KawachinaganoShokokaiScraper().execute("http://www.ksci.or.jp/web.html")
