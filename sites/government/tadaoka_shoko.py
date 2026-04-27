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

PAGE_URL = "https://www.tadaoka.or.jp/kaiin.html"


class TadaokaShokoCrawler(StaticCrawler):
    """忠岡町商工会 会員企業情報スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["業種"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)
        if soup is None:
            return

        for tr in soup.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue

            industry = re.sub(r"\s+", " ", tds[0].get_text(" ", strip=True)).strip()
            if not industry:
                continue

            for a in tds[1].find_all("a", href=True):
                href = (a.get("href") or "").strip()
                name = re.sub(r"\s+", " ", a.get_text(" ", strip=True)).strip()
                if not name or not href:
                    continue
                hp = urljoin(url, href)
                yield {
                    Schema.URL: url,
                    Schema.NAME: name,
                    Schema.HP: hp,
                    "業種": industry,
                }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    TadaokaShokoCrawler().execute(PAGE_URL)
