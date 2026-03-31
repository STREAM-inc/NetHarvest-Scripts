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

BASE_URL = "https://koujishi.com"
PREF_RE = re.compile(r"(北海道|東京都|大阪府|京都府|.{2,3}県)")


class KoujishiScraper(StaticCrawler):
    """工事士.com 電気工事会社情報スクレイパー"""

    DELAY = 0.6
    EXTRA_COLUMNS = ["情報更新日", "売上高"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls = self._collect_detail_urls(url)
        self.total_items = len(detail_urls)
        self.logger.info("詳細URL収集完了: %d 件", len(detail_urls))
        for detail_url in detail_urls:
            item = self._scrape_detail(detail_url)
            if item:
                yield item

    def _collect_detail_urls(self, list_url: str) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        page = 1
        while True:
            page_url = f"{list_url.rstrip('/')}/{page}/"
            soup = self.get_soup(page_url)
            if soup is None:
                break
            links = soup.select("h3.ListHeader_listItem__blueCatch__pc6_t a[href]")
            if not links:
                break
            added = 0
            for a in links:
                href = a.get("href", "").strip()
                if href.startswith("/detail/"):
                    full = urljoin(BASE_URL, href)
                    if full not in seen:
                        seen.add(full)
                        urls.append(full)
                        added += 1
            if added == 0:
                break
            page += 1
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        kv = {}
        for tr in soup.select("table tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            k = re.sub(r"\s+", " ", th.get_text(strip=True))
            v = re.sub(r"\s+", " ", td.get_text("\n", strip=True)).replace("\u3000", " ").strip()
            kv[k] = v

        data[Schema.NAME] = kv.get("社名", "")
        data[Schema.REP_NM] = kv.get("代表者", "")
        data[Schema.CAP] = kv.get("資本金", "")
        data["売上高"] = kv.get("売上高", "")
        data[Schema.EMP_NUM] = kv.get("従業員数", "")
        data[Schema.LOB] = kv.get("事業内容", "")

        # HP
        hp_th = soup.find("th", string=lambda x: x and "ホームページ" in x)
        if hp_th:
            hp_td = hp_th.find_next("td")
            if hp_td:
                a = hp_td.find("a", href=True)
                data[Schema.HP] = a["href"].strip() if a else kv.get("ホームページ", "")

        # 情報更新日
        for div in soup.find_all("div"):
            m = re.search(r"情報更新日[:：]\s*([0-9/]+)", div.get_text(" ", strip=True))
            if m:
                data["情報更新日"] = m.group(1)
                break

        # 事業所から郵便番号・住所
        office_th = soup.find("th", string=lambda x: x and "事業所" in x)
        if office_th:
            office_td = office_th.find_next("td")
            if office_td:
                html_str = office_td.decode_contents()
                s = re.sub(r"\s+", " ", html_str)
                idx = s.find("本社：")
                if idx != -1:
                    s = s[idx + len("本社："):]
                s_before_br = re.split(r"<br\s*/?>", s, maxsplit=1, flags=re.IGNORECASE)[0]
                s_text = re.sub(r"<[^>]+>", "", s_before_br).replace("\u3000", " ")
                s_text = re.sub(r"\s+", " ", s_text).strip()
                m = re.search(r"(〒\s*\d{3}-\d{4})", s_text)
                if m:
                    data[Schema.POST_CODE] = m.group(1).replace(" ", "")
                    data[Schema.ADDR] = re.sub(r"〒\s*\d{3}-\d{4}\s*", "", s_text).strip()
                else:
                    data[Schema.ADDR] = s_text
                pref_m = PREF_RE.search(data.get(Schema.ADDR, ""))
                if pref_m:
                    data[Schema.PREF] = pref_m.group(1)

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    KoujishiScraper().execute("https://koujishi.com/list/denkikoujishi-first/")
