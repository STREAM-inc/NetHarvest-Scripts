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

BASE_URL = "https://www.benrishi-navi.com/expert/"
LIST_URL = "https://www.benrishi-navi.com/expert/expert2_2.php"
START_URL = "https://www.benrishi-navi.com"


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("\u3000", " ")).strip()


class BenrishiNaviScraper(StaticCrawler):
    """弁理士ナビ 特許事務所スクレイパー（benrishi-navi.com）"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["FAX番号", "専門分野", "設立日"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls = self._collect_detail_urls()
        self.total_items = len(detail_urls)
        self.logger.info("事務所URL収集完了: %d 件", len(detail_urls))
        for detail_url in detail_urls:
            item = self._scrape_detail(detail_url)
            if item and item.get(Schema.NAME):
                yield item

    def _collect_detail_urls(self) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        payload = {
            "search": "1",
            "display_flag": "1",
            "search_count": "30",
            "s": "1",
        }
        page = 1

        while page <= 500:
            try:
                resp = self.session.post(
                    LIST_URL,
                    data=payload,
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=20,
                )
                resp.raise_for_status()
                resp.encoding = resp.apparent_encoding
            except Exception as e:
                self.logger.warning("リスト取得失敗: %s", e)
                break

            from bs4 import BeautifulSoup
            soup = BeautifulSoup(resp.text, "html.parser")

            # 詳細URL収集
            found = False
            for form in soup.find_all("form", action=True):
                action = form.get("action", "")
                if "expert2_3.php" not in action:
                    continue
                serial_input = form.find("input", {"name": "serial"})
                if not serial_input:
                    continue
                serial = serial_input.get("value", "")
                if not serial:
                    continue
                full_action = urljoin(BASE_URL, action)
                detail_url = f"{full_action}?serial={serial}"
                if detail_url not in seen:
                    seen.add(detail_url)
                    urls.append(detail_url)
                    found = True

            if not found:
                break

            # 次ページのペイロードを取得
            next_payload = None
            for form in soup.find_all("form"):
                page_input = form.find("input", {"name": "page"})
                if not page_input or page_input.get("value") != "next":
                    continue
                next_payload = {}
                for inp in form.find_all("input"):
                    name = inp.get("name")
                    if not name:
                        continue
                    if (inp.get("type") or "").lower() == "submit":
                        continue
                    next_payload[name] = inp.get("value", "")
                break

            if not next_payload:
                break
            payload = next_payload
            page += 1

        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        # 事務所名
        span = soup.find("span", class_="name")
        if span:
            text = " ".join(span.stripped_strings).replace("■", "").strip()
            m = re.search(r"(.+?)\s*[（(].*[）)]", text)
            data[Schema.NAME] = m.group(1).strip() if m else text

        # メイン情報テーブル
        table = soup.find("table", attrs={"width": "500", "bgcolor": "#939393"})
        if table:
            for tr in table.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) != 2:
                    continue
                label = tds[0].get_text(strip=True).replace("\u3000", "").replace(" ", "")
                val_td = tds[1]
                val = _clean(" ".join(val_td.stripped_strings))

                if "事務所所在地" in label:
                    data[Schema.ADDR] = val
                elif "電話番号" in label:
                    data[Schema.TEL] = val
                elif "FAX番号" in label:
                    data["FAX番号"] = val
                elif "設立年月日" in label:
                    data["設立日"] = val
                elif "専門分野" in label:
                    data["専門分野"] = val
                elif "ホームページ" in label:
                    a = val_td.find("a", href=True)
                    data[Schema.HP] = a["href"] if a else val

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    BenrishiNaviScraper().execute(START_URL)
