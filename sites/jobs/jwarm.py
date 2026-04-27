import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

BASE_URL = "https://www.jwarm.net"
LIST_URL = "https://www.jwarm.net/uni_items.php?pg={}&ig=i"


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class JwarmScraper(DynamicCrawler):
    """ジェイウォーム 求人スクレイパー（jwarm.net）"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["業種", "代表者", "資本金", "設立日", "事業内容"]

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
        page = 1
        while True:
            soup = self.get_soup(LIST_URL.format(page), wait_until="networkidle")
            if soup is None:
                break

            item_list = soup.find("div", id="itemList")
            if not item_list:
                break

            links = [a.get("href", "") for a in item_list.select("span.detail_btn a")]
            if not links:
                break

            for link in links:
                full = BASE_URL + "/" + link.lstrip("/")
                if full not in seen:
                    seen.add(full)
                    urls.append(full)

            page += 1

        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url, wait_until="networkidle")
        if soup is None:
            return None

        data = {Schema.URL: url}

        tel_span = soup.find("span", class_="Tel")
        if tel_span:
            data[Schema.TEL] = tel_span.get_text(strip=True)

        target_div = soup.find("div", id="kigyou_data")
        if target_div:
            for row in target_div.find_all("tr"):
                th = row.find("th")
                td = row.find("td")
                if not th or not td:
                    continue
                label = th.get_text(strip=True)
                value = re.sub(r"[\u3000\xa0]", " ", td.get_text(strip=True)).strip()

                if label == "企業名称":
                    data[Schema.NAME] = value
                elif label == "掲載住所":
                    if value.startswith("〒"):
                        parts = re.split(r"\s+", value, maxsplit=1)
                        if len(parts) == 2:
                            data[Schema.POST_CODE] = parts[0]
                            data[Schema.ADDR] = parts[1].strip()
                        else:
                            data[Schema.ADDR] = value
                    else:
                        data[Schema.ADDR] = value
                elif label == "設立":
                    data["設立日"] = value
                elif label == "URL":
                    data[Schema.HP] = value
                elif label == "代表者":
                    data[Schema.REP_NM] = value
                elif label == "資本金":
                    data[Schema.CAP] = value
                elif label == "事業内容":
                    data["事業内容"] = value
                elif label == "売上高":
                    data[Schema.SALES] = value
                elif label == "従業員数":
                    data[Schema.EMP_NUM] = value

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    JwarmScraper().execute("https://www.jwarm.net/uni_items.php?pg=1&ig=i")
