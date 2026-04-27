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

BASE_URL = "https://meetsmore.com"
PREF_RE = re.compile(r"^(東京都|北海道|大阪府|京都府|.{2,3}県)")


class MeetsMorScraper(DynamicCrawler):
    """ミツモア 業者情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["エリア", "従業員数", "創業"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        """サービス一覧 → 各サービスページ → 業者カードからスクレイプ"""
        service_urls = self._get_service_urls(url)
        self.logger.info("サービスURL収集完了: %d 件", len(service_urls))
        for svc_url, svc_name in service_urls:
            yield from self._scrape_service(svc_url, svc_name)

    def _get_service_urls(self, url: str) -> list[tuple[str, str]]:
        soup = self.get_soup(url, wait_until="networkidle")
        if soup is None:
            return []
        seen: set[str] = set()
        results = []
        for a in soup.select('a[href^="/services/"]'):
            href = a.get("href", "").rstrip("/")
            name = a.get_text(strip=True)
            full = urljoin(BASE_URL, href)
            if full not in seen and name:
                seen.add(full)
                results.append((full, name))
        return results

    def _scrape_service(self, svc_url: str, svc_name: str) -> Generator[dict, None, None]:
        # スクロールして全カードを読み込む
        try:
            self.page.goto(svc_url, timeout=60000, wait_until="domcontentloaded")
            self.page.wait_for_selector("a[href^='/p/']", timeout=20000)
            for _ in range(12):
                last = self.page.locator("a[href^='/p/']").count()
                self.page.mouse.wheel(0, 2200)
                self.page.wait_for_timeout(700)
                if self.page.locator("a[href^='/p/']").count() <= last:
                    break
        except Exception:
            return

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(self.page.content(), "html.parser")

        seen_hrefs: set[str] = set()
        for a in soup.select("a[href^='/p/']"):
            href = a.get("href", "").split("?")[0].rstrip("/")
            if not href or href in seen_hrefs:
                continue
            seen_hrefs.add(href)

            card = a
            for _ in range(5):
                parent = card.parent
                if parent is None:
                    break
                card = parent

            data = {Schema.URL: urljoin(BASE_URL, href), Schema.CAT_SITE: svc_name}

            # 名称
            for cls in ["css-142s7ap", "css-1iiro1j"]:
                el = a.select_one(f"div.{cls}, span.{cls}")
                if el:
                    data[Schema.NAME] = el.get_text(strip=True).split("\n")[0].strip()
                    break

            # テーブルから各フィールド
            for table in card.select("table.medium-editor-table, tbody[id^='medium-editor-table']"):
                for tr in table.select("tr"):
                    cells = tr.select("th, td")
                    if len(cells) < 2:
                        continue
                    key = cells[0].get_text(strip=True)
                    val_el = cells[1]
                    val = val_el.get_text(" ", strip=True)
                    link_a = val_el.find("a", href=True)
                    href_val = link_a["href"].strip() if link_a else ""

                    if "所在地" in key and Schema.ADDR not in data:
                        data[Schema.ADDR] = val
                        m = PREF_RE.match(val.strip())
                        if m:
                            data[Schema.PREF] = m.group(1)
                    elif "対応エリア" in key:
                        data["エリア"] = val
                    elif "営業時間" in key:
                        data[Schema.TIME] = val
                    elif "電話" in key:
                        data[Schema.TEL] = val
                    elif "URL" in key or "ホームページ" in key:
                        data[Schema.HP] = href_val or val
                    elif "従業員数" in key:
                        data["従業員数"] = val
                    elif "創業" in key:
                        data["創業"] = val

            if data.get(Schema.NAME):
                yield data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    MeetsMorScraper().execute("https://meetsmore.com/services")
