import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_COMPANY_PATTERN = re.compile(r"^https://www\.ielove\.co\.jp/company/co-\d+/?$")


class IeloveScraper(StaticCrawler):
    """いえらぶ 不動産会社情報スクレイパー"""

    DELAY = 0.5

    def parse(self, url: str) -> Generator[dict, None, None]:
        """サイトマップから会社ページURL(co-\d+)を収集してスクレイプ"""
        company_urls = self._collect_company_urls(url)
        self.total_items = len(company_urls)
        self.logger.info("会社URL収集完了: %d 件", len(company_urls))
        for cu in company_urls:
            item = self._scrape_detail(cu)
            if item:
                yield item

    def _collect_company_urls(self, sitemap_url: str) -> list[str]:
        urls = []
        try:
            resp = self.session.get(sitemap_url, timeout=self.TIMEOUT)
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
            for el in root.iter():
                if el.tag.endswith("loc") and el.text:
                    u = el.text.strip()
                    if _COMPANY_PATTERN.match(u):
                        urls.append(u)
        except Exception as e:
            self.logger.warning("サイトマップ取得エラー: %s", e)
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        # 社名
        name_tag = soup.select_one("h1.company-name, h1.companyName, h1")
        if name_tag:
            data[Schema.NAME] = name_tag.get_text(strip=True)

        # 会社情報テーブル（th/td形式）
        for tr in soup.select("tr, .company-info-row"):
            th = tr.find("th") or tr.find(".label")
            td = tr.find("td") or tr.find(".value")
            if not th or not td:
                continue
            label = th.get_text(strip=True)
            value = td.get_text(" ", strip=True)

            if "所在地" in label or "住所" in label:
                m = re.match(r"^〒?\s*(\d{3})[-]?(\d{4})", value)
                if m:
                    data[Schema.POST_CODE] = f"{m.group(1)}-{m.group(2)}"
                    value = value[m.end():].strip()
                data[Schema.ADDR] = value
            elif "TEL" in label or "電話" in label:
                data[Schema.TEL] = value
            elif "代表者" in label:
                data[Schema.REP_NM] = value
            elif "営業時間" in label:
                data[Schema.TIME] = value
            elif "定休日" in label:
                data[Schema.HOLIDAY] = value
            elif "免許" in label or "宅建" in label:
                data[Schema.LOB] = value

        # 都道府県（パンくず or 住所から）
        pref_tag = soup.select_one("span.pref, .breadcrumb a[href*='/pref/']")
        if pref_tag:
            data[Schema.PREF] = pref_tag.get_text(strip=True)

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    IeloveScraper().execute("https://www.ielove.co.jp/sitemap/sitemap-company.xml")
