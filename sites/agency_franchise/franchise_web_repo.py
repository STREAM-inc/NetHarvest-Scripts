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

_COMPANY_PATTERN = re.compile(r"^https://web-repo\.jp/fc-company/\d+$")
PREFS = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]


class FranchiseWebRepoScraper(StaticCrawler):
    """フランチャイズWEBリポート 企業情報スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["設立", "JFA加盟"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        company_urls = self._collect_company_urls(url)
        self.total_items = len(company_urls)
        self.logger.info("企業URL収集完了: %d 件", len(company_urls))
        for cu in company_urls:
            item = self._scrape_detail(cu)
            if item:
                yield item

    def _collect_company_urls(self, sitemap_url: str) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        self._recurse_sitemap(sitemap_url, urls, seen)
        return urls

    def _recurse_sitemap(self, sitemap_url: str, urls: list, seen: set) -> None:
        try:
            r = self.session.get(sitemap_url, timeout=self.TIMEOUT)
            r.raise_for_status()
            root = ET.fromstring(r.content)
            locs = [el.text.strip() for el in root.iter() if el.tag.endswith("loc") and el.text]
            if root.tag.lower().endswith("sitemapindex") or any(
                l.endswith(".xml") for l in locs
            ):
                for child in locs:
                    if child.endswith(".xml"):
                        self._recurse_sitemap(child, urls, seen)
                    elif _COMPANY_PATTERN.match(child) and child not in seen:
                        seen.add(child)
                        urls.append(child)
            else:
                for u in locs:
                    if _COMPANY_PATTERN.match(u) and u not in seen:
                        seen.add(u)
                        urls.append(u)
        except Exception as e:
            self.logger.warning("サイトマップ取得エラー %s: %s", sitemap_url, e)

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        table = soup.find("table", class_="chain_tb")
        if not table:
            return None

        for row in table.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if not th or not td:
                continue
            key = re.sub(r"\s+", " ", th.get_text(strip=True))
            val = re.sub(r"\s+", " ", td.get_text(separator="\n", strip=True))
            # 注意書き削除
            val = re.sub(r"※お電話の際は.+", "", val).strip()

            if "会社名" in key:
                data[Schema.NAME] = val
            elif "設立" in key:
                data["設立"] = val
            elif "事業内容" in key:
                data[Schema.LOB] = val
            elif "資本金" in key:
                data[Schema.CAP] = val
            elif "代表" in key:
                data[Schema.REP_NM] = val
            elif "電話" in key:
                data[Schema.TEL] = val
            elif key == "HP" or "ホームページ" in key:
                a = td.find("a", href=True)
                data[Schema.HP] = a["href"].strip() if a else val
            elif "所在地" in key:
                val_clean = re.sub(r"\[?\s*地図\s*\]?", "", val).strip()
                m = re.search(r"(〒?\s*\d{3}-?\d{4})", val_clean)
                if m:
                    digits = re.sub(r"\D", "", m.group(1))
                    if len(digits) == 7:
                        data[Schema.POST_CODE] = f"〒{digits[:3]}-{digits[3:]}"
                    addr = val_clean[m.end():].strip()
                else:
                    addr = val_clean
                pref = next((p for p in PREFS if p in addr), "")
                if pref:
                    data[Schema.PREF] = pref
                data[Schema.ADDR] = addr
            elif "売上" in key:
                data[Schema.SALES] = val
            elif "JFA" in key:
                data["JFA加盟"] = val

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    FranchiseWebRepoScraper().execute("https://web-repo.jp/sitemap.xml")
