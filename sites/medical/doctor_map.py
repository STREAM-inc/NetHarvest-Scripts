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

_DTL_PATTERN = re.compile(r"^https://www\.doctor-map\.info/dtl/\d+$")
_ZIP_RE = re.compile(r"(〒\s*\d{3}-\d{4})")
PREFS = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]


class DoctorMapScraper(StaticCrawler):
    """ドクターマップ 医療施設情報スクレイパー"""

    DELAY = 2.0
    EXTRA_COLUMNS = ["エリア", "駐車場"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        dtl_urls = self._collect_dtl_urls(url)
        self.total_items = len(dtl_urls)
        self.logger.info("詳細URL収集完了: %d 件", len(dtl_urls))
        for dtl_url in dtl_urls:
            item = self._scrape_detail(dtl_url)
            if item:
                yield item

    def _collect_dtl_urls(self, base_sitemap: str) -> list[str]:
        """sitemap.xml + sitemap1.xml ~ sitemap15.xml を試して /dtl/ URLを収集"""
        urls: list[str] = []
        seen: set[str] = set()

        sitemap_candidates = [base_sitemap] + [
            base_sitemap.replace("/sitemap.xml", f"/sitemap{n}.xml") for n in range(1, 16)
        ]

        for sm_url in sitemap_candidates:
            try:
                r = self.session.get(sm_url, timeout=self.TIMEOUT)
                if r.status_code == 404:
                    continue
                r.raise_for_status()
                root = ET.fromstring(r.content)

                child_locs = [el.text.strip() for el in root.iter() if el.tag.endswith("loc") and el.text]

                # sitemap index → recurse into children
                if root.tag.lower().endswith("sitemapindex"):
                    for child_url in child_locs:
                        try:
                            cr = self.session.get(child_url, timeout=self.TIMEOUT)
                            cr.raise_for_status()
                            child_root = ET.fromstring(cr.content)
                            for loc in child_root.iter():
                                if loc.tag.endswith("loc") and loc.text:
                                    u = loc.text.strip()
                                    if _DTL_PATTERN.match(u) and u not in seen:
                                        seen.add(u)
                                        urls.append(u)
                        except Exception:
                            pass
                else:
                    for u in child_locs:
                        if _DTL_PATTERN.match(u) and u not in seen:
                            seen.add(u)
                            urls.append(u)
            except Exception as e:
                self.logger.debug("サイトマップ取得スキップ %s: %s", sm_url, e)

        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        # 都道府県・エリア
        loc = soup.select_one("div.location")
        if loc:
            loc_text = re.sub(r"\s+", " ", loc.get_text(" ", strip=True).replace("■", "")).strip().rstrip("／/ ")
            for p in PREFS:
                if loc_text.startswith(p):
                    data[Schema.PREF] = p
                    data["エリア"] = loc_text[len(p):].strip()
                    break

        # 業種・事業内容
        cats = [
            re.sub(r"[｜|]+$", "", re.sub(r"\s+", " ", el.get_text(strip=True)).strip())
            for el in soup.select("div.fn_cate p.facility_category_name")
        ]
        cats = [c for c in cats if c]
        if cats:
            data[Schema.CAT_SITE] = cats[0]
            if len(cats) > 1:
                data[Schema.LOB] = ", ".join(cats[1:])

        # 名称
        name_el = soup.select_one("dd.data.name")
        if name_el:
            data[Schema.NAME] = re.sub(r"\s+", " ", name_el.get_text(strip=True)).strip()

        # 所在地（郵便番号・住所）
        for dl in soup.select("dl.table, dl.table.adrs"):
            dt = dl.find("dt")
            dd = dl.find("dd")
            if not dt or not dd:
                continue
            if "所在地" in dt.get_text(strip=True):
                val = dd.get_text("\n", strip=True)
                m = _ZIP_RE.search(val)
                if m:
                    data[Schema.POST_CODE] = m.group(1).replace(" ", "")
                    data[Schema.ADDR] = re.sub(r"\s+", " ", val[m.end():]).strip()
                else:
                    data[Schema.ADDR] = re.sub(r"\s+", " ", val).strip()
                break

        # TEL
        tel_el = soup.select_one("dl.table.adrs dd.data.tel")
        if tel_el:
            data[Schema.TEL] = re.sub(r"\s+", " ", tel_el.get_text(strip=True)).strip()
        else:
            for dl in soup.select("dl.table, dl.table.adrs"):
                dt = dl.find("dt")
                dd = dl.find("dd")
                if dt and dd and "TEL" in dt.get_text(strip=True):
                    data[Schema.TEL] = re.sub(r"\s+", " ", dd.get_text(strip=True)).strip()
                    break

        # 駐車場
        park = soup.select_one("#si_PARKING")
        if park:
            data["駐車場"] = re.sub(r"\s+", " ", park.get_text(strip=True)).strip()

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    DoctorMapScraper().execute("https://www.doctor-map.info/sitemap.xml")
