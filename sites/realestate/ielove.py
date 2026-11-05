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
_DETAIL_SITEMAP = re.compile(r"sitemap-company-detail-\d+\.xml$")
# h1 にページタイトル由来の「（所在地）の店舗情報・口コミ・評判｜いえらぶ不動産会社検索」が付くので落とす
_NAME_TAIL = re.compile(r"（[^（）]*）の店舗情報.*$|の店舗情報・口コミ・評判.*$")
_ADDR_TAIL = re.compile(r"\s*地図を表示\s*$")
_PREFS = (
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県", "茨城県", "栃木県", "群馬県",
    "埼玉県", "千葉県", "東京都", "神奈川県", "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県", "徳島県", "香川県", "愛媛県", "高知県", "福岡県",
    "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
)


class IeloveScraper(StaticCrawler):
    """いえらぶ 不動産会社情報スクレイパー

    sitemap-company.xml はサイトマップ索引（sitemapindex）。子サイトマップ
    sitemap-company-detail-N.xml を1本ずつ読み、会社詳細ページ /company/co-{ID}/ を
    順に取得する（全URLを先に集めず、子サイトマップ単位で逐次処理する）。
    """

    DELAY = 0.5

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        try:
            is_index, locs = self._locs(url)
        except Exception as e:  # noqa: BLE001
            self.logger.warning("サイトマップ取得エラー: %s", e)
            return
        children = [u for u in locs if _DETAIL_SITEMAP.search(u)] if is_index else [url]
        self.logger.info("詳細サイトマップ: %d 本", len(children))
        self.total_items = 0
        for child in children:
            try:
                clocs = locs if child == url else self._locs(child)[1]
            except Exception as e:  # noqa: BLE001
                self.logger.warning("子サイトマップ取得エラー %s: %s", child, e)
                continue
            batch = []
            for u in clocs:
                u = u if u.endswith("/") else u + "/"
                if _COMPANY_PATTERN.match(u) and u not in seen:
                    seen.add(u)
                    batch.append(u)
            self.total_items += len(batch)
            self.logger.info("%s: 会社URL %d 件（累計 %d）", child, len(batch), self.total_items)
            for cu in batch:
                item = self._scrape_detail(cu)
                if item:
                    yield item

    def _locs(self, sitemap_url: str) -> tuple[bool, list[str]]:
        resp = self.session.get(sitemap_url, timeout=self.TIMEOUT)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        is_index = root.tag.endswith("sitemapindex")
        locs = [el.text.strip() for el in root.iter() if el.tag.endswith("loc") and el.text]
        return is_index, locs

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        name_tag = soup.select_one("h1.company-name, h1.companyName, h1")
        if name_tag:
            name = _NAME_TAIL.sub("", name_tag.get_text(strip=True)).strip()
            if name:
                data[Schema.NAME] = name

        for tr in soup.select("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            label = th.get_text(strip=True)
            value = td.get_text(" ", strip=True)

            if ("所在地" in label or "住所" in label) and Schema.ADDR not in data:
                m = re.match(r"^〒?\s*(\d{3})[-－]?(\d{4})", value)
                if m:
                    data[Schema.POST_CODE] = f"{m.group(1)}-{m.group(2)}"
                    value = value[m.end():].strip()
                data[Schema.ADDR] = _ADDR_TAIL.sub("", value)
            elif ("TEL" in label or "電話" in label) and Schema.TEL not in data:
                m = re.search(r"0\d{1,4}[-－]?\d{1,4}[-－]?\d{3,4}", value)
                data[Schema.TEL] = m.group(0) if m else value
            elif "代表者" in label:
                data[Schema.REP_NM] = value
            elif "営業時間" in label:
                data[Schema.TIME] = value
            elif "定休日" in label:
                data[Schema.HOLIDAY] = value
            elif "免許" in label or "宅建" in label:
                data[Schema.LOB] = value

        addr = data.get(Schema.ADDR, "")
        for p in _PREFS:
            if addr.startswith(p):
                data[Schema.PREF] = p
                break

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    IeloveScraper().execute("https://www.ielove.co.jp/sitemap/sitemap-company.xml")
