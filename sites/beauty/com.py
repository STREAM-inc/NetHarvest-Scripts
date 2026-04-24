"""
バーバーナビ.com — 理容室・バーバー・レディースシェービング専門ポータル

取得対象:
    - /shop/{slug}/ (MENS) および /salon/{slug}/ (LADIES) の詳細ページ

取得フロー:
    WordPress の sitemap (wp-sitemap-posts-*_richtemplate-*.xml) から
    全店舗 URL を収集 → 各詳細ページの .info-table と SNS リンクを抽出。

実行方法:
    python scripts/sites/beauty/com.py
    python bin/run_flow.py --site-id com
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://barbernavi.com"

SITEMAP_URLS = [
    f"{BASE_URL}/wp-sitemap-posts-mens_richtemplate-1.xml",
    f"{BASE_URL}/wp-sitemap-posts-mens_richtemplate-2.xml",
    f"{BASE_URL}/wp-sitemap-posts-mens_richtemplate-3.xml",
    f"{BASE_URL}/wp-sitemap-posts-ladies_richtemplate-1.xml",
]

_PREF_PATTERN = re.compile(
    r"^(北海道|東京都|京都府|大阪府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


def _clean_multiline(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


class BarberNaviScraper(StaticCrawler):
    """バーバーナビ.com スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["アクセス", "平日営業時間", "土日営業時間", "予約URL"]

    _FIELD_MAP = {
        "STORE NAME": Schema.NAME,
        "ADDRESS": Schema.ADDR,
        "ACCESS": "アクセス",
        "PHONE": Schema.TEL,
        "WEEKDAYS": "平日営業時間",
        "WEEKENDS": "土日営業時間",
        "CLOSED": Schema.HOLIDAY,
        "PAYMENT": Schema.PAYMENTS,
    }

    def parse(self, url: str) -> Generator[dict, None, None]:
        shop_urls = self._collect_urls()
        self.total_items = len(shop_urls)
        self.logger.info("店舗URL収集完了: %d 件", len(shop_urls))

        for shop_url in shop_urls:
            try:
                item = self._scrape_detail(shop_url)
                if item and item.get(Schema.NAME):
                    yield item
            except Exception as e:
                self.logger.warning("詳細取得失敗 url=%s err=%s", shop_url, e)
                continue

    def _collect_urls(self) -> list[str]:
        urls: list[str] = []
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

        for sm_url in SITEMAP_URLS:
            try:
                resp = self.session.get(sm_url, timeout=30)
                if resp.status_code != 200:
                    self.logger.warning("sitemap取得失敗 %s status=%d", sm_url, resp.status_code)
                    continue
                root = ET.fromstring(resp.content)
            except Exception as e:
                self.logger.warning("sitemap解析失敗 %s err=%s", sm_url, e)
                continue

            for loc_el in root.findall(".//sm:url/sm:loc", ns):
                loc = loc_el.text and loc_el.text.strip()
                if not loc:
                    continue
                path = urlparse(loc).path
                if path.startswith("/shop/") or path.startswith("/salon/"):
                    urls.append(loc)

        return list(dict.fromkeys(urls))

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        h1 = soup.find("h1")
        if h1 and "404" in h1.get_text():
            return None

        data: dict = {Schema.URL: url}

        table = soup.select_one("table.info-table")
        if table:
            for tr in table.find_all("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                key = _clean(th.get_text())
                schema_key = self._FIELD_MAP.get(key)

                if schema_key == Schema.NAME:
                    data[Schema.NAME] = _clean(td.get_text())
                elif schema_key == Schema.ADDR:
                    addr = _clean(td.get_text())
                    m = _PREF_PATTERN.match(addr)
                    if m:
                        data[Schema.PREF] = m.group(1)
                        data[Schema.ADDR] = addr[m.end():].strip()
                    else:
                        data[Schema.ADDR] = addr
                elif schema_key == Schema.TEL:
                    tel = _clean(td.get_text())
                    if tel:
                        data[Schema.TEL] = tel
                elif schema_key == Schema.HOLIDAY:
                    data[Schema.HOLIDAY] = _clean(td.get_text())
                elif schema_key == Schema.PAYMENTS:
                    data[Schema.PAYMENTS] = _clean(td.get_text())
                elif schema_key in ("アクセス", "平日営業時間", "土日営業時間"):
                    # <br> を区切りとして保持しつつテキスト抽出
                    text = _clean(td.get_text(separator=" "))
                    data[schema_key] = text
                elif key == "WEBSITE":
                    a = td.find("a", href=True)
                    if a:
                        data[Schema.HP] = a["href"]
                elif key == "WEB BOOKING":
                    a = td.find("a", href=True)
                    if a:
                        data["予約URL"] = a["href"]

        # 名称のフォールバック: h1
        if not data.get(Schema.NAME) and h1:
            data[Schema.NAME] = _clean(h1.get_text())

        # SNS リンク抽出（ページ全体から）
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not href:
                continue
            if Schema.INSTA not in data and re.search(r"(?:www\.)?instagram\.com/[^/?#]+", href):
                data[Schema.INSTA] = href
            elif Schema.LINE not in data and re.search(r"(?:page\.line\.me|lin\.ee|line\.me/R/ti/p)", href):
                data[Schema.LINE] = href
            elif Schema.X not in data and re.search(r"(?:^|//)(?:www\.)?(?:twitter\.com|x\.com)/[^/?#]+", href):
                data[Schema.X] = href
            elif Schema.FB not in data and re.search(r"(?:www\.)?facebook\.com/[^/?#]+", href):
                data[Schema.FB] = href
            elif Schema.TIKTOK not in data and re.search(r"(?:www\.)?tiktok\.com/@[^/?#]+", href):
                data[Schema.TIKTOK] = href

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BarberNaviScraper()
    scraper.execute(BASE_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
