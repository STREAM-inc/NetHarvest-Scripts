"""
サンマリエ (sunmarie_2) — トップページ起点・リージョン巡回版スクレイパー
対象URL: https://www.sunmarie.co.jp/

取得対象 (全 23 サロン):
    - サロン名 / 都道府県 / 郵便番号 / 住所
    - 電話番号 / 営業時間 / 定休日 / HP

取得フロー:
    トップページ (`/`) をフェッチ
        → `/store/<region>/` 形式のリージョンリンクを抽出 (6 リージョン)
            → 各リージョンページをフェッチ
                → `[class*="StoreListSalons_t-list-salon"]` の各サロン要素から
                  h2 (名称) と dl/dt/dd (住所・電話・営業時間・定休日) を解析して yield

    既存 `sunmarie.py` (`/store/` 単一ページ) との差分:
        - 起点をトップページに変更し、リージョンページを巡回する構成
        - EXTRA カラムを使用せず Schema 9 カラムに限定
        - サテライト出張所 (`StoreListSatellite_*`) は住所情報が無いため除外

実行方法:
    python scripts/sites/service/sunmarie_2.py
    python bin/run_flow.py --site-id sunmarie_2
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://www.sunmarie.co.jp"
DEFAULT_URL = "https://www.sunmarie.co.jp/"
HP_URL = "https://www.sunmarie.co.jp/"

# トップページから region link を発見できなかった場合のフォールバック
_FALLBACK_REGIONS = [
    "/store/hokkaido-tohoku/",
    "/store/kanto/",
    "/store/chubu/",
    "/store/kinki/",
    "/store/chugoku-shikoku/",
    "/store/kyusyu/",
]

_REGION_HREF_RE = re.compile(r"^/store/[a-z-]+/$")

_POST_RE = re.compile(r"〒?\s*(\d{3}-\d{4})\s*(.*)$")
_PREF_RE = re.compile(
    r"^(北海道|東京都|大阪府|京都府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


class Sunmarie2Scraper(StaticCrawler):
    """サンマリエ サロン一覧スクレイパー (トップページ起点・リージョン巡回版)"""

    DELAY = 1.5
    EXTRA_COLUMNS = []

    def parse(self, url: str):
        top_soup = self.get_soup(url)
        region_urls = self._discover_region_urls(top_soup)

        # 件数を先に把握して total_items を設定
        region_pages = []
        total = 0
        for region_url in region_urls:
            soup = self.get_soup(region_url)
            items = self._select_salon_items(soup)
            region_pages.append((region_url, items))
            total += len(items)
        self.total_items = total

        for region_url, items in region_pages:
            for item in items:
                try:
                    row = self._parse_item(item)
                    if row:
                        yield row
                except Exception as exc:  # noqa: BLE001
                    self.logger.warning("item parse failed (%s): %s", region_url, exc)
                    continue

    def _discover_region_urls(self, soup) -> list[str]:
        hrefs = []
        seen = set()
        for a in soup.select('a[href^="/store/"]'):
            href = a.get("href", "")
            if _REGION_HREF_RE.match(href) and href not in seen:
                seen.add(href)
                hrefs.append(href)
        if not hrefs:
            hrefs = _FALLBACK_REGIONS
        return [urljoin(BASE_URL, h) for h in hrefs]

    def _select_salon_items(self, soup):
        # サテライト出張所 (StoreListSatellite_*) は除外
        items = soup.select('[class^="StoreListSalons_t-list-salon"]')
        if not items:
            items = soup.select('[class*="StoreListSalons_t-list-salon"]')
        return items

    def _parse_item(self, item) -> dict | None:
        anchor = item.select_one('a[href^="/store/"]') or item.select_one("a[href]")
        href = anchor.get("href") if anchor else None
        detail_url = urljoin(BASE_URL, href) if href else ""

        h2 = item.select_one("h2") or item.select_one("h3")
        name = ""
        if h2:
            span = h2.select_one("span")
            if span:
                span.extract()
            name = re.sub(r"\s+", " ", h2.get_text(" ", strip=True)).strip()

        pairs = {
            dt.get_text(strip=True): dd.get_text(" ", strip=True)
            for dt, dd in zip(item.select("dt"), item.select("dd"))
        }
        raw_addr = pairs.get("住所", "")
        post_code = ""
        addr = raw_addr.strip()
        m_post = _POST_RE.match(addr)
        if m_post:
            post_code = m_post.group(1)
            addr = m_post.group(2).strip()
        pref = ""
        m_pref = _PREF_RE.match(addr)
        if m_pref:
            pref = m_pref.group(1)
            addr = addr[m_pref.end():].strip()

        if not name:
            return None

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: pairs.get("電話番号", ""),
            Schema.TIME: pairs.get("営業時間", ""),
            Schema.HOLIDAY: pairs.get("定休日", ""),
            Schema.HP: HP_URL,
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Sunmarie2Scraper()
    scraper.execute(DEFAULT_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
