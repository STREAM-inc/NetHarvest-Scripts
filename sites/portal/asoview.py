"""
アソビュー — 全国のレジャー・体験予約ポータルサイト

取得対象:
    - 全国 12,371 施設 (掲載拠点) の基本情報
      店舗名、住所、営業時間、定休日、ジャンル、緯度/経度、
      価格帯、評価、口コミ数、設備情報、画像URL 等

取得フロー:
    1. /base/ (単一ページ) から 47都道府県ごとにグルーピングされた
       全施設 (/base/{id}/) リンクと名称・都道府県を収集
    2. 各詳細ページ /base/{id}/ から basic-information / facility-information
       テーブル、.base-data__genres、JSON-LD (LocalBusiness) を解析

実行方法:
    # ローカルテスト
    python scripts/sites/portal/asoview.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id asoview
"""

import json
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

BASE_URL = "https://www.asoview.com"
INDEX_URL = f"{BASE_URL}/base/"

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_CODE_PATTERN = re.compile(r"〒?\s*(\d{3}-\d{4})")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


class AsoviewScraper(StaticCrawler):
    """アソビュー (asoview.com) 拠点情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "アクセス",
        "緯度",
        "経度",
        "価格帯",
        "評価点",
        "口コミ数",
        "設備情報",
        "画像URL",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        index_soup = self.get_soup(url)
        if index_soup is None:
            return

        targets: list[tuple[str, str, str]] = []  # (detail_url, name, prefecture)
        for region in index_soup.select(".page-base__region-wrap"):
            pref_el = region.select_one(".page-base__region")
            pref_text = _clean(pref_el.get_text()) if pref_el else ""
            for a in region.select("a.page-base__base-link"):
                href = a.get("href", "").strip()
                if not href.startswith("/base/"):
                    continue
                targets.append((
                    urljoin(BASE_URL, href),
                    _clean(a.get_text()),
                    pref_text,
                ))

        self.total_items = len(targets)
        self.logger.info("asoview: インデックスから %d 施設を検出", self.total_items)

        for detail_url, list_name, pref_from_index in targets:
            try:
                item = self._scrape_detail(detail_url, list_name, pref_from_index)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("詳細ページ取得失敗 (スキップ): %s — %s", detail_url, e)
                continue

    def _scrape_detail(self, url: str, list_name: str, pref_from_index: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {
            Schema.URL: url,
            Schema.NAME: list_name,
            Schema.PREF: pref_from_index if _PREF_PATTERN.fullmatch(pref_from_index) else "",
        }

        h1 = soup.select_one("h1.base-name")
        if h1:
            for tag in h1.select("span, small, em"):
                tag.decompose()
            name_text = _clean(h1.get_text())
            if name_text:
                data[Schema.NAME] = name_text

        self._extract_basic_info(soup, data)
        self._extract_facility_info(soup, data)
        self._extract_genres(soup, data)
        self._extract_jsonld(soup, data)

        return data

    def _extract_basic_info(self, soup, data: dict) -> None:
        table = soup.select_one("table.basic-information__contents")
        if not table:
            return

        for tr in table.select("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            key = _clean(th.get_text())
            val = _clean(td.get_text(" "))

            if "店舗名" in key:
                if val and not data.get(Schema.NAME):
                    data[Schema.NAME] = val
            elif "住所" in key:
                self._parse_address(val, data)
            elif "営業時間" in key:
                data[Schema.TIME] = val
            elif "定休日" in key:
                data[Schema.HOLIDAY] = val
            elif "アクセス" in key:
                data["アクセス"] = val

    def _parse_address(self, text: str, data: dict) -> None:
        if not text:
            return
        remainder = text
        m_post = _POST_CODE_PATTERN.search(remainder)
        if m_post:
            data[Schema.POST_CODE] = m_post.group(1)
            remainder = _POST_CODE_PATTERN.sub("", remainder, count=1).strip()

        m_pref = _PREF_PATTERN.search(remainder)
        if m_pref:
            if not data.get(Schema.PREF):
                data[Schema.PREF] = m_pref.group(1)
            data[Schema.ADDR] = remainder[m_pref.end():].strip()
        else:
            data[Schema.ADDR] = remainder

    def _extract_facility_info(self, soup, data: dict) -> None:
        table = soup.select_one("table.facility-information__contents")
        if not table:
            return
        pairs: list[str] = []
        for tr in table.select("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            pairs.append(f"{_clean(th.get_text())}:{_clean(td.get_text(' '))}")
        if pairs:
            data["設備情報"] = " / ".join(pairs)

    def _extract_genres(self, soup, data: dict) -> None:
        genres = [
            _clean(li.get_text())
            for li in soup.select(".base-data__genres li, .base-data__genre-type")
        ]
        genres = [g for g in genres if g]
        if genres:
            seen = set()
            unique = [g for g in genres if not (g in seen or seen.add(g))]
            data[Schema.CAT_SITE] = " / ".join(unique)

    def _extract_jsonld(self, soup, data: dict) -> None:
        for script in soup.select('script[type="application/ld+json"]'):
            raw = script.string or script.text
            if not raw:
                continue
            cleaned = raw.strip().rstrip(";").rstrip()
            try:
                payload = json.loads(cleaned)
            except json.JSONDecodeError:
                continue

            if isinstance(payload, list):
                candidates = payload
            else:
                candidates = [payload]

            for obj in candidates:
                if not isinstance(obj, dict):
                    continue
                if obj.get("@type") != "LocalBusiness":
                    continue

                image = obj.get("image")
                if image:
                    data["画像URL"] = str(image)

                price_range = obj.get("priceRange")
                if price_range:
                    data["価格帯"] = _clean(str(price_range))

                geo = obj.get("geo") or {}
                if isinstance(geo, dict):
                    lat = geo.get("latitude")
                    lng = geo.get("longitude")
                    if lat:
                        data["緯度"] = str(lat)
                    if lng:
                        data["経度"] = str(lng)

                rating = obj.get("aggregateRating") or {}
                if isinstance(rating, dict):
                    rv = rating.get("ratingValue")
                    rc = rating.get("ratingCount")
                    if rv:
                        data["評価点"] = str(rv)
                    if rc:
                        data["口コミ数"] = str(rc)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = AsoviewScraper()
    scraper.execute(INDEX_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
