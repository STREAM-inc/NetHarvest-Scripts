"""
サンマリエ — 全国結婚相談所店舗スクレイパー

取得対象:
    - 全国主要サロン23件 + サテライト44件（計67件）
    - 住所・TEL・営業時間・定休日・アクセス・価格帯・緯度経度・評価など

取得フロー:
    https://www.sunmarie.co.jp/store/ を1回取得し全件一覧を取得（ページネーションなし）
    主要サロン: 一覧ページ取得 → 詳細ページ JSON-LD 取得（住所構造化・緯度経度・評価）
    サテライト: 一覧ページのみ取得

実行方法:
    # ローカルテスト
    python scripts/sites/service/sunmarie.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id sunmarie
"""

import json
import re
import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://www.sunmarie.co.jp"
LIST_URL = f"{BASE_URL}/store/"

_POST_RE = re.compile(r"〒(\d{3}-\d{4})")
_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


class SunmarieCrawler(StaticCrawler):
    """サンマリエ 全国店舗スクレイパー（sunmarie.co.jp）"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["アクセス", "価格帯", "緯度", "経度", "GoogleマップURL", "評価スコア", "レビュー数", "店舗種別"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(LIST_URL)
        if soup is None:
            return

        salon_els = soup.select('div[class*="StoreListSalons_t-list-salon"]')
        sat_els = soup.select('div[class*="StoreListSatellite_t-list-salon"]')

        self.total_items = len(salon_els) + len(sat_els)
        self.logger.info("主要サロン: %d件, サテライト: %d件", len(salon_els), len(sat_els))

        for el in salon_els:
            try:
                item = self._parse_salon(el)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("サロン取得エラー: %s", e)

        for el in sat_els:
            try:
                item = self._parse_satellite(el)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("サテライト取得エラー: %s", e)

    def _parse_salon(self, el) -> dict | None:
        name_el = el.select_one('h3[class*="t-list__heading"]')
        if not name_el:
            return None
        raw_name = name_el.get_text(strip=True)
        name = re.sub(r"^.+?サンマリエ\s*", "", raw_name).strip() or raw_name

        a = el.find("a", href=True)
        detail_url = BASE_URL + a["href"] if a and a["href"].startswith("/") else (a["href"] if a else "")

        dl = el.find("dl")
        access = time_ = holiday = tel = addr_raw = ""
        if dl:
            dts = dl.find_all("dt")
            dds = dl.find_all("dd")
            for dt, dd in zip(dts, dds):
                key_el = dt.find("h3")
                key = key_el.get_text(strip=True) if key_el else dt.get_text(strip=True)
                val = dd.get_text(strip=True)
                if "アクセス" in key:
                    access = val
                elif "住所" in key:
                    addr_raw = val
                elif "営業時間" in key:
                    time_ = val
                elif "定休日" in key:
                    holiday = val
                elif "電話" in key:
                    tel = val

        # 住所フォールバック（一覧ページ由来）
        post_code = pref = addr = ""
        m_post = _POST_RE.search(addr_raw)
        if m_post:
            post_code = m_post.group(1)
            rest = addr_raw[m_post.end():].strip()
            m_pref = _PREF_RE.search(rest)
            if m_pref:
                pref = m_pref.group(1)
                addr = rest[m_pref.start():].strip()

        price_range = lat = lng = maps_url = rating = review_count = ""
        if detail_url:
            ld = self._get_json_ld(detail_url)
            if ld:
                address_ld = ld.get("address", {})
                post_code = address_ld.get("postalCode", post_code)
                pref = address_ld.get("addressRegion", pref)
                locality = address_ld.get("addressLocality", "")
                street = address_ld.get("streetAddress", "")
                if locality or street:
                    addr = (locality + street).strip()
                price_range = ld.get("priceRange", "")
                geo = ld.get("geo", {})
                if geo.get("latitude") is not None:
                    lat = str(geo["latitude"])
                if geo.get("longitude") is not None:
                    lng = str(geo["longitude"])
                maps_url = ld.get("hasMap", "")
                ag = ld.get("aggregateRating", {})
                if ag:
                    if ag.get("ratingValue") is not None:
                        rating = str(round(float(ag["ratingValue"]), 2))
                    if ag.get("ratingCount") is not None:
                        review_count = str(int(ag["ratingCount"]))

        return {
            Schema.NAME: name,
            Schema.POST_CODE: post_code,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.TIME: time_,
            Schema.HOLIDAY: holiday,
            Schema.URL: detail_url,
            "アクセス": access,
            "価格帯": price_range,
            "緯度": lat,
            "経度": lng,
            "GoogleマップURL": maps_url,
            "評価スコア": rating,
            "レビュー数": review_count,
            "店舗種別": "サロン",
        }

    def _parse_satellite(self, el) -> dict | None:
        name_el = el.select_one('h3[class*="font-sm"]')
        if not name_el:
            return None
        name = name_el.get_text(strip=True)

        tel_m = re.search(r"0120-\d{3}-\d{3}", el.get_text())
        tel = tel_m.group(0) if tel_m else ""

        return {
            Schema.NAME: name,
            Schema.POST_CODE: "",
            Schema.PREF: "",
            Schema.ADDR: "",
            Schema.TEL: tel,
            Schema.TIME: "",
            Schema.HOLIDAY: "",
            Schema.URL: LIST_URL,
            "アクセス": "",
            "価格帯": "",
            "緯度": "",
            "経度": "",
            "GoogleマップURL": "",
            "評価スコア": "",
            "レビュー数": "",
            "店舗種別": "サテライト",
        }

    def _get_json_ld(self, url: str) -> dict:
        soup = self.get_soup(url)
        if soup is None:
            return {}
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string)
                if data.get("@type") == "LocalBusiness":
                    return data
            except Exception:
                pass
        return {}


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = SunmarieCrawler()
    scraper.execute(LIST_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
