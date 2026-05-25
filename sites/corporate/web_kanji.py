"""
楽天トラベル 宿泊施設一覧

取得対象:
    国内24,000件以上の宿泊施設（名称・URL・評価・アクセス・設備・最低料金）
    RAKUTEN_APP_ID 設定時: 客室数・電話番号・住所・チェックイン時間をAPI経由で追加取得

取得フロー:
    インデックスページ (/group/TIKU/) → 297エリアを巡回
    → 各エリアの全ページ (1p目: /yado/{pref}/{area}.html, 2p目以降: search.travel.rakuten.co.jp/ds/yado/...)

実行方法:
    python scripts/sites/travel/rakuten_travel.py
    python bin/run_flow.py --site-id rakuten_travel
"""

import math
import os
import re
import sys
import time
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup

from src.const.schema import Schema
from src.framework.static import StaticCrawler

_RAKUTEN_APP_ID = os.environ.get("RAKUTEN_APP_ID", "")

_PREF_MAP = {
    "hokkaido": "北海道",
    "aomori": "青森県",
    "iwate": "岩手県",
    "miyagi": "宮城県",
    "akita": "秋田県",
    "yamagata": "山形県",
    "fukushima": "福島県",
    "ibaraki": "茨城県",
    "tochigi": "栃木県",
    "gunma": "群馬県",
    "saitama": "埼玉県",
    "chiba": "千葉県",
    "tokyo": "東京都",
    "kanagawa": "神奈川県",
    "niigata": "新潟県",
    "toyama": "富山県",
    "ishikawa": "石川県",
    "fukui": "福井県",
    "yamanashi": "山梨県",
    "nagano": "長野県",
    "gifu": "岐阜県",
    "shizuoka": "静岡県",
    "aichi": "愛知県",
    "mie": "三重県",
    "shiga": "滋賀県",
    "kyoto": "京都府",
    "osaka": "大阪府",
    "hyogo": "兵庫県",
    "nara": "奈良県",
    "wakayama": "和歌山県",
    "tottori": "鳥取県",
    "shimane": "島根県",
    "okayama": "岡山県",
    "hiroshima": "広島県",
    "yamaguchi": "山口県",
    "tokushima": "徳島県",
    "kagawa": "香川県",
    "ehime": "愛媛県",
    "kochi": "高知県",
    "fukuoka": "福岡県",
    "saga": "佐賀県",
    "nagasaki": "長崎県",
    "kumamoto": "熊本県",
    "oita": "大分県",
    "miyazaki": "宮崎県",
    "kagoshima": "鹿児島県",
    "okinawa": "沖縄県",
}


class RakutenTravelCrawler(StaticCrawler):
    """楽天トラベル 宿泊施設一覧スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "hotel_id",
        "review_score",
        "review_count",
        "catchphrase",
        "access",
        "price_min",
        "area_en",
        # API拡張 (RAKUTEN_APP_ID 設定時のみ値が入る)
        "num_rooms",
        "tel",
        "postal_code",
        "addr_detail",
        "checkin_time",
        "checkout_time",
    ]

    def parse(self, url: str):
        soup = self.get_soup(url)
        if not soup:
            return

        area_hrefs = list(
            dict.fromkeys(
                a.get("href", "")
                for a in soup.select('a[href*="/03"], a[href*="/04"]')
                if "/group/tiku/" in a.get("href", "")
            )
        )

        self.total_items = 24205
        self.logger.info("エリア数: %d", len(area_hrefs))

        for href in area_hrefs:
            yield from self._scrape_area("https://travel.rakuten.co.jp" + href)
            time.sleep(self.DELAY)

    def _scrape_area(self, area_group_url: str):
        try:
            resp = self.session.get(area_group_url, timeout=self.TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            self.logger.warning("エリア取得失敗: %s — %s", area_group_url, e)
            return

        m = re.search(r"/yado/([^/]+)/([^/.]+)", resp.url)
        if not m:
            self.logger.warning("yado URL解析不可: %s", resp.url)
            return
        pref_en, area_en = m.group(1), m.group(2)
        pref_ja = _PREF_MAP.get(pref_en, pref_en)

        soup = BeautifulSoup(resp.text, "html.parser")

        total_el = soup.select_one(".pagination__info-text--total")
        area_total = 0
        if total_el:
            digits = re.sub(r"\D", "", total_el.get_text())
            area_total = int(digits) if digits else 0

        yield from self._parse_page(soup, pref_ja, area_en, resp.url)

        if area_total <= 30:
            return

        next_link = soup.select_one('link[rel="next"]')
        if not next_link:
            return
        base_search = re.sub(r"-p\d+$", "", next_link.get("href", ""))
        if not base_search:
            return

        total_pages = math.ceil(area_total / 30)
        for page_num in range(2, total_pages + 1):
            page_url = f"{base_search}-p{page_num}"
            page_soup = self.get_soup(page_url)
            if page_soup:
                yield from self._parse_page(page_soup, pref_ja, area_en, page_url)
            time.sleep(self.DELAY)

    def _parse_page(self, soup, pref_ja: str, area_en: str, page_url: str):
        for card in soup.select("li.htl-list-card"):
            try:
                yield self._parse_card(card, pref_ja, area_en, page_url)
            except Exception as e:
                self.logger.warning("カード解析エラー: %s", e)

    def _parse_card(self, card, pref_ja: str, area_en: str, page_url: str) -> dict:
        link = card.select_one("h2.hotel-list__title-text a")
        hotel_url = link.get("href", "") if link else ""
        name = link.get_text(strip=True) if link else ""

        m = re.search(r"/HOTEL/(\d+)/", hotel_url)
        hotel_id = m.group(1) if m else ""

        review_text = ""
        review_el = card.select_one(".cstmrEvl")
        if review_el:
            review_text = review_el.get_text(strip=True)
        score_m = re.search(r"^(\d+\.\d+)", review_text)
        count_m = re.search(r"（(\d+)件）", review_text)

        access_el = card.select_one(".htlAccess")
        access = ""
        if access_el:
            for a in access_el.select("a"):
                a.decompose()
            access = re.sub(r"^アクセス\s*[：:]\s*", "", access_el.get_text(strip=True))

        features = list(dict.fromkeys(f.get_text(strip=True) for f in card.select(".hotelInfo_features label")))

        price_el = card.select_one(".htlLowprice strong")
        price_min = re.sub(r"\D", "", price_el.get_text()) if price_el else ""

        special_el = card.select_one(".htlSpecial")

        item = {
            Schema.NAME: name,
            Schema.URL: hotel_url or page_url,
            Schema.PREF: pref_ja,
            Schema.CAT_SITE: "・".join(features),
            "hotel_id": hotel_id,
            "review_score": score_m.group(1) if score_m else "",
            "review_count": count_m.group(1) if count_m else "",
            "catchphrase": special_el.get_text(strip=True) if special_el else "",
            "access": access,
            "price_min": price_min,
            "area_en": area_en,
            "num_rooms": "",
            "tel": "",
            "postal_code": "",
            "addr_detail": "",
            "checkin_time": "",
            "checkout_time": "",
        }

        if _RAKUTEN_APP_ID and hotel_id:
            item.update(self._fetch_api(hotel_id))

        return item

    def _fetch_api(self, hotel_id: str) -> dict:
        import urllib.parse

        params = urllib.parse.urlencode(
            {
                "applicationId": _RAKUTEN_APP_ID,
                "formatVersion": "2",
                "hotelNo": hotel_id,
            }
        )
        url = f"https://app.rakuten.co.jp/services/api/Travel/SimpleHotelSearch/20170426?{params}"
        try:
            resp = self.session.get(url, timeout=10)
            data = resp.json()
            hotels = data.get("hotels", [])
            if not hotels:
                return {}
            info = hotels[0].get("hotelBasicInfo", {})
            return {
                "num_rooms": str(info.get("numberOfRooms", "")),
                "tel": info.get("telephoneNo", ""),
                "postal_code": info.get("postalCode", ""),
                "addr_detail": (str(info.get("address1", "")) + str(info.get("address2", ""))).strip(),
                "checkin_time": info.get("checkinTime", ""),
                "checkout_time": info.get("checkoutTime", ""),
            }
        except Exception as e:
            self.logger.warning("API取得失敗 hotel_id=%s: %s", hotel_id, e)
            return {}


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = RakutenTravelCrawler()
    scraper.execute("https://travel.rakuten.co.jp/group/TIKU/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
