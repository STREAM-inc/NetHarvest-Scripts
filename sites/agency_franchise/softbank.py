"""
ソフトバンクショップ — ショップ一覧

取得対象:
    - ソフトバンクショップ全店舗情報（運営会社、住所、電話番号、営業時間等）

取得フロー:
    1. 都市インデックス API から全市区町村 ID を取得
    2. spots > 0 の都市のみ shop-search API を呼び出す（最大 50 件/リクエスト）
    3. 運営会社は spare7 フィールドから取得

実行方法:
    # ローカルテスト
    python scripts/sites/agency_franchise/softbank.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id softbank
"""

import re
import sys
from pathlib import Path
from urllib.parse import urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都"
    r"|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県"
    r"|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県"
    r"|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_FLAG_MAP = {"1": "あり", "0": "なし", "": ""}

_DAY_KEYS = [
    ("mon", "月"), ("tue", "火"), ("wed", "水"), ("thu", "木"),
    ("fri", "金"), ("sat", "土"), ("sun", "日"), ("hol", "祝"),
]


def _format_hours(hours_dict: dict) -> str:
    if not isinstance(hours_dict, dict):
        return ""
    parts = []
    for key, label in _DAY_KEYS:
        day = hours_dict.get(key) or {}
        if isinstance(day, dict):
            bh = day.get("business_hours", "")
        else:
            bh = str(day)
        if bh:
            parts.append(f"{label}:{bh}")
    return " ".join(parts)


def _flag(value) -> str:
    return _FLAG_MAP.get(str(value), "")


class SoftbankCrawler(StaticCrawler):
    """ソフトバンクショップ スクレイパー"""

    DELAY = 0.5
    EXTRA_COLUMNS = ["運営会社", "緯度", "経度", "Wi-Fi", "修理受付", "スマホ教室", "駐車場", "来店予約"]

    def parse(self, url: str):
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        index_api = f"{base}/shop/d/system/v1/api/shop-info-list/"
        shop_api = f"{base}/shop/d/system/v1/api/shop-search/"

        headers = {
            "Referer": url,
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
        }

        index_resp = self.session.get(index_api, headers=headers, timeout=30)
        index_resp.raise_for_status()
        index_data = index_resp.json()

        if isinstance(index_data, list):
            cities = index_data
        elif isinstance(index_data, dict):
            # レスポンスが {"cities": [...]} 等のラッパー辞書の場合
            cities = next((v for v in index_data.values() if isinstance(v, list)), [])
        else:
            cities = []

        active_cities = [c for c in cities if isinstance(c, dict) and c.get("spots", 0) > 0]
        self.total_items = sum(c.get("spots", 0) for c in active_cities)

        for city in active_cities:
            city_id = city.get("id")
            if not city_id:
                continue

            params = {
                "type": "city",
                "sort": "0",
                "appleWatch": "0",
                "spadv": "0",
                "hikariadv": "0",
                "repair": "0",
                "nearStation": "0",
                "parking": "0",
                "kids": "0",
                "barrierFreeEntrance": "0",
                "results": "50",
                "city": city_id,
            }

            try:
                resp = self.session.get(shop_api, params=params, headers=headers, timeout=30)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                self.logger.warning(f"city {city_id} fetch failed: {e}")
                continue

            shops = data.get("item") or []
            item_count = data.get("item_count", 0)
            if item_count > len(shops):
                self.logger.warning(
                    f"city {city_id}: item_count={item_count} but got {len(shops)} — some shops may be missing"
                )

            for shop in shops:
                try:
                    yield self._map_shop(shop, base)
                except Exception as e:
                    self.logger.warning(f"shop {shop.get('shop_id')} map error: {e}")
                    continue

    def _map_shop(self, shop: dict, base: str) -> dict:
        shop_id = shop.get("shop_id", "")
        detail_url = f"{base}/shop/search/detail/{shop_id}/" if shop_id else ""

        name_obj = shop.get("shop_name") or {}
        name = name_obj.get("name", "") if isinstance(name_obj, dict) else str(name_obj)

        address = shop.get("address", "") or ""
        pref, addr = "", address
        m = _PREF_PATTERN.match(address)
        if m:
            pref = m.group(1)
            addr = address[m.end():].strip()

        geo = shop.get("geo") or {}

        return {
            Schema.NAME: name,
            Schema.URL: detail_url,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: shop.get("tel", "") or "",
            Schema.HOLIDAY: shop.get("holiday", "") or "",
            Schema.TIME: _format_hours(shop.get("hours") or {}),
            "運営会社": shop.get("spare7", "") or "",
            "緯度": str(geo.get("lat", "")) if geo.get("lat") is not None else "",
            "経度": str(geo.get("lon", "")) if geo.get("lon") is not None else "",
            "Wi-Fi": _flag(shop.get("wifi", "")),
            "修理受付": _flag(shop.get("repair_flg", "")),
            "スマホ教室": _flag(shop.get("classroom", "")),
            "駐車場": _flag(shop.get("parking", "")),
            "来店予約": _flag(shop.get("visit_reservation_flg", "")),
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = SoftbankCrawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.softbank.jp/shop/search/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
