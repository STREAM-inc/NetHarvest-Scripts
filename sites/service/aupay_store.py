# -*- coding: utf-8 -*-
"""
au PAY 利用可能店舗スクレイパー（全国ジオAPI走査）

取得対象: au PAY（コード決済）が利用可能な実店舗
取得フロー: REST API (api.aupay.wallet.auone.jp/store-search) を
           東京中心からリング状に座標を広げてページング取得

実行方法:
    python scripts/sites/service/aupay_store.py
    python bin/run_flow.py --site-id aupay-store
"""

import math
import time
import random
import sys
from pathlib import Path
from typing import Generator, Set, Tuple

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

STORE_URL = "https://aupay.wallet.auone.jp/store/"
BASE_API = "https://api.aupay.wallet.auone.jp/store-search"
BBOX_JAPAN = (24.0, 122.5, 46.2, 146.5)  # (south, west, north, east)
KM_PER_DEG_LAT = 111.32

PREFS_47 = [
    "北海道",
    "青森県",
    "岩手県",
    "宮城県",
    "秋田県",
    "山形県",
    "福島県",
    "茨城県",
    "栃木県",
    "群馬県",
    "埼玉県",
    "千葉県",
    "東京都",
    "神奈川県",
    "新潟県",
    "富山県",
    "石川県",
    "福井県",
    "山梨県",
    "長野県",
    "岐阜県",
    "静岡県",
    "愛知県",
    "三重県",
    "滋賀県",
    "京都府",
    "大阪府",
    "兵庫県",
    "奈良県",
    "和歌山県",
    "鳥取県",
    "島根県",
    "岡山県",
    "広島県",
    "山口県",
    "徳島県",
    "香川県",
    "愛媛県",
    "高知県",
    "福岡県",
    "佐賀県",
    "長崎県",
    "熊本県",
    "大分県",
    "宮崎県",
    "鹿児島県",
    "沖縄県",
]

_API_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Referer": STORE_URL,
    "Accept-Language": "ja,en;q=0.8",
}


class AuPayStoreScraper(StaticCrawler):
    DELAY = 0.2
    EXTRA_COLUMNS = ["is_new"]

    KM_STEP = 20.0
    CENTER_LAT = 35.6895
    CENTER_LON = 139.6917
    MAX_PAGES = 50
    EARLY_STOP_PAGES = 1

    def parse(self, url: str) -> Generator[dict, None, None]:
        self.session.headers.update(_API_HEADERS)
        seen: Set[str] = set()

        for lat, lon in self._ring_points():
            yield from self._collect(lat, lon, seen)
            time.sleep(random.uniform(0.1, 0.3))

    def _collect(self, lat, lon, seen):
        page = 1
        consecutive_zero = 0
        while True:
            try:
                r = self.session.get(
                    BASE_API,
                    params={
                        "flag": 1,
                        "latitude": lat,
                        "longitude": lon,
                        "device_latitude": lat,
                        "device_longitude": lon,
                        "page": page,
                    },
                    timeout=15,
                )
                r.raise_for_status()
                data = r.json()
            except Exception as e:
                self.logger.warning("API error (%f,%f) p=%d: %s", lat, lon, page, e)
                break

            stores = data.get("stores", [])
            new_count = 0
            for s in stores:
                row = self._to_row(s, seen)
                if row:
                    yield row
                    new_count += 1

            if new_count == 0:
                consecutive_zero += 1
            else:
                consecutive_zero = 0

            if (
                not stores
                or page >= self.MAX_PAGES
                or consecutive_zero >= self.EARLY_STOP_PAGES
            ):
                break
            page += 1

    def _to_row(self, s, seen):
        name = s.get("store_name")
        if not name or name in seen:
            return None
        addr = (s.get("address") or "").replace("\n", " ").replace("\r", " ").strip()
        if not addr:
            return None
        seen.add(name)
        return {
            Schema.URL: STORE_URL,
            Schema.NAME: name,
            Schema.TEL: s.get("store_phone_number"),
            Schema.PREF: self._extract_pref(addr),
            Schema.ADDR: addr,
            Schema.CAT_SITE: s.get("genre"),
            "is_new": s.get("is_new"),
        }

    def _extract_pref(self, addr):
        for p in PREFS_47:
            if p in addr:
                return p
        return ""

    def _latlon_to_xy_km(self, lat0: float, lon0: float, lat: float, lon: float) -> Tuple[float, float]:
        y = (lat - lat0) * KM_PER_DEG_LAT
        x = (lon - lon0) * (KM_PER_DEG_LAT * max(1e-6, math.cos(math.radians(lat0))))
        return x, y

    def _xy_km_to_latlon(self, lat0: float, lon0: float, x_km: float, y_km: float) -> Tuple[float, float]:
        lat = lat0 + (y_km / KM_PER_DEG_LAT)
        lon = lon0 + (x_km / (KM_PER_DEG_LAT * max(1e-6, math.cos(math.radians(lat0)))))
        return lat, lon

    def _ring_points(self) -> Generator[Tuple[float, float], None, None]:
        south, west, north, east = BBOX_JAPAN

        corners = [
            (south, west),
            (south, east),
            (north, west),
            (north, east),
        ]
        corner_xy = [
            self._latlon_to_xy_km(self.CENTER_LAT, self.CENTER_LON, lat, lon)
            for lat, lon in corners
        ]

        xs = [x for x, _ in corner_xy]
        ys = [y for _, y in corner_xy]
        min_ix = math.floor(min(xs) / self.KM_STEP) - 1
        max_ix = math.ceil(max(xs) / self.KM_STEP) + 1
        min_iy = math.floor(min(ys) / self.KM_STEP) - 1
        max_iy = math.ceil(max(ys) / self.KM_STEP) + 1

        max_r = max(
            abs(min_ix),
            abs(max_ix),
            abs(min_iy),
            abs(max_iy),
        )

        self.logger.info(
            "[CENTER] (%.5f,%.5f)", self.CENTER_LAT, self.CENTER_LON
        )
        self.logger.info(
            "[BBOX] (%.2f,%.2f)-(%.2f,%.2f)", south, west, north, east
        )
        self.logger.info(
            "[GRID] km_step=%.1f -> ix=[%d,%d] iy=[%d,%d] max_r=%d",
            self.KM_STEP,
            min_ix,
            max_ix,
            min_iy,
            max_iy,
            int(max_r),
        )

        for r in range(0, int(max_r) + 1):
            if r == 0:
                ring_points = [(0, 0)]
            else:
                ring_points = []
                for ix in range(-r, r + 1):
                    ring_points.append((ix, r))
                    ring_points.append((ix, -r))
                for iy in range(-r + 1, r):
                    ring_points.append((r, iy))
                    ring_points.append((-r, iy))

            filtered = []
            for ix, iy in ring_points:
                if ix < min_ix or ix > max_ix or iy < min_iy or iy > max_iy:
                    continue
                filtered.append((ix, iy))

            if not filtered:
                continue

            self.logger.debug("[RING] r=%d points=%d", r, len(filtered))

            for ix, iy in filtered:
                x_km = ix * self.KM_STEP
                y_km = iy * self.KM_STEP
                lat, lon = self._xy_km_to_latlon(
                    self.CENTER_LAT, self.CENTER_LON, x_km, y_km
                )

                if not (south <= lat <= north and west <= lon <= east):
                    continue

                yield lat, lon


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = AuPayStoreScraper()
    scraper.execute(STORE_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
