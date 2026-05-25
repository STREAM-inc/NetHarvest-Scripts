"""
au PAY — 全国店舗スキャンスクレイパー

取得対象:
    - au PAY (コード決済) が使える全国の実店舗情報
    - 名称・TEL・都道府県・住所・業種・新規フラグ

取得フロー:
    https://api.aupay.wallet.auone.jp/store-search に
    緯度経度パラメータを東京中心のリング状グリッドで送信し、
    日本全国 (BBOX) をカバーして店舗情報を収集する。
    店舗名で重複排除しながら CSV に逐次保存する。

実行方法:
    # ローカルテスト
    python scripts/sites/service/aupay_japan.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id aupay_japan
"""

import math
import random
import sys
import time
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

API_URL = "https://api.aupay.wallet.auone.jp/store-search"
SCRAPE_URL = "https://aupay.wallet.auone.jp/store/"

BBOX_JAPAN = (24.0, 122.5, 46.2, 146.5)  # (south, west, north, east)
KM_PER_DEG_LAT = 111.32

PREFS_47 = [
    "北海道",
    "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県",
    "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県",
    "沖縄県",
]


class AuPayJapanScraper(StaticCrawler):
    """au PAY 全国店舗スキャンスクレイパー（APIグリッド走査）"""

    KM_STEP: float = 20.0       # グリッド間隔(km)。全国スキャンには 10〜30 推奨
    CENTER_LAT: float = 35.6895  # 走査開始中心緯度（東京）
    CENTER_LON: float = 139.6917  # 走査開始中心経度（東京）
    MAX_PAGES: int = 50          # 1座標あたり最大ページ数
    EARLY_STOP_PAGES: int = 1    # 連続0件ページ数でその座標をスキップ
    SLEEP_MIN: float = 0.10
    SLEEP_MAX: float = 0.30
    EXTRA_COLUMNS = ["is_new"]

    _API_HEADERS = {
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://aupay.wallet.auone.jp/store/",
        "Accept-Language": "ja,en;q=0.8",
    }

    def prepare(self):
        self.session.headers.update(self._API_HEADERS)
        self._seen_names: set[str] = set()

    # ---- 座標変換ユーティリティ ----

    def _latlon_to_xy(self, lat0: float, lon0: float, lat: float, lon: float):
        y = (lat - lat0) * KM_PER_DEG_LAT
        x = (lon - lon0) * (KM_PER_DEG_LAT * max(1e-6, math.cos(math.radians(lat0))))
        return x, y

    def _xy_to_latlon(self, lat0: float, lon0: float, x_km: float, y_km: float):
        lat = lat0 + (y_km / KM_PER_DEG_LAT)
        lon = lon0 + (x_km / (KM_PER_DEG_LAT * max(1e-6, math.cos(math.radians(lat0)))))
        return lat, lon

    def _extract_pref(self, addr: str) -> str:
        if not addr:
            return ""
        for p in PREFS_47:
            if p in addr:
                return p
        if "東京" in addr:
            return "東京都"
        if "大阪" in addr:
            return "大阪府"
        if "京都" in addr:
            return "京都府"
        return ""

    # ---- API アクセス ----

    def _fetch_stores(self, lat: float, lon: float, page: int) -> list:
        params = {
            "flag": 1,
            "latitude": lat,
            "longitude": lon,
            "device_latitude": lat,
            "device_longitude": lon,
            "page": page,
        }
        try:
            r = self.session.get(API_URL, params=params, timeout=15)
            r.raise_for_status()
            return r.json().get("stores", [])
        except Exception as e:
            self.logger.warning("APIエラー (%.5f, %.5f) page=%d: %s", lat, lon, page, e)
            return []

    def _scan_center(self, lat: float, lon: float) -> Generator[dict, None, None]:
        """1座標分のページングを行い、新規店舗を yield する。"""
        consecutive_zero = 0
        for page in range(1, self.MAX_PAGES + 1):
            stores = self._fetch_stores(lat, lon, page)
            new_on_page = 0

            for s in stores:
                name = s.get("store_name")
                if not name or name in self._seen_names:
                    continue
                addr = (s.get("address") or "").replace("\n", " ").replace("\r", " ").strip()
                if not addr:
                    continue

                self._seen_names.add(name)
                new_on_page += 1
                yield {
                    Schema.NAME: name,
                    Schema.TEL: s.get("store_phone_number") or "",
                    Schema.PREF: self._extract_pref(addr),
                    Schema.ADDR: addr,
                    Schema.CAT_SITE: s.get("genre") or "",
                    Schema.URL: SCRAPE_URL,
                    "is_new": s.get("is_new"),
                }

            if new_on_page == 0:
                consecutive_zero += 1
            else:
                consecutive_zero = 0

            if not stores or consecutive_zero >= self.EARLY_STOP_PAGES:
                break

            time.sleep(random.uniform(self.SLEEP_MIN, self.SLEEP_MAX))

    # ---- メインロジック ----

    def parse(self, url: str) -> Generator[dict, None, None]:
        south, west, north, east = BBOX_JAPAN
        lat0, lon0 = self.CENTER_LAT, self.CENTER_LON

        corners = [(south, west), (south, east), (north, west), (north, east)]
        corner_xy = [self._latlon_to_xy(lat0, lon0, la, lo) for la, lo in corners]
        xs = [x for x, _ in corner_xy]
        ys = [y for _, y in corner_xy]

        min_ix = math.floor(min(xs) / self.KM_STEP) - 1
        max_ix = math.ceil(max(xs) / self.KM_STEP) + 1
        min_iy = math.floor(min(ys) / self.KM_STEP) - 1
        max_iy = math.ceil(max(ys) / self.KM_STEP) + 1
        max_r = max(abs(min_ix), abs(max_ix), abs(min_iy), abs(max_iy))

        self.logger.info(
            "グリッドスキャン開始: km_step=%.1f, 中心=(%.4f, %.4f), max_r=%d",
            self.KM_STEP, lat0, lon0, max_r,
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

            for ix, iy in ring_points:
                if ix < min_ix or ix > max_ix or iy < min_iy or iy > max_iy:
                    continue
                lat, lon = self._xy_to_latlon(lat0, lon0, ix * self.KM_STEP, iy * self.KM_STEP)
                if not (south <= lat <= north and west <= lon <= east):
                    continue

                yield from self._scan_center(lat, lon)
                time.sleep(random.uniform(self.SLEEP_MIN, self.SLEEP_MAX))


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = AuPayJapanScraper()
    scraper.execute(SCRAPE_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
