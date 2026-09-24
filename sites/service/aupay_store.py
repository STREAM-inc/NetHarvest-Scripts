# -*- coding: utf-8 -*-
"""
au PAY 使えるお店 (aupay.wallet.auone.jp/store/) スクレイパー

取得対象:
    au PAY (コード決済) が利用できる実店舗。
    名称 / 郵便番号 / 都道府県 / 住所 / TEL / ジャンル / HP。

取得フロー:
    1. live: 起点 URL (https://aupay.wallet.auone.jp/store/) から店舗検索 API
       (https://api.aupay.wallet.auone.jp/store-search) を導出し、東京中心の
       リング状グリッドで座標を広げながらページング取得する。
       取得した店舗は 1 件ずつ即 yield する (全件バッファしない)。
    2. fallback: live が使えない場合は Wayback Machine の生スナップショット
       (…id_/…) にフォールバックし、CDX API で保存済み API レスポンス
       (store-search / feature-detail / store-detail / premium-stores) を
       列挙して同じ JSON 構造から店舗を取り出す。

    ※ 2026-08-30 より au PAY の「使えるお店」は全ページがメンテナンス画面
      (「ただいまメンテナンス中です」) になり、API 側も CloudFront が全リクエストを
      403 で拒否している。国外/国内を問わず (r.jina.ai 経由でも) 同じ応答のため
      IP 遮断ではなくサービス停止。メンテナンス中は Wayback 経路のみが値を返す。
      メンテナンス明けは自動的に live 経路に戻る。

実行方法:
    # ローカルテスト
    python scripts/sites/service/aupay_store.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id aupay_store
"""

import logging
import math
import random
import sys
import time
from pathlib import Path
from typing import Generator, Iterable, Set, Tuple
from urllib.parse import urlsplit, urlunsplit, urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# sites.yml に登録済みの正規 URL (parse() へ渡される起点)
STORE_URL = "https://aupay.wallet.auone.jp/store/"

BBOX_JAPAN = (24.0, 122.5, 46.2, 146.5)  # (south, west, north, east)
KM_PER_DEG_LAT = 111.32

# Wayback CDX で列挙する API パス (いずれも同じ店舗 JSON 構造)
_WAYBACK_API_PATHS = ["store-search", "feature-detail", "store-detail", "premium-stores"]
_CDX_ENDPOINT = "http://web.archive.org/cdx/search/cdx"

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


class AuPayStoreScraper(StaticCrawler):
    DELAY = 0.2
    ITEM_DELAY = 0.0  # 1 スナップショット/1 ページで 100 件 yield するため待機は入れない
    EXTRA_COLUMNS = ["店舗ID", "is_new", "取得経路"]

    TIMEOUT = 20

    # --- live グリッド走査の設定 ---
    KM_STEP = 20.0
    CENTER_LAT = 35.6895
    CENTER_LON = 139.6917
    MAX_PAGES = 50
    EARLY_STOP_PAGES = 1
    # 連続でこの回数 API が失敗したら live は停止中とみなし Wayback へ切り替える
    LIVE_FAIL_LIMIT = 3

    def parse(self, url: str) -> Generator[dict, None, None]:
        """引数 url を唯一の起点として店舗を 1 件ずつ yield する。"""
        root = url if url.endswith("/") else url + "/"
        api_base = self._api_base(root)
        self.session.headers.update(
            {
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "ja,en;q=0.8",
                "Referer": root,
                "Origin": f"{urlsplit(root).scheme}://{urlsplit(root).netloc}",
            }
        )

        seen: Set[str] = set()
        count = 0

        if self._live_available(api_base):
            for row in self._crawl_live(api_base, root, seen):
                count += 1
                yield row
        else:
            logger.warning(
                "live API が利用できません (メンテナンス/遮断) — Wayback へフォールバックします: %s",
                api_base,
            )

        if count == 0:
            for row in self._crawl_wayback(api_base, root, seen):
                yield row

    # ------------------------------------------------------------------
    # URL 導出
    # ------------------------------------------------------------------
    def _api_base(self, root: str) -> str:
        """起点 URL から店舗検索 API の URL を導出する (aupay… → api.aupay…)。"""
        parts = urlsplit(root)
        host = parts.netloc if parts.netloc.startswith("api.") else f"api.{parts.netloc}"
        return urlunsplit((parts.scheme, host, "/store-search", "", ""))

    def _detail_url(self, root: str, store_id) -> str:
        """店舗詳細ページ URL (起点 URL から派生)。"""
        if store_id in (None, ""):
            return root
        return urljoin(root, f"list/detail/?storeid={store_id}")

    # ------------------------------------------------------------------
    # live 経路
    # ------------------------------------------------------------------
    def _live_available(self, api_base: str) -> bool:
        """中心座標で 1 回だけ叩き、store-search API が生きているか確認する。"""
        data = self._get_json(
            api_base,
            params=self._search_params(self.CENTER_LAT, self.CENTER_LON, 1),
        )
        return bool(isinstance(data, dict) and isinstance(data.get("stores"), list))

    def _search_params(self, lat: float, lon: float, page: int) -> dict:
        return {
            "flag": 1,
            "latitude": lat,
            "longitude": lon,
            "device_latitude": lat,
            "device_longitude": lon,
            "page": page,
        }

    def _get_json(self, url: str, params: dict | None = None) -> dict | None:
        try:
            r = self.session.get(url, params=params, timeout=self.TIMEOUT)
            if r.status_code != 200:
                logger.warning("HTTP %s: %s", r.status_code, r.url)
                return None
            return r.json()
        except ValueError as e:  # JSON でない (メンテナンス HTML 等)
            logger.warning("JSON ではない応答: %s — %s", url, e)
            return None
        except Exception as e:
            logger.warning("取得エラー: %s — %s", url, e)
            return None

    def _crawl_live(self, api_base: str, root: str, seen: Set[str]):
        fails = 0
        for lat, lon in self._ring_points():
            got_any = False
            for row in self._collect(api_base, root, lat, lon, seen):
                got_any = True
                yield row
            fails = 0 if got_any else fails + 1
            if fails >= self.LIVE_FAIL_LIMIT and not seen:
                logger.warning("live API が連続 %d 回無応答のため中断します", fails)
                return
            time.sleep(random.uniform(0.1, 0.3))

    def _collect(self, api_base: str, root: str, lat: float, lon: float, seen: Set[str]):
        page = 1
        consecutive_zero = 0
        while True:
            data = self._get_json(api_base, params=self._search_params(lat, lon, page))
            if not data:
                break

            stores = data.get("stores") or []
            new_count = 0
            for s in stores:
                row = self._to_row(s, root, seen, "live")
                if row:
                    yield row
                    new_count += 1

            consecutive_zero = 0 if new_count else consecutive_zero + 1

            if (
                not stores
                or page >= self.MAX_PAGES
                or consecutive_zero >= self.EARLY_STOP_PAGES
            ):
                break
            page += 1

    # ------------------------------------------------------------------
    # Wayback 経路 (live 停止時のフォールバック)
    # ------------------------------------------------------------------
    def _crawl_wayback(self, api_base: str, root: str, seen: Set[str]):
        api_host = urlsplit(api_base).netloc
        for path in _WAYBACK_API_PATHS:
            for timestamp, original in self._cdx(f"{api_host}/{path}"):
                snap = f"https://web.archive.org/web/{timestamp}id_/{original}"
                data = self._get_json(snap)
                if not data:
                    continue
                for s in self._iter_stores(data):
                    row = self._to_row(s, root, seen, f"wayback:{timestamp}")
                    if row:
                        yield row

    def _cdx(self, url_pattern: str) -> Iterable[Tuple[str, str]]:
        """CDX API で 200 応答のスナップショット (timestamp, original) を列挙する。

        url_pattern は末尾ワイルドカード無しの前方一致 (matchType=prefix)。
        `*` を付けると matchType と競合して 0 件になるため付けないこと。
        """
        params = {
            "url": url_pattern,
            "output": "json",
            "fl": "timestamp,original",
            "filter": "statuscode:200",
            "collapse": "digest",
            "matchType": "prefix",
        }
        data = self._get_json(_CDX_ENDPOINT, params=params)
        if not isinstance(data, list) or len(data) < 2:
            logger.warning("Wayback スナップショット無し: %s", url_pattern)
            return []
        rows = [(r[0], r[1]) for r in data[1:] if len(r) >= 2]
        logger.info("[WAYBACK] %s -> %d snapshots", url_pattern, len(rows))
        return rows

    def _iter_stores(self, data: dict) -> Iterable[dict]:
        """API レスポンス JSON から店舗 dict を取り出す (複数形/単数形の両方に対応)。"""
        if not isinstance(data, dict):
            return []
        stores = data.get("stores")
        if isinstance(stores, list):
            return [s for s in stores if isinstance(s, dict)]
        store = data.get("store")
        if isinstance(store, dict):
            return [store]
        return []

    # ------------------------------------------------------------------
    # 整形
    # ------------------------------------------------------------------
    def _to_row(self, s: dict, root: str, seen: Set[str], source: str) -> dict | None:
        name = (s.get("store_name") or "").strip()
        addr = (s.get("address") or "").replace("\n", " ").replace("\r", " ").strip()
        if not name or not addr:
            return None

        store_id = s.get("id") or s.get("store_id") or ""
        key = str(store_id) if store_id else f"{name}|{addr}"
        if key in seen:
            return None
        seen.add(key)

        building = (s.get("building_name") or "").strip()
        pref = self._extract_pref(addr)
        rest = addr[len(pref):].strip() if pref and addr.startswith(pref) else addr
        if building:
            rest = f"{rest} {building}".strip()

        hp = (s.get("url") or "").strip()
        return {
            Schema.URL: self._detail_url(root, store_id),
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: (s.get("postal_code") or "").strip(),
            Schema.ADDR: rest,
            Schema.TEL: (s.get("store_phone_number") or "").strip(),
            Schema.CAT_SITE: (s.get("genre") or "").strip(),
            Schema.HP: hp if hp.startswith("http") else "",
            "店舗ID": store_id,
            "is_new": s.get("is_new"),
            "取得経路": source,
        }

    def _extract_pref(self, addr: str) -> str:
        for p in PREFS_47:
            if p in addr:
                return p
        return ""

    # ------------------------------------------------------------------
    # 全国グリッド (live 経路)
    # ------------------------------------------------------------------
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

        max_r = max(abs(min_ix), abs(max_ix), abs(min_iy), abs(max_iy))

        logger.info("[CENTER] (%.5f,%.5f)", self.CENTER_LAT, self.CENTER_LON)
        logger.info("[BBOX] (%.2f,%.2f)-(%.2f,%.2f)", south, west, north, east)
        logger.info(
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

            filtered = [
                (ix, iy)
                for ix, iy in ring_points
                if min_ix <= ix <= max_ix and min_iy <= iy <= max_iy
            ]
            if not filtered:
                continue

            logger.debug("[RING] r=%d points=%d", r, len(filtered))

            for ix, iy in filtered:
                lat, lon = self._xy_km_to_latlon(
                    self.CENTER_LAT, self.CENTER_LON, ix * self.KM_STEP, iy * self.KM_STEP
                )
                if south <= lat <= north and west <= lon <= east:
                    yield lat, lon


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = AuPayStoreScraper()
    scraper.execute("https://aupay.wallet.auone.jp/store/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
