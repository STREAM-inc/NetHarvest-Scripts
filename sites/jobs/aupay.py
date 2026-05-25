# -*- coding: utf-8 -*-
"""
au PAY 店舗スキャン（全国版 / 東京中心から外側へリング拡張）
------------------------------------------------------------
- 東京(中心点)から外側へ「リング（正方形の輪）」を広げて走査
- 最終的に BBOX 全域をカバー
- CSV 逐次追記 / 店舗名重複除外 / 都道府県抽出

例:
  python aupay_scan_japan_spiral.py --km-step 20 --out japan_new.csv
  python aupay_scan_japan_spiral.py --km-step 15 --center-lat 35.6895 --center-lon 139.6917

⚠ 注意:
km-step が小さいほど点数が爆増します。全国は 10〜30 推奨。
"""

import csv
import os
import time
import random
import argparse
from typing import Dict, Any, Set, Optional, Tuple
import math
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime

BASE_API = "https://api.aupay.wallet.auone.jp/store-search"

# --- プロキシ設定（必要なければ None） ---
# 例: PROXIES = {"http": "http://127.0.0.1:8080", "https": "http://127.0.0.1:8080"}
PROXIES: Optional[dict] = None

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) aupay-japan/1.0",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://aupay.wallet.auone.jp/store/",
    "Accept-Language": "ja,en;q=0.8",
}

# --- 日本全体（おおまか） ---
# 南(沖縄)〜北(北海道) / 西(与那国)〜東(択捉あたり)
BBOX_JAPAN = (24.0, 122.5, 46.2, 146.5)  # (south, west, north, east)

# --- 出力カラム（要求仕様通り） ---
CSV_COLS = ["取得日時", "取得URL", "名称", "TEL", "都道府県", "住所", "業種", "is_new"]

# 全行共通のスクレイピング元URL
SCRAPE_URL = "https://aupay.wallet.auone.jp/store/"

# 47都道府県
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

KM_PER_DEG_LAT = 111.32


# ------------------ SESSION ------------------ #
def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s


SESSION = build_session()


def _get(url: str, *, params=None, timeout=15):
    if PROXIES:
        return SESSION.get(url, params=params, timeout=timeout, proxies=PROXIES)
    return SESSION.get(url, params=params, timeout=timeout)


def call_store_search(
    lat: float, lon: float, page: int = 1, timeout: int = 15
) -> Dict[str, Any]:
    params = {
        "flag": 1,
        "latitude": lat,
        "longitude": lon,
        "device_latitude": lat,
        "device_longitude": lon,
        "page": page,
    }
    r = _get(BASE_API, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


# ------------------ FILTER + ROW BUILDER ------------------ #
def extract_pref_from_address(addr: str) -> str:
    """住所文字列から都道府県名を抽出（見つからなければ空文字）"""
    if not addr:
        return ""
    for p in PREFS_47:
        if p in addr:
            return p
    # 例外（必要なら拡張）
    if "東京" in addr:
        return "東京都"
    if "大阪" in addr:
        return "大阪府"
    if "京都" in addr:
        return "京都府"
    return ""


def row_from_store(s: Dict[str, Any], seen_names: Set[str]) -> Optional[Dict[str, Any]]:
    """条件に合う店舗だけ行データへ変換"""

    # --- is_new → 1 のみ保存したい場合はコメント解除 ---
    # if s.get("is_new") != 1:
    #     return None

    name = s.get("store_name")
    if not name or name in seen_names:
        return None

    addr = (s.get("address") or "").replace("\n", " ").replace("\r", " ").strip()
    if not addr:
        return None

    pref = extract_pref_from_address(addr)
    seen_names.add(name)

    return {
        "取得日時": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "取得URL": SCRAPE_URL,
        "名称": name,
        "TEL": s.get("store_phone_number"),
        "都道府県": pref,
        "住所": addr,
        "業種": s.get("genre"),
        "is_new": s.get("is_new"),
    }


# ------------------ CSV IO ------------------ #
def open_csv_for_append(path: str):
    file_exists = os.path.exists(path)
    if file_exists:
        f = open(path, "a", newline="", encoding="utf-8")
        writer = csv.DictWriter(f, fieldnames=CSV_COLS)
    else:
        f = open(path, "w", newline="", encoding="utf-8-sig")
        writer = csv.DictWriter(f, fieldnames=CSV_COLS)
        writer.writeheader()
        f.flush()
    return f, writer


# ------------------ CORE: fetch at center ------------------ #
def collect_new_at_center(
    lat: float,
    lon: float,
    *,
    writer,
    csv_file_handle,
    seen_names: Set[str],
    verbose: bool,
    quiet: bool,
    max_pages: int,
    early_stop_pages: int,
    flush_every: int,
):
    page = 1
    wrote_since_flush = 0
    consecutive_zero = 0

    while True:
        try:
            data = call_store_search(lat, lon, page=page)
        except Exception as e:
            if not quiet:
                print(f"[WARN] request failed ({lat:.5f},{lon:.5f}) page={page}: {e}")
            break

        stores = data.get("stores", [])

        if verbose:
            print(f"[DEBUG] ({lat:.5f},{lon:.5f}) page={page} stores={len(stores)}")

        new_on_page = 0

        for s in stores:
            row = row_from_store(s, seen_names)
            if row is None:
                continue

            writer.writerow(row)
            new_on_page += 1
            wrote_since_flush += 1

            if not quiet:
                print(f"  NEW → {row['名称']} / {row['住所']} / is_new={row['is_new']}")

            if wrote_since_flush >= flush_every:
                csv_file_handle.flush()
                wrote_since_flush = 0

        if new_on_page == 0:
            consecutive_zero += 1
        else:
            consecutive_zero = 0

        if not stores or page >= max_pages or consecutive_zero >= early_stop_pages:
            break

        page += 1
        time.sleep(random.uniform(0.1, 0.3))

    if wrote_since_flush > 0:
        csv_file_handle.flush()


# ------------------ NEW: Tokyo-centered ring scan ------------------ #
def _clamp(v, lo, hi):
    return max(lo, min(hi, v))


def latlon_to_xy_km(
    lat0: float, lon0: float, lat: float, lon: float
) -> Tuple[float, float]:
    """
    東京中心(lat0,lon0)基準の簡易平面近似（equirectangular）
    x: 東(+), y: 北(+), 単位km
    ※lon方向のスケールは cos(lat0) で固定（リングが歪みにくい）
    """
    y = (lat - lat0) * KM_PER_DEG_LAT
    x = (lon - lon0) * (KM_PER_DEG_LAT * max(1e-6, math.cos(math.radians(lat0))))
    return x, y


def xy_km_to_latlon(
    lat0: float, lon0: float, x_km: float, y_km: float
) -> Tuple[float, float]:
    lat = lat0 + (y_km / KM_PER_DEG_LAT)
    lon = lon0 + (x_km / (KM_PER_DEG_LAT * max(1e-6, math.cos(math.radians(lat0)))))
    return lat, lon


def ring_scan_bbox_from_center(
    *,
    center_lat: float,
    center_lon: float,
    bbox: Tuple[float, float, float, float],
    km_step: float,
    writer,
    csv_file_handle,
    seen_names: Set[str],
    verbose: bool,
    quiet: bool,
    max_pages: int,
    early_stop_pages: int,
    flush_every: int,
    sleep_min: float,
    sleep_max: float,
):
    """
    BBOX を「中心から外側へ」リング状に走査。
    リング r は格子インデックスの max(|ix|,|iy|)=r の境界点のみをなめる。
    """
    south, west, north, east = bbox

    # BBOX四隅を中心基準のkm平面へ
    corners = [
        (south, west),
        (south, east),
        (north, west),
        (north, east),
    ]
    corner_xy = [
        latlon_to_xy_km(center_lat, center_lon, lat, lon) for lat, lon in corners
    ]

    # ix/iy の走査範囲（km_step格子）
    xs = [x for x, _ in corner_xy]
    ys = [y for _, y in corner_xy]
    min_ix = math.floor(min(xs) / km_step) - 1
    max_ix = math.ceil(max(xs) / km_step) + 1
    min_iy = math.floor(min(ys) / km_step) - 1
    max_iy = math.ceil(max(ys) / km_step) + 1

    max_r = max(
        abs(min_ix),
        abs(max_ix),
        abs(min_iy),
        abs(max_iy),
    )

    if not quiet:
        print(f"[CENTER] ({center_lat},{center_lon})")
        print(f"[BBOX] ({south},{west})-({north},{east})")
        print(
            f"[GRID] km_step={km_step} -> ix=[{min_ix},{max_ix}] iy=[{min_iy},{max_iy}] max_r={max_r}"
        )

    total_points = 0
    inside_points = 0

    for r in range(0, int(max_r) + 1):
        # ring boundary points: max(|ix|,|iy|) == r
        if r == 0:
            ring_points = [(0, 0)]
        else:
            ring_points = []
            # top/bottom edges (iy = ±r)
            for ix in range(-r, r + 1):
                ring_points.append((ix, r))
                ring_points.append((ix, -r))
            # left/right edges (ix = ±r) excluding corners to avoid duplicates
            for iy in range(-r + 1, r):
                ring_points.append((r, iy))
                ring_points.append((-r, iy))

        # 範囲外リング点の除外（BBOXの外側に大きくはみ出す部分を削る）
        filtered = []
        for ix, iy in ring_points:
            if ix < min_ix or ix > max_ix or iy < min_iy or iy > max_iy:
                continue
            filtered.append((ix, iy))

        if not filtered:
            continue

        if verbose and not quiet:
            print(f"[RING] r={r} points={len(filtered)}")

        for ix, iy in filtered:
            total_points += 1
            x_km = ix * km_step
            y_km = iy * km_step
            lat, lon = xy_km_to_latlon(center_lat, center_lon, x_km, y_km)

            # BBOX内だけ叩く（外はスキップ）
            if not (south <= lat <= north and west <= lon <= east):
                continue

            inside_points += 1

            collect_new_at_center(
                lat,
                lon,
                writer=writer,
                csv_file_handle=csv_file_handle,
                seen_names=seen_names,
                verbose=verbose,
                quiet=quiet,
                max_pages=max_pages,
                early_stop_pages=early_stop_pages,
                flush_every=flush_every,
            )

            time.sleep(random.uniform(sleep_min, sleep_max))

    if not quiet:
        print(
            f"[SCAN] total_grid_points={total_points}, inside_bbox_points={inside_points}"
        )


# ------------------ CLI + RUNNER ------------------ #
def parse_args():
    p = argparse.ArgumentParser(
        description="au PAY 店舗スキャン（全国 / 東京中心→外側へ / 重複除外 / CSV逐次保存）"
    )
    p.add_argument(
        "--km-step",
        type=float,
        default=1.0,
        help="グリッド間隔(km)。全国は 10〜30 推奨",
    )
    p.add_argument("--out", default="data_list_202602.csv", help="出力CSV")
    p.add_argument("--verbose", action="store_true", help="デバッグ表示")
    p.add_argument("--quiet", action="store_true", help="NEW表示なども抑制")
    p.add_argument(
        "--early-stop-pages",
        type=int,
        default=1,
        help="連続で0件ページが続いたら停止するページ数",
    )
    p.add_argument(
        "--max-pages-per-center", type=int, default=50, help="1座標あたり最大ページ数"
    )
    p.add_argument("--flush-every", type=int, default=1, help="何件ごとにflushするか")

    # 東京中心（デフォルト）
    p.add_argument(
        "--center-lat", type=float, default=35.6895, help="中心緯度（デフォルト: 東京）"
    )
    p.add_argument(
        "--center-lon",
        type=float,
        default=139.6917,
        help="中心経度（デフォルト: 東京）",
    )

    # API叩きの待機（全体負荷軽減）
    p.add_argument(
        "--sleep-min", type=float, default=0.10, help="座標ごとの最小sleep秒"
    )
    p.add_argument(
        "--sleep-max", type=float, default=0.30, help="座標ごとの最大sleep秒"
    )
    return p.parse_args()


def main():
    args = parse_args()

    print("=== au PAY 店舗スキャン（全国 / 東京中心→外側へ） ===")
    print(f"[CFG] km_step={args.km_step}, out={args.out}")
    print(f"[CFG] center=({args.center_lat},{args.center_lon})")
    print(
        f"[CFG] early_stop_pages={args.early_stop_pages}, max_pages_per_center={args.max_pages_per_center}"
    )
    print(f"[CFG] sleep=[{args.sleep_min},{args.sleep_max}]")

    csv_fh, writer = open_csv_for_append(args.out)
    seen_names: Set[str] = set()  # 店舗名の重複チェック

    try:
        ring_scan_bbox_from_center(
            center_lat=args.center_lat,
            center_lon=args.center_lon,
            bbox=BBOX_JAPAN,
            km_step=args.km_step,
            writer=writer,
            csv_file_handle=csv_fh,
            seen_names=seen_names,
            verbose=args.verbose,
            quiet=args.quiet,
            max_pages=args.max_pages_per_center,
            early_stop_pages=args.early_stop_pages,
            flush_every=args.flush_every,
            sleep_min=args.sleep_min,
            sleep_max=args.sleep_max,
        )
    finally:
        csv_fh.flush()
        csv_fh.close()

    print(f"[DONE] CSV に保存完了: {args.out}")
    print(f"保存したユニーク店舗数: {len(seen_names)}")


if __name__ == "__main__":
    main()
