"""
対象サイト: https://pay.rakuten.co.jp/shop/map/
楽天ペイ 加盟店マップ スクレイパー

取得対象:
    - 楽天ペイ (service_id に "rpay" を含む) が使える全国の加盟店。Edy のみ・ポイントカードのみの店舗は除外。
    - 件数目安: 約22万件 (2026-08-22 取得分 221,713 件)。大幅に少なければ取得漏れ。

取得フロー:
    1. 起点 URL (加盟店マップ) の HTML から設定スクリプト (config_phaseNNN.js) を特定し、
       一覧/詳細 API の URL テンプレートとジャンル辞書 (コード→名称) を読み取る。
    2. 一覧 API (stores?latitude=&longitude=) は「指定地点から半径 2km 以内の店舗を近い順に最大 100 件」
       返す。日本全国 (離島含む) を覆う矩形群を、一辺 2.75km (外接円半径 < 2km) の格子に切って順に呼ぶ。
       - 100 件未満: 半径 2km 内は全件取得済み → そのセルは完了
       - 100 件かつ最遠店舗がセル外接円半径以上: セル内は全件取得済み → 完了
       - 100 件かつ最遠店舗がセル外接円半径より近い (密集地): セルを 4 分割して呼び直す
    3. map_store_id で重複除去し、rpay 店舗ごとに詳細 API を呼んで即 yield する。

補足 (2026-10 調査):
    - 一覧 API に半径・件数パラメータは無い (radius/limit 等は無視される)。海上は空配列を返す。
    - 東京駅周辺は 100 件で半径約 0.12km。同一座標に 100 件超が重なる場合は分割で解消できないため、
      最小セル (半径 5m) で打ち切り警告を出す。
    - 詳細 API の phone が "0" のものはサイト側でも非表示 (shopDetailHook) → 空にする。
    - introduction (店舗紹介文) は自由記述のため取得しない (著作権リスク)。
    - 利用規約 (https://pay.rakuten.co.jp/agreement/) にスクレイピング・クローリングの明示禁止なし。
    - 格子は東京駅から近い順に巡回する (最初の 1 件を即座に返すため)。

実行方法:
    python scripts/sites/service/pay.py
    docker compose exec worker python /app/bin/run_flow.py --site-id pay
"""
import codecs
import json
import math
import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import requests

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 設定 JS が読めなかった場合のみ使うフォールバック (2026-10 時点の値)
_FALLBACK_LIST_API = "https://api-catalog-jpe-gateway.rakuten.co.jp/mmeu/api/v3/stores?latitude={lat}&longitude={lng}&client_id=integrated"
_FALLBACK_DETAIL_API = "https://api-catalog-jpe-gateway.rakuten.co.jp/mmeu/api/v3/store/{id}?client_id=integrated"

SEARCH_RADIUS_KM = 2.0      # 一覧 API の検索半径上限
API_LIMIT = 100             # 一覧 API の最大返却件数
CELL_SIDE_KM = 2.75         # 初期格子の一辺 (外接円半径 1.94km < 2km)
MIN_CELL_RADIUS_KM = 0.005  # これ以上は分割しない (同一座標に 100 件超が重なるケース)
KM_PER_DEG_LAT = 111.0
ORIGIN = (35.6812, 139.7671)  # 巡回順の基準 (東京駅)

# 日本全国 (離島含む) を覆う矩形 (lat_min, lat_max, lng_min, lng_max)。重なりは格子キーで重複除去される
JAPAN_BOXES = [
    (41.30, 42.60, 139.30, 143.40),  # 北海道 道南・日高・十勝南部
    (42.60, 43.70, 139.80, 145.90),  # 北海道 道央・十勝・釧路・根室
    (43.70, 45.60, 140.90, 145.50),  # 北海道 道北・オホーツク・利尻礼文
    (37.70, 41.60, 139.40, 142.10),  # 東北
    (34.55, 37.90, 137.40, 141.10),  # 関東・甲信・静岡・新潟
    (37.00, 38.60, 138.00, 139.50),  # 新潟北部・佐渡・粟島
    (32.40, 34.80, 139.10, 140.00),  # 伊豆諸島
    (33.40, 37.60, 134.90, 137.50),  # 近畿・東海西部・北陸
    (33.90, 35.70, 130.80, 135.00),  # 中国
    (35.95, 36.40, 132.95, 133.45),  # 隠岐
    (32.70, 34.50, 132.00, 134.80),  # 四国
    (30.95, 34.00, 129.40, 132.10),  # 九州
    (34.00, 34.75, 129.10, 129.55),  # 対馬
    (32.50, 33.35, 128.50, 129.40),  # 五島
    (30.00, 31.00, 130.20, 131.15),  # 種子島・屋久島
    (29.00, 30.00, 129.20, 130.00),  # トカラ列島
    (27.00, 28.60, 128.30, 130.10),  # 奄美群島
    (26.00, 27.15, 126.65, 128.35),  # 沖縄本島・慶良間・久米島
    (25.80, 26.00, 131.15, 131.40),  # 大東諸島
    (24.60, 24.95, 125.05, 125.50),  # 宮古島
    (24.00, 24.65, 122.90, 124.35),  # 八重山
    (26.55, 27.15, 142.10, 142.30),  # 小笠原
]

_PREF_RE = re.compile(r"^(北海道|東京都|京都府|大阪府|.{2,3}?県)")


def _haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    p = math.radians
    a = (math.sin(p(lat2 - lat1) / 2) ** 2
         + math.cos(p(lat1)) * math.cos(p(lat2)) * math.sin(p(lng2 - lng1) / 2) ** 2)
    return 2 * 6371.0 * math.asin(math.sqrt(a))


def _unescape_js(s: str) -> str:
    return codecs.decode(s, "unicode_escape") if "\\u" in s else s


class PayScraper(StaticCrawler):
    """楽天ペイ 加盟店マップ スクレイパー (公開 JSON API)"""
    DELAY = 0.5          # 詳細 API 1 件 (= yield 1 件) ごとの待機
    LIST_DELAY = 0.5     # 一覧 API 呼び出しごとの待機
    MAX_ATTEMPTS = 4
    EXTRA_COLUMNS = ["map_store_id", "ジャンルコード", "緯度", "経度"]

    # ------------------------------------------------------------------
    # 通信
    # ------------------------------------------------------------------
    def _get_json(self, api_url: str) -> dict | None:
        """JSON API を取得する。429/5xx/通信エラーは指数バックオフで上限付きリトライ、404 は None。"""
        last_error = None
        for attempt in range(self.MAX_ATTEMPTS):
            try:
                r = self.session.get(api_url, timeout=self.TIMEOUT,
                                     headers={"Accept": "application/json"})
                if r.status_code == 404:
                    return None
                if r.status_code == 429 or r.status_code >= 500:
                    last_error = f"HTTP {r.status_code}"
                else:
                    r.raise_for_status()
                    return r.json()
            except (requests.exceptions.RequestException, ValueError) as e:
                last_error = repr(e)
            self.logger.warning("API 取得失敗 (%d/%d): %s — %s", attempt + 1, self.MAX_ATTEMPTS, api_url, last_error)
            time.sleep(min(2 ** (attempt + 1), 30))
        raise RuntimeError(f"API 取得に {self.MAX_ATTEMPTS} 回失敗: {api_url} ({last_error})")

    def _load_config(self, url: str) -> None:
        """加盟店マップの設定 JS から API テンプレートとジャンル辞書を読み取る。"""
        self.list_api = _FALLBACK_LIST_API
        self.detail_api = _FALLBACK_DETAIL_API
        self.genre_labels: dict[str, str] = {}

        soup = self.get_soup(url)
        src = None
        if soup is not None:
            for s in soup.select("script[src]"):
                if re.search(r"/config_phase\d+\.js", s["src"]):
                    src = urljoin(url, s["src"])
                    break
        if not src:
            self.logger.warning("設定 JS が見つかりません。既定の API URL を使用します。")
            return
        try:
            r = self.session.get(src, timeout=self.TIMEOUT)
            r.raise_for_status()
            r.encoding = "utf-8"
            js = r.text
        except requests.exceptions.RequestException as e:
            self.logger.warning("設定 JS の取得に失敗 (%s)。既定の API URL を使用します。", e)
            return

        m = re.search(r'return"(https://[^"]+/stores\?latitude=)"\.concat\(e,"(&longitude=)"\.concat\(a,"(&client_id=)"\.concat\(o\|\|"(\w+)"\)', js)
        if m:
            self.list_api = m.group(1) + "{lat}" + m.group(2) + "{lng}" + m.group(3) + m.group(4)
        m = re.search(r'return"(https://[^"]+/store/)"\.concat\(e,"(\?client_id=)"\.concat\(a\|\|"(\w+)"\)', js)
        if m:
            self.detail_api = m.group(1) + "{id}" + m.group(2) + m.group(3)
        for code, label in re.findall(r'code:"(\d{2,4})",label:"([^"]+)"', js):
            self.genre_labels[code] = _unescape_js(label)
        self.logger.info("一覧API: %s / 詳細API: %s / ジャンル %d 件",
                         self.list_api, self.detail_api, len(self.genre_labels))

    # ------------------------------------------------------------------
    # 格子
    # ------------------------------------------------------------------
    @staticmethod
    def _initial_cells() -> list[tuple[float, float, float, float]]:
        """全国の初期セル (中心lat, 中心lng, 半辺lat度, 半辺lng度) を東京駅から近い順に返す。"""
        dlat = CELL_SIDE_KM / KM_PER_DEG_LAT
        cells: dict[tuple[int, int], tuple[float, float, float, float]] = {}
        for lat_min, lat_max, lng_min, lng_max in JAPAN_BOXES:
            for row in range(math.floor(lat_min / dlat), math.ceil(lat_max / dlat)):
                # 行ごとの経度幅は行の高緯度側で決める (セル外接円半径が 2km を超えないように)
                top = (row + 1) * dlat
                dlng = CELL_SIDE_KM / (111.32 * math.cos(math.radians(top)))
                for col in range(math.floor(lng_min / dlng), math.ceil(lng_max / dlng)):
                    cells[(row, col)] = ((row + 0.5) * dlat, (col + 0.5) * dlng, dlat / 2, dlng / 2)
        return sorted(cells.values(), key=lambda c: _haversine_km(ORIGIN[0], ORIGIN[1], c[0], c[1]))

    # ------------------------------------------------------------------
    # 変換
    # ------------------------------------------------------------------
    def _genre(self, genre: dict | None) -> tuple[str, str]:
        """genre_code → (サイト定義業種ラベル, コード列)。"""
        if not genre:
            return "", ""
        major = genre.get("major") or ""
        codes = [major] if major else []
        labels = [self.genre_labels.get(major, "")] if major else []
        for key in ("minor1", "minor2", "minor3"):
            minor = genre.get(key)
            if minor:
                code = f"{major}{minor}"
                codes.append(code)
                if code in self.genre_labels:
                    labels.append(self.genre_labels[code])
        return " > ".join(x for x in labels if x), ",".join(codes)

    def _build_item(self, store: dict, detail: dict | None, detail_url: str) -> dict:
        d = detail or {}
        addr = (d.get("address") or "").strip()
        m = _PREF_RE.match(addr)
        postal = re.sub(r"\D", "", d.get("postal_code") or "")
        if len(postal) == 7:
            postal = f"{postal[:3]}-{postal[3:]}"
        phone = (d.get("phone") or "").strip()
        if phone == "0":
            phone = ""
        site_url = (d.get("site_url") or "").strip()
        cat_site, codes = self._genre(d.get("genre_code") or store.get("genre_code"))
        return {
            Schema.URL: detail_url,
            Schema.NAME: (d.get("store_name") or store.get("store_name") or "").strip(),
            Schema.PREF: m.group(1) if m else "",
            Schema.POST_CODE: postal,
            Schema.ADDR: addr,
            Schema.TEL: phone,
            Schema.TIME: (d.get("open_time") or "").strip(),
            Schema.HOLIDAY: (d.get("regular_holiday") or "").strip(),
            Schema.CAT_SITE: cat_site,
            Schema.HP: site_url if site_url.startswith("http") else "",
            Schema.PAYMENTS: ",".join(d.get("service_id") or store.get("service_id") or []),
            "map_store_id": str(store["map_store_id"]),
            "ジャンルコード": codes,
            "緯度": store.get("latitude", ""),
            "経度": store.get("longitude", ""),
        }

    # ------------------------------------------------------------------
    # メイン
    # ------------------------------------------------------------------
    def parse(self, url: str) -> Generator[dict, None, None]:
        self._load_config(url)
        seen: set[int] = set()
        cells = self._initial_cells()
        self.logger.info("初期セル数: %d", len(cells))
        rpay_count = 0

        for idx, root in enumerate(cells, 1):
            stack = [root]
            while stack:
                lat, lng, hlat, hlng = stack.pop()
                time.sleep(self.LIST_DELAY)
                data = self._get_json(self.list_api.format(lat=f"{lat:.6f}", lng=f"{lng:.6f}"))
                stores = (data or {}).get("stores") or []

                for st in stores:
                    sid = st.get("map_store_id")
                    if sid is None or sid in seen:
                        continue
                    seen.add(sid)
                    if "rpay" not in (st.get("service_id") or []):
                        continue
                    detail_url = self.detail_api.format(id=sid)
                    try:
                        detail = self._get_json(detail_url)
                    except RuntimeError as e:
                        self.logger.warning("詳細取得失敗。一覧情報のみ出力: %s", e)
                        detail = None
                    rpay_count += 1
                    yield self._build_item(st, detail, detail_url)

                if len(stores) < API_LIMIT:
                    continue  # 半径 2km 内は全件取得済み
                farthest = max(_haversine_km(lat, lng, s["latitude"], s["longitude"]) for s in stores)
                radius = _haversine_km(lat, lng, lat + hlat, lng + hlng)
                if farthest >= radius:
                    continue  # セル外接円内は全件取得済み
                if radius / 2 < MIN_CELL_RADIUS_KM:
                    self.logger.warning("同一地点に %d 件超が集中し分割不能: (%.6f, %.6f)", API_LIMIT, lat, lng)
                    continue
                qlat, qlng = hlat / 2, hlng / 2
                for dy in (-qlat, qlat):
                    for dx in (-qlng, qlng):
                        stack.append((lat + dy, lng + dx, qlat, qlng))

            if idx % 1000 == 0:
                self.logger.info("セル進捗 %d/%d — 既出店舗 %d / 楽天ペイ店舗 %d", idx, len(cells), len(seen), rpay_count)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    scraper = PayScraper()
    scraper.execute("https://pay.rakuten.co.jp/shop/map/")
