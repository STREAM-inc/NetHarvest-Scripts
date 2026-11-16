"""
ファンくる (fancrew.jp) — 覆面調査モニター掲載店舗

取得対象:
    - ファンくるに覆面調査モニター案件を掲載している店舗
      (対象カテゴリ: 1=グルメ / 2=美容 / 5=サービスチェック)
    - 3=店頭購入 / 4=通販 / 6=アンケート は対象外 (依頼者指定)
    - 同一店舗に複数のモニター案件があるため、店舗ID (shopId) で 1 行に集約する (約4,300店舗)

取得フロー:
    サイトは Next.js のクライアント描画のため HTML ではなく公開 JSON API を使う。
    1. 一覧: POST {url}api/toc-search-v2  body {"categoryId": N, "offset": P}
       - P は 0 始まりのページ番号 (1ページ20件)、応答の allSearchNum が総件数
    2. 一覧の各モニターについて shopId で重複排除し、未取得店舗のみ
       GET {url}api/monitor/{monitorId} で詳細 (電話・住所・HP・営業時間・breadcrumb) を取得
    3. 詳細取得ごとに即 yield (Pattern B)

規約:
    - 利用規約 (/terms) 第14条(禁止事項) にスクレイピング・クローリングの明示的禁止は無い (2026-11 確認)。
    - robots.txt は *-oem/ 系パスのみ Disallow、/api/ は対象外。
    - アクセス間隔は依頼者指定により 0.4 秒以上空ける (本実装は 0.5 秒)。

著作権への配慮:
    - introduction (店舗紹介文)・catchPhrase・設問文などの自由記述プロースは取得しない。

実行方法:
    # ローカルテスト
    python scripts/sites/service/fancrew.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id fancrew
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

import requests

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 対象カテゴリ (1=グルメ, 2=美容, 5=サービスチェック)。3/4/6 は対象外。
_TARGET_CATEGORY_IDS = (1, 2, 5)
_PAGE_SIZE = 20
_MAX_ATTEMPTS = 4

_PREF_PATTERN = re.compile(r"^(北海道|東京都|京都府|大阪府|.{2,3}県)")


class FancrewScraper(StaticCrawler):
    """ファンくる スクレイパー (公開 JSON API)"""

    DELAY = 0
    ITEM_DELAY = 0
    TIMEOUT = 60
    # 依頼者指定: アクセス間隔 0.4 秒以上
    _MIN_INTERVAL = 0.5

    EXTRA_COLUMNS = [
        "モニターID",
        "店舗ID",
        "カテゴリ",
        "ジャンル",
        "エリア",
        "ファンくる掲載URL",
    ]

    # ------------------------------------------------------------------ utils
    @staticmethod
    def _clean(value) -> str:
        if value is None:
            return ""
        return re.sub(r"\s+", " ", str(value)).strip()

    def _throttle(self) -> None:
        last = getattr(self, "_last_req_at", 0.0)
        remain = self._MIN_INTERVAL - (time.monotonic() - last)
        if remain > 0:
            time.sleep(remain)
        self._last_req_at = time.monotonic()

    def _request_json(self, method: str, url: str, payload: dict | None = None) -> dict:
        """上限付きリトライで JSON を取得する。全試行失敗で RuntimeError。"""
        last_err: Exception | None = None
        for attempt in range(_MAX_ATTEMPTS):
            self._throttle()
            try:
                if method == "POST":
                    resp = self.session.post(url, json=payload, timeout=self.TIMEOUT)
                else:
                    resp = self.session.get(url, timeout=self.TIMEOUT)
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code == 404:
                    return {}
                last_err = RuntimeError(f"HTTP {resp.status_code}")
            except (requests.RequestException, ValueError) as e:
                last_err = e
            self.logger.warning("取得失敗 (%d/%d) %s %s: %s", attempt + 1, _MAX_ATTEMPTS, method, url, last_err)
            time.sleep(min(2 ** attempt, 10))
        raise RuntimeError(f"取得に失敗しました: {method} {url}: {last_err}")

    # ------------------------------------------------------------------ parse
    def parse(self, url: str) -> Generator[dict, None, None]:
        base = url if url.endswith("/") else url + "/"
        search_api = urljoin(base, "api/toc-search-v2")
        seen_shops: set[str] = set()

        for category_id in _TARGET_CATEGORY_IDS:
            page = 0
            total = None
            while True:
                data = self._request_json("POST", search_api, {"categoryId": category_id, "offset": page})
                monitors = data.get("monitorList") or []
                if total is None:
                    total = int(data.get("allSearchNum") or 0)
                    self.logger.info("categoryId=%d 総モニター数: %d", category_id, total)
                    self.total_items = (self.total_items or 0) + total
                if not monitors:
                    break

                for mon in monitors:
                    shop = mon.get("shop") or {}
                    shop_id = str(shop.get("shopId") or "")
                    monitor_id = mon.get("monitorId")
                    if not monitor_id:
                        continue
                    key = shop_id or f"m{monitor_id}"
                    if key in seen_shops:
                        continue
                    seen_shops.add(key)
                    item = self._scrape_detail(base, monitor_id, shop_id, category_id, shop)
                    if item:
                        yield item

                end = int(data.get("endPosition") or 0)
                if end >= total or len(monitors) < _PAGE_SIZE:
                    break
                page += 1

    def _scrape_detail(self, base: str, monitor_id, shop_id: str, category_id: int, shop: dict) -> dict | None:
        detail = self._request_json("GET", urljoin(base, f"api/monitor/{monitor_id}"))

        crumbs = {b.get("urlType"): self._clean(b.get("label")) for b in (detail.get("breadcrumb") or [])}
        cat = crumbs.get(1, "") or self._clean(shop.get("categoryName"))
        genre = crumbs.get(2, "")

        address = self._clean(detail.get("address") or shop.get("address"))
        pref = crumbs.get(3, "")
        if not pref:
            plist = shop.get("prefectureList") or []
            pref = self._clean(plist[0].get("name")) if plist else ""
        m = _PREF_PATTERN.match(address)
        if m:
            pref = pref or m.group(1)
            address = address[m.end():].strip()

        name = self._clean(detail.get("shopName") or shop.get("name"))
        if not name:
            return None
        listing_url = urljoin(base, f"detail/{monitor_id}?categoryId={category_id}")

        return {
            Schema.NAME: name,
            Schema.URL: listing_url,
            Schema.PREF: pref,
            Schema.ADDR: address,
            Schema.TEL: self._clean(detail.get("phoneNumber")),
            Schema.HP: self._clean(detail.get("homepageUrl")),
            Schema.TIME: self._clean(detail.get("businessHour")),
            Schema.HOLIDAY: self._clean(detail.get("regularHoliday")),
            Schema.CAT_SITE: "/".join(x for x in (cat, genre) if x),
            "モニターID": str(monitor_id),
            "店舗ID": shop_id or self._clean(detail.get("shopId")),
            "カテゴリ": cat,
            "ジャンル": genre,
            "エリア": crumbs.get(4, "") or self._clean(detail.get("area")),
            "ファンくる掲載URL": listing_url,
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = FancrewScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.fancrew.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
