# scripts/sites/nightlife/shisha_suitai.py
"""
シーシャスイタイ (shisha-suitai.com) — シーシャ店舗スクレイパー

取得対象:
    - 店舗名、住所、都道府県、電話番号、HP
    - SNS (X, Instagram)
    - 曜日別営業時間、定休日
    - 予算、料金メニュー、お店の特徴、設備、予約方法、備考、アクセス、閉店フラグ

取得フロー:
    1. /search にアクセスし __NEXT_DATA__ から buildId・総件数・ページ1 データを取得
    2. Next.js Data API (_next/data/{buildId}/search.json?page=N) で各ページの JSON を取得
    3. 各ページの shopList.list[] から全フィールドを解析

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/shisha_suitai.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id shisha_suitai
"""

import json
import sys
import time
from pathlib import Path

root_path = Path(__file__).resolve().parent.parent.parent.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

# 曜日マッピング: Next.js JSON の dayOfWeek → Schema 定数
_DAY_MAP = {
    "Monday": Schema.TIME_MON,
    "Tuesday": Schema.TIME_TUE,
    "Wednesday": Schema.TIME_WED,
    "Thursday": Schema.TIME_THU,
    "Friday": Schema.TIME_FRI,
    "Saturday": Schema.TIME_SAT,
    "Sunday": Schema.TIME_SUN,
}

ITEMS_PER_PAGE = 10
BASE_URL = "https://shisha-suitai.com"


class ShishaSuitaiScraper(DynamicCrawler):
    """シーシャスイタイ スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "予算",
        "料金メニュー",
        "お店の特徴",
        "設備",
        "予約方法",
        "備考",
        "アクセス",
        "閉店フラグ",
    ]

    def parse(self, url: str):
        # Step 1: 初回アクセスで __NEXT_DATA__ を取得 (buildId + ページ1データ)
        self.page.goto(url, wait_until="networkidle")

        data = self._extract_next_data()
        if not data:
            self.logger.error("__NEXT_DATA__ の取得に失敗しました")
            return

        build_id = data.get("buildId", "")
        if not build_id:
            self.logger.error("buildId の取得に失敗しました")
            return

        shop_list = data.get("props", {}).get("pageProps", {}).get("shopList", {})
        total = shop_list.get("totalNum", 0)
        self.total_items = total
        total_pages = (total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
        self.logger.info("総店舗数: %d (%dページ), buildId: %s", total, total_pages, build_id)

        # Step 2: ページ1のデータを処理
        shops = shop_list.get("list", [])
        for entry in shops:
            parsed = self._parse_shop(entry.get("shop", {}))
            if parsed:
                yield parsed

        # Step 3: ページ2以降を Next.js Data API で取得
        for page_num in range(2, total_pages + 1):
            if self.DELAY > 0:
                time.sleep(self.DELAY)

            self.logger.info("ページ %d / %d を取得中...", page_num, total_pages)

            try:
                page_shops = self._fetch_page_via_api(build_id, page_num)
                if not page_shops:
                    self.logger.warning("ページ %d のデータが空です。スキップします。", page_num)
                    continue

                for entry in page_shops:
                    parsed = self._parse_shop(entry.get("shop", {}))
                    if parsed:
                        yield parsed

            except Exception as e:
                self.logger.warning("ページ %d の取得に失敗: %s", page_num, e)
                continue

    def _extract_next_data(self) -> dict | None:
        """ページの __NEXT_DATA__ JSON を取得"""
        try:
            raw = self.page.evaluate("""() => {
                const el = document.getElementById('__NEXT_DATA__');
                return el ? el.textContent : null;
            }""")
            return json.loads(raw) if raw else None
        except Exception as e:
            self.logger.warning("__NEXT_DATA__ パース失敗: %s", e)
            return None

    def _fetch_page_via_api(self, build_id: str, page_num: int) -> list:
        """Next.js Data API を使ってページデータを取得"""
        api_url = f"{BASE_URL}/_next/data/{build_id}/search.json?page={page_num}"

        raw = self.page.evaluate("""(url) => {
            return fetch(url).then(r => r.text()).catch(e => null);
        }""", api_url)

        if not raw:
            return []

        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            self.logger.warning("JSON パース失敗 (page %d)", page_num)
            return []

        page_props = data.get("pageProps", {})
        shop_list = page_props.get("shopList", {})
        return shop_list.get("list", [])

    def _parse_shop(self, shop: dict) -> dict | None:
        """shop JSON オブジェクトを NetHarvest の item dict に変換"""
        if not shop or not shop.get("name"):
            return None

        item = {
            Schema.NAME: shop.get("name", ""),
            Schema.ADDR: shop.get("address", ""),
            Schema.PREF: shop.get("prefecture", {}).get("name", "") if shop.get("prefecture") else "",
            Schema.TEL: shop.get("phoneNumber", ""),
            Schema.HP: shop.get("homepageUrl", "") or "",
            Schema.HOLIDAY: shop.get("regularClosingDay", "") or "",
            Schema.URL: f"{BASE_URL}/shop/{shop.get('id', '')}",
        }

        # SNS アカウント
        for sns in shop.get("shopSnsAccounts", []):
            service = sns.get("serviceId", "")
            sns_url = sns.get("url", "")
            if service == "twitter":
                item[Schema.X] = sns_url
            elif service == "instagram":
                item[Schema.INSTA] = sns_url

        # 曜日別営業時間
        for bh in shop.get("businessHours", []):
            day_key = _DAY_MAP.get(bh.get("dayOfWeek", ""))
            if day_key:
                start = bh.get("start", "")[:5]  # "09:00:00" → "09:00"
                end = bh.get("end", "")[:5]
                item[day_key] = f"{start}〜{end}"

        # EXTRA_COLUMNS
        item["予算"] = shop.get("budget", "") or ""
        item["料金メニュー"] = (shop.get("menu", "") or "").strip().replace("\n", " / ")
        item["お店の特徴"] = (shop.get("description", "") or "").strip().replace("\n", " ")
        item["設備"] = "、".join(f.get("name", "") for f in shop.get("facilities", []))
        item["予約方法"] = shop.get("reservation", "") or ""
        item["備考"] = (shop.get("note", "") or "").strip().replace("\n", " / ")
        item["アクセス"] = shop.get("accessDescription", "") or ""
        item["閉店フラグ"] = "閉店" if shop.get("isClosed") else ""

        return item


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = ShishaSuitaiScraper()
    scraper.execute("https://shisha-suitai.com/search")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
