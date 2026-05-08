"""
全国コンカフェマップ (con-cafe.jp) — コンセプトカフェ店舗情報スクレイパー

取得対象:
    - 店舗名、住所（都道府県/番地）、電話番号、業種・ジャンル
    - 公式サイト (HP)、SNS（X/Instagram/Facebook/Line）、エリア

取得フロー:
    JSON API `/api/shop?region=areaXX&page=N` で店舗ID・エリア情報を取得 →
    詳細ページ `/list/{regionSlug}/{prefSlug}/{areaSlug}/{id}` の HTML を取得し、
    JSON-LD (LocalBusiness) と詳細テーブル（ホームページ / SNS 行）を解析する。

対象エリア:
    area01 (北海道・東北) ～ area08 (九州・沖縄)、area09 (その他)

実行方法:
    python scripts/sites/portal/con_cafe.py
"""

import json
import math
import re
import sys
import time
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

BASE_URL = "https://con-cafe.jp"
API_BASE = f"{BASE_URL}/api/shop"
PAGE_SIZE = 20  # 一覧API のデフォルト1ページ件数

# area01 から順に巡回するエリア定義（slugとラベル）
AREA_SLUGS = [
    ("area01", "北海道・東北"),
    ("area02", "関東"),
    ("area03", "甲信越"),
    ("area04", "北陸"),
    ("area05", "東海"),
    ("area06", "関西"),
    ("area07", "中国・四国"),
    ("area08", "九州・沖縄"),
    ("area09", "その他"),
]

# 詳細ページ「SNS」行の img alt → Schema 定数 マッピング
SNS_MAP = {
    "Twitter": Schema.X,
    "X": Schema.X,
    "Instagram": Schema.INSTA,
    "Facebook": Schema.FB,
    "Line": Schema.LINE,
    "LINE": Schema.LINE,
}

PREF_RE = re.compile(r"(東京都|北海道|(?:京都|大阪)府|.+?県)")


class ConCafeScraper(StaticCrawler):
    """全国コンカフェマップ スクレイパー"""

    DELAY = 0.5  # 詳細ページ取得時のリクエスト間隔（秒）
    EXTRA_COLUMNS = ["エリア", "営業時間"]

    # CLIで上書き可能なページ範囲（None=制限なし）
    page_start: int = 1
    page_end: int | None = None

    # parse() の url 引数を「area01」「area02」等の slug として解釈する。
    # 「all」または BASE_URL を渡すと全エリアを巡回する。
    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_ids: set[int] = set()

        targets: list[tuple[str, str]]
        if url and url.startswith("area") and len(url) == 6:
            label = dict(AREA_SLUGS).get(url, "")
            targets = [(url, label)]
        else:
            targets = list(AREA_SLUGS)

        for area_slug, area_label in targets:
            self.logger.info("=== エリア開始: %s (%s) ===", area_slug, area_label)
            yield from self._scrape_area(area_slug, seen_ids)

    # ------------------------------------------------------------------
    # エリア別ページネーション
    # ------------------------------------------------------------------

    def _scrape_area(
        self, area_slug: str, seen_ids: set[int]
    ) -> Generator[dict, None, None]:
        """指定エリアの一覧APIをページネーションし、各店舗の詳細をyieldする"""
        page = self.page_start
        total_pages: int | None = None

        while True:
            params = {
                "page": page,
                "displayed": 1,
                "request": 1,
                "front_displayed": 1,
                "region": area_slug,
                "sort": "priority",
                "order": "desc",
            }
            self.logger.info("一覧API取得 (%s p.%d)", area_slug, page)

            data = self._fetch_api(API_BASE, params)
            if data is None:
                break

            shops = data.get("shops") or []
            if not shops:
                self.logger.info("店舗なし — エリア終了: %s", area_slug)
                break

            if total_pages is None:
                total = int(data.get("count") or 0)
                total_pages = max(1, math.ceil(total / PAGE_SIZE))
                self.logger.info(
                    "総件数 %d 件 / 全 %d ページ (%s)", total, total_pages, area_slug
                )

            for shop in shops:
                shop_id = shop.get("id")
                if shop_id is None or shop_id in seen_ids:
                    continue
                seen_ids.add(shop_id)

                detail_url = self._build_detail_url(shop)
                if not detail_url:
                    continue

                time.sleep(self.DELAY)
                item = self._scrape_detail(detail_url, shop)
                if item:
                    yield item

            limit = total_pages if self.page_end is None else min(total_pages, self.page_end)
            if page >= limit:
                break
            page += 1

    def _fetch_api(self, url: str, params: dict) -> dict | None:
        """JSON API を取得して dict を返す。エラー時は None。"""
        try:
            response = self.session.get(url, params=params, timeout=self.TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.logger.warning("APIエラー: %s params=%s — %s", url, params, e)
            if self.CONTINUE_ON_ERROR:
                return None
            raise

    @staticmethod
    def _build_detail_url(shop: dict) -> str | None:
        """ /list/{region}/{pref}/{sub}/{id} の詳細URLを組み立てる"""
        area = shop.get("area") or {}
        region = (area.get("region") or {}).get("slug")
        pref = (area.get("prefecture") or {}).get("slug")
        sub = area.get("slug")
        shop_id = shop.get("id")
        if not all([region, pref, sub, shop_id]):
            return None
        return f"{BASE_URL}/list/{region}/{pref}/{sub}/{shop_id}"

    # ------------------------------------------------------------------
    # 詳細ページ処理
    # ------------------------------------------------------------------

    def _scrape_detail(self, url: str, shop_meta: dict) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        item: dict[str, str] = {Schema.URL: url}

        # 1. JSON-LD (LocalBusiness) から 名称 / 住所 / TEL / 都道府県 を取得
        self._parse_json_ld(soup, item)

        # 2. 詳細テーブルから HP / 住所(フォールバック) を取得
        self._parse_detail_tables(soup, item)

        # 3. SNS 行から 店舗SNSアカウント を取得
        self._parse_sns(soup, item)

        # 4. APIメタデータで補完
        genre = (shop_meta.get("genre") or {}).get("name") or ""
        if genre.strip():
            item.setdefault(Schema.CAT_SITE, genre.strip())

        area_name = (shop_meta.get("area") or {}).get("name") or ""
        if area_name.strip():
            item["エリア"] = area_name.strip()

        api_name = (shop_meta.get("name") or "").strip()
        if api_name:
            item.setdefault(Schema.NAME, api_name)

        api_twitter = (shop_meta.get("twitter") or "").strip()
        if api_twitter:
            item.setdefault(Schema.X, api_twitter)

        if Schema.NAME not in item:
            return None
        return item

    def _parse_json_ld(self, soup, item: dict) -> None:
        """JSON-LD の LocalBusiness ノードから情報抽出"""
        for script in soup.select('script[type="application/ld+json"]'):
            raw = script.string or script.get_text(strip=True)
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue

            candidates = payload if isinstance(payload, list) else [payload]
            for data in candidates:
                if not isinstance(data, dict) or data.get("@type") != "LocalBusiness":
                    continue
                name = (data.get("name") or "").strip()
                if name:
                    item[Schema.NAME] = name
                tel = (data.get("telephone") or "").strip()
                if tel:
                    item[Schema.TEL] = tel
                addr = data.get("address") or {}
                if isinstance(addr, dict):
                    pref = (addr.get("addressRegion") or "").strip()
                    if pref:
                        item[Schema.PREF] = pref
                    street = (addr.get("streetAddress") or "").strip()
                    if street:
                        item[Schema.ADDR] = street
                return

    def _parse_detail_tables(self, soup, item: dict) -> None:
        """詳細ページのテーブル行から ホームページ / 住所(フォールバック) を抽出"""
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                th = row.find("th")
                td = row.find("td")
                if not th or not td:
                    continue
                label = th.get_text(strip=True)

                if label == "ホームページ" and Schema.HP not in item:
                    a = td.find("a", href=True)
                    href = (a["href"] if a else td.get_text(strip=True)).strip()
                    if href and href.startswith(("http://", "https://")):
                        item[Schema.HP] = href

                elif label == "住所" and Schema.ADDR not in item:
                    text = td.get_text(" ", strip=True)
                    if text:
                        m = PREF_RE.match(text)
                        if m:
                            item.setdefault(Schema.PREF, m.group(1))
                            item[Schema.ADDR] = text[len(m.group(1)):].strip()
                        else:
                            item[Schema.ADDR] = text

                elif label == "営業時間" and "営業時間" not in item:
                    # 曜日と時間が <div class="d-flex"> の子divに分かれて並ぶ
                    parts: list[str] = []
                    for flex in td.find_all("div", class_="d-flex"):
                        children = flex.find_all("div", recursive=False)
                        if len(children) >= 2:
                            day = children[0].get_text(strip=True)
                            hours = children[1].get_text(strip=True)
                            if day and hours:
                                parts.append(f"{day}: {hours}")
                    if parts:
                        item["営業時間"] = " / ".join(parts)

    def _parse_sns(self, soup, item: dict) -> None:
        """SNS 行内の店舗専用SNSリンクのみを抽出（フッタの公式アカウントは拾わない）"""
        for row in soup.find_all("tr"):
            th = row.find("th")
            if not th or th.get_text(strip=True) != "SNS":
                continue
            td = row.find("td")
            if not td:
                continue
            for a in td.find_all("a", href=True):
                img = a.find("img")
                alt = img.get("alt", "").strip() if img else ""
                schema_key = SNS_MAP.get(alt)
                if schema_key and schema_key not in item:
                    item[schema_key] = a["href"].strip()
            return


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # 引数: [area_slug] [page_start] [page_end]
    #   python con_cafe.py            # 全エリア
    #   python con_cafe.py area01     # area01 全ページ
    #   python con_cafe.py area02 1 17  # area02 ページ1〜17のみ
    target = sys.argv[1] if len(sys.argv) > 1 else "all"

    scraper = ConCafeScraper()
    if len(sys.argv) > 2:
        scraper.page_start = int(sys.argv[2])
    if len(sys.argv) > 3:
        scraper.page_end = int(sys.argv[3])
    scraper.execute(target)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
