# scripts/sites/portal/epark_hotel.py
"""
EPARK ホテル・宿泊 — 宿泊施設情報 全件回収スクレイパー

取得フロー:
    1. 一覧ページから :shops JSONを抽出し、各施設の基本情報と詳細URLを取得。
    2. 各詳細ページへアクセスし、BeautifulSoupで詳細データ（TEL、住所、時間等）を回収。
    3. 次のページへ遷移し、全件終了まで繰り返す。
"""

import html
import json
import re
import sys
import time
from pathlib import Path
from typing import Generator
from bs4 import BeautifulSoup

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

LIST_URL = "https://epark.jp/hotel-stay/list/"
DETAIL_URL = "https://epark.jp/shopinfo/{prefix}{shop_id}/"


def _clean(s) -> str:
    """文字列の改行や余分な空白を正規化する"""
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def _extract_vue_attr(content: str, attr: str) -> dict | None:
    """HTML内の Vue prop 属性からJSONデータを抽出する"""
    marker = f'{attr}="{{'
    idx = content.find(marker)
    if idx < 0:
        return None
    start = idx + len(marker) - 1
    end = content.find('}"', start)
    if end < 0:
        return None
    raw = content[start : end + 1]
    try:
        return json.loads(html.unescape(raw))
    except (json.JSONDecodeError, ValueError):
        return None


class EparkHotelScraper(StaticCrawler):
    """EPARK ホテル・宿泊 全件回収用スクレイパー"""

    # サーバー負荷とタイムアウト対策のため遅延を長めに設定
    DELAY = 3.0
    # タイムアウト設定（フレームワーク側で上書き可能な場合を考慮）
    TIMEOUT = 30

    EXTRA_COLUMNS = ["アクセス", "条件", "楽天トラベルID"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        page = 1
        last_page = None

        while True:
            self.logger.info(f"一覧ページ {page} を解析中...")
            try:
                resp = self.session.get(f"{url}?page={page}", timeout=self.TIMEOUT)
                resp.raise_for_status()
            except Exception as e:
                self.logger.error(f"一覧ページ {page} の取得でエラーが発生したためスキップする: {e}")
                time.sleep(self.DELAY)
                page += 1
                continue

            shops_data = _extract_vue_attr(resp.text, ":shops")
            if not shops_data or not shops_data.get("data"):
                self.logger.warning(f"ページ {page} のデータ取得に失敗したか、全件終了した。")
                break

            # 初回ページで総件数と最終ページ数を取得
            if page == 1:
                last_page = shops_data.get("last_page", 1)
                self.total_items = shops_data.get("total", 0)
                self.logger.info(f"総件数: {self.total_items} 件 / 総ページ: {last_page}")

            for list_item in shops_data["data"]:
                shop_id = list_item.get("shop_id")
                prefix = list_item.get("media_prefix", "")
                if not shop_id or not prefix:
                    continue

                detail_url = DETAIL_URL.format(prefix=prefix, shop_id=shop_id)

                try:
                    # 詳細ページ情報を統合
                    record = self._scrape_detail(detail_url, list_item)
                    if record:
                        yield record
                except Exception as e:
                    self.logger.warning(f"詳細ページ取得エラー ({detail_url})、スキップする: {e}")

                # ループごとのアクセス負荷軽減処理
                time.sleep(self.DELAY)

            # 最終ページに達したら終了
            if last_page is None or page >= last_page:
                self.logger.info("全ページの回収が完了した。")
                break

            page += 1

    def _scrape_detail(self, url: str, list_data: dict) -> dict | None:
        """詳細ページを解析し、一覧データと結合する"""
        resp = self.session.get(url, timeout=self.TIMEOUT)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        # 一覧JSONから取得済みの項目
        name = _clean(list_data.get("name"))
        pref = _clean(list_data.get("area_level1_name", ""))
        kana = _clean(list_data.get("kana"))
        cat_lv1 = _clean(list_data.get("genre_name"))
        access = _clean(list_data.get("access") or list_data.get("station"))
        conditions = _clean(list_data.get("conditions"))
        rakuten_id = str(list_data.get("media_shop_id", ""))

        if not name:
            return None

        # 詳細ページのDOMから追加項目を抽出
        addr_body = ""
        tel = ""
        check_time = ""

        basic_table = soup.select_one(".shopBasicInformation__table")
        if basic_table:
            for row in basic_table.select("tr.shopBasicInformation__row"):
                th = row.select_one("th")
                td = row.select_one("td")
                if not th or not td:
                    continue

                header = _clean(th.get_text())

                if header == "住所":
                    pc_map = td.select_one(".staticMap .pc")
                    if pc_map:
                        # 不要なナビゲーションリンクを削除
                        for a in pc_map.select("a"):
                            a.decompose()
                        full_addr = _clean(pc_map.get_text().replace("()", "").replace("( )", ""))
                        # 都道府県を分離して保存
                        if pref and full_addr.startswith(pref):
                            addr_body = full_addr[len(pref) :].strip()
                        else:
                            addr_body = full_addr

                elif header == "電話":
                    tel = _clean(td.get_text())

                elif header == "チェックイン・チェックアウト":
                    check_time = _clean(td.get_text(separator=" "))

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.NAME_KANA: kana,
            Schema.PREF: pref,
            Schema.ADDR: addr_body,
            Schema.TEL: tel,
            Schema.TIME: check_time,
            Schema.CAT_LV1: cat_lv1,
            "アクセス": access,
            "条件": conditions,
            "楽天トラベルID": rakuten_id,
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = EparkHotelScraper()
    scraper.execute(LIST_URL)

    print(f"\n回収終了")
    print(f"出力ファイル: {scraper.output_filepath}")
    print(f"最終取得件数: {scraper.item_count}")
