"""
Panasonic_cycle (パナソニック サイクル 販売店検索) — 販売店情報スクレイパー

取得対象:
    - パナソニック サイクルテックの全国正規販売店ネットワーク
    - 店舗名 / 住所 / 電話 / HP / 定休日 / 営業時間 / 取扱サービス種別フラグ

取得フロー:
    /shoplist/json/shoplist.json (単一 JSON) を取得
      → 各店舗レコードを Schema + EXTRA_COLUMNS にマッピング
      ※ フロント側は都道府県/郵便番号/キーワードで絞り込んで表示するが、
        マスターデータ自体は全国分が 1 ファイルに集約されている

実行方法:
    python scripts/sites/agency_franchise/panasonic_cycle.py
    python bin/run_flow.py --site-id panasonic_cycle
"""

import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://cycle.panasonic.com/"
JSON_URL = "https://cycle.panasonic.com/shoplist/json/shoplist.json"

COL_LAT = "緯度"
COL_LON = "経度"
COL_EB_DEALER = "電動アシスト自転車_取扱店"
COL_EB_TESTRIDE = "電動アシスト自転車_試乗店"
COL_EB_SUPPORT = "電動アシスト自転車_修理サポート店"
COL_XEALT = "XEALT展示店"
COL_POS = "POS取扱店"
COL_MU_DEALER = "MU取扱店"
COL_MU_TESTRIDE = "MU試乗店"


class PanasonicCycleScraper(StaticCrawler):
    """Panasonic_cycle 販売店検索スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        COL_LAT,
        COL_LON,
        COL_EB_DEALER,
        COL_EB_TESTRIDE,
        COL_EB_SUPPORT,
        COL_XEALT,
        COL_POS,
        COL_MU_DEALER,
        COL_MU_TESTRIDE,
    ]

    def parse(self, url: str):
        response = self.session.get(JSON_URL, timeout=self.TIMEOUT)
        response.raise_for_status()
        shops = response.json()

        self.total_items = len(shops)
        self.logger.info("shoplist.json 取得完了: %d 件", len(shops))

        for shop in shops:
            try:
                item = self._build_item(shop)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("店舗変換失敗: %s (%s)", shop.get("HBIFNAMA"), e)
                continue

    def _build_item(self, shop: dict) -> dict | None:
        name = _clean(shop.get("HBIFNAMA"))
        if not name:
            return None

        item: dict = {
            Schema.URL: BASE_URL,
            Schema.NAME: name,
        }

        pref = _clean(shop.get("HBIFADRA"))
        if pref:
            item[Schema.PREF] = pref

        addr_parts = [_clean(shop.get("HBIFADRB")), _clean(shop.get("HBIFADRC"))]
        addr = "".join(p for p in addr_parts if p)
        if addr:
            item[Schema.ADDR] = addr

        post_code = _clean(shop.get("HBIFADNO"))
        if post_code:
            item[Schema.POST_CODE] = post_code

        tel = _clean(shop.get("HBIFTELN"))
        if tel:
            item[Schema.TEL] = tel

        hp = _clean(shop.get("HBIFHURL"))
        if hp and hp.startswith(("http://", "https://")):
            item[Schema.HP] = hp

        holiday = _clean(shop.get("HBIFHLDY"))
        if holiday:
            item[Schema.HOLIDAY] = holiday

        opening = _clean(shop.get("HBIFOPNT"))
        if opening:
            item[Schema.TIME] = opening

        lat = shop.get("HBIFPIDO")
        lon = shop.get("HBIFPKDO")
        if lat not in (None, ""):
            item[COL_LAT] = str(lat)
        if lon not in (None, ""):
            item[COL_LON] = str(lon)

        item[COL_EB_DEALER] = _flag(shop.get("HBIFASTK"))
        item[COL_EB_TESTRIDE] = _flag(shop.get("HBIFASSK"))
        item[COL_EB_SUPPORT] = _flag(shop.get("HBIFRPSK"))
        item[COL_XEALT] = _flag(shop.get("HBIFXM1K"))
        item[COL_POS] = _flag(shop.get("HBIFPOSK"))
        item[COL_MU_DEALER] = _flag(shop.get("HBIFMUTK"))
        item[COL_MU_TESTRIDE] = _flag(shop.get("HBIFMUSK"))

        return item


def _clean(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _flag(value) -> str:
    v = _clean(value)
    if v == "1":
        return "1"
    if v == "0":
        return "0"
    return ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = PanasonicCycleScraper()
    scraper.execute(BASE_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
