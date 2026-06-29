"""
片町ナイトNAVI (k-navi.happyjob.jp) — 金沢・片町のクラブ/キャバクラ/ラウンジ/バー店舗情報スクレイパー

取得対象:
    - 掲載店舗一覧 (全 29 件前後 / 金沢・片町エリア中心)
    - 店舗名 / カナ / 都道府県 / 住所 / サイト定義業種(バー/キャバクラ/クラブ/ラウンジ等) /
      営業時間 / 定休日
    - サイト固有: エリア / 席数 / 予算目安 / 求人情報 / 詳細リンク

取得フロー:
    1. 引数 url (https://k-navi.happyjob.jp/) を起点に一覧ページ /shop/ を派生
    2. /shop/ は全店舗を 1 ページに掲載 (ページネーション・JS レンダリング無し) なので
       静的取得した HTML から .shop-col__list_col 各ブロックをパース
    3. 1 件パースするごとに即 yield
    ※ 個別の店舗詳細ページ (/shop/{slug}/) はサーバ側リダイレクトで本文が空 (JS リダイレクト) の
      ため使用不可。住所/業種/営業時間/定休日/席数/予算/求人 すべて一覧ブロックに含まれる。
    ※ 店舗キャッチコピー (.copy) は運営者による自由記述プロースのため著作権リスクで取得対象外。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/navi_4.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id navi_4
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import Tag

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# 都道府県抽出 (このサイトは金沢=石川県限定。住所に都道府県が無い場合は石川県を既定値とする)
_PREF_PATTERN = re.compile(
    r"(北海道|東京都|京都府|大阪府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
# 店名末尾の【エリア 業種】ブロック (例: 【片町 バー】【ラウンジ】)
_BRACKET_PATTERN = re.compile(r"【([^】]*)】")
_DEFAULT_PREF = "石川県"


class KatamachiNightNavi(StaticCrawler):
    """片町ナイトNAVI スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["エリア", "席数", "予算目安", "求人情報", "詳細リンク"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        list_url = urljoin(url if url.endswith("/") else url + "/", "shop/")
        soup = self.get_soup(list_url)

        items = soup.select(".shop-col__list_col")
        self.total_items = len(items)

        for item in items:
            try:
                record = self._parse_item(item, list_url)
                if record:
                    yield record
            except Exception as exc:  # 個別ブロックの失敗は握りつぶして継続
                self.logger.warning("店舗ブロックのパースに失敗: %s", exc)
                continue

    def _parse_item(self, item: Tag, list_url: str) -> dict | None:
        raw_name = self._text(item, ".name h3")
        if not raw_name:
            return None

        # 店名から【エリア 業種】を分離。業種はブロック内の最後のトークン (例:「片町 バー」→バー)
        bracket_m = _BRACKET_PATTERN.search(raw_name)
        cat_site = ""
        if bracket_m:
            tokens = bracket_m.group(1).split()
            cat_site = tokens[-1] if tokens else ""
        name = _BRACKET_PATTERN.sub("", raw_name).strip()

        kana = self._text(item, ".kana")
        area = self._text(item, ".area")

        addr = self._strip_label(self._text(item, ".adr"))
        pref = _DEFAULT_PREF
        pref_m = _PREF_PATTERN.search(addr)
        if pref_m:
            pref = pref_m.group(1)

        seat = self._strip_label(self._text(item, ".seat"))
        business_hours = self._info_value(item, ".info .open")
        holiday = self._info_value(item, ".info .close")
        budget = self._info_value(item, ".info .budget")
        recruit = self._info_value(item, ".info .recruit")

        link_el = item.find("a", href=True)
        detail_url = urljoin(list_url, link_el["href"]) if link_el else ""

        return {
            Schema.NAME: name,
            Schema.NAME_KANA: kana,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.CAT_SITE: cat_site,
            Schema.TIME: business_hours,
            Schema.HOLIDAY: holiday,
            Schema.URL: list_url,
            "エリア": area,
            "席数": seat,
            "予算目安": budget,
            "求人情報": recruit,
            "詳細リンク": detail_url,
        }

    @staticmethod
    def _text(parent: Tag, selector: str) -> str:
        el = parent.select_one(selector)
        return el.get_text(" ", strip=True) if el else ""

    @staticmethod
    def _strip_label(text: str) -> str:
        """先頭の「住所｜」「席数｜」等のラベルを除去"""
        return re.sub(r"^[^｜|:：]{0,6}[｜|:：]\s*", "", text).strip()

    @staticmethod
    def _info_value(parent: Tag, selector: str) -> str:
        """<div><span>ラベル</span>値</div> 形式から span を除いた値を返す"""
        el = parent.select_one(selector)
        if el is None:
            return ""
        label_el = el.find("span")
        label = label_el.get_text(strip=True) if label_el else ""
        value = el.get_text(" ", strip=True)
        if label and value.startswith(label):
            value = value[len(label):]
        return value.strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = KatamachiNightNavi()
    # 🔒 この URL は sites.yml に登録する url と完全一致 (SSOT = sites.yml)
    scraper.execute("https://k-navi.happyjob.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
