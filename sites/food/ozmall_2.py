"""
OZmall【レストラン】 — レストラン情報スクレイパー

取得対象:
    - OZmall の全国レストラン一覧から各店舗の基本情報・詳細情報
    - エントリ: https://www.ozmall.co.jp/restaurant/list/?pageNo={N}  (約 20 件/ページ × 215 ページ)

取得フロー:
    一覧ページ (/restaurant/list/?pageNo=N) を順にクロール
      → 各 .resultlist-box から店舗ID・名前・エリア・ジャンル・評点を取得
      → 詳細ページ (/restaurant/{id}/) を取得して店舗情報テーブル (table.restaurant__data--box) から
         住所・TEL・営業時間・定休日・支払方法などを抽出

実行方法:
    python scripts/sites/food/ozmall_2.py
    python bin/run_flow.py --site-id ozmall_2
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_BASE_URL = "https://www.ozmall.co.jp"
_LIST_URL_TEMPLATE = "https://www.ozmall.co.jp/restaurant/list/?pageNo={page}"
_SHOP_PATH_PATTERN = re.compile(r"^/restaurant/(\d+)/$")
# "店名 （フリガナ）" / "店名 (フリガナ)" を分離
_NAME_KANA_PATTERN = re.compile(r"^(.+?)\s*[（(]([^（()）]+)[）)]\s*$")
# TEL 末尾の注記「※OZmallのご予約は…」を除去するためのパターン
_TEL_NOTE_SPLIT = re.compile(r"[※*]")


class Ozmall2Scraper(StaticCrawler):
    """OZmall【レストラン】 スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "名称_フリガナ",
        "キャッチコピー",
        "紹介文",
        "アクセス",
        "付近の駅",
        "サービス料",
        "総席数",
        "ドレスコード",
        "お子様",
        "車椅子",
        "駐車場",
        "たばこ",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        shop_entries = self._collect_shop_entries(url)
        self.total_items = len(shop_entries)
        self.logger.info("店舗URL収集完了: %d 件", len(shop_entries))

        for entry in shop_entries:
            try:
                item = self._scrape_detail(entry)
            except Exception as e:
                self.logger.warning("詳細ページ解析エラー (%s): %s", entry.get("url"), e)
                continue
            if item:
                yield item

    def _collect_shop_entries(self, entry_url: str) -> list[dict]:
        """全ページを巡回し、一覧ページから取得できる情報と詳細ページURLを収集。"""
        seen_ids: set[str] = set()
        entries: list[dict] = []
        page = 1
        while True:
            page_url = _LIST_URL_TEMPLATE.format(page=page)
            soup = self.get_soup(page_url)
            if soup is None:
                self.logger.warning("一覧ページ取得失敗: %s", page_url)
                break

            boxes = soup.select(".resultlist-box")
            if not boxes:
                self.logger.info("これ以上の一覧アイテムが見つからないため終了: page=%d", page)
                break

            new_in_page = 0
            for box in boxes:
                a = box.select_one('a[href^="/restaurant/"]')
                if not a:
                    continue
                href = (a.get("href") or "").strip()
                m = _SHOP_PATH_PATTERN.match(href)
                if not m:
                    continue
                shop_id = m.group(1)
                if shop_id in seen_ids:
                    continue
                seen_ids.add(shop_id)
                new_in_page += 1

                name_el = box.select_one("h3.shop-name")
                address_el = box.select_one("p.address")
                category_el = box.select_one("p.category")
                score_el = box.select_one(".rate__score")
                catch_el = box.select_one(".shop-catch")
                txt_el = box.select_one(".shop-txt")

                entries.append({
                    "shop_id": shop_id,
                    "url": urljoin(_BASE_URL, href),
                    "list_name": name_el.get_text(strip=True) if name_el else "",
                    "list_area": address_el.get_text(" ", strip=True) if address_el else "",
                    "list_category": category_el.get_text(" ", strip=True) if category_el else "",
                    "list_score": score_el.get_text(strip=True) if score_el else "",
                    "list_catch": catch_el.get_text(" ", strip=True) if catch_el else "",
                    "list_txt": txt_el.get_text(" ", strip=True) if txt_el else "",
                })

            if new_in_page == 0:
                self.logger.info("新規アイテムなしのため終了: page=%d", page)
                break
            page += 1

        return entries

    def _scrape_detail(self, entry: dict) -> dict | None:
        soup = self.get_soup(entry["url"])
        if soup is None:
            return None

        data: dict = {Schema.URL: entry["url"]}

        # 詳細の th-td ペアを収集 (restaurant__data--box)
        detail_rows: dict[str, str] = {}
        for table in soup.select("table.restaurant__data--box"):
            for tr in table.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                header = th.get_text(" ", strip=True)
                value = td.get_text(" ", strip=True)
                if header and value and header not in detail_rows:
                    detail_rows[header] = value

        # 店名 / フリガナ
        shop_name_raw = detail_rows.get("店名") or entry.get("list_name", "")
        if shop_name_raw:
            m = _NAME_KANA_PATTERN.match(shop_name_raw)
            if m:
                data[Schema.NAME] = m.group(1).strip()
                data["名称_フリガナ"] = m.group(2).strip()
            else:
                data[Schema.NAME] = shop_name_raw.strip()

        if not data.get(Schema.NAME):
            return None

        # 住所 → ADDR + PREF
        address = detail_rows.get("住所", "").strip()
        if address:
            pref_match = _PREF_PATTERN.match(address)
            if pref_match:
                data[Schema.PREF] = pref_match.group(1)
                data[Schema.ADDR] = address[pref_match.end():].strip()
            else:
                data[Schema.ADDR] = address

        # TEL (注記を除去)
        tel_raw = detail_rows.get("TEL", "").strip()
        if tel_raw:
            tel_clean = _TEL_NOTE_SPLIT.split(tel_raw, maxsplit=1)[0].strip()
            if tel_clean:
                data[Schema.TEL] = tel_clean

        # 営業時間・定休日・支払方法
        if detail_rows.get("営業時間"):
            data[Schema.TIME] = detail_rows["営業時間"]
        if detail_rows.get("定休日"):
            data[Schema.HOLIDAY] = detail_rows["定休日"]
        if detail_rows.get("支払方法"):
            data[Schema.PAYMENTS] = detail_rows["支払方法"]

        # EXTRA: 詳細テーブルから直接コピーするフィールド
        extra_map = {
            "アクセス": "アクセス",
            "付近の駅": "付近の駅",
            "サービス料": "サービス料",
            "総席数": "総席数",
            "ドレスコード": "ドレスコード",
            "車椅子": "車椅子",
            "駐車場": "駐車場",
            "たばこ": "たばこ",
        }
        for src_key, dst_key in extra_map.items():
            if detail_rows.get(src_key):
                data[dst_key] = detail_rows[src_key]

        # 「お子様」は th に「※最新情報は店舗へご確認ください」が付くケースがあるので
        # 部分一致で探す。
        for header, value in detail_rows.items():
            if header.startswith("お子様") and value:
                data["お子様"] = value
                break

        # 一覧ページ由来のカラム
        if entry.get("list_category"):
            data[Schema.CAT_SITE] = entry["list_category"]
        if entry.get("list_score"):
            data[Schema.SCORES] = entry["list_score"]
        if entry.get("list_catch"):
            data["キャッチコピー"] = entry["list_catch"]
        if entry.get("list_txt"):
            data["紹介文"] = entry["list_txt"]

        return data


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ozmall2Scraper()
    scraper.execute("https://www.ozmall.co.jp/restaurant/list/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
