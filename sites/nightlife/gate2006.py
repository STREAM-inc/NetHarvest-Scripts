"""
ゲート (gate2006.jp) — 広島県のナイトビジネス情報サイト スクレイパー

取得対象:
    広島県のキャバクラ・スナック・ラウンジ・ガールズバー等の掲載店舗一覧

取得フロー:
    1. ショップリスト (/pc/shoplist/?pageID=N) を巡回し店舗カードを収集
       - リストの並び順はリクエストごとにランダムに変わるため、shop_id で
         重複除外しつつ、掲載総件数 (「NN件」表示) に到達するまでページングする
    2. 各店舗の詳細ページ (/pc/shop/?shop_id=N) を取得し、
       ショップデータ table / 料金 table / Instagram リンク等を抽出
       - 詳細を1件取得するごとに即 yield する (Pattern B)

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/gate2006.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id gate2006
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_TOTAL_PATTERN = re.compile(r"(\d+)\s*件")
_SHOP_ID_PATTERN = re.compile(r"shop_id=(\d+)")


class Gate2006Scraper(StaticCrawler):
    """ゲート (広島県ナイトビジネス情報サイト) スクレイパー"""

    DELAY = 1.5

    # 掲載店舗が異常に多い場合の暴走防止 (通常は総件数到達で早期終了)
    MAX_PAGES = 50
    # 新規 shop_id が連続で得られないページ数の上限 (終了条件)
    MAX_EMPTY_PAGES = 3

    EXTRA_COLUMNS = [
        "アクセス",     # 最寄り駅・電停からの案内
    ]

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _text(el) -> str:
        return el.get_text(" ", strip=True) if el else ""

    def _parse_total(self, soup) -> int | None:
        """「NN件」表示から掲載総件数を取得する。"""
        m = _TOTAL_PATTERN.search(soup.get_text(" ", strip=True))
        return int(m.group(1)) if m else None

    def _parse_card(self, card) -> dict | None:
        """ショップリストの1カードから shop_id / 名称 / ジャンルを取得。"""
        h4a = card.select_one("h4 a[href*='shop_id=']")
        if not h4a:
            return None
        m = _SHOP_ID_PATTERN.search(h4a.get("href", ""))
        if not m:
            return None

        genre = ""
        ag = card.select("ul.s_areagenre li")
        if len(ag) >= 2:
            genre = self._text(ag[1])

        return {
            "shop_id": m.group(1),
            "name": self._text(h4a),
            "genre": genre,
        }

    def _table_dict(self, soup, heading: str) -> dict:
        """h3 見出し直後の table を {ラベル: 値} 辞書にして返す。

        このサイトの table は 1 行に th/td ペアが 2 組並ぶため、
        セルを2つずつ組にして辞書化する。
        """
        table = None
        for h3 in soup.find_all("h3"):
            if heading in h3.get_text():
                table = h3.find_next("table")
                break
        if table is None:
            return {}

        data: dict[str, str] = {}
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"])
            for i in range(0, len(cells) - 1, 2):
                key = self._text(cells[i])
                val = self._text(cells[i + 1])
                if key and key not in data:
                    data[key] = val
        return data

    # -------------------------------------------------------------------- parse
    def parse(self, url: str):
        list_base = urljoin(url, "pc/shoplist/")

        seen: set[str] = set()
        total: int | None = None
        empty_pages = 0
        page = 1

        while page <= self.MAX_PAGES:
            list_url = f"{list_base}?pageID={page}"
            soup = self.get_soup(list_url)
            if soup is None:
                self.logger.warning("ショップリスト取得失敗: %s", list_url)
                break

            if total is None:
                total = self._parse_total(soup)
                if total:
                    self.total_items = total
                    self.logger.info("掲載総件数: %d 件", total)

            cards = soup.select("li:has(h4 a[href*='shop_id='])")
            if not cards:
                break

            new_in_page = 0
            for card in cards:
                info = self._parse_card(card)
                if not info or info["shop_id"] in seen:
                    continue
                seen.add(info["shop_id"])
                new_in_page += 1

                try:
                    item = self._scrape_detail(url, info)
                except Exception as e:
                    self.logger.warning("詳細取得失敗 shop_id=%s: %s", info["shop_id"], e)
                    continue
                if item:
                    yield item

            # 終了条件: 全件収集済み / 新規が連続で得られない
            if total and len(seen) >= total:
                break
            if new_in_page == 0:
                empty_pages += 1
                if empty_pages >= self.MAX_EMPTY_PAGES:
                    break
            else:
                empty_pages = 0

            page += 1

    def _scrape_detail(self, root_url: str, info: dict) -> dict | None:
        detail_url = urljoin(root_url, f"pc/shop/?shop_id={info['shop_id']}")
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        data = self._table_dict(soup, "ショップデータ")
        price = self._table_dict(soup, "料金")

        name = data.get("店舗名") or info["name"]
        address = re.sub(r"^\s*広島県", "", data.get("住所", "")).strip()
        credit = price.get("クレジットカード") or data.get("クレジットカード", "")

        ig = soup.select_one("a[href*='instagram.com']")

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: "広島県",
            Schema.ADDR: address,
            Schema.TEL: data.get("電話", ""),
            Schema.CAT_SITE: info["genre"],
            Schema.TIME: data.get("営業時間", ""),
            Schema.HOLIDAY: data.get("定休日", ""),
            Schema.PAYMENTS: credit,
            Schema.INSTA: ig["href"] if ig and ig.get("href") else "",
            "アクセス": data.get("アクセス", ""),
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Gate2006Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://gate2006.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
