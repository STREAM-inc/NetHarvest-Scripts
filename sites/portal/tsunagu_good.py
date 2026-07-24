"""
つなぐっど (tsunagu-good.com) — あなたの街の情報サイト

取得対象:
    - 全国の掲載店舗 (飲食店・美容院・整骨院など、インタビュー形式の街情報ポータル)
    - 店舗基本情報 (店名・連絡先・住所・営業時間・定休日・支払い方法・各種SNS/HP)

取得フロー:
    /shop-search (一覧, 12件/ページ, 全855ページ・約10,253件) を
    /shop-search/page/{N} で巡回し、各カード (a.parts_shop_item) の
    詳細ページ /archives/{id} を 1 件ずつ取得して即 yield する (Pattern B)。
    詳細の基本情報は td(ラベル)+td(値) の 2 列テーブル。ラベルは表記揺れあり
    (例: ご連絡先 / 電話番号) のためキーワードで照合する。

除外:
    - 一覧/詳細の長文紹介文 (.desc, インタビュー本文) は自由記述プロースのため
      著作権リスクを考慮して取得しない。

実行方法:
    python scripts/sites/portal/tsunagu_good.py
    docker compose exec worker python /app/bin/run_flow.py --site-id tsunagu_good
"""

import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_PATTERN = re.compile(r"〒?\s*(\d{3}[-－]\d{4})")
_TEL_PATTERN = re.compile(r"0\d{1,4}[-\(－]?\d{1,4}[-\)－]?\d{3,4}")


class TsunaguGood(StaticCrawler):
    """つなぐっど スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["予約可否", "Googleビジネス", "その他リンク"]

    def parse(self, url: str):
        page = 1
        while True:
            page_url = url if page == 1 else f"{url}/page/{page}"
            soup = self.get_soup(page_url)

            # 総件数を初回に設定 (例: "1 / 855（全10253件中1〜12件）")
            if page == 1:
                pager = soup.select_one('[class*="pager"], [class*="page-nav"]')
                if pager:
                    m = re.search(r"全\s*([\d,]+)\s*件", pager.get_text())
                    if m:
                        self.total_items = int(m.group(1).replace(",", ""))

            cards = soup.select("a.parts_shop_item")
            if not cards:
                break

            for card in cards:
                detail_url = card.get("href")
                if not detail_url:
                    continue
                # 一覧カードで取れる情報 (名称・サイト定義ジャンル・エリア)
                name_el = card.select_one(".name")
                type_el = card.select_one(".type")
                area_el = card.select_one(".area")
                list_ctx = {
                    "name": name_el.get_text(strip=True) if name_el else "",
                    "cat": type_el.get_text(strip=True) if type_el else "",
                    "area": area_el.get_text(strip=True) if area_el else "",
                }
                try:
                    item = self._scrape_detail(detail_url, list_ctx)
                except Exception as e:
                    self.logger.warning("詳細ページ取得失敗 %s: %s", detail_url, e)
                    continue
                if item:
                    yield item

            page += 1

    def _scrape_detail(self, url: str, list_ctx: dict) -> dict | None:
        soup = self.get_soup(url)

        item = {
            Schema.URL: url,
            Schema.NAME: list_ctx["name"],
            Schema.CAT_SITE: list_ctx["cat"],
        }

        # 基本情報テーブル (td ラベル + td 値)。ギャラリー等を除外し、
        # 「店名/住所」等のラベルを含むテーブルを採用する。
        info_table = None
        for tbl in soup.select("table"):
            if "gallery-item" in (tbl.get("class") or []):
                continue
            txt = tbl.get_text()
            if "住所" in txt or "店名" in txt:
                info_table = tbl
        if info_table is None:
            # テーブルが無くても一覧情報だけで返す
            self._apply_area(item, list_ctx["area"])
            return item

        rows = {}  # ラベル -> 値td (BeautifulSoup 要素)
        for tr in info_table.select("tr"):
            tds = tr.find_all("td", recursive=False)
            if len(tds) >= 2:
                label = tds[0].get_text(strip=True)
                if label:
                    rows[label] = tds[1]

        # 全 <a> を集約してリンク種別で振り分ける
        google_links, other_links = [], []
        for td in rows.values():
            for a in td.select("a[href]"):
                href = a.get("href", "").strip()
                if not href or href.startswith("#") or href.startswith("mailto:"):
                    continue
                low = href.lower()
                if "instagram.com" in low:
                    item.setdefault(Schema.INSTA, href)
                elif "line.me" in low:
                    item.setdefault(Schema.LINE, href)
                elif "x.com" in low or "twitter.com" in low:
                    item.setdefault(Schema.X, href)
                elif "facebook.com" in low:
                    item.setdefault(Schema.FB, href)
                elif "tiktok.com" in low:
                    item.setdefault(Schema.TIKTOK, href)
                elif re.search(r"(maps\.app\.goo\.gl|goo\.gl/maps|google\.[^/]+/maps)", low):
                    google_links.append(href)
                else:
                    other_links.append((a.get_text(strip=True), href))

        def _val(*keywords):
            for label, td in rows.items():
                if any(kw in label for kw in keywords):
                    return self._text(td)
            return ""

        # 店名: テーブル優先、無ければ一覧カード名
        tbl_name = _val("店名")
        if tbl_name:
            item[Schema.NAME] = tbl_name

        # 連絡先 / 電話番号 -> TEL (数字のみ、"―" 等は除外)
        tel_raw = _val("連絡先", "電話")
        m = _TEL_PATTERN.search(tel_raw)
        if m:
            item[Schema.TEL] = m.group(0)

        # 住所
        self._apply_address(item, _val("住所"), list_ctx["area"])

        item[Schema.TIME] = _val("営業時間")
        item[Schema.HOLIDAY] = _val("定休日")
        item[Schema.PAYMENTS] = _val("支払")

        # HP: HP 行のリンク先頭 (Google マップ以外) を採用
        hp = ""
        for label, td in rows.items():
            if "HP" in label or "ホームページ" in label:
                for a in td.select("a[href]"):
                    href = a.get("href", "").strip()
                    low = href.lower()
                    if not href or "instagram.com" in low or "line.me" in low:
                        continue
                    if re.search(r"(maps\.app\.goo\.gl|goo\.gl/maps|google\.[^/]+/maps)", low):
                        continue
                    hp = href
                    break
                break
        if hp:
            item[Schema.HP] = hp

        # EXTRA
        item["予約可否"] = _val("予約")
        item["Googleビジネス"] = " | ".join(dict.fromkeys(google_links))
        # HP に採用したリンクは除外して残りを URL のみで格納
        item["その他リンク"] = " | ".join(
            dict.fromkeys(href for _, href in other_links if href != hp)
        )

        return item

    def _apply_address(self, item: dict, addr_text: str, area: str):
        if not addr_text:
            self._apply_area(item, area)
            return
        text = re.sub(r"\s+", " ", addr_text).strip()
        pm = _POST_PATTERN.search(text)
        if pm:
            item[Schema.POST_CODE] = pm.group(1).replace("－", "-")
            text = _POST_PATTERN.sub("", text).strip(" 　")
        prm = _PREF_PATTERN.search(text)
        if prm:
            item[Schema.PREF] = prm.group(1)
        elif area:
            item[Schema.PREF] = area
        item[Schema.ADDR] = text.strip()

    def _apply_area(self, item: dict, area: str):
        if area and not item.get(Schema.PREF):
            item[Schema.PREF] = area

    @staticmethod
    def _text(td) -> str:
        """<br> を改行に、余分な空白を整理して 1 行のテキストにする。"""
        for br in td.select("br"):
            br.replace_with("\n")
        parts = [p.strip() for p in td.get_text("\n").split("\n")]
        return " ".join(p for p in parts if p)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = TsunaguGood()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://tsunagu-good.com/shop-search")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
