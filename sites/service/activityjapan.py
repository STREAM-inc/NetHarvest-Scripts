"""
アクティビティジャパン (activityjapan.com) — 提携事業者スクレイパー

取得対象:
    全国の体験・レジャー事業者（アクティビティジャパン提携事業者一覧）

取得フロー:
    1. エリア別一覧ページ (?areaName={area}) を11エリア順に巡回し、
       事業者ID (/publish/feature/{id}) を重複除外しながら収集
    2. 各事業者詳細ページから情報を取得
       - h1 / 基本情報テーブル (.baseinfo) / カテゴリ情報 (.category--item) / 評価

実行方法:
    # ローカルテスト (テスト時は _max_ids を設定)
    python scripts/sites/service/activityjapan.py

    # Prefect Flow 経由 (全件)
    python bin/run_flow.py --site-id activityjapan
"""

import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://activityjapan.com"
LIST_URL = "https://activityjapan.com/publish/feature"

AREAS = [
    "hokkaido", "tohoku", "kanto", "koushinetsu", "hokuriku",
    "tokai", "kansai", "sanin-sanyo", "shikoku", "kyusyu", "okinawa",
]

_PREF_PATTERN = re.compile(
    r"(北海道|(?:東京|大阪|京都|神奈川|愛知|兵庫|福岡|埼玉|千葉"
    r"|静岡|広島|宮城|茨城|新潟|栃木|群馬|長野|岐阜|福島|三重"
    r"|熊本|鹿児島|岡山|山口|愛媛|長崎|滋賀|奈良|沖縄|青森|岩手"
    r"|秋田|山形|富山|石川|福井|山梨|和歌山|鳥取|島根|香川|高知"
    r"|徳島|佐賀|大分|宮崎)都?道?府?県?)"
)

_POST_CODE_PATTERN = re.compile(r"〒?\s*(\d{3})[-\s]?(\d{4})")


class ActivityJapanScraper(StaticCrawler):
    """アクティビティジャパン 提携事業者スクレイパー"""

    DELAY = 1.5

    EXTRA_COLUMNS = [
        "エリア",
        "エリア詳細",
        "加入保険の情報",
        "所持ライセンス・資格名",
        "加盟団体・協会",
        "在籍スタッフ数",
        "インストラクター数",
        "安全面に対するアピールポイント",
        "評価",
        "レビュー件数",
    ]

    _max_ids: int | None = None

    @staticmethod
    def _text(el) -> str:
        return el.get_text(strip=True) if el else ""

    @staticmethod
    def _clean(text: str) -> str:
        return re.sub(r"\s+", " ", text).strip()

    def _split_addr(self, raw: str) -> tuple[str, str, str]:
        """住所文字列から 郵便番号・都道府県・残り住所 を抽出する。"""
        text = raw.replace("\xa0", " ").strip()

        post_code = ""
        pc = _POST_CODE_PATTERN.search(text)
        if pc:
            post_code = f"{pc.group(1)}-{pc.group(2)}"
            text = _POST_CODE_PATTERN.sub("", text, count=1).strip()

        text = self._clean(text)

        pref = ""
        addr = text
        m = _PREF_PATTERN.match(text)
        if m:
            pref = m.group(1)
            addr = text[m.end():].strip()
        return post_code, pref, addr

    def parse(self, url: str):
        collected_ids: dict[str, str] = {}

        for area in AREAS:
            area_url = f"{LIST_URL}?areaName={area}"
            self.logger.info("エリア一覧取得: %s", area_url)
            soup = self.get_soup(area_url)
            if not soup:
                self.logger.warning("エリア一覧の取得失敗: %s", area)
                continue

            anchors = soup.select('a[href^="/publish/feature/"]')
            new_count = 0
            for a in anchors:
                href = a.get("href", "")
                m = re.match(r"^/publish/feature/(\d+)", href)
                if not m:
                    continue
                sid = m.group(1)
                if sid in collected_ids:
                    continue
                collected_ids[sid] = self._text(a)
                new_count += 1
            self.logger.info("エリア %s: 新規 %d 件 (累計 %d)", area, new_count, len(collected_ids))

        if not collected_ids:
            self.logger.error("事業者IDが1件も取得できませんでした")
            return

        ids = list(collected_ids.keys())
        if self._max_ids is not None:
            ids = ids[: self._max_ids]
            self.logger.info("テスト上限により %d 件に絞り込み", len(ids))

        self.total_items = len(ids)
        self.logger.info("詳細ページ対象: %d 件", self.total_items)

        for idx, sid in enumerate(ids, start=1):
            detail_url = f"{BASE_URL}/publish/feature/{sid}"
            try:
                if self.DELAY > 0:
                    time.sleep(self.DELAY)
                item = self._scrape_detail(detail_url, fallback_name=collected_ids[sid])
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("詳細ページ取得失敗: %s (%s)", detail_url, e)

    def _scrape_detail(self, detail_url: str, fallback_name: str = "") -> dict | None:
        self.logger.info("詳細ページ取得: %s", detail_url)
        soup = self.get_soup(detail_url)
        if not soup:
            return None

        item: dict = {Schema.URL: detail_url}

        h1 = soup.select_one("h1.ptnname, h1")
        name = self._text(h1) or fallback_name
        if not name:
            self.logger.warning("事業者名が取得できませんでした: %s", detail_url)
            return None
        item[Schema.NAME] = name

        self._parse_baseinfo(soup, item)
        self._parse_categories(soup, item)
        self._parse_rating(soup, item)

        return item

    def _parse_baseinfo(self, soup, item: dict) -> None:
        """詳細ページの基本情報テーブル (table.baseinfo) を解析する。"""
        tables = soup.select("table.baseinfo, table.normal-table")
        for table in tables:
            for tr in table.select("tr"):
                th = tr.select_one("th")
                td = tr.select_one("td")
                if not th or not td:
                    continue
                label = self._text(th)
                value = self._clean(td.get_text(separator=" "))

                if label == "住所":
                    post_code, pref, addr = self._split_addr(value)
                    if post_code:
                        item[Schema.POST_CODE] = post_code
                    if pref:
                        item[Schema.PREF] = pref
                    if addr:
                        item[Schema.ADDR] = addr

                elif label == "営業時間":
                    item[Schema.TIME] = value

                elif label == "定休日":
                    item[Schema.HOLIDAY] = value

                elif label == "ホームページ":
                    a_tag = td.select_one("a[href]")
                    item[Schema.HP] = a_tag.get("href", "").strip() if a_tag else value

                elif label == "加入保険の情報":
                    item["加入保険の情報"] = value

                elif label == "所持ライセンス・資格名":
                    item["所持ライセンス・資格名"] = value

                elif label == "加盟団体・協会":
                    item["加盟団体・協会"] = value

                elif label == "在籍スタッフ数":
                    item["在籍スタッフ数"] = value

                elif label == "インストラクター数":
                    item["インストラクター数"] = value

                elif label == "安全面に対するアピールポイント":
                    item["安全面に対するアピールポイント"] = value

    def _parse_categories(self, soup, item: dict) -> None:
        """.company_baseinfo 内の エリア / アクティビティ 情報を解析する。"""
        base = soup.select_one(".company_baseinfo")
        if not base:
            return

        area_values: list[str] = []
        category_values: list[str] = []

        for cat_block in base.select(".category"):
            item_el = cat_block.select_one(".category--item")
            if not item_el:
                continue
            divs = item_el.select("div")
            if not divs:
                continue
            label = self._text(divs[0])
            values: list[str] = []
            for a in item_el.select('a[href*="/search/"]'):
                txt = self._text(a)
                if txt and txt not in values:
                    values.append(txt)

            if label == "エリア":
                area_values = values
            elif label == "アクティビティ":
                category_values = values

        if area_values:
            item["エリア"] = area_values[0]
            if len(area_values) > 1:
                item["エリア詳細"] = "、".join(area_values[1:])

        if category_values:
            item[Schema.CAT_SITE] = "、".join(category_values)

    def _parse_rating(self, soup, item: dict) -> None:
        rating_score = soup.select_one(".rating__score")
        if rating_score:
            score = self._text(rating_score)
            if score:
                item["評価"] = score

        review_link = soup.select_one('a[href*="/publish/review/"]')
        if review_link:
            txt = self._text(review_link)
            m = re.search(r"(\d+)", txt)
            if m:
                item["レビュー件数"] = m.group(1)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = ActivityJapanScraper()
    scraper._max_ids = 5  # テスト: 詳細ページ 5 件だけ
    scraper.execute(LIST_URL)

    print("\n" + "=" * 40)
    print("📊 実行結果サマリ")
    print("=" * 40)
    print(f"  出力ファイル:   {scraper.output_filepath}")
    print(f"  取得件数:       {scraper.item_count}")
    print(f"  観測カラム数:   {len(scraper.observed_columns)}")
    print("=" * 40)
