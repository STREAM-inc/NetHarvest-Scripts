"""
クックドア (cookdoor_2) — 全国のグルメ・飲食店情報スクレイパー (詳細スペック拡充版)

ホームメイト・リサーチ系列の飲食店検索サイト。
既存の `cookdoor` クローラと同じ 一覧→詳細 経路を辿るが、詳細ページの
構造化スペック (予約・貸切・禁煙/喫煙・最寄り駅) を追加取得する拡充版。

取得対象:
    - 全国の飲食店の店舗情報
      (店名・ジャンル・住所・TEL・営業時間・定休日・支払い方法
       ＋ 平均予算・座席・駐車場・予約可否・貸切可否・禁煙喫煙・最寄り駅)

取得フロー (一覧 → 詳細, Pattern B: 詳細を1件取得するごとに即 yield):
    1. 47都道府県ページ (/{pref}/) から市区町村の一覧URL (/{pref}/jc{id}/list/) を収集
    2. 各市区町村の一覧をページ送り (/{pref}/jc{id}/list/{N}/) し、店舗詳細URL (/dtl/{id}/) を収集
    3. 各 ranking_item から 店名・ジャンル・住所・平均予算 を取得
    4. 詳細ページ (/dtl/{id}/) から TEL・営業時間・定休日・カード・座席・駐車場・
       予約・貸切・禁煙喫煙 を取得し、交通アクセス文から最寄り駅を抽出
    5. 1件ずつマージして即 yield

実行方法:
    # ローカルテスト
    python scripts/sites/food/cookdoor_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id cookdoor_2
"""

import re
import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_BASE = "https://www.cookdoor.jp"

# 47都道府県の romaji スラッグ (サイトの /{pref}/ パスに対応)
_PREFS = [
    "hokkaido", "aomori", "iwate", "miyagi", "akita", "yamagata", "fukushima",
    "ibaraki", "tochigi", "gunma", "saitama", "chiba", "tokyo", "kanagawa",
    "niigata", "toyama", "ishikawa", "fukui", "yamanashi", "nagano", "gifu",
    "shizuoka", "aichi", "mie", "shiga", "kyoto", "osaka", "hyogo", "nara",
    "wakayama", "tottori", "shimane", "okayama", "hiroshima", "yamaguchi",
    "tokushima", "kagawa", "ehime", "kochi", "fukuoka", "saga", "nagasaki",
    "kumamoto", "oita", "miyazaki", "kagoshima", "okinawa",
]

# 市区町村の一覧URL: /{pref}/jc{数字}/list/
_CITY_RE = re.compile(r"^/[a-z\-]+/jc\d+/list/$")
# 店舗詳細URL: /dtl/{数字}/
_DTL_RE = re.compile(r"^/dtl/\d+/$")
# 郵便番号
_POST_RE = re.compile(r"〒?\s*(\d{3}-\d{4})")
# 交通アクセス文から最寄り駅 (最初の「○○駅」)
_STATION_RE = re.compile(r"「\s*([^「」]+?駅)\s*」")
# 都道府県
_PREF_RE = re.compile(
    r"(北海道|東京都|(?:大阪|京都)府|(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|"
    r"埼玉|千葉|神奈川|新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|"
    r"和歌山|鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|"
    r"鹿児島|沖縄)県)"
)


class Cookdoor2Scraper(StaticCrawler):
    """クックドア (cookdoor_2) 飲食店スクレイパー (詳細スペック拡充版)"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["平均予算", "座席", "駐車場", "予約", "貸切", "禁煙喫煙", "最寄り駅"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        for pref in _PREFS:
            for city_url in self._collect_city_urls(pref):
                yield from self._scrape_city(city_url, seen)

    # ------------------------------------------------------------------ #
    # 市区町村一覧URLの収集
    # ------------------------------------------------------------------ #
    def _collect_city_urls(self, pref: str) -> list[str]:
        soup = self.get_soup(f"{_BASE}/{pref}/")
        if soup is None:
            self.logger.warning("都道府県ページ取得失敗: %s", pref)
            return []
        urls: list[str] = []
        seen: set[str] = set()
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if _CITY_RE.match(href) and href not in seen:
                seen.add(href)
                urls.append(_BASE + href)
        self.logger.info("%s: 市区町村 %d 件", pref, len(urls))
        return urls

    # ------------------------------------------------------------------ #
    # 1市区町村分のページ送り → 詳細スクレイプ (取得即 yield)
    # ------------------------------------------------------------------ #
    def _scrape_city(self, city_url: str, seen: set[str]) -> Generator[dict, None, None]:
        page = 1
        while True:
            list_url = city_url if page == 1 else f"{city_url}{page}/"
            soup = self.get_soup(list_url)
            if soup is None:
                break
            items = soup.select("section.ranking_item")
            if not items:
                break
            for item in items:
                a = item.select_one("h3.restaurant_name a[href]")
                if not a:
                    continue
                href = a.get("href", "")
                if not _DTL_RE.match(href) or href in seen:
                    continue
                seen.add(href)
                try:
                    record = self._build_record(item, _BASE + href)
                except Exception as e:  # 個別店舗の失敗は握りつぶして継続
                    self.logger.warning("店舗処理エラー (%s): %s", href, e)
                    continue
                if record:
                    yield record
            page += 1

    # ------------------------------------------------------------------ #
    # 一覧アイテム + 詳細ページ をマージして1レコード生成
    # ------------------------------------------------------------------ #
    def _build_record(self, item, detail_url: str) -> dict | None:
        data: dict = {Schema.URL: detail_url}

        # --- 一覧アイテムから取得 ---
        name_a = item.select_one("h3.restaurant_name a")
        if name_a:
            data[Schema.NAME] = name_a.get_text(strip=True)

        genre = item.select_one("div.classify_area p")
        if genre:
            g = genre.get_text(strip=True)
            if g:
                data[Schema.CAT_SITE] = g

        spans = item.select("p.address span")
        if len(spans) >= 2:
            self._fill_address(data, spans[-1].get_text(" ", strip=True))

        budget = item.select_one("dl.budget dd")
        if budget:
            b = budget.get_text(strip=True)
            if b:
                data["平均予算"] = b

        # --- 詳細ページから取得 (TEL/営業時間/各種スペック 等) ---
        self._scrape_detail(detail_url, data)

        if not data.get(Schema.NAME):
            return None
        return data

    def _scrape_detail(self, url: str, data: dict) -> None:
        soup = self.get_soup(url)
        if soup is None:
            return

        # 詳細ページには「店舗スペック表」と「検索フィルタ(全選択肢を列挙)」の
        # 2種類の th/td が存在する。先に出現するスペック表の値のみを採用するため、
        # ラベルごとに最初の1件だけを記録する。フィルタ側は別ラベル
        # ('禁煙・喫煙' / '席数') なので、正規ラベルとの完全一致で除外される。
        th_to_key = {
            "TEL": Schema.TEL,
            "営業時間": Schema.TIME,
            "定休日": Schema.HOLIDAY,
            "カード": Schema.PAYMENTS,
            "平均予算": "平均予算",
            "座席": "座席",
            "駐車場": "駐車場",
            "予約": "予約",
            "貸切": "貸切",
            "禁煙/喫煙": "禁煙喫煙",
            "交通アクセス": "_access",  # 最寄り駅の抽出用 (本文は保存しない)
            "所在地": "_addr",          # 一覧で取れなかった場合の補完用
        }
        filled: set[str] = set()
        for th in soup.find_all("th"):
            label = th.get_text(strip=True)
            key = th_to_key.get(label)
            if not key or key in filled:
                continue
            # スペック表は検索フィルタ(同名ラベルで全選択肢を列挙)より前に出現する。
            # 最初の出現を必ず消費済みにして、後続のフィルタ側を拾わないようにする。
            filled.add(key)
            td = th.find_next("td")
            if not td:
                continue
            val = td.get_text(" ", strip=True).replace("地図を見る", "").strip()
            val = re.sub(r"\s+", " ", val)
            if not val or val == "―":
                continue
            if key == "_addr":
                if not data.get(Schema.ADDR):
                    self._fill_address(data, val)
            elif key == "_access":
                # 交通アクセスは自動生成の文章のため保存せず、最寄り駅のみ抽出
                m = _STATION_RE.search(val)
                if m:
                    data["最寄り駅"] = m.group(1).strip()
            else:
                data[key] = val

    # ------------------------------------------------------------------ #
    # 住所 → 郵便番号 / 都道府県 / 住所 に分解
    # ------------------------------------------------------------------ #
    def _fill_address(self, data: dict, raw: str) -> None:
        if not raw:
            return
        rest = raw
        m = _POST_RE.search(rest)
        if m:
            data[Schema.POST_CODE] = m.group(1)
            rest = rest[m.end():].strip()
        pm = _PREF_RE.search(rest)
        if pm:
            data[Schema.PREF] = pm.group(1)
            rest = rest[pm.end():].strip()
        data[Schema.ADDR] = rest.strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Cookdoor2Scraper()
    scraper.execute(_BASE + "/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
