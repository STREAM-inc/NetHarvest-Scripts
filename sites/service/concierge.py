"""
ダイエットコンシェルジュ — 全国のパーソナルトレーニングジム検索・比較サイト

取得対象:
    - 全47都道府県の掲載パーソナルトレーニングジム店舗情報

取得フロー:
    /area/{prefecture_romaji}/ → /area/{prefecture_romaji}/page/N/ をループで全ページ巡回
    一覧ページ自体に必要なデータが揃っているため詳細ページへは遷移しない (スケール優先)

実行方法:
    python scripts/sites/service/concierge.py
    python bin/run_flow.py --site-id concierge
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


# 47都道府県: site の URL romaji → 公式表記 (kanji)
PREFECTURES: dict[str, str] = {
    "hokkaido": "北海道",
    "aomori": "青森県",
    "iwate": "岩手県",
    "miyagi": "宮城県",
    "akita": "秋田県",
    "yamagata": "山形県",
    "fukushima": "福島県",
    "ibaraki": "茨城県",
    "tochigi": "栃木県",
    "gunma": "群馬県",
    "saitama": "埼玉県",
    "chiba": "千葉県",
    "tokyo": "東京都",
    "kanagawa": "神奈川県",
    "niigata": "新潟県",
    "toyama": "富山県",
    "ishikawa": "石川県",
    "fukui": "福井県",
    "yamanashi": "山梨県",
    "nagano": "長野県",
    "gifu": "岐阜県",
    "shizuoka": "静岡県",
    "aichi": "愛知県",
    "mie": "三重県",
    "shiga": "滋賀県",
    "kyoto": "京都府",
    "osaka": "大阪府",
    "hyogo": "兵庫県",
    "nara": "奈良県",
    "wakayama": "和歌山県",
    "tottori": "鳥取県",
    "shimane": "島根県",
    "okayama": "岡山県",
    "hiroshima": "広島県",
    "yamaguchi": "山口県",
    "tokushima": "徳島県",
    "kagawa": "香川県",
    "ehime": "愛媛県",
    "kochi": "高知県",
    "fukuoka": "福岡県",
    "saga": "佐賀県",
    "nagasaki": "長崎県",
    "kumamoto": "熊本県",
    "oita": "大分県",
    "miyazaki": "宮崎県",
    "kagoshima": "鹿児島県",
    "okinawa": "沖縄県",
}

BASE_URL = "https://concierge.diet"


class ConciergeDietScraper(StaticCrawler):
    """ダイエットコンシェルジュ パーソナルジムスクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "店舗ID",
        "アクセス",
        "キャッシュバック金額",
        "特徴ポイント",
        "設備タグ",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # `url` 引数は execute() の互換性のため受け取るが、47都道府県を内部で巡回する。
        # 1ページ目から総ページ数を見積もって total_items に合算する方式は不可
        # (ページネーション総数の取得が都道府県ごとに必要なため、最初のパス時に集計)。
        all_pages: list[tuple[str, str, int]] = []
        for romaji, pref_jp in PREFECTURES.items():
            pref_url = f"{BASE_URL}/area/{romaji}/"
            total_pages = self._detect_total_pages(pref_url)
            self.logger.info("[%s] %s: %d ページ", romaji, pref_jp, total_pages)
            for p in range(1, total_pages + 1):
                all_pages.append((romaji, pref_jp, p))

        # 各ページ約10件として推定
        self.total_items = len(all_pages) * 10

        seen_ids: set[str] = set()
        for romaji, pref_jp, p in all_pages:
            page_url = f"{BASE_URL}/area/{romaji}/" if p == 1 else f"{BASE_URL}/area/{romaji}/page/{p}/"
            soup = self.get_soup(page_url)
            if soup is None:
                continue
            items = soup.select(".gym-search_list__item")
            for item in items:
                try:
                    record = self._parse_item(item, romaji, pref_jp)
                except Exception as e:
                    self.logger.warning("アイテム解析エラー (%s p%d): %s", romaji, p, e)
                    continue
                if not record:
                    continue
                shop_id = record.get("店舗ID")
                if shop_id and shop_id in seen_ids:
                    continue
                if shop_id:
                    seen_ids.add(shop_id)
                yield record

    def _detect_total_pages(self, pref_url: str) -> int:
        soup = self.get_soup(pref_url)
        if soup is None:
            return 0
        # 1件もない場合
        if not soup.select_one(".gym-search_list__item"):
            return 0
        pager = soup.select_one(".pagination_2")
        if not pager:
            return 1
        nums: list[int] = []
        for a in pager.select("a.page-numbers, span.page-numbers"):
            txt = a.get_text(strip=True)
            if txt.isdigit():
                nums.append(int(txt))
        return max(nums) if nums else 1

    def _parse_item(self, item, romaji: str, pref_jp: str) -> dict | None:
        # 名称: PC版 h5 を優先、無ければ任意の h5
        name_el = item.select_one("h5.pc") or item.select_one("h5")
        name = name_el.get_text(strip=True) if name_el else ""
        if not name:
            return None

        # 詳細URL
        link_el = item.select_one('a[href*="/shops/"]')
        detail_url = link_el.get("href", "").strip() if link_el else ""

        # 店舗ID: id="gym-1234567"
        shop_id = ""
        item_id = item.get("id", "")
        m = re.match(r"gym-(\d+)", item_id)
        if m:
            shop_id = m.group(1)

        # 住所・アクセス: gym_information_details__txt の p (1: 住所, 2: アクセス)
        info_ps = item.select(".gym_information_details__txt p")
        addr = info_ps[0].get_text(strip=True) if len(info_ps) >= 1 else ""
        access = info_ps[1].get_text(strip=True) if len(info_ps) >= 2 else ""

        # キャッシュバック金額 (任意)
        cashback = ""
        cb_el = item.select_one(".gym-search__cashback ._price strong")
        if cb_el:
            cashback = re.sub(r"[\s,]", "", cb_el.get_text(strip=True))

        # 特徴ポイント (3つの売り)
        points = [p.get_text(strip=True) for p in item.select(".about-list_item p")]
        points_joined = " / ".join([p for p in points if p])

        # 設備タグ
        tags = [p.get_text(strip=True) for p in item.select(".--gym-about-tags p")]
        tags_joined = ", ".join([t for t in tags if t])

        # 営業時間 (月-日)
        day_to_schema = {
            "月": Schema.TIME_MON,
            "火": Schema.TIME_TUE,
            "水": Schema.TIME_WED,
            "木": Schema.TIME_THU,
            "金": Schema.TIME_FRI,
            "土": Schema.TIME_SAT,
            "日": Schema.TIME_SUN,
        }
        time_data: dict[str, str] = {}
        for tr in item.select(".hour_table tbody tr"):
            th = tr.select_one("th")
            td = tr.select_one("td")
            if not th or not td:
                continue
            day = th.get_text(strip=True)
            hours = td.get_text(strip=True)
            if day in day_to_schema:
                time_data[day_to_schema[day]] = hours

        record: dict = {
            Schema.NAME: name,
            Schema.URL: detail_url or f"{BASE_URL}/area/{romaji}/",
            Schema.PREF: pref_jp,
            Schema.ADDR: addr,
            Schema.CAT_SITE: "パーソナルトレーニングジム",
            "店舗ID": shop_id,
            "アクセス": access,
            "キャッシュバック金額": cashback,
            "特徴ポイント": points_joined,
            "設備タグ": tags_joined,
        }
        record.update(time_data)
        return record


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = ConciergeDietScraper()
    scraper.execute("https://concierge.diet/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
