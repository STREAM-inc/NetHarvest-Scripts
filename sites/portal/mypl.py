"""
まいぷれ — 地域情報ポータル (mypl.net)

取得対象:
    - 全国 284+ の地域サブドメイン配下の店舗情報
    - 店舗名・フリガナ・住所・郵便番号・電話・営業時間・ジャンル・
      支払い方法・HP・曜日別営業時間・SNS 等

取得フロー:
    1. https://mypl.net/ からエリアサブドメイン一覧を抽出
    2. 各サブドメインで /shop/list?c={cat}&pg={n} をページング
    3. 各店舗の詳細ページ /shop/{shop_id}/ を取得して
       table.table01 から全フィールドを抽出

実行方法:
    python scripts/sites/portal/mypl.py
    python bin/run_flow.py --site-id mypl
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


BASE_URL = "https://mypl.net/"

# メインカテゴリID (c パラメータ)
#   1: グルメ / 2: 学ぶ・スクール / 3: 遊び・トラベル / 4: 美容・健康
#   5: ショッピング / 6: 暮らし・相談 / 18: 官公署 / 20: 病院・医院・薬局 / 21: 住宅
_CATEGORY_IDS = [1, 2, 3, 4, 5, 6, 18, 20, 21]

_SUBDOMAIN_EXCLUDE = {
    "www", "static", "img", "img2", "partner-mypl", "promotion",
    "pop-up-shop", "recruit", "blog", "wwws",
}

_POSTCODE_RE = re.compile(r"〒?\s*(\d{3}-\d{4})\s*(.*)", re.DOTALL)
_SUBDOMAIN_RE = re.compile(r"//([a-z0-9-]+)\.mypl\.net")

_DAY_COLS = {
    "月曜日": Schema.TIME_MON,
    "火曜日": Schema.TIME_TUE,
    "水曜日": Schema.TIME_WED,
    "木曜日": Schema.TIME_THU,
    "金曜日": Schema.TIME_FRI,
    "土曜日": Schema.TIME_SAT,
    "日曜日": Schema.TIME_SUN,
}


class MyplCrawler(StaticCrawler):
    """まいぷれ (地域情報ポータル) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "サブドメイン",
        "ジャンル大",
        "ジャンル小",
        "アクセス",
        "ファックス番号",
        "駐車場",
        "クレジットカード",
        "電子マネー",
        "禁煙喫煙",
        "こだわり",
        "関連ページ",
        "紹介PR",
    ]

    def parse(self, url: str):
        top_soup = self.get_soup(url)
        subdomains = self._extract_subdomains(top_soup)
        self.logger.info("discovered %d subdomains", len(subdomains))

        for sub in subdomains:
            for cat in _CATEGORY_IDS:
                yield from self._crawl_category(sub, cat)

    def _extract_subdomains(self, soup) -> list[str]:
        seen: set[str] = set()
        for a in soup.select("a[href]"):
            href = a.get("href") or ""
            m = _SUBDOMAIN_RE.search(href)
            if m:
                sub = m.group(1)
                if sub in _SUBDOMAIN_EXCLUDE:
                    continue
                seen.add(sub)
        return sorted(seen)

    def _crawl_category(self, subdomain: str, cat_id: int):
        page = 1
        while True:
            list_url = f"https://{subdomain}.mypl.net/shop/list?c={cat_id}&pg={page}"
            try:
                soup = self.get_soup(list_url)
            except Exception as e:
                self.logger.warning("list fetch failed %s: %s", list_url, e)
                return

            cards = soup.select("article.card_list_article")
            if not cards:
                return

            for card in cards:
                title_a = card.select_one("a.card_list_ttl")
                if not title_a or not title_a.get("href"):
                    continue
                detail_url = urljoin(list_url, title_a.get("href"))
                try:
                    item = self._scrape_detail(detail_url, subdomain, card)
                except Exception as e:
                    self.logger.warning("detail failed %s: %s", detail_url, e)
                    continue
                if item:
                    yield item

            page += 1

    def _scrape_detail(self, url: str, subdomain: str, list_card) -> dict | None:
        soup = self.get_soup(url)
        item: dict = {
            Schema.URL: url,
            "サブドメイン": subdomain,
        }

        # --- 一覧カードから取れる情報 ---
        if list_card is not None:
            genre_main = list_card.select_one(".card_list_genre")
            if genre_main:
                item["ジャンル大"] = genre_main.get_text(strip=True)
            genre_sub = list_card.select_one(".card_list_span .fontL")
            if genre_sub:
                item["ジャンル小"] = genre_sub.get_text(strip=True).strip("[]")
            pr_el = list_card.select_one(".card_list_pr")
            if pr_el:
                item["紹介PR"] = pr_el.get_text(strip=True)

        # --- 詳細ページの th-td テーブル ---
        for tr in soup.select("table.table01 tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            key = th.get_text(strip=True)
            val = td.get_text(" ", strip=True)

            if key == "名称":
                item[Schema.NAME] = val
            elif key == "フリガナ":
                item[Schema.NAME_KANA] = val
            elif key == "住所":
                m = _POSTCODE_RE.match(val)
                if m:
                    item[Schema.POST_CODE] = m.group(1)
                    item[Schema.ADDR] = m.group(2).strip()
                else:
                    item[Schema.ADDR] = val
            elif key == "電話番号":
                item[Schema.TEL] = val
            elif key == "ファックス番号":
                item["ファックス番号"] = val
            elif key == "営業時間":
                item[Schema.TIME] = val
            elif key == "アクセス":
                item["アクセス"] = val
            elif key == "駐車場":
                item["駐車場"] = val
            elif key == "支払い方法":
                item[Schema.PAY] = val
            elif key == "クレジットカード":
                item["クレジットカード"] = val
            elif key in ("電子マネー・その他", "電子マネー"):
                item["電子マネー"] = val
            elif key == "禁煙・喫煙":
                item["禁煙喫煙"] = val
            elif key == "こだわり":
                item["こだわり"] = val
            elif key == "関連ページ":
                a = td.find("a")
                item["関連ページ"] = a.get("href") if a and a.get("href") else val
            elif key == "ホームページ":
                a = td.find("a")
                item[Schema.HP] = a.get("href") if a and a.get("href") else val
            elif key == "ジャンル":
                item[Schema.CAT_SITE] = val
            elif key in ("Instagramアカウント", "Instagram"):
                item[Schema.INSTA] = val
            elif key in ("Lineアカウント", "LINEアカウント"):
                item[Schema.LINE] = val
            elif key in ("Xアカウント", "Twitterアカウント"):
                item[Schema.X] = val
            elif key == "Facebookアカウント":
                item[Schema.FB] = val
            elif key == "TikTokアカウント":
                item[Schema.TIKTOK] = val

        # --- 曜日別営業時間 (dl dt/dd) ---
        for dl in soup.select("dl"):
            dts = dl.find_all("dt")
            dds = dl.find_all("dd")
            if len(dts) >= 7 and any(d.get_text(strip=True) in _DAY_COLS for d in dts):
                for i, dt in enumerate(dts):
                    if i >= len(dds):
                        break
                    col = _DAY_COLS.get(dt.get_text(strip=True))
                    if col:
                        item[col] = dds[i].get_text(strip=True)
                break

        if Schema.NAME not in item:
            return None
        return item


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = MyplCrawler()
    scraper.execute(BASE_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
