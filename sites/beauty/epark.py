"""
EPARKリラク＆エステ (mitsuraku.jp) — リラク・エステ・フィットネス店舗情報スクレイパー

取得対象:
    - 3ジャンル (リラク・マッサージ / エステ / フィットネス) × 全47都道府県
    - 店舗名, 住所, TEL, 営業時間, 定休日, ジャンル, 口コミ情報 等

取得フロー:
    1. 各ジャンル × 各都道府県の一覧ページを全ページ巡回して詳細URLを収集
    2. 各店舗の詳細ページから情報を取得して yield

実行方法:
    python scripts/sites/beauty/epark.py
    python bin/run_flow.py --site-id epark
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


_PREF_RE = re.compile(
    r"^(北海道|東京都|大阪府|京都府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# ジャンル: (カテゴリ名, URLプレフィックス)
_GENRES = [
    ("リラク・マッサージ", ""),
    ("エステ", "esthe/"),
    ("フィットネス", "fitness/"),
]

_PREFS = [
    "hokkaido", "aomori", "iwate", "miyagi", "akita", "yamagata", "fukushima",
    "ibaraki", "tochigi", "gumma", "saitama", "chiba", "tokyo", "kanagawa",
    "niigata", "toyama", "ishikawa", "fukui", "yamanashi", "nagano",
    "gifu", "shizuoka", "aichi", "mie", "shiga", "kyoto", "osaka", "hyogo",
    "nara", "wakayama", "tottori", "shimane", "okayama", "hiroshima",
    "yamaguchi", "tokushima", "kagawa", "ehime", "kochi", "fukuoka", "saga",
    "nagasaki", "kumamoto", "oita", "miyazaki", "kagoshima", "okinawa",
]


class EparkScraper(StaticCrawler):
    """EPARKリラク＆エステ (mitsuraku.jp) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["最寄駅", "アクセス", "駐車場", "個室", "キャンセル料"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        base = url.rstrip("/")
        seen: set[str] = set()

        for cat_name, gprefix in _GENRES:
            for pref in _PREFS:
                list_url = f"{base}/{gprefix}{pref}/"
                for detail_url in self._iter_list(list_url, seen):
                    try:
                        item = self._scrape_detail(detail_url, cat_name)
                        if item:
                            yield item
                    except Exception as e:
                        self.logger.warning("詳細取得エラー: %s — %s", detail_url, e)

    def _iter_list(self, list_url: str, seen: set) -> Generator[str, None, None]:
        """一覧ページをページネーションして新規詳細URLを yield する"""
        page = 1
        while True:
            paged_url = list_url if page == 1 else f"{list_url}?page={page}"
            soup = self.get_soup(paged_url)
            if soup is None:
                break

            links = soup.select("h2.search_shopname a.js-salon-link[href]")
            if not links:
                break

            found_new = False
            for a in links:
                href = a.get("href", "").strip()
                if not href:
                    continue
                full = href if href.startswith("http") else urljoin(list_url, href)
                # 詳細ページのみ (一覧ページURLは除く)
                if full not in seen:
                    seen.add(full)
                    found_new = True
                    yield full

            if not found_new:
                break
            page += 1

    def _scrape_detail(self, url: str, cat_name: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        item: dict = {
            Schema.URL: url,
            Schema.CAT_SITE: cat_name,
        }

        # --- 店舗名 ---
        h1 = soup.select_one("h1.salon-detail__name, h1.shopdetail_title, h1.shop_name, h1")
        if h1:
            item[Schema.NAME] = h1.get_text(strip=True)

        # --- dl / table の構造化フィールドを横断的に取得 ---
        labels = self._collect_labels(soup)

        _MAP = {
            "住所":   Schema.ADDR,
            "TEL":    Schema.TEL,
            "電話番号": Schema.TEL,
            "営業時間": Schema.TIME,
            "定休日":  Schema.HOLIDAY,
            "支払方法": Schema.PAYMENTS,
            "支払い方法": Schema.PAYMENTS,
            "最寄駅":  "最寄駅",
            "アクセス": "アクセス",
            "駐車場":  "駐車場",
            "個室":   "個室",
            "キャンセル料": "キャンセル料",
        }
        for label, col in _MAP.items():
            if label in labels and labels[label]:
                item.setdefault(col, labels[label])

        # --- 住所から都道府県を分離 ---
        addr = item.get(Schema.ADDR, "")
        if addr:
            m = _PREF_RE.match(addr)
            if m:
                item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = addr[m.end():].strip()

        # --- 口コミ ---
        score_el = soup.select_one("[class*='score'], [class*='rating']")
        if score_el:
            score_text = score_el.get_text(strip=True)
            if re.search(r"\d", score_text):
                item[Schema.SCORES] = score_text

        count_el = soup.select_one("[class*='review-count'], [class*='kuchikomi']")
        if count_el:
            count_text = count_el.get_text(strip=True)
            if re.search(r"\d", count_text):
                item[Schema.REV_SCR] = count_text

        if not item.get(Schema.NAME):
            return None
        return item

    @staticmethod
    def _collect_labels(soup) -> dict:
        """dl dt→dd および table th→td からラベル:値辞書を構築する"""
        labels: dict[str, str] = {}

        for dl in soup.find_all("dl"):
            for dt in dl.find_all("dt"):
                dd = dt.find_next_sibling("dd")
                if dd is None:
                    continue
                key = dt.get_text(strip=True)
                val = re.sub(r"\s+", " ", dd.get_text(" ", strip=True))
                if key and key not in labels and val:
                    labels[key] = val

        for tr in soup.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            key = th.get_text(strip=True)
            val = re.sub(r"\s+", " ", td.get_text(" ", strip=True))
            if key and key not in labels and val:
                labels[key] = val

        return labels


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = EparkScraper()
    scraper.execute("https://mitsuraku.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
