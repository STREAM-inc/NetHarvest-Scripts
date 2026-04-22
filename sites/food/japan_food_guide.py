# scripts/sites/food/japan_food_guide.py
"""
Japan Food Guide — 飲食店・グルメ予約サイト

取得対象:
    - 飲食店情報（名称・住所・電話・営業時間・定休日・ジャンル・支払い方法等）

取得フロー:
    一覧ページ(約60ページ)を巡回 → 各店舗詳細ページを取得

実行方法:
    # ローカルテスト
    python scripts/sites/food/japan_food_guide.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id japan_food_guide
"""

import re
import sys
from pathlib import Path
from urllib.parse import urlparse, parse_qs

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_BASE = "https://japan-food.guide"
_LIST_URL = f"{_BASE}/restaurants/search?q%5Breservable_and_items_reservable%5D=1"

_PREF_RE = re.compile(
    r"^(北海道|東京都|大阪府|京都府|"
    r"神奈川県|埼玉県|千葉県|兵庫県|福岡県|愛知県|静岡県|茨城県|広島県|"
    r"宮城県|新潟県|長野県|群馬県|栃木県|岡山県|島根県|山口県|香川県|"
    r"徳島県|愛媛県|高知県|福井県|石川県|富山県|滋賀県|奈良県|和歌山県|"
    r"鳥取県|岐阜県|三重県|山梨県|長崎県|佐賀県|熊本県|大分県|宮崎県|"
    r"鹿児島県|沖縄県|青森県|岩手県|秋田県|山形県|福島県)"
)


class JapanFoodGuideScraper(StaticCrawler):
    """Japan Food Guide スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["名称_英語", "エリア", "アクセス", "価格帯", "特徴", "備考"]

    def parse(self, url: str):
        page = 1
        while True:
            list_url = (
                url
                if page == 1
                else f"{_BASE}/restaurants/search?page={page}&q%5Breservable_and_items_reservable%5D=1"
            )
            soup = self.get_soup(list_url)
            if soup is None:
                break

            if page == 1:
                total_el = soup.select_one("h2.section-title span")
                if total_el:
                    try:
                        self.total_items = int(total_el.get_text(strip=True))
                    except ValueError:
                        pass

            items = soup.select("li.item.card")
            if not items:
                break

            for item in items:
                link = item.select_one("a.main")
                if not link or not link.get("href"):
                    continue
                detail_url = _BASE + link["href"]
                try:
                    result = self._scrape_detail(detail_url)
                    if result:
                        yield result
                except Exception as e:
                    self.logger.warning("詳細取得エラー %s: %s", detail_url, e)
                    continue

            if not soup.select_one(".pager a[rel='next']"):
                break
            page += 1

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        # 店名（英語 / 日本語）
        h1 = soup.select_one("h1")
        if not h1:
            return None
        title = h1.get_text(strip=True)
        parts = title.split(" / ", 1)
        if len(parts) == 2:
            data["名称_英語"] = parts[0].strip()
            data[Schema.NAME] = parts[1].strip()
        else:
            data[Schema.NAME] = title
            data["名称_英語"] = ""

        # カテゴリ・エリア（icon-pin=エリア, icon-hash=カテゴリ）
        for li in soup.select("section.restaurant-detail .detail-header ul.options li"):
            use_el = li.select_one("svg use")
            span_el = li.select_one("span")
            if not use_el or not span_el:
                continue
            href_val = use_el.get("xlink:href", "") or use_el.get("href", "")
            text = span_el.get_text(strip=True)
            if "icon-pin" in href_val:
                data["エリア"] = text
            elif "icon-hash" in href_val:
                data[Schema.CAT_SITE] = text

        # Google Maps リンクから日本語住所・郵便番号・都道府県を抽出
        maps_a = soup.select_one('a[href*="maps/search"]')
        if maps_a:
            href = maps_a.get("href", "")
            parsed = urlparse(href)
            query_text = parse_qs(parsed.query).get("query", [""])[0]
            m = re.match(r"〒(\d{7})\s+(.*)", query_text)
            if m:
                raw = m.group(1)
                data[Schema.POST_CODE] = raw[:3] + "-" + raw[3:]
                full_addr = m.group(2).strip()
                pm = _PREF_RE.match(full_addr)
                if pm:
                    data[Schema.PREF] = pm.group(1)
                    data[Schema.ADDR] = full_addr[pm.end():].strip()
                else:
                    data[Schema.ADDR] = full_addr

        # store-info dl > dt/dd ペアを解析
        dl = soup.select_one(".store-info-frame dl")
        if dl:
            for dt in dl.find_all("dt"):
                dt_text = dt.get_text(strip=True)
                dd = dt.find_next_sibling("dd")
                if not dd:
                    continue
                val = dd.get_text(" ", strip=True)

                if "Address" in dt_text and Schema.ADDR not in data:
                    # Googleマップから取得できなかった場合の英語住所フォールバック
                    data[Schema.ADDR] = val
                elif "Access" in dt_text:
                    data["アクセス"] = val
                elif "Phone" in dt_text:
                    data[Schema.TEL] = val
                elif "Business Days" in dt_text or "Hours" in dt_text:
                    data[Schema.TIME] = val
                elif "Regular holiday" in dt_text:
                    data[Schema.HOLIDAY] = val.strip()
                elif "Features" in dt_text:
                    data["特徴"] = val
                elif "Payment Method" in dt_text:
                    data[Schema.PAYMENTS] = val
                elif "Remarks" in dt_text:
                    data["備考"] = val

        # 最低価格（From X(JPY) ~）
        price_el = soup.select_one(".floating-nav .price")
        if price_el:
            price_text = price_el.get_text(strip=True)
            pm = re.search(r"([\d,]+)\(JPY\)", price_text)
            if pm:
                data["価格帯"] = pm.group(1).replace(",", "")

        return data if data.get(Schema.NAME) else None


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = JapanFoodGuideScraper()
    scraper.execute(_LIST_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
