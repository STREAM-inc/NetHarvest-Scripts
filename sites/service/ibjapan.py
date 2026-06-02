# scripts/sites/service/ibjapan.py
"""
アイビージェージャパン（IBJ Japan）— 全国加盟結婚相談所スクレイパー

取得対象:
    - 47都道府県＋海外エリアの全加盟相談所（約4,500〜5,000件）

取得フロー:
    48エリアページ（ページネーション ?page=N）→ 各相談所詳細ページ

実行方法:
    # ローカルテスト
    python scripts/sites/service/ibjapan.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ibjapan
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

BASE_URL = "https://www.ibjapan.com"

PREFECTURES = [
    "hokkaido", "aomori", "iwate", "miyagi", "akita", "yamagata", "fukushima",
    "tokyo", "kanagawa", "saitama", "chiba", "ibaraki", "tochigi", "gunma",
    "yamanashi", "niigata", "nagano", "toyama", "ishikawa", "fukui",
    "aichi", "gifu", "shizuoka", "mie",
    "osaka", "hyogo", "kyoto", "shiga", "nara", "wakayama",
    "tottori", "shimane", "okayama", "hiroshima", "yamaguchi",
    "tokushima", "kagawa", "ehime", "kochi",
    "fukuoka", "saga", "nagasaki", "kumamoto", "oita", "miyazaki", "kagoshima", "okinawa",
    "foreign_country",
]

_POST_RE = re.compile(r"〒\s*(\d{3}-\d{4})")
_TEL_RE = re.compile(r"([\d\-]+)")
_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


class IBJapanCrawler(StaticCrawler):
    """アイビージェージャパン 全国加盟相談所スクレイパー（ibjapan.com）"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["IBJ加盟番号", "最寄り駅", "サービス対応地域"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()

        for pref in PREFECTURES:
            area_url = f"{BASE_URL}/area/{pref}/"
            self.logger.info("エリア取得: %s", pref)
            page = 1

            while True:
                page_url = area_url if page == 1 else f"{area_url}?page={page}"
                soup = self.get_soup(page_url)
                if soup is None:
                    break

                store_urls = self._extract_store_urls(soup, pref)
                if not store_urls:
                    break

                for store_url in store_urls:
                    if store_url in seen:
                        continue
                    seen.add(store_url)
                    try:
                        item = self._scrape_detail(store_url)
                        if item:
                            yield item
                    except Exception as e:
                        self.logger.warning("詳細取得エラー (スキップ): %s — %s", store_url, e)

                page += 1

    def _extract_store_urls(self, soup, pref: str) -> list[str]:
        pattern = re.compile(rf"/area/{re.escape(pref)}/\d+/?$")
        return list(dict.fromkeys(
            urljoin(BASE_URL, a["href"])
            for a in soup.find_all("a", href=pattern)
        ))

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        item: dict = {Schema.URL: url}

        # 相談所名: <h1>
        h1 = soup.find("h1")
        if not h1:
            return None
        item[Schema.NAME] = h1.get_text(strip=True)

        # 相談所情報: table.agency-info_table の th/td ペアを全て走査
        tbl = soup.find("table", class_="agency-info_table")
        if tbl:
            for tr in tbl.find_all("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                label = th.get_text(strip=True)
                value = td.get_text(" ", strip=True)

                if label == "住所":
                    post_m = _POST_RE.search(value)
                    if post_m:
                        item[Schema.POST_CODE] = post_m.group(1)
                        addr_text = value[post_m.end():].strip()
                        pref_m = _PREF_RE.search(addr_text)
                        if pref_m:
                            item[Schema.PREF] = pref_m.group(1)
                            item[Schema.ADDR] = addr_text[pref_m.start():].strip()
                elif label == "電話番号":
                    # "03-XXXX-XXXX ​※営業を目的とした…" から番号のみ抽出
                    tel_m = _TEL_RE.match(value.strip())
                    item[Schema.TEL] = tel_m.group(1) if tel_m else value
                elif label == "営業時間":
                    item[Schema.TIME] = value
                elif label == "定休日":
                    item[Schema.HOLIDAY] = value
                elif label == "Webサイト":
                    hp_a = td.find("a", href=True)
                    item[Schema.HP] = hp_a["href"] if hp_a else value
                elif label == "支払い方法":
                    item[Schema.PAYMENTS] = value
                elif label == "IBJ公認加盟番号":
                    item["IBJ加盟番号"] = value
                elif label == "最寄り駅":
                    item["最寄り駅"] = value
                elif label == "サービス対応地域":
                    item["サービス対応地域"] = value

        return item if item.get(Schema.ADDR) else None


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = IBJapanCrawler()
    scraper.execute("https://www.ibjapan.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
