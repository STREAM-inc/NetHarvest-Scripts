"""
ぱちタウン — DMMぱちタウン（全国パチンコ・パチスロ店舗情報）

取得対象:
    - 全国47都道府県のパチンコ・パチスロ店舗情報

取得フロー:
    1. /shops/{pref} から市区町村エリアリストを取得（47都道府県）
    2. 各エリアページ /shops/{pref}/area/{code} から店舗カードを取得
    3. 各店舗の詳細ページ /shops/{pref}/{id} から店舗情報を取得

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/pachitown.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id pachitown
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


_BASE = "https://p-town.dmm.com"

_PREF_SLUGS = {
    "hokkaido": "北海道", "aomori": "青森県", "iwate": "岩手県",
    "miyagi": "宮城県", "akita": "秋田県", "yamagata": "山形県",
    "fukushima": "福島県", "ibaraki": "茨城県", "tochigi": "栃木県",
    "gunma": "群馬県", "saitama": "埼玉県", "chiba": "千葉県",
    "tokyo": "東京都", "kanagawa": "神奈川県", "niigata": "新潟県",
    "toyama": "富山県", "ishikawa": "石川県", "fukui": "福井県",
    "yamanashi": "山梨県", "nagano": "長野県", "gifu": "岐阜県",
    "shizuoka": "静岡県", "aichi": "愛知県", "mie": "三重県",
    "shiga": "滋賀県", "kyoto": "京都府", "osaka": "大阪府",
    "hyogo": "兵庫県", "nara": "奈良県", "wakayama": "和歌山県",
    "tottori": "鳥取県", "shimane": "島根県", "okayama": "岡山県",
    "hiroshima": "広島県", "yamaguchi": "山口県", "tokushima": "徳島県",
    "kagawa": "香川県", "ehime": "愛媛県", "kochi": "高知県",
    "fukuoka": "福岡県", "saga": "佐賀県", "nagasaki": "長崎県",
    "kumamoto": "熊本県", "oita": "大分県", "miyazaki": "宮崎県",
    "kagoshima": "鹿児島県", "okinawa": "沖縄県",
}

_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


class PachitownCrawler(StaticCrawler):
    """ぱちタウン スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["アクセス", "最寄り駅", "遊技金額・台数", "駐車場"]

    def parse(self, url: str):
        seen: set[str] = set()
        self.total_items = 8000  # 全国推計

        for slug, pref_name in _PREF_SLUGS.items():
            pref_url = f"{_BASE}/shops/{slug}"
            try:
                pref_soup = self.get_soup(pref_url)
            except Exception as e:
                self.logger.warning(f"Prefecture page failed ({slug}): {e}")
                continue

            area_links = pref_soup.find_all(
                "a", href=re.compile(rf"/shops/{slug}/area/")
            )

            for area_a in area_links:
                count_m = re.search(r"（(\d+)）", area_a.get_text())
                if count_m and int(count_m.group(1)) == 0:
                    continue

                area_url = f"{_BASE}{area_a['href']}"
                try:
                    area_soup = self.get_soup(area_url)
                except Exception as e:
                    self.logger.warning(f"Area page failed ({area_url}): {e}")
                    continue

                for card in area_soup.find_all("div", class_="shop-card-button"):
                    shop_path = card.get("data-url", "")
                    if not shop_path or shop_path in seen:
                        continue
                    seen.add(shop_path)

                    shop_url = f"{_BASE}{shop_path}"
                    record = self._scrape_detail(shop_url, pref_name)
                    if record:
                        yield record

    def _scrape_detail(self, url: str, pref_from_slug: str):
        try:
            soup = self.get_soup(url)
            tbl = soup.find("table", class_="default-table")
            if not tbl:
                return None

            rows: dict = {}
            for row in tbl.find_all("tr"):
                th = row.find("th")
                td = row.find("td")
                if th and td:
                    rows[th.get_text(strip=True)] = td

            # NAME
            name_td = rows.get("店舗名")
            name = name_td.get_text(strip=True) if name_td else ""

            # ADDR + PREF（住所の <p> タグから取得）
            addr_td = rows.get("住所")
            if addr_td:
                p_tag = addr_td.find("p")
                addr_full = p_tag.get_text(strip=True) if p_tag else addr_td.get_text(strip=True)
            else:
                addr_full = ""
            pref, addr = self._split_pref(addr_full)
            if not pref:
                pref = pref_from_slug

            # TEL
            tel_td = rows.get("電話番号")
            tel = tel_td.get_text(strip=True) if tel_td else ""

            # 公式HP/SNS: textlink → HP、icon img alt で分類
            sns_td = rows.get("公式HP/SNS")
            hp = line_url = x_url = insta_url = ""
            if sns_td:
                textlink = sns_td.find("a", class_="textlink")
                if textlink:
                    hp = textlink.get("href", "")
                for a in sns_td.find_all("a", class_="icon"):
                    img = a.find("img")
                    if not img:
                        continue
                    alt = img.get("alt", "").lower()
                    href = a.get("href", "")
                    if alt == "line":
                        line_url = href
                    elif alt == "x":
                        x_url = href
                    elif alt == "instagram":
                        insta_url = href

            # TIME
            time_td = rows.get("営業時間")
            time_val = time_td.get_text(strip=True) if time_td else ""

            # HOLIDAY
            holiday_td = rows.get("定休日")
            holiday = holiday_td.get_text(strip=True) if holiday_td else ""

            # EXTRA
            access_td = rows.get("アクセス")
            access = access_td.get_text(strip=True) if access_td else ""

            station_td = rows.get("最寄り駅・沿線")
            station = station_td.get_text(separator=" ", strip=True) if station_td else ""

            machine_td = rows.get("遊技金額・台数")
            machine = machine_td.get_text(separator=" ", strip=True)[:200] if machine_td else ""

            parking_td = rows.get("駐車場")
            parking = ""
            if parking_td:
                p_tag = parking_td.find("p")
                parking = p_tag.get_text(strip=True) if p_tag else parking_td.get_text(strip=True)

            return {
                Schema.NAME: name,
                Schema.URL: url,
                Schema.PREF: pref,
                Schema.ADDR: addr,
                Schema.TEL: tel,
                Schema.HP: hp,
                Schema.LINE: line_url,
                Schema.X: x_url,
                Schema.INSTA: insta_url,
                Schema.TIME: time_val,
                Schema.HOLIDAY: holiday,
                "アクセス": access,
                "最寄り駅": station,
                "遊技金額・台数": machine,
                "駐車場": parking,
            }
        except Exception as e:
            self.logger.error(f"Detail scrape failed ({url}): {e}")
            return None

    def _split_pref(self, address: str):
        m = _PREF_RE.match(address)
        if m:
            return m.group(1), address[m.end():].strip()
        return "", address


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = PachitownCrawler()
    scraper.execute(f"{_BASE}/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
