"""
スナックガイド — 全国スナック・パブ店舗一覧

取得対象:
    全国47都道府県のスナック・パブ情報

取得フロー:
    1. 都道府県ごとに路線リストを取得 (POST /search/cond.php?func=search_line)
    2. 路線ごとに駅リストを取得 (POST /search/cond.php?func=search_station)
    3. 駅ごとに店舗一覧を取得 (GET /search/?station_cd=XXXXX)
    4. セッションベースのページネーション (GET /search/result.php?page=N)
    5. 店舗 URL をキーに重複排除

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/snack_guide.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id snack_guide
"""

import re
import sys
import time
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup
from src.framework.static import StaticCrawler
from src.const.schema import Schema

_POST_CODE_RE = re.compile(r"〒(\d{3}-?\d{4})")
_PREF_RE = re.compile(
    r"^(北海道|東京都|大阪府|京都府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|新潟|富山|"
    r"石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|和歌山|鳥取|島根|"
    r"岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|"
    r"沖縄)県?)"
)

# 都道府県スラッグ → pref_id (JIS X 0401 に準拠、Tokyo=13 / Osaka=27 確認済み)
_PREF_ID_MAP = {
    "hokkaido": 1,  "aomori": 2,   "iwate": 3,    "miyagi": 4,   "akita": 5,
    "yamagata": 6,  "fukushima": 7, "ibaraki": 8,  "tochigi": 9,  "gunma": 10,
    "saitama": 11,  "chiba": 12,    "tokyo": 13,   "kanagawa": 14, "niigata": 15,
    "toyama": 16,   "ishikawa": 17, "fukui": 18,   "yamanashi": 19, "nagano": 20,
    "gifu": 21,     "shizuoka": 22, "aichi": 23,   "mie": 24,     "shiga": 25,
    "kyoto": 26,    "osaka": 27,    "hyogo": 28,   "nara": 29,    "wakayama": 30,
    "tottori": 31,  "shimane": 32,  "okayama": 33, "hiroshima": 34, "yamaguchi": 35,
    "tokushima": 36, "kagawa": 37,  "ehime": 38,   "kochi": 39,   "fukuoka": 40,
    "saga": 41,     "nagasaki": 42, "kumamoto": 43, "oita": 44,   "miyazaki": 45,
    "kagoshima": 46, "okinawa": 47,
}


class SnackGuideCrawler(StaticCrawler):
    """スナックガイド スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = []

    def parse(self, url: str):
        base = url.rstrip("/")
        seen: set[str] = set()

        for pref_slug, pref_id in _PREF_ID_MAP.items():
            # 路線コード一覧を取得
            try:
                line_codes = self._get_line_codes(base, pref_id)
            except Exception as e:
                self.logger.warning("路線取得失敗 pref=%s pref_id=%s: %s", pref_slug, pref_id, e)
                continue

            if not line_codes:
                continue

            # 駅URL一覧を取得 (路線ごと)
            station_hrefs: list[str] = []
            seen_stations: set[str] = set()
            for line_cd in line_codes:
                try:
                    for href in self._get_station_hrefs(base, pref_id, line_cd):
                        if href not in seen_stations:
                            seen_stations.add(href)
                            station_hrefs.append(href)
                    time.sleep(0.3)
                except Exception as e:
                    self.logger.warning(
                        "駅取得失敗 pref=%s line=%s: %s", pref_slug, line_cd, e
                    )

            if not station_hrefs:
                continue

            self.logger.info("pref=%s 駅数=%d", pref_slug, len(station_hrefs))

            # 駅ごとに店舗を取得
            for stn_href in station_hrefs:
                try:
                    yield from self._crawl_station(base, stn_href, seen)
                except Exception as e:
                    self.logger.warning("駅クロール失敗 %s: %s", stn_href, e)

    # ------------------------------------------------------------------
    # 内部ヘルパー
    # ------------------------------------------------------------------

    def _get_line_codes(self, base: str, pref_id: int) -> list[str]:
        resp = self.session.post(
            f"{base}/search/cond.php",
            data={"func": "search_line", "ken": "", "pref_id": pref_id},
            timeout=self.TIMEOUT,
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        return [
            inp["value"]
            for inp in soup.find_all("input", {"name": "line_cd_list[]"})
            if inp.get("value")
        ]

    def _get_station_hrefs(self, base: str, pref_id: int, line_cd: str) -> list[str]:
        resp = self.session.post(
            f"{base}/search/cond.php",
            data={"func": "search_station", "pref_id": pref_id, "line_cd_list[]": line_cd},
            timeout=self.TIMEOUT,
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        return [
            a["href"]
            for a in soup.find_all("a", href=True)
            if "station_cd" in a.get("href", "")
        ]

    def _crawl_station(self, base: str, stn_href: str, seen: set[str]):
        page = 1
        while True:
            if page == 1:
                url = f"{base}{stn_href}"
            else:
                url = f"{base}/search/result.php?page={page}"

            soup = self.get_soup(url)
            if soup is None:
                break

            items = soup.select(".shop-items")
            if not items:
                break

            for item in items:
                try:
                    link = item.select_one("a[href]")
                    if not link:
                        continue
                    detail_url = f"{base}{link['href']}"
                    if detail_url in seen:
                        continue
                    seen.add(detail_url)

                    record = self._parse_item(item, detail_url)
                    if record:
                        yield record
                except Exception as e:
                    self.logger.warning("アイテムパース失敗: %s", e)

            # 次ページ判定
            pager = soup.find(class_="pagination")
            if not pager or f"page={page + 1}" not in str(pager):
                break
            page += 1

    def _parse_item(self, item, detail_url: str) -> dict | None:
        name_el = item.select_one("h3.shop-name")
        if not name_el:
            return None
        name = name_el.get_text(strip=True)
        if not name:
            return None

        addr_raw = ""
        tel = ""
        hours = ""

        dl = item.find("dl", class_="shop-access")
        if dl:
            for div in dl.find_all("div"):
                dt = div.find("dt")
                dd = div.find("dd")
                if not dt or not dd:
                    continue
                label = dt.get_text(strip=True)
                if label == "住所":
                    addr_raw = dd.get_text(separator=" ", strip=True)
                elif label == "連絡先":
                    tel_a = dd.find("a", href=lambda h: h and h.startswith("tel:"))
                    if tel_a:
                        tel = tel_a.get_text(strip=True)
                elif label == "営業時間":
                    hours = dd.get_text(strip=True)

        # 郵便番号 / 都道府県 / 住所 を分離
        post_code = ""
        addr = addr_raw
        pc_m = _POST_CODE_RE.search(addr_raw)
        if pc_m:
            post_code = pc_m.group(1).replace("-", "")
            addr = addr_raw[pc_m.end():].strip()

        pref = ""
        pr_m = _PREF_RE.match(addr)
        if pr_m:
            pref = pr_m.group(1)
            addr = addr[pr_m.end():].strip()

        return {
            Schema.NAME: name,
            Schema.URL: detail_url,
            Schema.POST_CODE: post_code,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.TIME: hours,
            Schema.CAT_SITE: "スナック・パブ",
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = SnackGuideCrawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://www.snack-guide.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
