"""
アルバイトナイツ — キャバクラ/ガールズバー等のナイトワーク求人ポータル (baito.nights.fun)

取得対象:
    - 全国47都道府県の掲載店舗情報 (約2091店掲載)
    - 店名 / 住所 / 電話番号 / 業種(キャバクラ・ガールズバー 等)
    - アクセス・最寄駅・職種・給与・勤務日時・応募資格 等

取得フロー:
    1. 都道府県ごとの一覧 /A{NN}/job-list/ をページング (1ページ100件)
    2. 各店舗カード (div.shop_box[data-commu-id]) から詳細URLを収集
    3. 詳細ページの table.basic_information_table から構造化データを抽出

実行方法:
    python scripts/sites/nightlife/baito.py
    python bin/run_flow.py --site-id baito
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


_PREF_PATTERN = re.compile(
    r"^(北海道|東京都|京都府|大阪府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_LATLNG_PATTERN = re.compile(r"/place/(-?\d+\.\d+),(-?\d+\.\d+)")


class BaitoNightsScraper(StaticCrawler):
    """アルバイトナイツ (baito.nights.fun) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "エリア",
        "アクセス",
        "最寄駅",
        "職種",
        "給与",
        "勤務日時",
        "応募資格",
        "平均年齢",
        "採用担当",
        "緯度",
        "経度",
        "応募ページURL",
    ]

    BASE_URL = "https://baito.nights.fun"
    PREF_CODES = [f"A{n:02d}" for n in range(1, 48)]  # A01..A47

    def parse(self, url: str):
        # 一覧→詳細の二段構成。先に全都道府県を巡回して詳細URLを収集する。
        detail_urls = []
        seen = set()
        for pref_code in self.PREF_CODES:
            pref_urls = self._collect_detail_urls_for_pref(pref_code)
            for u in pref_urls:
                if u not in seen:
                    seen.add(u)
                    detail_urls.append(u)
            self.logger.info(
                "都道府県 %s: 累計 %d 件",
                pref_code,
                len(detail_urls),
            )

        self.total_items = len(detail_urls)
        self.logger.info("総店舗数(ユニーク): %d", self.total_items)

        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("詳細ページ取得失敗 %s: %s", detail_url, e)
                continue

    def _collect_detail_urls_for_pref(self, pref_code: str) -> list[str]:
        """都道府県ごとの一覧ページから詳細URLを全ページ巡回して収集する"""
        urls: list[str] = []
        page = 1
        while True:
            list_url = (
                f"{self.BASE_URL}/{pref_code}/job-list/"
                if page == 1
                else f"{self.BASE_URL}/{pref_code}/job-list/{page}/"
            )
            soup = self.get_soup(list_url)
            if soup is None:
                break

            boxes = soup.select("div.shop_box[data-commu-id]")
            if not boxes:
                break

            page_urls: list[str] = []
            for box in boxes:
                a = box.select_one("h2.shop_name a.anothertab[href]")
                if a is None:
                    a = box.select_one("a.anothertab[href]")
                if a is None:
                    continue
                href = a.get("href", "").strip()
                if not href:
                    continue
                page_urls.append(urljoin(self.BASE_URL, href))

            urls.extend(page_urls)

            # 次ページが存在しなければ終了
            next_page_href = f"/{pref_code}/job-list/{page + 1}/"
            has_next = soup.find("a", href=lambda h: bool(h) and h.endswith(next_page_href))
            if not has_next:
                break
            page += 1

        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        # 基本情報テーブル: PC/SP で2つ存在するが th-td 構造は同一
        rows: dict[str, str] = {}
        for table in soup.select("table.basic_information_table"):
            for tr in table.select("tr"):
                th = tr.select_one("th")
                td = tr.select_one("td")
                if not th or not td:
                    continue
                key = th.get_text(strip=True)
                if not key or key in rows:
                    continue
                # 「所在地」は中の <div class="address"> を優先 (MAPボタン文字列を除く)
                if key == "所在地":
                    addr_div = td.select_one(".address")
                    value = (
                        addr_div.get_text(" ", strip=True)
                        if addr_div
                        else td.get_text(" ", strip=True)
                    )
                # 「最寄駅」は <div> 区切りに / を入れる
                elif key == "最寄駅":
                    stations = [
                        d.get_text(strip=True)
                        for d in td.select("div")
                        if d.get_text(strip=True)
                    ]
                    value = " / ".join(stations) if stations else td.get_text(" ", strip=True)
                else:
                    value = td.get_text(" ", strip=True)
                rows[key] = re.sub(r"\s+", " ", value).strip()

        name = rows.get("店名", "")
        addr = rows.get("所在地", "")
        pref = ""
        addr_rest = addr
        if addr:
            m = _PREF_PATTERN.match(addr)
            if m:
                pref = m.group(1)
                addr_rest = addr[m.end():].strip()

        # エリア (一覧ページに表示されているもの)
        area = ""
        title_tag = soup.find("title")
        if title_tag:
            t = title_tag.get_text(strip=True)
            # 例: "...の求人・アルバイト - 梅田/キャバクラ [アルバイトナイツ]"
            m = re.search(r"-\s*([^\[/]+?)/[^/\[]+\s*\[", t)
            if m:
                area = m.group(1).strip()

        # 緯度経度 (Google Maps URL から抽出)
        lat = ""
        lng = ""
        map_a = soup.select_one('a[href*="google.com/maps"], a[href*="maps.google"]')
        if map_a:
            mm = _LATLNG_PATTERN.search(map_a.get("href", ""))
            if mm:
                lat, lng = mm.group(1), mm.group(2)

        # 応募ページ
        apply_a = soup.select_one('a[href*="/entry/web/"]')
        apply_url = urljoin(self.BASE_URL, apply_a["href"]) if apply_a and apply_a.get("href") else ""

        return {
            Schema.NAME: name,
            Schema.URL: url,
            Schema.PREF: pref,
            Schema.ADDR: addr_rest,
            Schema.TEL: rows.get("電話番号", ""),
            Schema.CAT_SITE: rows.get("業種", ""),
            "エリア": area,
            "アクセス": rows.get("アクセス", ""),
            "最寄駅": rows.get("最寄駅", ""),
            "職種": rows.get("職種", ""),
            "給与": rows.get("給与", ""),
            "勤務日時": rows.get("勤務日／時", ""),
            "応募資格": rows.get("応募資格", ""),
            "平均年齢": rows.get("平均年齢", ""),
            "採用担当": rows.get("採用担当", ""),
            "緯度": lat,
            "経度": lng,
            "応募ページURL": apply_url,
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BaitoNightsScraper()
    scraper.execute("https://baito.nights.fun/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
