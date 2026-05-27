"""
バニラ求人 — 全国風俗求人サイト qzin.jp のスクレイパー

取得対象:
    - 全国約10,000件の風俗求人店舗情報（8地域サブドメイン）

取得フロー:
    1. 8つの地域サブドメインのトップページからエリアリンクを収集
    2. 各エリアページを全ページ分ページネーション
    3. 各店舗の詳細ページをスクレイピング

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/qzin.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id qzin
"""

import re
import sys
import time
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

REGION_SUBDOMAINS = [
    "https://hokkaido-tohoku.qzin.jp",
    "https://kitakanto.qzin.jp",
    "https://kanto.qzin.jp",
    "https://hokuriku-koshinetsu.qzin.jp",
    "https://tokai.qzin.jp",
    "https://kansai.qzin.jp",
    "https://chugoku-shikoku.qzin.jp",
    "https://kyusyu-okinawa.qzin.jp",
]

_PREF_RE = re.compile(
    r"^(北海道|東京都|大阪府|京都府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|"
    r"新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|"
    r"滋賀|兵庫|奈良|和歌山|鳥取|島根|岡山|広島|山口|"
    r"徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県)"
)


class QzinScraper(StaticCrawler):
    """バニラ求人 (qzin.jp) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["地域", "エリア", "職種", "勤務地", "勤務日", "交通", "応募資格", "待遇"]

    def parse(self, url: str):
        shop_urls: list[str] = []
        seen: set[str] = set()

        for base in REGION_SUBDOMAINS:
            area_urls = self._collect_area_urls(base)
            self.logger.info("%s: %d エリア検出", base, len(area_urls))
            for area_url in area_urls:
                for shop_url in self._collect_shop_urls(area_url, base):
                    if shop_url not in seen:
                        seen.add(shop_url)
                        shop_urls.append(shop_url)

        self.total_items = len(shop_urls)
        self.logger.info("店舗URL収集完了: %d 件", len(shop_urls))

        for shop_url in shop_urls:
            try:
                item = self._scrape_detail(shop_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("詳細取得失敗 %s: %s", shop_url, e)

    def _collect_area_urls(self, base: str) -> list[str]:
        soup = self.get_soup(base + "/")
        if soup is None:
            return []
        time.sleep(self.DELAY)
        seen: set[str] = set()
        results: list[str] = []
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            # 相対パス: /area{id}_1.html
            if re.match(r"^/area\d+_1\.html$", href):
                full = base + href
            # 同一サブドメインの絶対URL
            elif href.startswith(base + "/area") and re.search(r"_1\.html$", href):
                full = href
            else:
                continue
            if full not in seen:
                seen.add(full)
                results.append(full)
        return results

    def _collect_shop_urls(self, area_url: str, base: str) -> list[str]:
        shop_urls: list[str] = []
        seen: set[str] = set()

        def extract_shops(soup):
            for item in soup.select("li.searchResult-shop-item"):
                a = item.select_one("h3.shop-name a[href]")
                if not a:
                    continue
                href = a.get("href", "")
                if href.startswith("/"):
                    full_url = base + href
                elif href.startswith("http"):
                    full_url = href
                else:
                    continue
                if full_url not in seen:
                    seen.add(full_url)
                    shop_urls.append(full_url)

        # 1ページ目取得 & 最終ページ番号を把握
        soup = self.get_soup(area_url)
        if soup is None:
            return shop_urls
        extract_shops(soup)
        time.sleep(self.DELAY)

        pager_next_links = soup.select(".pager-item.next a")
        last_page = 1
        if pager_next_links:
            last_href = pager_next_links[-1].get("href", "")
            m = re.search(r"_(\d+)\.html", last_href)
            if m:
                last_page = int(m.group(1))

        for page_num in range(2, last_page + 1):
            page_url = re.sub(r"_\d+\.html$", f"_{page_num}.html", area_url)
            soup = self.get_soup(page_url)
            if soup is None:
                continue
            extract_shops(soup)
            time.sleep(self.DELAY)

        return shop_urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        # TEL: hidden input
        tel_el = soup.select_one("input.shop_tel")
        if tel_el:
            val = tel_el.get("value", "").strip()
            if val:
                data[Schema.TEL] = val

        # 都道府県・地域・エリア: JavaScript datalayer から確実に取得
        html_str = str(soup)
        pref_m = re.search(r"'shop_prefecture':'([^']+)'", html_str)
        region_m = re.search(r"'shop_region':'([^']+)'", html_str)
        area_m = re.search(r"'shop_area':'([^']+)'", html_str)
        if pref_m:
            data[Schema.PREF] = pref_m.group(1)
        if region_m:
            data["地域"] = region_m.group(1)
        if area_m:
            data["エリア"] = area_m.group(1)

        # LINE URL
        line_el = soup.select_one(".line_url")
        if line_el:
            line_url = line_el.get_text(strip=True)
            if line_url:
                data[Schema.LINE] = line_url

        # shopInfo-tbl: 最後のテーブルが店舗情報
        tables = soup.select("table.shopInfo-tbl")
        if tables:
            tbl = tables[-1]
            for tr in tbl.select("tr"):
                th_els = tr.select("th")
                td_els = tr.select("td")
                if not th_els or not td_els:
                    continue
                label = re.sub(r"\s+", "", th_els[0].get_text())
                value = td_els[0].get_text(" ", strip=True)
                self._map_field(data, label, value, tr)

        if not data.get(Schema.NAME):
            return None
        return data

    def _map_field(self, data: dict, label: str, value: str, tr):
        if label in ("店名", "店　名"):
            data[Schema.NAME] = value
        elif label == "住所":
            data[Schema.ADDR] = value
            if Schema.PREF not in data:
                m = _PREF_RE.match(value)
                if m:
                    data[Schema.PREF] = m.group(1)
        elif label == "業種":
            data[Schema.CAT_SITE] = value
        elif label == "勤務時間":
            data[Schema.TIME] = value
        elif label in ("オフィシャル", "お店HP"):
            a = tr.select_one("td a[href]")
            if a:
                href = a.get("href", "")
                if href.startswith("http"):
                    data[Schema.HP] = href
        elif label == "職種":
            data["職種"] = value
        elif label == "勤務地":
            data["勤務地"] = value
        elif label == "勤務日":
            data["勤務日"] = value
        elif label in ("交通", "交　通"):
            data["交通"] = value
        elif label == "応募資格":
            data["応募資格"] = value
        elif label == "待遇":
            data["待遇"] = value


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = QzinScraper()
    scraper.execute("https://qzin.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
