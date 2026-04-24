"""
ぐるなび — 全国の飲食店情報スクレイパー

取得対象:
    - 全国のぐるなび掲載飲食店
    - 店名 / フリガナ / 郵便番号 / 住所 / 都道府県 / TEL /
      営業時間 / 定休日 / ジャンル / アクセス / 駐車場 / エリア / 業態

取得フロー:
    1. 一覧ページ https://r.gnavi.co.jp/area/jp/rs/?p=N で article 要素から店舗URLを収集
    2. 各店舗の /map/ ページに遷移してテーブル形式の店舗情報を抽出
    3. ページ上限 334 まで巡回 (ぐるなびのページング上限仕様)

実行方法:
    python scripts/sites/food/gnavi.py
    python bin/run_flow.py --site-id gnavi
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

_SHOP_URL_PATTERN = re.compile(r"^https://r\.gnavi\.co\.jp/[0-9a-z]+/?$")
_ZIP_PATTERN = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|"
    r"三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_WS_PATTERN = re.compile(r"\s+")


class GnaviScraper(StaticCrawler):
    """ぐるなび 飲食店情報スクレイパー"""

    DELAY = 1.5
    MAX_PAGE = 334  # ぐるなび全国一覧のページング上限 (2026-04 時点)
    EXTRA_COLUMNS = ["アクセス", "駐車場", "エリア", "業態"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        shop_urls = self._collect_shop_urls(url)
        self.total_items = len(shop_urls)
        self.logger.info("店舗URL収集完了: %d 件", len(shop_urls))
        for shop_url in shop_urls:
            item = self._scrape_detail(shop_url)
            if item:
                yield item

    def _collect_shop_urls(self, base_url: str) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        for page in range(1, self.MAX_PAGE + 1):
            page_url = base_url if page == 1 else f"{base_url}?p={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                self.logger.warning("一覧ページ取得失敗: %s", page_url)
                break
            articles = soup.select("article")
            if not articles:
                self.logger.info("ページ %d で一覧アイテムなし、終了", page)
                break
            new_count = 0
            for art in articles:
                a = art.select_one('a[href*="r.gnavi.co.jp/"]')
                if not a:
                    continue
                href = a.get("href", "").strip()
                if not _SHOP_URL_PATTERN.match(href):
                    continue
                if href in seen:
                    continue
                seen.add(href)
                urls.append(href)
                new_count += 1
            self.logger.info(
                "ページ %d/%d: 新規 %d 件 (累計 %d 件)",
                page, self.MAX_PAGE, new_count, len(urls),
            )
            if new_count == 0:
                break
        return urls

    def _scrape_detail(self, shop_url: str) -> dict | None:
        map_url = shop_url.rstrip("/") + "/map/"
        soup = self.get_soup(map_url)
        if soup is None:
            return None

        data: dict = {Schema.URL: shop_url}

        name_el = soup.select_one("#info-name")
        if name_el:
            data[Schema.NAME] = name_el.get_text(strip=True)

        kana_el = soup.select_one("#info-kana")
        if kana_el:
            data[Schema.NAME_KANA] = kana_el.get_text(strip=True)

        adr_el = soup.select_one("p.adr")
        if adr_el:
            adr_text = adr_el.get_text(" ", strip=True)
            zip_match = _ZIP_PATTERN.search(adr_text)
            if zip_match:
                data[Schema.POST_CODE] = zip_match.group(1)
            region = adr_el.select_one(".region")
            locality = adr_el.select_one(".locality")
            parts = []
            if region:
                parts.append(region.get_text(strip=True))
            if locality:
                parts.append(locality.get_text(strip=True))
            full_addr = " ".join(p for p in parts if p)
            if full_addr:
                pref_match = _PREF_PATTERN.match(full_addr)
                if pref_match:
                    data[Schema.PREF] = pref_match.group(1)
                    data[Schema.ADDR] = full_addr[pref_match.end():].strip()
                else:
                    data[Schema.ADDR] = full_addr

        th_td = {}
        for th in soup.find_all("th"):
            key = th.get_text(strip=True)
            td = th.find_next_sibling("td")
            if td is None:
                continue
            th_td[key] = td

        if "電話番号" in th_td:
            tel_var = th_td["電話番号"].find("var")
            if tel_var:
                data[Schema.TEL] = tel_var.get_text(strip=True)
            else:
                tel_text = th_td["電話番号"].get_text(" ", strip=True)
                m = re.search(r"0\d[\d\-]{7,12}", tel_text)
                if m:
                    data[Schema.TEL] = m.group(0)

        label_to_schema = {
            "営業時間": Schema.TIME,
            "定休日": Schema.HOLIDAY,
            "ジャンル": Schema.CAT_SITE,
        }
        for label, schema_key in label_to_schema.items():
            if label in th_td:
                data[schema_key] = self._clean(th_td[label].get_text(" ", strip=True))

        extra_labels = {
            "アクセス": "アクセス",
            "駐車場": "駐車場",
            "エリア": "エリア",
            "業態": "業態",
        }
        for label, col in extra_labels.items():
            if label in th_td:
                data[col] = self._clean(th_td[label].get_text(" ", strip=True))

        if not data.get(Schema.NAME):
            return None
        return data

    @staticmethod
    def _clean(text: str) -> str:
        return _WS_PATTERN.sub(" ", text).strip()


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = GnaviScraper()
    scraper.execute("https://r.gnavi.co.jp/area/jp/rs/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
