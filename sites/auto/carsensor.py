"""
カーセンサー — 中古車販売店一覧スクレイパー

取得対象:
    - 全国 47 都道府県の中古車販売店

取得フロー:
    1. 都道府県ごとに `/shop/{pref}/index.html` (1ページ目) を取得します
    2. ページャから最終ページ番号を取得し、`/shop/{pref}/{N}/index.html` を巡回
    3. 一覧 `.caset.caset--shopAll` から店舗カード情報 + 詳細URL を取得
    4. 詳細ページ `/shop/{pref}/{shop_id}/` から `.shopnaviHeader__contents__spec` の
       法人名 / 住所 / 営業時間 / 定休日、在庫の本体価格（単価）、telno スクリプト変数のTELを抽出
    5. 各種サービスページ `/shop/{pref}/{shop_id}/service/` から支払い方法に関する記述を抽出

実行方法:
    # ローカルテスト (1都道府県のみ確認したい場合は引数で渡す)
    python scripts/sites/auto/carsensor.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id carsensor
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


_BASE = "https://www.carsensor.net"

_PREFECTURES = [
    ("hokkaido", "北海道"),
    ("aomori", "青森県"), ("iwate", "岩手県"), ("miyagi", "宮城県"),
    ("akita", "秋田県"), ("yamagata", "山形県"), ("fukushima", "福島県"),
    ("niigata", "新潟県"), ("toyama", "富山県"), ("ishikawa", "石川県"),
    ("fukui", "福井県"), ("yamanashi", "山梨県"), ("nagano", "長野県"),
    ("tokyo", "東京都"), ("kanagawa", "神奈川県"), ("saitama", "埼玉県"),
    ("chiba", "千葉県"), ("ibaraki", "茨城県"), ("tochigi", "栃木県"),
    ("gunma", "群馬県"),
    ("osaka", "大阪府"), ("hyogo", "兵庫県"), ("kyoto", "京都府"),
    ("shiga", "滋賀県"), ("nara", "奈良県"), ("wakayama", "和歌山県"),
    ("aichi", "愛知県"), ("gifu", "岐阜県"), ("shizuoka", "静岡県"),
    ("mie", "三重県"),
    ("tottori", "鳥取県"), ("shimane", "島根県"), ("okayama", "岡山県"),
    ("hiroshima", "広島県"), ("yamaguchi", "山口県"),
    ("tokushima", "徳島県"), ("kagawa", "香川県"), ("ehime", "愛媛県"),
    ("kouchi", "高知県"),
    ("fukuoka", "福岡県"), ("saga", "佐賀県"), ("kumamoto", "熊本県"),
    ("ooita", "大分県"), ("nagasaki", "長崎県"), ("miyazaki", "宮崎県"),
    ("kagoshima", "鹿児島県"), ("okinawa", "沖縄県"),
]

_TEL_PATTERN = re.compile(r'telno\s*=\s*"tel:([\d\-]+)"')
_LAST_PAGE_PATTERN = re.compile(r"/shop/[a-z]+/(\d+)/index\.html")
_ADDR_TRIM = re.compile(r"\s*MAP\s*$")
_PRICE_WS = re.compile(r"\s+")
_PAY_KEYWORDS = re.compile(r"支払|ローン|分割|クレジット|カード|頭金|現金|振込")

# 文字化けの原因となる不可視文字（NBSP・全角スペース・各種 Unicode 空白・ゼロ幅文字・BOM）を
# 半角スペースへ正規化するためのパターン。
_INVISIBLE_WS = re.compile(
    "[\u00a0\u1680\u2000-\u200d\u202f\u205f\u2028\u2029\u3000\ufeff]"
)
# タブ・改行を除く制御文字（除去対象）
_CONTROL_CHARS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
# 連続する空白を 1 つにまとめるためのパターン
_MULTI_WS = re.compile(r"\s+")


class CarsensorScraper(StaticCrawler):
    """カーセンサー 中古車販売店スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["法人名", "エリア", "キャッチコピー", "単価"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        for pref_slug, pref_name in _PREFECTURES:
            yield from self._scrape_prefecture(pref_slug, pref_name)

    def _scrape_prefecture(self, pref_slug: str, pref_name: str) -> Generator[dict, None, None]:
        first_url = f"{_BASE}/shop/{pref_slug}/index.html"
        soup = self.get_soup(first_url)
        if soup is None:
            self.logger.warning("一覧取得失敗: %s", first_url)
            return

        last_page = 1
        for a in soup.select(".paging a"):
            href = a.get("href", "")
            m = _LAST_PAGE_PATTERN.search(href)
            if m:
                last_page = max(last_page, int(m.group(1)))

        self.logger.info("[%s] 全%dページ", pref_name, last_page)

        for page in range(1, last_page + 1):
            page_url = first_url if page == 1 else f"{_BASE}/shop/{pref_slug}/{page}/index.html"
            page_soup = soup if page == 1 else self.get_soup(page_url)
            if page_soup is None:
                self.logger.warning("ページ取得失敗: %s", page_url)
                continue

            cards = page_soup.select(".caset.caset--shopAll")
            if not cards:
                break

            for card in cards:
                listing = self._parse_listing(card, pref_name)
                if not listing:
                    continue
                detail = self._scrape_detail(listing["__detail_url"])
                merged = {**listing, **(detail or {})}
                merged.pop("__detail_url", None)
                if merged.get(Schema.NAME):
                    yield merged

    def _parse_listing(self, card, pref_name: str) -> dict | None:
        name_a = card.select_one("h3.hd2nd a, h3 a")
        if not name_a:
            return None
        name = self._clean_text(name_a.get_text(strip=True))
        href = name_a.get("href", "")
        detail_url = urljoin(_BASE, href.split("?")[0])

        area = ""
        area_p = card.select_one("div.l-box.va-mid p.txt.txt-c")
        if area_p:
            parts = [self._clean_text(t) for t in area_p.get_text("|", strip=True).split("|")]
            parts = [t for t in parts if t]
            area = parts[1] if len(parts) >= 2 else (parts[0] if parts else "")

        catch = ""
        catch_p = card.select_one("p.ttl")
        if catch_p:
            catch = self._clean_text(catch_p.get_text(strip=True))

        rev_count = ""
        rev_a = card.select_one("a.daisu")
        if rev_a:
            rev_count = rev_a.get_text(strip=True)

        score = ""
        score_b = card.select_one("td .numS b")
        if score_b:
            score = score_b.get_text(strip=True)

        return {
            "__detail_url": detail_url,
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref_name,
            Schema.REV_SCR: rev_count,
            Schema.SCORES: score,
            "エリア": area,
            "キャッチコピー": catch,
        }

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {}
        dl = soup.select_one(".shopnaviHeader__contents__spec")
        if dl:
            dts = dl.find_all("dt")
            dds = dl.find_all("dd")
            for dt, dd in zip(dts, dds):
                label = self._clean_text(dt.get_text(strip=True))
                value = self._clean_text(dd.get_text(" ", strip=True))
                if label == "法人名":
                    data["法人名"] = value
                elif label == "住所":
                    data[Schema.ADDR] = self._clean_text(_ADDR_TRIM.sub("", value))
                elif label == "営業時間":
                    data[Schema.TIME] = value
                elif label == "定休日":
                    data[Schema.HOLIDAY] = value

        for script in soup.find_all("script"):
            text = script.string or ""
            m = _TEL_PATTERN.search(text)
            if m:
                data[Schema.TEL] = m.group(1)
                break

        unit_prices = self._extract_unit_prices(soup)
        if unit_prices:
            data["単価"] = unit_prices

        payments = self._scrape_payments(url)
        if payments:
            data[Schema.PAYMENTS] = payments

        return data

    @staticmethod
    def _clean_text(text: str) -> str:
        """文字を含むカラム用のクリーニング。

        スクレイピングで取得した文字列には、HTML 中の NBSP (\\xa0) や全角スペース、
        ゼロ幅文字・BOM などの不可視文字が混入しており、CSV 等へ出力した際に
        文字化け（読めない記号）として現れる。これらを半角スペースへ正規化し、
        制御文字を除去したうえで連続空白を 1 つにまとめて前後をトリムする。
        """
        if not text:
            return ""
        cleaned = _INVISIBLE_WS.sub(" ", str(text))
        cleaned = _CONTROL_CHARS.sub("", cleaned)
        cleaned = _MULTI_WS.sub(" ", cleaned)
        return cleaned.strip()

    @staticmethod
    def _normalize_price(text: str) -> str:
        return _PRICE_WS.sub("", text.strip())

    def _extract_unit_prices(self, soup) -> str:
        prices: list[str] = []
        seen: set[str] = set()
        for dd in soup.select(".shopnaviCarStockItem__basePrice dd"):
            price = self._normalize_price(dd.get_text("", strip=True))
            if not price or price in seen:
                continue
            seen.add(price)
            prices.append(price)
        return " / ".join(prices)

    def _scrape_payments(self, shop_url: str) -> str:
        service_url = shop_url.rstrip("/") + "/service/"
        soup = self.get_soup(service_url)
        if soup is None:
            return ""

        parts: list[str] = []
        seen: set[str] = set()
        for p in soup.select(".shopnaviContents__item .media__obj--col3 p"):
            text = self._clean_text(p.get_text(strip=True))
            if not text or text in seen or not _PAY_KEYWORDS.search(text):
                continue
            seen.add(text)
            parts.append(text)
        return " / ".join(parts)


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = CarsensorScraper()
    scraper.execute(f"{_BASE}/shop/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
