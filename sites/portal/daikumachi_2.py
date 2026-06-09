# scripts/sites/portal/daikumachi_2.py
"""
水戸市大工町地区繁華街なび (daikumachi.jp) — 店舗情報スクレイパー v2

取得対象:
    - post-sitemap.xml に列挙される /about_shop/* の詳細ページ群

取得フロー:
    post-sitemap.xml → 詳細URL一覧 → 各詳細ページから店舗情報取得
    （/about_shop/car/* のカテゴリトップ placeholder は table.demo01 が無いためスキップ）

実行方法:
    python scripts/sites/portal/daikumachi_2.py
    python bin/run_flow.py --site-id daikumachi_2
"""

import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

SITEMAP_URL = "https://daikumachi.jp/post-sitemap.xml"

_POST_CODE_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|"
    r"静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|"
    r"奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|"
    r"熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_TEL_RE = re.compile(r"\d{2,4}-\d{2,4}-\d{3,4}")
_TEL_NOISE_RE = re.compile(r"店舗へ電話する|店舗に電話する")


class Daikumachi2Scraper(StaticCrawler):
    """水戸市大工町地区繁華街なび 店舗情報スクレイパー v2"""

    DELAY = 1.5
    EXTRA_COLUMNS = []  # 備考: EXTRA カラムなし（自由記述プローズの混入による著作権リスク回避）

    def parse(self, url: str):
        detail_urls = self._collect_detail_urls()
        self.total_items = len(detail_urls)
        self.logger.info("詳細ページURL収集完了: %d 件", len(detail_urls))

        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("詳細ページ取得失敗: %s (%s)", detail_url, e)
                continue

    def _collect_detail_urls(self) -> list[str]:
        soup = self.get_soup(SITEMAP_URL)
        if soup is None:
            return []
        urls: list[str] = []
        seen: set[str] = set()
        for loc in soup.find_all("loc"):
            href = loc.get_text(strip=True)
            if "/about_shop/" in href and href not in seen:
                seen.add(href)
                urls.append(href)
        return urls

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        table = soup.find("table", class_="demo01")
        if not table:
            self.logger.info("詳細データなし（スキップ）: %s", detail_url)
            return None

        item: dict = {Schema.URL: detail_url}

        # 店名 / カナ
        h4 = soup.find("h4")
        if h4:
            span = h4.find("span")
            kana = span.get_text(strip=True) if span else ""
            if span:
                span.extract()
            name = h4.get_text(strip=True)
            if name:
                item[Schema.NAME] = name
            if kana:
                item[Schema.NAME_KANA] = kana

        # table.demo01 (th/td)
        for tr in table.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            label = th.get_text(strip=True)
            value_text = td.get_text(" ", strip=True)
            if not value_text:
                continue

            if label == "所在地":
                self._parse_address(value_text, item)
            elif label == "営業時間":
                item[Schema.TIME] = value_text
            elif label == "定休日":
                item[Schema.HOLIDAY] = value_text
            elif label == "電話番号":
                cleaned = _TEL_NOISE_RE.sub("", value_text).strip()
                m = _TEL_RE.search(cleaned)
                item[Schema.TEL] = m.group(0) if m else cleaned
            elif label == "メールアドレス":
                item[Schema.EMAIL] = value_text
            elif label in ("ホームページ", "HP", "Webサイト"):
                a = td.find("a", href=True)
                if a:
                    item[Schema.HP] = a["href"]
                elif value_text.startswith("http"):
                    item[Schema.HP] = value_text

        # カテゴリ (breadcrumb 3階層目)
        bc = soup.select_one(".breadcrumbs")
        if bc:
            spans = [s.get_text(strip=True) for s in bc.find_all("span", property="name")]
            if len(spans) >= 3:
                item[Schema.CAT_SITE] = spans[2]

        # SNS リンク (運営公式アカウント daikumachi_navi は除外)
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "daikumachi_navi" in href:
                continue
            if Schema.INSTA not in item and "instagram.com/" in href:
                item[Schema.INSTA] = href
            elif Schema.FB not in item and "facebook.com/" in href:
                item[Schema.FB] = href
            elif Schema.X not in item and re.search(r"(twitter\.com|x\.com)/", href):
                item[Schema.X] = href
            elif Schema.LINE not in item and "line.me" in href:
                item[Schema.LINE] = href
            elif Schema.TIKTOK not in item and "tiktok.com" in href:
                item[Schema.TIKTOK] = href

        if Schema.NAME not in item:
            return None
        return item

    def _parse_address(self, raw: str, item: dict) -> None:
        text = raw.replace("　", " ").strip()

        m = _POST_CODE_RE.search(text)
        if m:
            code = m.group(1)
            if "-" not in code:
                code = f"{code[:3]}-{code[3:]}"
            item[Schema.POST_CODE] = code
            text = _POST_CODE_RE.sub("", text, count=1).strip()

        text = text.lstrip("〒 ").strip()
        m = _PREF_RE.match(text)
        if m:
            item[Schema.PREF] = m.group(1)
            item[Schema.ADDR] = text[m.end():].strip()
        else:
            item[Schema.ADDR] = text


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Daikumachi2Scraper()
    scraper.execute("https://daikumachi.jp/")

    print("\n" + "=" * 60)
    print("実行結果サマリ")
    print("=" * 60)
    print(f"  出力ファイル:     {scraper.output_filepath}")
    print(f"  取得件数:         {scraper.item_count}")
    print(f"  観測カラム数:     {len(scraper.observed_columns)}")
    print(f"  観測カラム:       {scraper.observed_columns}")
    print("=" * 60)
