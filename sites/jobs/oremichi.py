# scripts/sites/jobs/oremichi.py
"""
俺の風 (oremichi.com) — 風俗男性求人スクレイパー

取得対象:
    - 全国の掲載店舗 (約1,787件, sitemap.shop.xml 経由)

取得フロー:
    sitemap.shop.xml → 各詳細ページをスクレイピング

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/oremichi.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id oremichi
"""

import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

SITEMAP_URL = "https://www.oremichi.com/sitemap/sitemap.shop.xml"

# 都道府県から始まる住所を特定・抽出するための正規表現パターン
_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)[^・\n<]+"
)


class OremichiScraper(StaticCrawler):
    """俺の風 風俗男性求人スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["Youtube"]
    _test_limit: int | None = None  # テスト時にオーバーライド

    def parse(self, url: str):
        shop_urls = self._collect_shop_urls(url)
        self.total_items = len(shop_urls)
        self.logger.info("店舗URL収集完了: %d 件", len(shop_urls))

        if self._test_limit:
            shop_urls = shop_urls[: self._test_limit]

        for shop_url in shop_urls:
            try:
                item = self._scrape_detail(shop_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("詳細ページ取得失敗: %s (%s)", shop_url, e)

    def _collect_shop_urls(self, sitemap_url: str) -> list[str]:
        """sitemap.shop.xml から全店舗URLを取得"""
        resp = self.session.get(sitemap_url, timeout=self.TIMEOUT)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        urls = []
        for el in root.iter():
            if el.tag.endswith("loc") and el.text:
                u = el.text.strip()
                if "/shop/" in u and u.endswith(".html"):
                    urls.append(u)
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)

        profile = soup.select_one("div.shop_databox dl.profile")
        if not profile:
            return None

        item: dict = {Schema.URL: url}

        # --- dt/dd ペアをパース ---
        current_dt = None
        for child in profile.find_all(["dt", "dd"], recursive=False):
            if child.name == "dt":
                current_dt = child.get_text(strip=True)
            elif child.name == "dd" and current_dt:
                self._parse_dd(current_dt, child, item)
                current_dt = None

        # --- 住所/TEL のクレンジング処理 ---
        act_map = profile.select_one("a.act_map")
        if act_map:
            raw_addr = act_map.get("data-address", "").strip()
            if raw_addr:
                # 1. HTMLタグの残骸（<br />など）を文字列置換で完全に消去
                clean_raw = re.sub(r"<[^>]+>", " ", raw_addr)

                # 2. 正規表現で都道府県から始まる「純粋な住所部分」だけを前方からサーチ
                match = _PREF_RE.search(clean_raw)
                if match:
                    # アクセス情報（「・京急線」など）が後ろに混ざるため、・ や 改行 で分断して前半部分のみをキープ
                    full_address = match.group(0).split("・")[0].split("\n")[0].strip()

                    # 都道府県名を取り出して定数にセット
                    item[Schema.PREF] = match.group(1)
                    # 都道府県名を含んだ「完全な住所」をそのまま Schema.ADDR に格納（ビル名だけになるのを防止）
                    item[Schema.ADDR] = full_address
                else:
                    # 都道府県がどうしても見つからない場合のセーフティ
                    item[Schema.ADDR] = clean_raw.split("・")[0].strip()

            phone = act_map.get("data-phone1", "").strip()
            if phone:
                item[Schema.TEL] = phone

        if not item.get(Schema.NAME):
            return None
        return item

    def _parse_dd(self, label: str, dd, item: dict) -> None:
        """dl.profile の各 dt ラベルに応じて dd から値を抽出する"""
        if "店舗" in label and "企業" in label:
            kana_el = dd.select_one("span.shop_kana")
            kana = ""
            if kana_el:
                kana = kana_el.get_text(strip=True)
                kana_el.extract()
            item[Schema.NAME] = dd.get_text(strip=True)
            if kana:
                item[Schema.NAME_KANA] = kana

        elif label == "業種":
            item[Schema.CAT_SITE] = dd.get_text(strip=True)

        elif label == "事業内容":
            text = dd.get_text("\n", strip=True)
            item[Schema.LOB] = re.sub(r"\n{2,}", "\n", text)

        elif label == "求人用HP":
            a = dd.select_one("a[href]")
            if a:
                href = a.get("href", "").strip()
                if href and href.startswith("http"):
                    item[Schema.HP] = href

        elif label == "SNS":
            for a in dd.select("a[href]"):
                img = a.select_one("img[alt]")
                if not img:
                    continue
                alt = img.get("alt", "").strip().lower()
                href = a.get("href", "").strip()
                if not href:
                    continue
                if alt == "x":
                    item[Schema.X] = href
                elif alt == "instagram":
                    item[Schema.INSTA] = href
                elif alt == "tiktok":
                    item[Schema.TIKTOK] = href
                elif alt == "facebook":
                    item[Schema.FB] = href
                elif alt == "line":
                    item[Schema.LINE] = href
                elif alt == "youtube":
                    item[Schema.YOUTUBE] = href  # 共通定数があれば割り当て、なければEXTRA_COLUMNSに紐づきます


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = OremichiScraper()
    # 動作テスト用に最初の5件だけで止めたい場合は、下のコメントアウトを解除してください
    # scraper._test_limit = 5
    scraper.execute(SITEMAP_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")