"""
SAVOR JAPAN (savorjapan.com) — レストラン情報スクレイパー

取得対象:
    - 店舗名、住所（日本語）、電話番号、業種・ジャンル

取得フロー:
    検索一覧ページをページネーション → 各詳細ページから情報取得
    東京は23区以外の市のみ取得

対象エリア:
    - 東京都（23区以外の市のみ）
    - 千葉県
    - 神奈川県

実行方法:
    python scripts/sites/portal/savorjapan.py
"""

import json
import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import parse_qs, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

BASE_URL = "https://savorjapan.com"

# 東京23区一覧（フィルタ用）
TOKYO_23_WARDS = {
    "千代田区", "中央区", "港区", "新宿区", "文京区", "台東区", "墨田区",
    "江東区", "品川区", "目黒区", "大田区", "世田谷区", "渋谷区", "中野区",
    "杉並区", "豊島区", "北区", "荒川区", "板橋区", "練馬区", "足立区",
    "葛飾区", "江戸川区",
}

# 日本語住所の都道府県パターン
PREF_RE = re.compile(r"(東京都|北海道|(?:京都|大阪)府|.+?県)")

# 検索対象URL
START_URLS = [
    ("https://savorjapan.com/search?prefecture_codes=13", True),   # 東京（23区以外のみ）
    ("https://savorjapan.com/search?prefecture_codes=12", False),  # 千葉
    ("https://savorjapan.com/search?prefecture_codes=14", False),  # 神奈川
]


class SavorJapanScraper(StaticCrawler):
    """SAVOR JAPAN レストランスクレイパー"""

    DELAY = 1.0

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_ids: set[str] = set()

        for search_url, filter_tokyo in START_URLS:
            self.logger.info("=== エリア開始: %s ===", search_url)
            yield from self._scrape_search(search_url, seen_ids, filter_tokyo)

    # ------------------------------------------------------------------
    # 一覧ページ処理
    # ------------------------------------------------------------------

    def _scrape_search(
        self, search_url: str, seen_ids: set[str], filter_non_23ward: bool
    ) -> Generator[dict, None, None]:
        """検索結果をページネーションしながら詳細ページURLを収集・スクレイピング"""
        page = 1

        while True:
            current_url = search_url if page == 1 else f"{search_url}&page={page}"
            self.logger.info("一覧ページ取得 (p.%d): %s", page, current_url)

            soup = self.get_soup(current_url)
            if soup is None:
                break

            # 詳細ページURLを抽出（10桁の数字ID）
            detail_urls: list[str] = []
            for a_tag in soup.find_all("a", href=True):
                match = re.search(r"/(\d{10})$", a_tag["href"])
                if match and match.group(1) not in seen_ids:
                    seen_ids.add(match.group(1))
                    detail_urls.append(f"{BASE_URL}/{match.group(1)}")

            if not detail_urls:
                self.logger.info("詳細リンクなし — ページネーション終了")
                break

            self.logger.info("詳細ページ数: %d", len(detail_urls))

            for detail_url in detail_urls:
                time.sleep(self.DELAY)
                item = self._scrape_detail(detail_url, filter_non_23ward)
                if item:
                    yield item

            # 次のページがあるか確認
            next_link = soup.find("a", attrs={"title": "pagination-next"})
            if next_link and next_link.get("href"):
                page += 1
            else:
                break

    # ------------------------------------------------------------------
    # 詳細ページ処理
    # ------------------------------------------------------------------

    def _scrape_detail(self, url: str, filter_non_23ward: bool) -> dict | None:
        """詳細ページからレストラン情報を取得"""
        soup = self.get_soup(url)
        if soup is None:
            return None

        item: dict[str, str] = {Schema.URL: url}

        # 1. JSON-LD から基本情報を取得
        self._parse_json_ld(soup, item)

        # 2. 日本語住所を取得（タクシー用住所セクション）
        jp_addr = self._find_japanese_address(soup)
        if jp_addr:
            pref_match = PREF_RE.match(jp_addr)
            if pref_match:
                item[Schema.PREF] = pref_match.group(1)
                item[Schema.ADDR] = jp_addr[len(pref_match.group(1)):]
            else:
                item[Schema.ADDR] = jp_addr

            # 東京23区フィルタ（23区以外は市・町・村すべて取得）
            if filter_non_23ward:
                addr_body = item.get(Schema.ADDR, "")
                if any(ward in addr_body for ward in TOKYO_23_WARDS):
                    return None
        elif filter_non_23ward:
            return None

        # 3. 電話番号（HTML の tel: リンクを優先）
        self._extract_phone(soup, item)

        # 4. TELが取れなかった場合、hitosara.com から問い合わせ番号を取得
        if Schema.TEL not in item:
            self._extract_phone_from_hitosara(soup, url, item)

        if Schema.NAME not in item:
            return None

        return item

    def _parse_json_ld(self, soup, item: dict) -> None:
        """JSON-LD 構造化データから情報を抽出"""
        for script in soup.select('script[type="application/ld+json"]'):
            raw = script.string or script.get_text(strip=True)
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue

            # リストの場合は中身を展開
            candidates = payload if isinstance(payload, list) else [payload]
            for data in candidates:
                if not isinstance(data, dict) or data.get("@type") != "Restaurant":
                    continue

                # 店舗名（alternateName は日本語名の場合がある）
                alt_name = (data.get("alternateName") or "").strip()
                name = (data.get("name") or "").strip()
                item[Schema.NAME] = alt_name or name

                # ジャンル
                cuisines = data.get("servesCuisine", [])
                if cuisines:
                    if isinstance(cuisines, list):
                        item[Schema.CAT_SITE] = ", ".join(cuisines)
                    else:
                        item[Schema.CAT_SITE] = str(cuisines)

                # 都道府県（フォールバック用）
                addr_data = data.get("address", {})
                if isinstance(addr_data, dict):
                    region = addr_data.get("addressRegion", "")
                    if region:
                        item.setdefault(Schema.PREF, region)

                return

    def _find_japanese_address(self, soup) -> str:
        """ページ内から日本語住所テキストを検索"""
        # 全テキストノードから日本語住所パターンにマッチするものを探す
        prefectures = "東京都|北海道|京都府|大阪府|" + "|".join(
            f"{p}県" for p in [
                "青森", "岩手", "宮城", "秋田", "山形", "福島",
                "茨城", "栃木", "群馬", "埼玉", "千葉", "神奈川",
                "新潟", "富山", "石川", "福井", "山梨", "長野",
                "岐阜", "静岡", "愛知", "三重", "滋賀", "兵庫",
                "奈良", "和歌山", "鳥取", "島根", "岡山", "広島",
                "山口", "徳島", "香川", "愛媛", "高知", "福岡",
                "佐賀", "長崎", "熊本", "大分", "宮崎", "鹿児島", "沖縄",
            ]
        )
        pattern = re.compile(rf"({prefectures})[^\s\n]{{3,60}}")

        for text_node in soup.find_all(string=pattern):
            match = pattern.search(text_node)
            if match:
                return match.group(0).strip()

        return ""

    def _extract_phone(self, soup, item: dict) -> None:
        """tel: リンクから電話番号を取得（店舗電話を優先、ヘルプデスク番号をスキップ）"""
        tel_links = soup.find_all("a", href=re.compile(r"^tel:"))
        phones: list[str] = []
        for tel_link in tel_links:
            text = tel_link.get_text(strip=True)
            phone_match = re.search(r"(0\d{1,4}-\d{1,4}-\d{3,4})", text)
            if phone_match:
                number = phone_match.group(1)
                if number not in phones:
                    phones.append(number)

        # サイトのヘルプデスク番号（050-2030-*）を除外し、店舗電話を優先
        shop_phones = [p for p in phones if not p.startswith("050-2030-")]
        if shop_phones:
            item[Schema.TEL] = shop_phones[0]
        elif phones:
            item[Schema.TEL] = phones[0]

    def _extract_phone_from_hitosara(self, soup, savor_url: str, item: dict) -> None:
        """hitosara.com の詳細ページから問い合わせ専用番号を取得する。
        予約専門番号（050-*）は除外し、一般の問い合わせ番号のみ取得する。
        """
        # SavorJapan ページから hitosara リンクを探す
        hitosara_url = None
        for a_tag in soup.find_all("a", href=re.compile(r"hitosara\.com/\d{10}")):
            hitosara_url = a_tag["href"].rstrip("/") + "/"
            break

        # リンクが見つからない場合、URLのIDから推測
        if not hitosara_url:
            id_match = re.search(r"/(\d{10})$", savor_url)
            if id_match:
                hitosara_url = f"https://hitosara.com/{id_match.group(1)}/"
            else:
                return

        self.logger.debug("hitosara から TEL 取得: %s", hitosara_url)
        time.sleep(self.DELAY)
        h_soup = self.get_soup(hitosara_url)
        if h_soup is None:
            return

        # hitosara ページ内の電話番号パターンをすべて収集
        phone_re = re.compile(r"0\d{1,4}-\d{1,4}-\d{3,4}")
        all_phones: list[str] = []

        # tel: リンクから取得
        for tel_link in h_soup.find_all("a", href=re.compile(r"^tel:")):
            text = tel_link.get_text(strip=True)
            m = phone_re.search(text)
            if m and m.group() not in all_phones:
                all_phones.append(m.group())

        # テキストノードからも取得（tel:リンクがない場合のフォールバック）
        if not all_phones:
            for text_node in h_soup.find_all(string=phone_re):
                for m in phone_re.finditer(text_node):
                    if m.group() not in all_phones:
                        all_phones.append(m.group())

        # 050 番号（予約専門）を除外し、問い合わせ番号を取得
        inquiry_phones = [p for p in all_phones if not p.startswith("050-")]
        if inquiry_phones:
            item[Schema.TEL] = inquiry_phones[0]


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = SavorJapanScraper()
    scraper.execute(START_URLS[0][0])

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
