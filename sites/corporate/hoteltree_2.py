"""
ホテルツリー (hoteltree_2) — 全国のホテル施設情報 (HTML SSR 経由)

取得対象:
    - hoteltree 掲載のホテル/旅館/ゲストハウス (約400施設)
    - 各 /hotel/{slug} ページの施設名・住所・公式サイト・開業日 など

取得フロー:
    1. sitemap-dynamic-hotel-s--c-slug.xml からホテルページ URL を列挙する
    2. 各 /hotel/{slug} を取得し、SSR で埋め込まれた `__NUXT_DATA__` JSON を解析する
    3. ホテルレベルのフィールドを取り出す

備考:
    旧版 (~2026-04 まで) は /company/{slug} に運営会社情報が掲載されていたが、
    サイトリニューアル後に company スキーマは廃止され、施設単位の構成に変更された。
    そのため本スクリプトもホテル施設情報の取得に変更している。

実行方法:
    python scripts/sites/corporate/hoteltree_2.py
    python bin/run_flow.py --site-id hoteltree_2
"""

import json
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


SITE_BASE = "https://hoteltree.jp"
HOTEL_SITEMAP = f"{SITE_BASE}/sitemap-dynamic/sitemap-dynamic-hotel-s--c-slug.xml"

# Studio.Design CMS の難読化フィールドキー (リニューアル後 SSR JSON を解析して確認)
F_TITLE = "title"
F_SLUG = "slug"
F_AVATAR = "avatar"
F_HEADLINE = "x5BGE5AG"     # キャッチコピー
F_OPEN_DATE = "S4tOlr07"    # 開業日 (例: "2021年3月16日")
F_INSTAGRAM = "J7saqAXd"    # Instagram URL
F_HP = "ID_IRiFT"           # 公式サイト URL
F_ADDRESS = "F6bHOoaY"      # 〒 + 住所 (<br> で改行)
F_DESCRIPTION = "IoJ8liKM"  # 施設説明文 (HTML)
F_SUB_DESC = "xWOdUZz8"     # 補足説明 (任意)

_POSTCODE_PATTERN = re.compile(r"〒\s*(\d{3})\s*-?\s*(\d{4})")
_PREF_PATTERN = re.compile(
    r"^(北海道|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_BR_RE = re.compile(r"<br\s*/?>", re.I)
_TAG_RE = re.compile(r"<[^>]+>")
_DATE_RE = re.compile(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日")


class HotelTree2Scraper(StaticCrawler):
    """ホテルツリー 施設情報スクレイパー (HTML SSR 経由)"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "スラッグ",
        "ロゴURL",
        "キャッチコピー",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        hotel_urls = self._fetch_hotel_urls()
        if not hotel_urls:
            self.logger.error("sitemap からホテル URL を取得できませんでした")
            return

        self.total_items = len(hotel_urls)
        self.logger.info("ホテルページ総数: %d", len(hotel_urls))

        for hotel_url in hotel_urls:
            try:
                item = self._scrape_hotel(hotel_url)
            except Exception as e:
                self.logger.warning("ホテルページ解析エラー: %s — %s", hotel_url, e)
                continue
            if item and item.get(Schema.NAME):
                yield item

    def _fetch_hotel_urls(self) -> list[str]:
        self.logger.info("sitemap 取得: %s", HOTEL_SITEMAP)
        resp = self.session.get(HOTEL_SITEMAP, timeout=self.TIMEOUT)
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        urls = [loc.text.strip() for loc in root.findall(".//sm:loc", ns) if loc.text]
        return [u for u in urls if "/hotel/" in u]

    def _scrape_hotel(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None
        script = soup.find("script", id="__NUXT_DATA__")
        if script is None or not script.string:
            return None
        try:
            data = json.loads(script.string)
        except json.JSONDecodeError:
            return None

        hotel = self._resolve_hotel(data)
        if not hotel:
            return None

        return self._build_item(url, data, hotel)

    @staticmethod
    def _resolve_hotel(data: list) -> dict | None:
        """`__NUXT_DATA__` の中から `dynamicDatahotel/{slug}` 配下のホテルオブジェクトを探す。"""
        if not isinstance(data, list) or len(data) < 4:
            return None
        outer = data[3] if isinstance(data[3], dict) else None
        if not outer:
            return None
        for key, idx in outer.items():
            if not key.startswith("dynamicDatahotel"):
                continue
            if not isinstance(idx, int) or idx >= len(data):
                continue
            target = data[idx]
            if isinstance(target, dict):
                return target
        return None

    def _build_item(self, url: str, data: list, hotel: dict) -> dict:
        def s(key: str) -> str:
            return self._resolve_str(data, hotel.get(key))

        record: dict = {Schema.URL: url}

        name = s(F_TITLE)
        if not name:
            return {}
        record[Schema.NAME] = name

        self._parse_address(s(F_ADDRESS), record)

        hp = s(F_HP)
        if hp and hp != "-":
            record[Schema.HP] = hp

        insta = s(F_INSTAGRAM)
        if insta and insta != "-":
            record[Schema.INSTA] = insta

        open_date_raw = s(F_OPEN_DATE)
        open_date = self._normalize_date(open_date_raw)
        if open_date:
            record[Schema.OPEN_DATE] = open_date

        description = self._html_to_text(s(F_DESCRIPTION))
        if description:
            record[Schema.DESCRIPTION] = description

        slug = s(F_SLUG)
        if slug:
            record["スラッグ"] = slug

        avatar = s(F_AVATAR)
        if avatar:
            record["ロゴURL"] = avatar

        headline = s(F_HEADLINE)
        if headline:
            record["キャッチコピー"] = _BR_RE.sub(" ", headline).strip()

        return record

    @staticmethod
    def _resolve_str(data: list, idx: Any) -> str:
        if not isinstance(idx, int) or idx < 0 or idx >= len(data):
            return ""
        v = data[idx]
        return v.strip() if isinstance(v, str) else ""

    @staticmethod
    def _normalize_date(value: str) -> str:
        if not value:
            return ""
        m = _DATE_RE.search(value.replace(" ", "").replace("　", ""))
        if not m:
            return ""
        y, mo, d = m.groups()
        return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"

    @staticmethod
    def _html_to_text(value: str) -> str:
        if not value:
            return ""
        text = _BR_RE.sub("\n", value)
        text = _TAG_RE.sub("", text)
        text = re.sub(r"[ \t]+", " ", text)
        return text.strip()

    @staticmethod
    def _parse_address(value: str, record: dict) -> None:
        if not value:
            return
        text = _BR_RE.sub(" ", value).strip()
        text = re.sub(r"\s+", " ", text)

        m = _POSTCODE_PATTERN.search(text)
        if m:
            record[Schema.POST_CODE] = f"{m.group(1)}-{m.group(2)}"
            text = (text[: m.start()] + text[m.end():]).strip()

        normalized = text.replace(" ", "")
        pm = _PREF_PATTERN.match(normalized)
        if pm:
            record[Schema.PREF] = pm.group(1)
            record[Schema.ADDR] = normalized[pm.end():]
        elif text:
            record[Schema.ADDR] = text


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = HotelTree2Scraper()
    scraper.execute(SITE_BASE)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
