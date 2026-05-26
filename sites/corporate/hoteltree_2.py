"""
ホテルツリー (hoteltree_2) — 全国のホテル運営会社情報 (HTML SSR 経由)

取得対象:
    - hoteltree 掲載の運営会社情報 (約170社)
    - 各社ページの会社概要 (基本情報) + 運営ホテル一覧 + ホテル施設数

取得フロー:
    1. sitemap-dynamic-company-s--c-slug.xml から会社ページ URL を列挙する
    2. 各 /company/{slug} を取得し、SSR で埋め込まれた `__NUXT_DATA__` JSON を解析する
    3. 会社レベルのフィールドと運営ホテル名一覧 (および施設数) を取り出す

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
COMPANY_SITEMAP = f"{SITE_BASE}/sitemap-dynamic/sitemap-dynamic-company-s--c-slug.xml"

# Studio.Design CMS の難読化フィールドキー (Phase 1 の SSR JSON 解析で確認)
F_TITLE = "title"
F_SLUG = "slug"
F_AVATAR = "avatar"
F_ADDRESS = "NMWeIjIS"
F_HP = "PHopKI0m"
F_EMPLOYEES = "OtZ3y23v"
F_FOUNDED = "o3SJjMws"
F_LOB = "f77UI4Ji"
F_CATEGORY = "heqs66JP"
F_AVG_AGE = "NS7gxE7N"
F_TURNOVER = "ys9DTWR2"
F_HOTELS_TEXT = "uiG5giJU"
F_HOTELS_REF = "OQrPBubl"

_NUXT_RE = re.compile(
    r'<script[^>]*id="__NUXT_DATA__"[^>]*>(.*?)</script>', re.S
)

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

_POSTCODE_PATTERN = re.compile(r"〒\s*(\d{3})\s*-?\s*(\d{4})")
_BR_RE = re.compile(r"<br\s*/?>", re.I)


class HotelTree2Scraper(StaticCrawler):
    """ホテルツリー 運営会社スクレイパー (HTML SSR 経由)"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "スラッグ",
        "ロゴURL",
        "平均年齢",
        "離職率",
        "運営ホテル",
        "ホテル施設数",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        company_urls = self._fetch_company_urls()
        if not company_urls:
            self.logger.error("sitemap から会社 URL を取得できませんでした")
            return

        self.total_items = len(company_urls)
        self.logger.info("会社ページ総数: %d", len(company_urls))

        for company_url in company_urls:
            try:
                item = self._scrape_company(company_url)
            except Exception as e:
                self.logger.warning("会社ページ解析エラー: %s — %s", company_url, e)
                continue
            if item and item.get(Schema.NAME):
                yield item

    def _fetch_company_urls(self) -> list[str]:
        self.logger.info("sitemap 取得: %s", COMPANY_SITEMAP)
        resp = self.session.get(COMPANY_SITEMAP, timeout=self.TIMEOUT)
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        urls = [loc.text.strip() for loc in root.findall(".//sm:loc", ns) if loc.text]
        return [u for u in urls if "/company/" in u]

    def _scrape_company(self, url: str) -> dict | None:
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

        company = self._resolve_company(data)
        if not company:
            return None

        return self._build_item(url, data, company)

    @staticmethod
    def _resolve_company(data: list) -> dict | None:
        """`__NUXT_DATA__` の中から `dynamicDatacompany/{slug}` 配下の会社オブジェクトを探す。"""
        if not isinstance(data, list) or len(data) < 4:
            return None
        # data[0]=['ShallowReactive', 1], data[1]={..., 'data': 2}, data[2]=['ShallowReactive', 3]
        # data[3]={'dynamicDatacompany/{slug}': N}
        outer = data[3] if isinstance(data[3], dict) else None
        if not outer:
            return None
        for key, idx in outer.items():
            if not key.startswith("dynamicDatacompany"):
                continue
            if not isinstance(idx, int) or idx >= len(data):
                continue
            target = data[idx]
            if isinstance(target, dict):
                return target
        return None

    def _build_item(self, url: str, data: list, company: dict) -> dict:
        def s(key: str) -> str:
            return self._resolve_str(data, company.get(key))

        record: dict = {Schema.URL: url}

        name = s(F_TITLE)
        if not name:
            return {}
        record[Schema.NAME] = name

        self._parse_address(s(F_ADDRESS), record)

        hp = s(F_HP)
        if hp and hp != "-":
            record[Schema.HP] = hp

        emp = s(F_EMPLOYEES)
        if emp and emp != "-":
            record[Schema.EMP_NUM] = emp

        founded = s(F_FOUNDED)
        if founded and founded != "-":
            record[Schema.OPEN_DATE] = founded

        lob = s(F_LOB)
        if lob and lob != "-":
            record[Schema.LOB] = self._clean_br(lob)

        category = self._resolve_first_ref_title(data, company.get(F_CATEGORY))
        if category:
            record[Schema.CAT_SITE] = category

        slug = s(F_SLUG)
        if slug:
            record["スラッグ"] = slug

        avatar = s(F_AVATAR)
        if avatar:
            record["ロゴURL"] = avatar

        age = s(F_AVG_AGE)
        if age and age != "-":
            record["平均年齢"] = age

        turnover = s(F_TURNOVER)
        if turnover and turnover != "-":
            record["離職率"] = turnover

        hotels = self._resolve_hotel_names(data, company)
        if hotels:
            record["運営ホテル"] = " / ".join(hotels)
            record["ホテル施設数"] = str(len(hotels))

        return record

    @staticmethod
    def _resolve_str(data: list, idx: Any) -> str:
        if not isinstance(idx, int) or idx < 0 or idx >= len(data):
            return ""
        v = data[idx]
        return v.strip() if isinstance(v, str) else ""

    @staticmethod
    def _resolve_first_ref_title(data: list, idx: Any) -> str:
        """カテゴリ参照配列 [n1, n2, ...] の先頭から title を取り出す。"""
        if not isinstance(idx, int) or idx < 0 or idx >= len(data):
            return ""
        arr = data[idx]
        if not isinstance(arr, list):
            return ""
        for elem in arr:
            if not isinstance(elem, int) or elem >= len(data):
                continue
            ref = data[elem]
            if not isinstance(ref, dict):
                continue
            title_idx = ref.get("title")
            if isinstance(title_idx, int) and title_idx < len(data):
                v = data[title_idx]
                if isinstance(v, str) and v.strip():
                    return v.strip()
        return ""

    def _resolve_hotel_names(self, data: list, company: dict) -> list[str]:
        """運営ホテル名のリストを返す。OQrPBubl (構造化参照) を優先し、無ければ uiG5giJU テキストを分解。"""
        names: list[str] = []
        ref_idx = company.get(F_HOTELS_REF)
        if isinstance(ref_idx, int) and 0 <= ref_idx < len(data):
            arr = data[ref_idx]
            if isinstance(arr, list):
                for elem in arr:
                    if not isinstance(elem, int) or elem >= len(data):
                        continue
                    hotel = data[elem]
                    if not isinstance(hotel, dict):
                        continue
                    t_idx = hotel.get("title")
                    if isinstance(t_idx, int) and t_idx < len(data):
                        v = data[t_idx]
                        if isinstance(v, str) and v.strip():
                            names.append(v.strip())

        if names:
            return names

        # フォールバック: テキストフィールドを <br> で分解
        text = self._resolve_str(data, company.get(F_HOTELS_TEXT))
        if not text or text == "-":
            return []
        for raw in _BR_RE.split(text):
            n = re.sub(r"^[・●■◆※\-\s　]+", "", raw).strip()
            if n:
                names.append(n)
        return names

    @staticmethod
    def _clean_br(text: str) -> str:
        parts = [p.strip(" 　・") for p in _BR_RE.split(text)]
        parts = [p for p in parts if p]
        return " / ".join(parts)

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
