# scripts/sites/portal/furugi_meguru.py
"""
古着屋巡りマップガイド MEGURU — 全国古着屋・ヴィンテージショップ情報

取得対象:
    - 全国47都道府県の古着屋・ヴィンテージショップ情報（推定 1,500〜3,000件）

取得フロー:
    1. /zenkoku/ から全都道府県エリアスラグを動的取得
    2. /category/area/{slug}/page/N/ を巡回し詳細URLとジャンルタグを収集
    3. 各詳細ページ (/area/{pref}/{id}/ or /area/{id}/) から全フィールドを抽出

実行方法:
    # ローカルテスト
    python scripts/sites/portal/furugi_meguru.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id furugi_meguru
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://furugi-meguru.com"
ZENKOKU_URL = f"{BASE_URL}/zenkoku/"
MAX_PAGES_PER_AREA = 200  # 安全上限（東京は49ページが現状最大）

_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県"
    r"|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県"
    r"|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県"
    r"|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# エリアスラグ → 都道府県名（住所に都道府県が含まれない場合のフォールバック）
_SLUG_TO_PREF = {
    "tokyo": "東京都", "osaka": "大阪府", "nagoya": "愛知県", "fukuoka": "福岡県",
    "hokkaido": "北海道", "kyoto": "京都府", "hyogo": "兵庫県", "aomori": "青森県",
    "iwate": "岩手県", "miyagi": "宮城県", "akita": "秋田県", "yamagata": "山形県",
    "fukushima": "福島県", "ibaraki": "茨城県", "tochigi": "栃木県", "gunma": "群馬県",
    "saitama": "埼玉県", "chiba": "千葉県", "kanagawa": "神奈川県", "niigata": "新潟県",
    "toyama": "富山県", "ishikawa": "石川県", "fukui": "福井県", "yamanashi": "山梨県",
    "nagano": "長野県", "gifu": "岐阜県", "shizuoka": "静岡県", "mie": "三重県",
    "shiga": "滋賀県", "nara": "奈良県", "wakayama": "和歌山県", "tottori": "鳥取県",
    "shimane": "島根県", "okayama": "岡山県", "hiroshima": "広島県", "yamaguchi": "山口県",
    "tokushima": "徳島県", "kagawa": "香川県", "ehime": "愛媛県", "kouchi": "高知県",
    "saga": "佐賀県", "nagasaki": "長崎県", "kumamoto": "熊本県", "ooita": "大分県",
    "miyazaki": "宮崎県", "kagoshima": "鹿児島県", "okinawa": "沖縄県",
}

_SKIP_DESC_KEYWORDS = ("位置情報", "広告掲載", "プライバシー", "利用規約", "お問い合わせ", "Meguru注目")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


class FurugiMeguruScraper(StaticCrawler):
    """古着屋巡りマップガイド MEGURU スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["エリア", "都道府県スラグ"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        area_slugs = self._get_area_slugs()
        self.logger.info("エリアスラグ取得: %d件", len(area_slugs))

        seen_urls: set[str] = set()

        for slug in area_slugs:
            self.logger.info("エリア取得中: %s (%s)", slug, _SLUG_TO_PREF.get(slug, "?"))
            list_items = list(self._collect_list_items(slug, seen_urls))

            if self.total_items is None and list_items:
                # 初回エリアの件数からざっくり推計
                self.total_items = len(list_items) * len(area_slugs)

            for detail_url, tags in list_items:
                try:
                    item = self._scrape_detail(detail_url, slug, tags)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.warning("詳細ページ取得失敗 (スキップ): %s — %s", detail_url, e)
                time.sleep(self.DELAY)

    # ------------------------------------------------------------------
    # zenkoku ページから全エリアスラグを取得
    # ------------------------------------------------------------------

    def _get_area_slugs(self) -> list[str]:
        soup = self.get_soup(ZENKOKU_URL)
        if soup is None:
            return []
        slugs: list[str] = []
        seen: set[str] = set()
        for a in soup.select('a[href*="/category/area/"]'):
            m = re.search(r"/category/area/([^/]+)/", a.get("href", ""))
            if m:
                slug = m.group(1)
                if slug not in seen:
                    seen.add(slug)
                    slugs.append(slug)
        return slugs

    # ------------------------------------------------------------------
    # 一覧ページ: 詳細URLとジャンルタグを収集
    # ------------------------------------------------------------------

    def _collect_list_items(
        self, slug: str, seen: set[str]
    ) -> Generator[tuple[str, str], None, None]:
        for page_idx in range(1, MAX_PAGES_PER_AREA + 1):
            if page_idx == 1:
                list_url = f"{BASE_URL}/category/area/{slug}/"
            else:
                list_url = f"{BASE_URL}/category/area/{slug}/page/{page_idx}/"

            soup = self.get_soup(list_url)
            if soup is None:
                break

            cards = soup.select(".top_shop")
            if not cards:
                break

            yielded = 0
            for card in cards:
                a = card.select_one("a[href]")
                if not a:
                    continue
                detail_url = urljoin(BASE_URL, a.get("href", ""))
                if detail_url in seen:
                    continue
                seen.add(detail_url)

                tags = " / ".join(
                    t.get_text(strip=True)
                    for t in card.select('[rel="tag"]')
                    if t.get_text(strip=True)
                )
                yielded += 1
                yield detail_url, tags

            next_link = soup.select_one("nav.navigation a.next.page-numbers")
            if not next_link:
                break
            if yielded == 0:
                break
            time.sleep(self.DELAY)

    # ------------------------------------------------------------------
    # 詳細ページ: 全フィールド抽出
    # ------------------------------------------------------------------

    def _scrape_detail(self, url: str, slug: str, tags: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        h1 = soup.select_one("h1")
        if not h1:
            return None
        name = _clean(h1.get_text())

        data: dict = {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: _SLUG_TO_PREF.get(slug, ""),
            Schema.ADDR: "",
            Schema.TEL: "",
            Schema.HP: "",
            Schema.INSTA: "",
            Schema.TIME: "",
            Schema.HOLIDAY: "",
            Schema.DESCRIPTION: "",
            Schema.CAT_SITE: tags,
            "エリア": "",
            "都道府県スラグ": slug,
        }

        # li 要素から基本情報を抽出（形式: 「ラベル：値」）
        for li in soup.select("main li, article li, .entry-content li"):
            text = _clean(li.get_text())
            if not text:
                continue

            if "：" in text:
                label, _, value = text.partition("：")
                label = label.strip()
                value = value.strip()

                if "住" in label:
                    m = _PREF_RE.match(value)
                    if m:
                        data[Schema.PREF] = m.group(1)
                        data[Schema.ADDR] = value[m.end():].strip()
                    else:
                        data[Schema.ADDR] = value
                elif "TEL" in label:
                    data[Schema.TEL] = value
                elif any(kw in label for kw in ("URL", "STORE", "SHOP", "サイト", "HP")):
                    if not data[Schema.HP]:
                        data[Schema.HP] = value
                elif "営業" in label:
                    data[Schema.TIME] = value
                elif "定休" in label:
                    data[Schema.HOLIDAY] = value
            elif "instagram.com" in text.lower():
                # ラベルなしの Instagram URL（li に直接 href が含まれる）
                a_tag = li.select_one('a[href*="instagram.com"]')
                if a_tag and not data[Schema.INSTA]:
                    data[Schema.INSTA] = _clean(a_tag.get("href", ""))

        # 説明文（main 内の p 要素）
        desc_parts: list[str] = []
        for p in soup.select("main p, article p, .entry-content p"):
            t = _clean(p.get_text())
            if t and not any(kw in t for kw in _SKIP_DESC_KEYWORDS):
                desc_parts.append(t)
        if desc_parts:
            data[Schema.DESCRIPTION] = " ".join(desc_parts)[:500]

        # パンくずリストから地域名（エリア）を抽出
        # 構造: TOP > すべてのエリア > {都道府県} > {地域名} > {店名}
        breadcrumb_el = soup.select_one('[class*="breadcrumb"], [class*="bread"]')
        if breadcrumb_el:
            crumbs = [_clean(c) for c in breadcrumb_el.get_text().split(">")]
            crumbs = [c for c in crumbs if c]
            # index 3 が地域名（高円寺、吉祥寺、東京・その他エリア 等）
            if len(crumbs) >= 4:
                data["エリア"] = crumbs[3]

        return data


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = FurugiMeguruScraper()
    scraper.execute(BASE_URL + "/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
