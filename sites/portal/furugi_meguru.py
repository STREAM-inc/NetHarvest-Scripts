# scripts/sites/portal/furugi_meguru.py
"""
古着屋巡りマップガイド MEGURU — 全国古着屋・ヴィンテージショップ情報

取得対象:
    - 全国47都道府県の古着屋・ヴィンテージショップ情報（推定 1,500〜3,000件）

取得フロー:
    1. サイトマップ (wp-sitemap.xml 等) から /area/ 配下の詳細URLを一括収集
    2. 各詳細ページ (/area/{pref}/{id}/ or /area/{id}/) から全フィールドを抽出
    ※ サイトマップが取得できない場合は /zenkoku/ 経由の一覧ページにフォールバック

実行方法:
    # ローカルテスト
    python scripts/sites/portal/furugi_meguru.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id furugi_meguru
"""

import re
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

import bs4
from requests.exceptions import ConnectionError as ReqConnError, Timeout

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://furugi-meguru.com"
ZENKOKU_URL = f"{BASE_URL}/zenkoku/"
MAX_PAGES_PER_AREA = 200

_SITEMAP_CANDIDATES = [
    "wp-sitemap.xml",
    "sitemap.xml",
    "sitemap_index.xml",
    "sitemap-index.xml",
]

_AREA_DETAIL_RE = re.compile(r"furugi-meguru\.com/area/")
_SLUG_FROM_URL_RE = re.compile(r"/area/([a-z]+)/\d+/")

_RETRYABLE = (Timeout, ReqConnError)
_SKIP = object()

_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県"
    r"|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県"
    r"|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県"
    r"|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

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
    BACKOFF = [0, 5, 60, 1_800]
    CB_THRESHOLD = 5
    CB_WAIT = 300
    EXTRA_COLUMNS = ["エリア", "都道府県スラグ"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        shop_urls = self._collect_shop_urls()
        self.total_items = len(shop_urls)
        self.logger.info("URL収集完了: %d件", len(shop_urls))

        consecutive = 0

        for shop_url in shop_urls:
            if consecutive >= self.CB_THRESHOLD:
                self.logger.warning(
                    "連続失敗 %d件: %d秒待機 (サーキットブレーカー)", consecutive, self.CB_WAIT
                )
                time.sleep(self.CB_WAIT)
                consecutive = 0

            result = self._try_fetch(shop_url)

            if result is _SKIP:
                continue
            if result is None:
                consecutive += 1
                self.logger.warning("失敗 (連続%d件): %s", consecutive, shop_url)
                continue

            consecutive = 0
            if result.get(Schema.NAME):
                yield result

    # ------------------------------------------------------------------
    # サイトマップから詳細URLを一括収集
    # ------------------------------------------------------------------

    def _collect_shop_urls(self) -> list[str]:
        urls = self._collect_from_sitemap()
        if urls:
            return urls
        self.logger.warning("サイトマップが見つかりません。一覧ページにフォールバックします。")
        return self._collect_from_list_pages()

    def _collect_from_sitemap(self) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        queue: list[str] = []

        for candidate in _SITEMAP_CANDIDATES:
            sm_url = f"{BASE_URL}/{candidate}"
            try:
                r = self.session.get(sm_url, timeout=self.TIMEOUT)
                if r.status_code == 200:
                    queue.append(sm_url)
                    break
            except Exception:
                continue

        if not queue:
            return []

        visited: set[str] = set()
        while queue:
            sm_url = queue.pop(0)
            if sm_url in visited:
                continue
            visited.add(sm_url)
            try:
                r = self.session.get(sm_url, timeout=self.TIMEOUT)
                if r.status_code != 200:
                    continue
                root = ET.fromstring(r.content)
                locs = [el.text.strip() for el in root.iter() if el.tag.endswith("loc") and el.text]
                if root.tag.lower().endswith("sitemapindex"):
                    queue.extend(locs)
                else:
                    for u in locs:
                        if _AREA_DETAIL_RE.search(u) and u not in seen:
                            seen.add(u)
                            urls.append(u)
            except Exception as e:
                self.logger.debug("サイトマップスキップ %s: %s", sm_url, e)

        self.logger.info("サイトマップから %d件 収集", len(urls))
        return urls

    # ------------------------------------------------------------------
    # フォールバック: /zenkoku/ → 一覧ページ巡回でURL収集
    # ------------------------------------------------------------------

    def _collect_from_list_pages(self) -> list[str]:
        area_slugs = self._get_area_slugs()
        self.logger.info("エリアスラグ取得: %d件", len(area_slugs))
        seen: set[str] = set()
        urls: list[str] = []
        for slug in area_slugs:
            for detail_url, _ in self._collect_list_items(slug, seen):
                urls.append(detail_url)
        return urls

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
    # バックオフ付きフェッチ
    # ------------------------------------------------------------------

    def _try_fetch(self, url: str) -> dict | None | object:
        for wait in self.BACKOFF:
            if wait:
                self.logger.info("%.0f秒待機後リトライ: %s", wait, url)
                time.sleep(wait)
            try:
                resp = self.session.get(url, timeout=self.TIMEOUT)
            except _RETRYABLE as e:
                self.logger.debug("接続エラー: %s → %s", url, e)
                continue
            except Exception as e:
                self.logger.warning("予期しないエラー: %s → %s", url, e)
                return _SKIP

            if resp.status_code == 403:
                self.logger.warning("403: %s", url)
                continue
            if resp.status_code in (404, 410) or 400 <= resp.status_code < 500:
                return _SKIP
            if resp.status_code != 200:
                return _SKIP

            ct = resp.headers.get("Content-Type", "")
            if "charset=" not in ct.lower():
                resp.encoding = resp.apparent_encoding

            time.sleep(self.DELAY)
            slug = self._slug_from_url(url)
            item = self._scrape_detail(url, bs4.BeautifulSoup(resp.text, "html.parser"), slug)
            return item if item is not None else _SKIP

        return None

    @staticmethod
    def _slug_from_url(url: str) -> str:
        m = _SLUG_FROM_URL_RE.search(url)
        return m.group(1) if m else ""

    # ------------------------------------------------------------------
    # 詳細ページ: 全フィールド抽出
    # ------------------------------------------------------------------

    def _scrape_detail(self, url: str, soup: bs4.BeautifulSoup, slug: str) -> dict | None:
        h1 = soup.select_one("h1")
        if not h1:
            return None
        name = _clean(h1.get_text())

        tags = " / ".join(
            t.get_text(strip=True)
            for t in soup.select('[rel="tag"]')
            if t.get_text(strip=True)
        )

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
                a_tag = li.select_one('a[href*="instagram.com"]')
                if a_tag and not data[Schema.INSTA]:
                    data[Schema.INSTA] = _clean(a_tag.get("href", ""))

        desc_parts: list[str] = []
        for p in soup.select("main p, article p, .entry-content p"):
            t = _clean(p.get_text())
            if t and not any(kw in t for kw in _SKIP_DESC_KEYWORDS):
                desc_parts.append(t)
        if desc_parts:
            data[Schema.DESCRIPTION] = " ".join(desc_parts)[:500]

        breadcrumb_el = soup.select_one('[class*="breadcrumb"], [class*="bread"]')
        if breadcrumb_el:
            crumbs = [_clean(c) for c in breadcrumb_el.get_text().split(">")]
            crumbs = [c for c in crumbs if c]
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
