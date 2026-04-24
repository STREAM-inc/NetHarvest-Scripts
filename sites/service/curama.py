"""
くらしのマーケット (curama.jp) — 全国の暮らしサービス出店者情報スクレイパー

取得対象:
    - /category/ 直下の全サービスカテゴリ × 全ページに掲載される出店サービス
    - 各サービス詳細ページ(/{category}/.../SER{9桁}/)の「店舗について」ブロック

取得フロー:
    1. https://curama.jp/category/ から全カテゴリの URL を列挙
    2. 各カテゴリ一覧ページを ?page=N でページネーション (末尾 or 次ページリンクなしで終了)
    3. 一覧カードの a[id^="service-details"] から詳細ページへ遷移
    4. 詳細ページ の h1 / [data-test-id="store-info"] 等から店舗情報を抽出
    5. 同一詳細 URL は重複除去

実行方法:
    python scripts/sites/service/curama.py
    python bin/run_flow.py --site-id curama
"""

import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


_PREF_PATTERN = re.compile(
    r"^(北海道|東京都|大阪府|京都府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_ZENKAKU = "０１２３４５６７８９－（）ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ"
_HANKAKU = "0123456789-()abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
_TRANS = str.maketrans(_ZENKAKU, _HANKAKU)

_EXCLUDED_SLUGS = {
    "about", "mypage", "bookmarks", "category", "coupon",
    "safety", "guarantee", "guide", "cleaning", "labs",
    "for-professionals", "shop", "magazine",
    "terms", "privacy", "security", "law", "customer-harassment",
}

_SER_RE = re.compile(r"/(SER\d+)/?$")
_NEXT_BTN_RE = re.compile(r"次の\d+件|次へ")
_CATEGORY_URL_RE = re.compile(r"^https://curama\.jp/([a-z][a-z0-9-]*)/$")


class CuramaScraper(StaticCrawler):
    """くらしのマーケット (curama.jp) 出店者スクレイパー"""

    DELAY = 1.5

    EXTRA_COLUMNS = [
        "店舗コード",
        "店舗ID",
        "サービスID",
        "ジャンル",
        "価格",
        "評価",
        "レビュー件数",
        "対応エリア",
        "特徴",
        "説明",
    ]

    _BASE_URL = "https://curama.jp/"

    def parse(self, url: str):
        category_index_url = urljoin(url, "/category/")
        self.logger.info("カテゴリ一覧取得: %s", category_index_url)
        soup = self.get_soup(category_index_url)
        if not soup:
            return

        categories = self._extract_categories(soup)
        self.logger.info("検出カテゴリ数: %d", len(categories))

        seen_urls: set[str] = set()
        for cat_url, cat_name in categories:
            yield from self._scrape_category(cat_url, cat_name, seen_urls)

    def _extract_categories(self, soup) -> list[tuple[str, str]]:
        categories: list[tuple[str, str]] = []
        seen_paths: set[str] = set()
        for a in soup.select("a[href]"):
            href = a.get("href") or ""
            absolute = urljoin(self._BASE_URL, href)
            match = _CATEGORY_URL_RE.match(absolute)
            if not match:
                continue
            slug = match.group(1)
            if slug in _EXCLUDED_SLUGS:
                continue
            if absolute in seen_paths:
                continue
            seen_paths.add(absolute)
            name = a.get_text(strip=True) or slug
            categories.append((absolute, name))
        return categories

    def _scrape_category(self, cat_url: str, cat_name: str, seen_urls: set[str]):
        page = 1
        while True:
            page_url = f"{cat_url}?page={page}&"
            self.logger.info("一覧ページ取得: [%s] %s", cat_name, page_url)
            soup = self.get_soup(page_url)
            if not soup:
                break

            items = soup.find_all("a", id=re.compile(r"^service-details\d+"))
            if not items:
                break

            for item in items:
                detail_href = item.get("href")
                if not detail_href:
                    continue
                detail_url = urljoin(self._BASE_URL, detail_href)
                if detail_url in seen_urls:
                    continue
                seen_urls.add(detail_url)

                list_info = self._parse_listing_card(item, cat_name)
                try:
                    record = self._scrape_detail(detail_url, list_info)
                except Exception as e:
                    self.logger.warning("詳細取得失敗: %s — %s", detail_url, e)
                    continue
                if record:
                    self.total_items = len(seen_urls)
                    yield record

            next_btn = soup.find("a", string=_NEXT_BTN_RE)
            if not next_btn:
                break

            page += 1
            time.sleep(self.DELAY)

    def _parse_listing_card(self, card, cat_name: str) -> dict:
        info: dict = {"ジャンル": cat_name}

        rating_el = card.select_one("p.col_ora10")
        if rating_el:
            info["評価"] = rating_el.get_text(strip=True)

        for p in card.select("p.col_bla08"):
            txt = p.get_text(strip=True)
            m = re.match(r"^\((\d[\d,]*)\)$", txt)
            if m:
                info["レビュー件数"] = m.group(1).replace(",", "")
                break

        h3 = card.select_one("h3")
        if h3:
            info["_service_name"] = h3.get_text(strip=True)

        price_el = card.select_one('[data-test-id="service-price"]')
        if price_el:
            info["価格"] = price_el.get_text(strip=True)

        pref_texts = [
            p.get_text(strip=True)
            for p in card.select("p.col_bla08.fon-s_12")
        ]
        for t in pref_texts:
            if _PREF_PATTERN.match(t):
                info["_pref_hint"] = t
                break

        return info

    def _scrape_detail(self, detail_url: str, list_info: dict) -> dict | None:
        soup = self.get_soup(detail_url)
        if not soup:
            return None

        item: dict = {
            Schema.URL: detail_url,
            Schema.CAT_SITE: list_info.get("ジャンル", ""),
            "ジャンル": list_info.get("ジャンル", ""),
            "価格": list_info.get("価格", ""),
            "評価": list_info.get("評価", ""),
            "レビュー件数": list_info.get("レビュー件数", ""),
        }

        m = _SER_RE.search(detail_url)
        if m:
            item["サービスID"] = m.group(1)

        h1 = soup.find("h1")
        if h1:
            item[Schema.NAME] = h1.get_text(strip=True).translate(_TRANS)

        store_info = soup.find(attrs={"data-test-id": "store-info"})
        if store_info:
            code = store_info.get("data-test-store-code")
            sid = store_info.get("data-test-store-id")
            if code:
                item["店舗コード"] = code
            if sid:
                item["店舗ID"] = sid

            manager_node = store_info.find(string=re.compile(r"(店長|代表|責任者)[：:]"))
            if manager_node:
                text = str(manager_node).strip().translate(_TRANS)
                mm = re.search(r"(店長|代表|責任者)[：:](.+)", text)
                if mm:
                    item[Schema.POS_NM] = mm.group(1).strip()
                    item[Schema.REP_NM] = mm.group(2).strip()

            desc_p = store_info.select_one("div.dis_f.mar-b_32 div p:nth-of-type(2)")
            if desc_p:
                item["説明"] = desc_p.get_text(" ", strip=True)[:500]

            for h3 in store_info.find_all("h3"):
                label = h3.get_text(strip=True)
                next_div = h3.find_next_sibling("div")
                if not next_div:
                    continue
                if label == "所在地":
                    raw = next_div.get_text(" ", strip=True).translate(_TRANS)
                    post_match = re.search(r"〒?\s*(\d{3}-?\d{4})", raw)
                    if post_match:
                        item[Schema.POST_CODE] = post_match.group(1)
                    cleaned = re.sub(r"〒\s*\d{3}-?\d{4}\s*", "", raw).strip()
                    item[Schema.ADDR] = cleaned
                    pm = _PREF_PATTERN.match(cleaned)
                    if pm:
                        item[Schema.PREF] = pm.group(1)
                elif label == "営業時間":
                    item[Schema.TIME] = next_div.get_text(" ", strip=True)
                elif label == "定休日":
                    item[Schema.HOLIDAY] = next_div.get_text(" ", strip=True)
                elif "対応エリア" in label:
                    lis = next_div.find_all("li")
                    if lis:
                        item["対応エリア"] = "、".join(
                            li.get_text(strip=True) for li in lis
                        )
                    else:
                        item["対応エリア"] = next_div.get_text(" ", strip=True)[:1000]

        features_el = soup.find(attrs={"data-test-id": "service-features-div"})
        if features_el:
            parts = [
                t.strip()
                for t in features_el.get_text("|", strip=True).split("|")
                if t.strip()
            ]
            if parts:
                item["特徴"] = "、".join(parts)

        if not item.get(Schema.PREF) and list_info.get("_pref_hint"):
            pm = _PREF_PATTERN.match(list_info["_pref_hint"])
            if pm:
                item[Schema.PREF] = pm.group(1)

        if not item.get(Schema.NAME):
            return None

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = CuramaScraper()
    scraper.execute("https://curama.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
