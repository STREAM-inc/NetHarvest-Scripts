"""
スナックナビ — 全国スナック店舗スクレイパー

取得フロー:
    トップページ → /area2/{areaID}/ または /{region}/area.html?a=N&i={areaID}
          → ページ送り → 店舗詳細URL

実行方法:
    python scripts/sites/nightlife/snack_navi.py
    python bin/run_flow.py --site-id snack_navi
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler


class SnackNaviCrawler(StaticCrawler):
    """スナックナビ クローラー — 全国スナック店舗情報を取得"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["キャッチコピー"]

    BASE_URL = "https://snacknavi.com"
    TOP_URL = f"{BASE_URL}/"
    MAX_LIST_PAGES_PER_SOURCE = 10000

    REGIONS = [
        "hokkaidou",
        "tohoku",
        "hokuriku",
        "kantou",
        "tokai",
        "kansai",
        "shikoku",
        "kyushu",
        "okinawa",
    ]

    _PREF_RE = re.compile(
        r"^(北海道|東京都|大阪府|京都府|"
        r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|和歌山|"
        r"鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県)"
    )
    _TOKYO_LIST_RE = re.compile(r"^/area2/\d+/?(?:&p=\d+/?)?$")
    _REGIONAL_LIST_RE = re.compile(
        r"^/(?:%s)/area\.html\?(?:p=\d+&)?a=\d+&i=\d+$" % "|".join(REGIONS)
    )
    _TOKYO_SHOP_RE = re.compile(r"^/area/[^/?#]+/[^/?#]+/\d+/[^/?#]+/?$")
    _REGIONAL_SHOP_RE = re.compile(r"^/(?:%s)/shop\d+\.html$" % "|".join(REGIONS))

    def parse(self, url: str) -> Generator:
        shop_urls = self._collect_shop_urls(url)
        self.total_items = len(shop_urls)
        self.logger.info("店舗詳細URL候補数: %d", self.total_items)

        saved_count = 0
        failed_count = 0
        for index, shop_url in enumerate(shop_urls, start=1):
            remaining = self.total_items - index
            self.logger.info(
                "詳細取得中: %d/%d 残り%d件 取得済み%d件 失敗%d件 URL=%s",
                index,
                self.total_items,
                remaining,
                saved_count,
                failed_count,
                shop_url,
            )

            try:
                record = self._scrape_detail(shop_url)
            except Exception as e:
                failed_count += 1
                self.logger.warning(
                    "詳細取得失敗: %d/%d 残り%d件 取得済み%d件 失敗%d件 URL=%s (%s)",
                    index,
                    self.total_items,
                    remaining,
                    saved_count,
                    failed_count,
                    shop_url,
                    e,
                )
                continue

            if record:
                saved_count += 1
                self.logger.info(
                    "詳細取得OK: %d/%d 残り%d件 取得済み%d件 失敗%d件 店舗=%s",
                    index,
                    self.total_items,
                    remaining,
                    saved_count,
                    failed_count,
                    record.get(Schema.NAME) or shop_url,
                )
                yield record
            else:
                failed_count += 1
                self.logger.warning(
                    "詳細取得スキップ: %d/%d 残り%d件 取得済み%d件 失敗%d件 URL=%s",
                    index,
                    self.total_items,
                    remaining,
                    saved_count,
                    failed_count,
                    shop_url,
                )

        self.logger.info(
            "詳細取得完了: 候補%d件 取得済み%d件 失敗/スキップ%d件",
            self.total_items,
            saved_count,
            failed_count,
        )

    # ------------------------------------------------------------------
    # 店舗一覧URL収集
    # ------------------------------------------------------------------

    def _collect_shop_urls(self, seed_url: str) -> list[str]:
        normalized_seed = self._normalize_url(seed_url)
        seed_path = self._path_with_query(normalized_seed)
        if self._is_shop_path(seed_path):
            return [normalized_seed]

        list_urls = self._collect_list_urls(normalized_seed)
        self.logger.info("店舗一覧URL数: %d", len(list_urls))

        shop_urls: list[str] = []
        seen: set[str] = set()
        for index, list_url in enumerate(list_urls, start=1):
            before_count = len(shop_urls)
            for shop_url in self._collect_shop_urls_from_list(list_url):
                if shop_url not in seen:
                    seen.add(shop_url)
                    shop_urls.append(shop_url)
            self.logger.info(
                "候補URL収集進捗: 一覧%d/%d 追加%d件 累計%d件 URL=%s",
                index,
                len(list_urls),
                len(shop_urls) - before_count,
                len(shop_urls),
                list_url,
            )
        return shop_urls

    def _collect_list_urls(self, seed_url: str) -> list[str]:
        seed_path = self._path_with_query(seed_url)
        if self._is_list_path(seed_path):
            return [seed_url]

        soup = self._fetch(self.TOP_URL)
        if soup is None:
            return []

        list_urls: list[str] = []
        seen: set[str] = set()
        for raw_url in self._iter_link_targets(soup):
            full_url = self._normalize_url(urljoin(self.TOP_URL, raw_url))
            if (
                self._is_list_path(self._path_with_query(full_url))
                and full_url not in seen
            ):
                seen.add(full_url)
                list_urls.append(full_url)

        return list_urls

    def _collect_shop_urls_from_list(self, list_url: str) -> list[str]:
        shop_urls: list[str] = []
        seen_shops: set[str] = set()
        seen_pages: set[str] = set()
        page_url = list_url
        page = 1

        while (
            page_url
            and page_url not in seen_pages
            and page <= self.MAX_LIST_PAGES_PER_SOURCE
        ):
            seen_pages.add(page_url)
            soup = self._fetch(page_url)
            if soup is None:
                break

            page_shop_urls = self._extract_shop_urls_from_soup(soup, page_url)
            self.logger.info(
                "一覧ページ取得: %s ページ%d 店舗URL%d件 一覧内累計%d件",
                page_url,
                page,
                len(page_shop_urls),
                len(shop_urls)
                + len([u for u in page_shop_urls if u not in seen_shops]),
            )
            if not page_shop_urls:
                break

            for shop_url in page_shop_urls:
                if shop_url not in seen_shops:
                    seen_shops.add(shop_url)
                    shop_urls.append(shop_url)

            page_url = self._extract_next_page_url(soup, page_url)
            page += 1
        return shop_urls

    def _extract_shop_urls_from_soup(
        self, soup: BeautifulSoup, base_url: str
    ) -> list[str]:
        shop_urls: list[str] = []
        seen: set[str] = set()
        for raw_url in self._iter_link_targets(soup):
            full_url = self._normalize_url(urljoin(base_url, raw_url), keep_query=False)
            if not self._is_shop_path(self._path_with_query(full_url)):
                continue
            if full_url not in seen:
                seen.add(full_url)
                shop_urls.append(full_url)
        return shop_urls

    def _extract_next_page_url(self, soup: BeautifulSoup, base_url: str) -> str:
        for a in soup.find_all("a", href=True):
            text = self._c(a.get_text(" ", strip=True))
            if "次のページ" not in text:
                continue
            next_url = self._normalize_url(urljoin(base_url, a["href"]))
            if self._is_list_path(self._path_with_query(next_url)):
                return next_url
        return ""

    def _iter_link_targets(self, soup: BeautifulSoup) -> Generator[str, None, None]:
        for a in soup.find_all("a", href=True):
            yield a["href"]
        for elem in soup.find_all(onclick=True):
            for m in re.finditer(
                r"location\.href\s*=\s*['\"]([^'\"]+)['\"]", elem["onclick"]
            ):
                yield m.group(1)

    def _is_list_path(self, path_with_query: str) -> bool:
        return bool(
            self._TOKYO_LIST_RE.match(path_with_query)
            or self._REGIONAL_LIST_RE.match(path_with_query)
        )

    def _is_shop_path(self, path_with_query: str) -> bool:
        return bool(
            self._TOKYO_SHOP_RE.match(path_with_query)
            or self._REGIONAL_SHOP_RE.match(path_with_query)
        )

    # ------------------------------------------------------------------
    # 共通ユーティリティ
    # ------------------------------------------------------------------

    def _fetch(self, url: str) -> BeautifulSoup | None:
        try:
            resp = self.session.get(
                url, timeout=15, headers={"User-Agent": "Mozilla/5.0"}
            )
            resp.raise_for_status()
            return BeautifulSoup(resp.content, "html.parser")
        except Exception as e:
            self.logger.warning(f"Fetch failed: {url} — {e}")
            return None

    def _normalize_url(self, url: str, keep_query: bool = True) -> str:
        parsed = urlparse(url)
        query = parsed.query if keep_query else ""
        return urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path.rstrip("/") or "/",
                "",
                query,
                "",
            )
        )

    def _path_with_query(self, url: str) -> str:
        parsed = urlparse(url)
        path = parsed.path.rstrip("/") or "/"
        if parsed.path.endswith("/") and path != "/":
            path += "/"
        return f"{path}?{parsed.query}" if parsed.query else path

    def _split_pref(self, address: str) -> tuple[str, str]:
        if not address:
            return "", ""
        m = self._PREF_RE.match(address)
        if not m:
            return "", address
        return m.group(1), address[m.end() :].strip()

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self._fetch(detail_url)
        if soup is None:
            return None

        record: dict = {Schema.URL: detail_url}

        # 店舗名
        h2 = soup.select_one("div.ttl-shop-name h2")
        name = self._c(h2.get_text(" ", strip=True) if h2 else "")
        if not name:
            self.logger.warning("店舗名なし: %s", detail_url)
            return None
        record[Schema.NAME] = name

        # 電話番号
        tel_a = soup.select_one("p.ttl-shop-tel a[href^='tel:']")
        if tel_a:
            record[Schema.TEL] = self._c(tel_a.get_text(" ", strip=True))

        # 郵便番号・住所（住所テキストの先頭 〒NNN-NNNN から抽出）
        addr_header = soup.select_one("div.ttl-shop-info")
        if addr_header:
            raw = addr_header.get_text(" ", strip=True)
            m = re.search(r"〒(\d{3}-\d{4})\s*(.+)", raw)
            if m:
                record[Schema.POST_CODE] = m.group(1)
                address = self._c(re.sub(r"【.*?】", "", m.group(2)))
                pref, addr_body = self._split_pref(address)
                if hasattr(Schema, "PREF") and pref:
                    record[Schema.PREF] = pref
                record[Schema.ADDR] = addr_body or address

        catch = soup.select_one("p.sd-catch")
        if catch:
            record["キャッチコピー"] = self._c(catch.get_text(" ", strip=True))

        return record

    def _c(self, value: str | None) -> str:
        if not value:
            return ""
        return re.sub(r"\s+", " ", str(value)).strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    scraper = SnackNaviCrawler()
    scraper.execute("https://snacknavi.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
