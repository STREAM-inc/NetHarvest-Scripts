"""
対象サイト: https://www.vinty.jp/search
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode

# VS Code の「Run Python File」(右上の実行) でも src パッケージを解決できるようにする
root_path = Path(__file__).resolve().parent.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from src.const.schema import Schema
from src.framework.static import StaticCrawler


class VintyScraper(StaticCrawler):
    """VINTY 検索ページから店舗URLを収集するスクレイパー (静的)"""

    DELAY = 5.0
    MAX_PAGES = 0
    MAX_DETAILS = 0
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

    def _setup(self):
        super()._setup()
        self.session.headers.update(
            {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
                "Referer": "https://www.vinty.jp/",
            }
        )

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_shop_urls: set[str] = set()
        shop_urls: list[str] = []
        page_num = 1
        max_pages = self._resolve_max_pages()
        max_details = self._resolve_max_details()

        while True:
            if max_pages > 0 and page_num > max_pages:
                self.logger.info(
                    "実行時指定の上限ページに到達したため終了: page=%d", max_pages
                )
                break

            list_url = self._build_paged_url(url, page_num)
            self.logger.info("一覧ページ取得: %s", list_url)

            soup = self.get_soup(list_url)

            new_count = 0
            for shop_url in self._extract_shop_urls(soup, base_url=list_url):
                if shop_url in seen_shop_urls:
                    continue

                seen_shop_urls.add(shop_url)
                shop_urls.append(shop_url)
                new_count += 1

            self.logger.info("page=%d 新規URL件数: %d", page_num, new_count)
            if new_count == 0:
                self.logger.info("新規URLがなくなったため終了: page=%d", page_num)
                break

            page_num += 1

        target_shop_urls = shop_urls[:max_details] if max_details > 0 else shop_urls
        self.total_items = len(target_shop_urls)
        self.logger.info(
            "詳細取得対象: %d 件 (収集URL総数: %d)",
            len(target_shop_urls),
            len(shop_urls),
        )

        for i, shop_url in enumerate(target_shop_urls, 1):
            self.logger.info(
                "詳細取得中 [%d/%d]: %s", i, len(target_shop_urls), shop_url
            )
            detail_soup = self.get_soup(shop_url)
            if detail_soup is None:
                continue
            yield self._extract_shop_info(detail_soup, shop_url)

    def _build_paged_url(self, base_url: str, page_num: int) -> str:
        parsed = urlparse(base_url)
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        query["page"] = str(page_num)
        return urlunparse(parsed._replace(query=urlencode(query)))

    def _resolve_max_pages(self) -> int:
        env_value = os.getenv("NH_MAX_PAGES", "").strip()
        if not env_value:
            return self.MAX_PAGES

        try:
            parsed = int(env_value)
        except ValueError:
            self.logger.warning(
                "NH_MAX_PAGES が不正なため無制限で実行します: %s", env_value
            )
            return self.MAX_PAGES

        if parsed < 0:
            self.logger.warning(
                "NH_MAX_PAGES が負数のため無制限で実行します: %s", env_value
            )
            return self.MAX_PAGES

        self.logger.info("実行時設定 NH_MAX_PAGES=%d", parsed)
        return parsed

    def _resolve_max_details(self) -> int:
        env_value = os.getenv("NH_MAX_DETAILS", "").strip()
        if not env_value:
            return self.MAX_DETAILS

        try:
            parsed = int(env_value)
        except ValueError:
            self.logger.warning(
                "NH_MAX_DETAILS が不正なため既定値で実行します: %s", env_value
            )
            return self.MAX_DETAILS

        if parsed < 0:
            self.logger.warning(
                "NH_MAX_DETAILS が負数のため既定値で実行します: %s", env_value
            )
            return self.MAX_DETAILS

        self.logger.info("実行時設定 NH_MAX_DETAILS=%d", parsed)
        return parsed

    def _extract_shop_urls(self, soup, base_url: str) -> list[str]:
        shop_urls: list[str] = []
        search_result_ul = soup.select_one("ul.SearchPanels_searchResult__fc9xm")
        if not search_result_ul:
            self.logger.warning("検索結果リストが見つかりませんでした")
            return shop_urls

        for a_tag in search_result_ul.select("a[href*='/shop/']"):
            href = (a_tag.get("href") or "").strip()
            if not href:
                continue

            absolute = urljoin(base_url, href)
            parsed = urlparse(absolute)

            if parsed.netloc not in ("", "www.vinty.jp"):
                continue
            if not parsed.path.startswith("/shop/"):
                continue

            normalized = f"https://www.vinty.jp{parsed.path}"
            shop_urls.append(normalized)

        return shop_urls

    def _extract_shop_info(self, soup, shop_url: str) -> dict:
        name = ""
        h1 = soup.select_one("h1")
        if h1:
            name = h1.get_text(strip=True)

        post_code = ""
        addr = ""
        time_text = ""
        pay = ""
        tel = ""
        hp = ""

        info_dl = soup.select_one("dl[class^='ShopInfo_table__']")
        if info_dl:
            for dt, dd in zip(info_dl.select("dt"), info_dl.select("dd")):
                label = dt.get_text(" ", strip=True)
                value_text = " ".join(dd.stripped_strings)

                if label == "営業時間":
                    time_text = value_text
                elif label in ("支払方法", "支払い方法"):
                    pay = value_text
                elif label in ("電話番号", "電話"):
                    tel = value_text
                elif label in ("ホームページ", "HP", "URL"):
                    link = dd.select_one("a[href]")
                    hp = (link.get("href") or "").strip() if link else value_text
                elif label == "住所":
                    spans = [
                        s.get_text(strip=True)
                        for s in dd.select("span")
                        if s.get_text(strip=True)
                    ]
                    if spans:
                        if spans[0].startswith("〒"):
                            post_code = spans[0]
                            addr = " ".join(spans[1:]).strip()
                        else:
                            addr = " ".join(spans).strip()
                    if not addr:
                        addr = value_text.replace(post_code, "").strip()

        return {
            Schema.URL: shop_url,
            Schema.NAME: name,
            Schema.TIME: time_text,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.PAYMENTS: pay,
            Schema.TEL: tel,
            Schema.HP: hp,
        }


def main() -> None:
    parser = argparse.ArgumentParser(description="VINTY の店舗URLを収集して CSV 保存")
    parser.add_argument(
        "--url",
        default="https://www.vinty.jp/search",
        help="収集対象の検索URL",
    )
    parser.add_argument(
        "--max-pages",
        "--max-page",
        type=int,
        default=None,
        help="取得上限ページ数。未指定で無制限",
    )
    parser.add_argument(
        "--max-details",
        type=int,
        default=None,
        help="詳細取得の上限件数。未指定で既定値(3件)",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)

    if args.max_pages is not None:
        os.environ["NH_MAX_PAGES"] = str(args.max_pages)
    if args.max_details is not None:
        os.environ["NH_MAX_DETAILS"] = str(args.max_details)

    scraper = VintyScraper()
    scraper.site_name = "vinty"
    scraper.site_id = ""
    scraper.execute(args.url)

    print(f"CSV保存先: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")


if __name__ == "__main__":
    main()
