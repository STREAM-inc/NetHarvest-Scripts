# ===== 【pip install -e . を実行していない場合のみ必要】===========
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
# ================================================================

import logging
import re
from urllib.parse import urlparse

from usp.tree import sitemap_tree_for_homepage

from src.framework.static import StaticCrawler
from src.const.schema import Schema

from datetime import datetime

class GBWalkerScraper(StaticCrawler):
    """gb-walker.jp のサイトマップURL収集 + 店舗詳細取得"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "予約可否",
        "アクセス",
        "客席数",
        "環境・サービス",
        "その他",
        "SNS",
        "Twitter",
        "LINE",
        "ブログ・その他URL",
    ]

    def _normalize_text(self, text: str) -> str:
        if not text:
            return ""
        return " ".join(text.replace("\xa0", " ").split())

    def _normalize_label(self, text: str) -> str:
        return (
            self._normalize_text(text)
            .replace("：", "")
            .replace(":", "")
            .replace("\n", "")
        )

    def _is_target_detail_url(self, page_url: str) -> bool:
        """
        欲しいURL:
        https://www.gb-walker.jp/kanto/shop/cruise/
        https://www.gb-walker.jp/kanto/shop/dear/
        のような /{area}/shop/{slug}/ 形式
        """
        if not page_url:
            return False

        parsed = urlparse(page_url)
        path = parsed.path or ""

        return bool(re.search(r"^/[^/]+/shop/[^/]+/?$", path))

    def _get_shop_table(self, soup):
        return soup.select_one("section.shop_data_contents table.shop-data")

    def _extract_rows(self, soup) -> dict[str, object]:
        """
        shop-data テーブルを
        {
            "店名": td要素,
            "電話番号": td要素,
            ...
        }
        の形にする
        """
        table = self._get_shop_table(soup)
        if not table:
            return {}

        rows = {}
        for tr in table.select("tr"):
            th = tr.select_one("th")
            td = tr.select_one("td")
            if not th or not td:
                continue

            label = self._normalize_label(th.get_text(" ", strip=True))
            if not label:
                continue

            rows[label] = td

        return rows

    def _get_row_text(self, rows: dict, labels: list[str]) -> str:
        for label in labels:
            td = rows.get(label)
            if not td:
                continue

            text = self._normalize_text(td.get_text(" ", strip=True))
            return text

        return ""

    def _get_tel(self, rows: dict) -> str:
        td = rows.get("電話番号")
        if not td:
            return ""

        tel_p = td.select_one("p.tell")
        if tel_p:
            return self._normalize_text(tel_p.get_text(strip=True))

        texts = list(td.stripped_strings)
        if not texts:
            return ""

        # 最初の電話番号っぽい文字列を返す
        for text in texts:
            norm = self._normalize_text(text)
            if re.search(r"\d{2,4}-\d{2,4}-\d{3,4}", norm):
                return norm

        return self._normalize_text(texts[0])

    def _get_service_list(self, rows: dict) -> str:
        td = rows.get("環境・サービス")
        if not td:
            return ""

        items = [
            self._normalize_text(li.get_text(strip=True)) for li in td.select("li")
        ]
        items = [item for item in items if item]
        if items:
            return " / ".join(items)

        return self._normalize_text(td.get_text(" ", strip=True))

    def _get_hp(self, rows: dict) -> str:
        td = rows.get("公式サイト")
        if not td:
            return ""

        a = td.select_one("a[href]")
        if a:
            return a.get("href", "").strip()

        return self._normalize_text(td.get_text(" ", strip=True))

    def _get_sns_data(self, rows: dict) -> tuple[str, str, str]:
        """
        return:
            sns_all, twitter_url, line_url
        """
        td = rows.get("SNS")
        if not td:
            return "", "", ""

        urls = []
        twitter_url = ""
        line_url = ""

        for a in td.select("a[href]"):
            href = a.get("href", "").strip()
            if not href:
                continue

            urls.append(href)

            lower_href = href.lower()
            if not twitter_url and "twitter.com" in lower_href:
                twitter_url = href
            if not line_url and ("line.me" in lower_href or "lin.ee" in lower_href):
                line_url = href

        # 重複除去しつつ順序維持
        unique_urls = list(dict.fromkeys(urls))
        sns_all = " / ".join(unique_urls)

        return sns_all, twitter_url, line_url

    def _get_blog_urls(self, rows: dict) -> str:
        td = rows.get("ブログ その他URL") or rows.get("ブログ・その他URL")
        if not td:
            return ""

        urls = []
        for a in td.select("a[href]"):
            href = a.get("href", "").strip()
            if href:
                urls.append(href)

        if urls:
            return " / ".join(dict.fromkeys(urls))

        return self._normalize_text(td.get_text(" ", strip=True))

    def parse(self, url: str):
        self.logger.info("=== Step1: サイトマップから店舗詳細URLを収集中 ===")
        tree = sitemap_tree_for_homepage(url)

        detail_urls = []
        seen = set()

        for page in tree.all_pages():
            page_url = getattr(page, "url", "")
            if not self._is_target_detail_url(page_url):
                continue
            if page_url in seen:
                continue

            seen.add(page_url)
            detail_urls.append(page_url)

        detail_urls = sorted(detail_urls)

        self.total_items = len(detail_urls)
        self.logger.info("店舗詳細URLを %s 件検出しました", self.total_items)

        if not detail_urls:
            self.logger.warning("対象の店舗詳細URLが見つかりませんでした")
            return

        self.logger.info("=== Step2: 詳細ページの情報取得を開始 ===")

        for idx, detail_url in enumerate(detail_urls, start=1):
            self.logger.info("[%s/%s] 取得中: %s", idx, self.total_items, detail_url)

            try:
                soup = self.get_soup(detail_url)
                if not soup:
                    self.logger.warning("HTML取得失敗: %s", detail_url)
                    continue

                rows = self._extract_rows(soup)
                if not rows:
                    self.logger.warning(
                        "SHOP DATA テーブルが見つかりません: %s", detail_url
                    )
                    continue

                sns_all, twitter_url, line_url = self._get_sns_data(rows)

                yield {
                    Schema.NAME: self._get_row_text(rows, ["店名"]),
                    Schema.TEL: self._get_tel(rows),
                    Schema.TIME: self._get_row_text(rows, ["営業時間"]),
                    Schema.HOLIDAY: self._get_row_text(rows, ["定休日"]),
                    Schema.ADDR: self._get_row_text(rows, ["住所"]),
                    Schema.HP: self._get_hp(rows),
                    Schema.URL: detail_url,
                    "取得日時": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "予約可否": self._get_row_text(rows, ["予約"]),
                    "アクセス": self._get_row_text(rows, ["アクセス"]),
                    "客席数": self._get_row_text(rows, ["客席数"]),
                    "環境・サービス": self._get_service_list(rows),
                    "その他": self._get_row_text(rows, ["その他"]),
                    "SNS": sns_all,
                    "Twitter": twitter_url,
                    "LINE": line_url,
                    "ブログ・その他URL": self._get_blog_urls(rows),
                }

            except Exception as e:
                self.logger.warning("スキップしました: %s | url=%s", e, detail_url)
                continue

        self.logger.info("=== 取得完了 ===")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )

    GBWalkerScraper().execute("https://www.gb-walker.jp/")
