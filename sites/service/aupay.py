"""
au PAY — Pontaアップ店 (au PAY が使えるおトクなお店) ブランド一覧スクレイパー

取得対象:
    - au PAY (コード支払い) の利用でポイントがさらにたまる「au PAY Pontaアップ店」の
      対象ブランド一覧
    - ブランド名、サイト定義カテゴリ (飲食/ドラッグストア/家電・スーパー/暮らし/エンタメ)、
      ロゴ画像URL、ブランド紹介ページURL

取得フロー:
    1. ルート (https://aupay.auone.jp/) の「おトクなお店」セクションから
       Pontaアップ店一覧ページ (/contents/lp/pontaup/) へのリンクを取得する
       (リンクが見つからない場合のみ既定パスへフォールバック)
    2. 一覧ページは静的 HTML。`ul.category > li` がブランド 1 件で、
       `data-category` 属性がカテゴリ、`img[alt]` がブランド名、`img[src]` がロゴ。
    3. 併せてページ上部のブランドスライダー (`.infinite-slider .swiper-slide img[alt]`)
       も読み、カテゴリ一覧に無いブランドを補完する (名称で重複排除)。

補足 (2026-09 時点の調査結果):
    - 旧実装が参照していた店舗検索 SPA (aupay.wallet.auone.jp/store/list/) は
      2026年8月30日からメンテナンス中で、全アクセスがメンテナンス画面を返すため
      ブランド一覧を取得できない (0件の原因)。
    - 代わりに正規ドメイン (aupay.auone.jp) 上の静的な Pontaアップ店一覧から取得する。

実行方法:
    # ローカルテスト
    python scripts/sites/service/aupay.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id aupay
"""

import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlsplit, urlunsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup
from src.framework.static import StaticCrawler
from src.const.schema import Schema

# sites.yml に登録済みの正規 URL (クロールの起点)
ROOT_URL = "https://aupay.auone.jp/"

# ルートから辿れなかった場合のみ使うフォールバック用の相対パス
_LIST_PATH = "/contents/lp/pontaup/index.html"


class AuPayScraper(StaticCrawler):
    """au PAY Pontaアップ店 ブランド一覧スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["ロゴURL", "紹介URL"]

    def _find_list_url(self, soup: BeautifulSoup, url: str) -> str:
        """ルートページから Pontaアップ店一覧ページの URL を導出する。"""
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if "/contents/lp/pontaup" in href:
                absolute = urljoin(url, href)
                # 計測用クエリ (utm_*) は不要なので落とす
                parts = urlsplit(absolute)
                return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))

        self.logger.warning("ルートに Pontaアップ店一覧へのリンクが見つかりません。既定パスを使用します。")
        return urljoin(url, _LIST_PATH)

    def parse(self, url: str) -> Generator[dict, None, None]:
        root_soup = self.get_soup(url)
        if root_soup is None:
            self.logger.error("ルートページを取得できませんでした: %s", url)
            return

        list_url = self._find_list_url(root_soup, url)
        self.logger.info("ブランド一覧ページ: %s", list_url)

        soup = self.get_soup(list_url)
        if soup is None:
            self.logger.error("ブランド一覧ページを取得できませんでした: %s", list_url)
            return

        items: list[dict] = []
        seen: set[str] = set()

        # 1) カテゴリ付きブランド一覧 (ul.category > li)
        for li in soup.select("ul.category > li"):
            img = li.select_one("img")
            if img is None:
                continue
            name = (img.get("alt") or "").strip()
            if not name or name in seen:
                continue
            seen.add(name)

            src = (img.get("src") or "").strip()
            logo = urljoin(list_url, src) if src else ""

            a = li.select_one("a[href]")
            detail = urljoin(list_url, a["href"]) if a else ""

            items.append({
                Schema.NAME: name,
                Schema.CAT_SITE: (li.get("data-category") or "").strip(),
                Schema.URL: list_url,
                "ロゴURL": logo,
                "紹介URL": detail,
            })

        # 2) 上部スライダー (カテゴリ一覧に載っていないブランドの補完)
        for img in soup.select(".infinite-slider .swiper-slide img[alt]"):
            name = (img.get("alt") or "").strip()
            if not name or name in seen:
                continue
            seen.add(name)

            src = (img.get("src") or "").strip()
            items.append({
                Schema.NAME: name,
                Schema.CAT_SITE: "",
                Schema.URL: list_url,
                "ロゴURL": urljoin(list_url, src) if src else "",
                "紹介URL": "",
            })

        self.total_items = len(items)
        self.logger.info("取得対象ブランド数: %d", self.total_items)

        yield from items


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = AuPayScraper()
    scraper.execute("https://aupay.auone.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
