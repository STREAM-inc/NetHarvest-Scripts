"""
便利屋パラダイス (benriya-paradise.com) — 全国の便利屋検索ポータル 出店便利屋情報スクレイパー

取得対象:
    - WordPress カスタム投稿タイプ vendor (/vendor/{slug}/) に登録された全便利屋
    - 各便利屋詳細ページの店舗情報 (名称 / 所在地 / 営業時間 / 実電話番号 /
      対応カテゴリ / 対応エリア / メールアドレス / 保有資格・許認可)

取得フロー:
    1. ルート URL から wp-sitemap-posts-vendor-1.xml を導出し、全 vendor 詳細 URL を列挙
    2. テンプレート用の /vendor/sample/ は除外
    3. 各詳細ページを 1 件取得するごとに即 yield (途中中断に強い Pattern B)

注意:
    - 詳細ページ上部 CTA の 0120-480-056 はポータル共通のフリーダイヤルであり、
      店舗の実電話番号ではない。実番号は会社概要 dl の「TEL」(tel: リンク) を使う。
    - 店舗の自由記述 PR 文 (紹介文) は著作権リスクのため取得しない。
    - div.section.category-sec 内の div.list はサイト共通のカテゴリ説明であり
      店舗固有ではないため取得しない。店舗の対応カテゴリは div.cat から取得する。

実行方法:
    # ローカルテスト
    python scripts/sites/service/benriya_paradise.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id benriya_paradise
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
    r"(北海道|東京都|大阪府|京都府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# vendor 投稿のサイトマップ (ルート相対)。parse() で url から urljoin して使用する。
_VENDOR_SITEMAP_PATH = "/wp-sitemap-posts-vendor-1.xml"

# テンプレート/サンプル投稿。実店舗ではないため除外する。
_EXCLUDED_SLUGS = {"sample"}


class BenriyaParadiseScraper(StaticCrawler):
    """便利屋パラダイス (benriya-paradise.com) 便利屋スクレイパー"""

    DELAY = 1.5

    EXTRA_COLUMNS = [
        "対応エリア",
        "メールアドレス",
        "保有資格・許認可",
    ]

    def parse(self, url: str):
        # 🔒 引数 url を唯一のルート(SSOT)として全 URL を派生させる。
        sitemap_url = urljoin(url, _VENDOR_SITEMAP_PATH)
        self.logger.info("vendor サイトマップ取得: %s", sitemap_url)
        sitemap = self.get_soup(sitemap_url)
        if not sitemap:
            self.logger.error("サイトマップ取得に失敗しました: %s", sitemap_url)
            return

        detail_urls: list[str] = []
        seen: set[str] = set()
        for loc in sitemap.find_all("loc"):
            href = loc.get_text(strip=True)
            if not href or "/vendor/" not in href:
                continue
            slug = href.rstrip("/").rsplit("/", 1)[-1]
            if slug in _EXCLUDED_SLUGS:
                continue
            if href in seen:
                continue
            seen.add(href)
            detail_urls.append(href)

        self.total_items = len(detail_urls)
        self.logger.info("vendor 件数: %d", len(detail_urls))

        for detail_url in detail_urls:
            try:
                record = self._scrape_detail(detail_url)
            except Exception as e:
                self.logger.warning("詳細取得失敗: %s — %s", detail_url, e)
                continue
            if record:
                # 1 件取得ごとに即 yield (全件収集してから一括 yield しない)
                yield record
            time.sleep(self.DELAY)

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if not soup:
            return None

        item: dict = {Schema.URL: detail_url}

        # 名称
        h1 = soup.select_one("h1")
        if h1:
            item[Schema.NAME] = h1.get_text(strip=True)

        # 所在地 (div.area > div.text)。都道府県を分離する。
        area = soup.select_one("div.area div.text")
        if area:
            addr = area.get_text(" ", strip=True)
            if addr:
                item[Schema.ADDR] = addr
                pm = _PREF_PATTERN.search(addr)
                if pm:
                    item[Schema.PREF] = pm.group(1)

        # 対応カテゴリ (div.cat 内の span.span)。サイト定義業種として保存。
        cat = soup.select_one("div.cat")
        if cat:
            cats = [
                sp.get_text(strip=True)
                for sp in cat.select("span.span")
                if sp.get_text(strip=True)
            ]
            if cats:
                item[Schema.CAT_SITE] = "、".join(cats)

        # 会社概要 dl (div.dl-flex > div.dt / div.dd) をラベルで振り分け
        for fl in soup.select("div.dl-flex"):
            dt = fl.select_one(".dt")
            dd = fl.select_one(".dd")
            if not dt or not dd:
                continue
            label = dt.get_text(strip=True)
            value = dd.get_text(" ", strip=True)
            if not value:
                continue
            if label == "TEL":
                # 実電話番号 (CTA のフリーダイヤルではなく店舗の番号)
                item[Schema.TEL] = value
            elif label == "営業時間":
                item[Schema.TIME] = value
            elif label == "対応エリア":
                item["対応エリア"] = value
            elif label == "メールアドレス":
                item["メールアドレス"] = value
            elif "保有資格" in label or "許認可" in label:
                item["保有資格・許認可"] = value

        if not item.get(Schema.NAME):
            return None

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BenriyaParadiseScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://benriya-paradise.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
