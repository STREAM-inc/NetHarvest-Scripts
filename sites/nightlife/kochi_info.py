# scripts/sites/nightlife/kochi_info.py
"""
よさこい無料案内所 (kochi-info.jp) — 掲載店舗情報スクレイパー

取得対象:
    - 高知市中心のナイトビジネス店舗 (ラウンジ・スナック・クラブ・バー等) の店舗概要

取得フロー:
    /partner/ 一覧ページ → ページネーション (/partner/page/N/) を巡回し、
    各店舗の詳細ページ (/partner/{slug}/) の概要テーブルをパースして 1 件ずつ yield する。

備考:
    - このサイトには店舗の電話番号・店舗固有の SNS/HP が掲載されていないため取得しない。
    - 料金の自由記述行 (主な料金 / 飲み放題 / 他の料金 / セット料金 等) は
      長文プロース (著作権リスク) のため取得しない。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/kochi_info.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id kochi_info
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import requests

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 47 都道府県の先頭一致パターン (「エリア」欄から都道府県を切り出す)
_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 概要テーブルのラベル → Schema 定数 の対応 (構造化された短い値のみ)
_LABEL_TO_SCHEMA = {
    "店名": Schema.NAME,
    "業種": Schema.CAT_SITE,
    "営業時間": Schema.TIME,
    "定休日": Schema.HOLIDAY,
    "クレジットカード": Schema.PAYMENTS,
}

# 概要テーブルのラベル → EXTRA_COLUMNS (構造化された短い値のみ)
_LABEL_TO_EXTRA = {
    "席数": "席数",
    "設備等": "設備等",
    "フロアレディの衣装": "フロアレディの衣装",
    "TAX": "TAX",
}

_MAX_PAGES = 50  # 無限ループ防止の安全上限 (実際は 4 ページ程度)


class KochiInfoScraper(StaticCrawler):
    """よさこい無料案内所 (kochi-info.jp) 店舗情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["席数", "設備等", "フロアレディの衣装", "TAX"]

    def parse(self, url: str):
        base = url.rstrip("/")
        seen: set[str] = set()

        for page in range(1, _MAX_PAGES + 1):
            page_url = url if page == 1 else f"{base}/page/{page}/"
            try:
                soup = self.get_soup(page_url)
            except requests.exceptions.RequestException:
                # ページネーション末尾 (404) 到達 → 巡回終了
                break
            if soup is None:
                break

            shop_links = [u for u in self._collect_shop_links(soup, url) if u not in seen]
            if not shop_links:
                # このページに新規店舗リンクが無い → 末尾とみなす
                break

            for shop_url in shop_links:
                seen.add(shop_url)
                item = self._scrape_detail(shop_url)
                if item:
                    yield item

    def _collect_shop_links(self, soup, base_url: str) -> list[str]:
        """一覧ページから店舗詳細リンク (/partner/{slug}/) を収集する。"""
        links: list[str] = []
        for a in soup.select("a[href]"):
            href = a.get("href", "").strip()
            if not href:
                continue
            absolute = urljoin(base_url, href)
            parsed = urlparse(absolute)
            m = re.match(r"^/partner/([a-z0-9][a-z0-9-]*)/$", parsed.path)
            if not m:
                continue
            slug = m.group(1)
            if slug in ("page", "feed"):
                continue
            links.append(f"{parsed.scheme}://{parsed.netloc}{parsed.path}")
        # ページ内重複を除去 (出現順を維持)
        return list(dict.fromkeys(links))

    def _scrape_detail(self, url: str) -> dict | None:
        try:
            soup = self.get_soup(url)
        except requests.exceptions.RequestException:
            return None
        if soup is None:
            return None

        # 概要テーブル (th/td) をラベル→値の辞書に変換する
        pairs: dict[str, str] = {}
        table = soup.select_one("table")
        if table:
            for tr in table.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                label = th.get_text(" ", strip=True)
                value = td.get_text(" ", strip=True)
                if label:
                    pairs[label] = value

        item: dict = {Schema.URL: url}

        # Schema マッピング (構造化された短い値のみ)
        for label, schema_key in _LABEL_TO_SCHEMA.items():
            if label in pairs:
                item[schema_key] = pairs[label]

        # 店名は概要テーブル優先、無ければ h1 で補完
        if not item.get(Schema.NAME):
            h1 = soup.select_one("h1")
            item[Schema.NAME] = h1.get_text(" ", strip=True) if h1 else ""

        # エリア → 都道府県 + 住所 に分解
        area = pairs.get("エリア", "")
        if area:
            m = _PREF_PATTERN.match(area)
            if m:
                item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = area[m.end():].strip()
            else:
                item[Schema.ADDR] = area

        # EXTRA_COLUMNS (構造化された短い値のみ)
        for label, col in _LABEL_TO_EXTRA.items():
            item[col] = pairs.get(label, "")

        # NAME が取れなければ不完全レコードとしてスキップ
        if not item.get(Schema.NAME):
            return None
        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = KochiInfoScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://kochi-info.jp/partner/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
