"""
サーチ大分 (search-oita.com) — 掲載店舗情報スクレイパー

取得対象:
    - 大分市 (都町中心) のナイトビジネス店舗 (キャバクラ・スナック・ガールズバー等) の店舗概要

取得フロー:
    /shop/ 一覧ページ → ページネーション (/shop/page/N/) を巡回し、
    各店舗の詳細ページ (/shop/{slug}/) の概要 (dl.flex_area の dt→dd) を
    パースして 1 件ずつ yield する (Pattern B: detail 取得ごとに即 yield)。

備考:
    - 全店舗が大分市内のため PREF は「大分県」固定。所在地欄には都道府県表記が無い。
    - SYSTEM(料金) セクション (セット料金 / シングルチャージ / 延長料金 / 飲み放題 /
      その他サービス 等) は自由記述プロース (著作権リスク) のため取得しない。
    - ページ上の Instagram はサイト運営者自身 (search_oita) のアカウントで店舗固有では
      ないため取得しない。
    - 電話番号は未掲載の店舗が多い (掲載時のみ取得)。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/search_oita.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id search_oita
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

# 概要 (dl.flex_area) のラベル → Schema 定数 の対応 (構造化された短い値のみ)
_LABEL_TO_SCHEMA = {
    "営業時間": Schema.TIME,
    "店休日": Schema.HOLIDAY,
    "クレジットカード": Schema.PAYMENTS,
    "電話番号": Schema.TEL,
}

# 概要 (dl.flex_area) のラベル → EXTRA_COLUMNS (構造化された短い値のみ)
_LABEL_TO_EXTRA = {
    "電子決済": "電子決済",
    "卓数・座席": "卓数・座席",
    "在籍キャスト数": "在籍キャスト数",
    "キャスト衣装": "キャスト衣装",
    "カラオケ": "カラオケ",
    "駐車場": "駐車場",
}

_MAX_PAGES = 50  # 無限ループ防止の安全上限 (実際は 5 ページ程度)


class SearchOitaScraper(StaticCrawler):
    """サーチ大分 (search-oita.com) 店舗情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "電子決済",
        "卓数・座席",
        "在籍キャスト数",
        "キャスト衣装",
        "カラオケ",
        "駐車場",
        "タグ",
    ]

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
        """一覧ページから店舗詳細リンク (/shop/{slug}/) を収集する。"""
        links: list[str] = []
        for a in soup.select("a[href]"):
            href = a.get("href", "").strip()
            if not href:
                continue
            absolute = urljoin(base_url, href)
            parsed = urlparse(absolute)
            m = re.match(r"^/shop/([a-z0-9][a-z0-9-]*)/$", parsed.path)
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

        # 概要 (dl.flex_area) を dt(ラベル)→dd(値) の辞書に変換する。
        # SYSTEM(料金) セクションも同じ dl 構造だが、下でホワイトリストのラベルだけ
        # 拾うため自由記述プロースは自然に除外される。
        pairs: dict[str, str] = {}
        for dl in soup.select("dl"):
            dt = dl.find("dt")
            dd = dl.find("dd")
            if not dt or dd is None:
                continue
            label = dt.get_text(" ", strip=True)
            if label:
                pairs[label] = dd.get_text(" ", strip=True)

        item: dict = {
            Schema.URL: url,
            Schema.PREF: "大分県",  # 全店舗が大分市内。所在地欄に都道府県表記が無い
        }

        # 店名・ジャンル (.ttl_block 内)
        ttl_block = soup.select_one(".ttl_block")
        name = ""
        genre = ""
        if ttl_block:
            h2 = ttl_block.select_one("h2.ttl")
            ctg = ttl_block.select_one(".ctg_shop")
            name = h2.get_text(" ", strip=True) if h2 else ""
            genre = ctg.get_text(" ", strip=True) if ctg else ""
        item[Schema.NAME] = name
        item[Schema.CAT_SITE] = genre

        # 所在地 (dl) → 住所
        item[Schema.ADDR] = pairs.get("所在地", "")

        # WEB サイト (dl 内の a href、無ければテキスト)
        website = ""
        for dl in soup.select("dl"):
            dt = dl.find("dt")
            if dt and dt.get_text(strip=True) == "WEBサイト":
                dd = dl.find("dd")
                if dd:
                    a = dd.find("a", href=True)
                    website = a["href"].strip() if a else dd.get_text(" ", strip=True)
                break
        item[Schema.WEBSITE] = website

        # 概要ラベル → Schema (構造化された短い値のみ)
        for label, schema_key in _LABEL_TO_SCHEMA.items():
            item[schema_key] = pairs.get(label, "")

        # 概要ラベル → EXTRA_COLUMNS (構造化された短い値のみ)
        for label, col in _LABEL_TO_EXTRA.items():
            item[col] = pairs.get(label, "")

        # タグ (.tag_area a.tag、構造化された短いラベルのみ)
        tags = [a.get_text(" ", strip=True) for a in soup.select(".tag_area a.tag")]
        item["タグ"] = " / ".join(t for t in tags if t)

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

    scraper = SearchOitaScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://search-oita.com/shop/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
