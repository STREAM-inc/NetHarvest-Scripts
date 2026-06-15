"""
ITトレンド — 法人向けIT製品比較ポータル スクレイパー

取得対象:
    - IT製品カタログ (412カテゴリ × 平均約20製品 ≒ 約8,000件)
    - 製品名・提供企業名・企業住所・代表者名・資本金・従業員数・設立日・HP

取得フロー:
    1. /category ページの __NEXT_DATA__ で全カテゴリ URI 取得 (412件)
    2. 各カテゴリページの __NEXT_DATA__ から製品一覧取得
    3. 各製品詳細ページの __NEXT_DATA__ から企業・製品データ取得し即 yield
    製品IDで重複排除しながら巡回する

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/it.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id it
"""

import json
import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


_PREF_PATTERN = re.compile(
    r"^(北海道|(?:東京|大阪|京都|神奈川|愛知|兵庫|福岡|埼玉|千葉"
    r"|静岡|広島|宮城|茨城|新潟|栃木|群馬|長野|岐阜|福島|三重"
    r"|熊本|鹿児島|岡山|山口|愛媛|長崎|滋賀|奈良|沖縄|青森|岩手"
    r"|秋田|山形|富山|石川|福井|山梨|和歌山|鳥取|島根|香川|高知"
    r"|徳島|佐賀|大分|宮崎)都?道?府?県?)"
)

_POST_PATTERN = re.compile(r"〒(\d{3}-\d{4})\s*")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def _parse_address(address: str):
    """住所文字列を郵便番号・都道府県・残り住所に分解する"""
    addr = _clean(address)
    post_code = ""
    pref = ""

    m = _POST_PATTERN.match(addr)
    if m:
        post_code = m.group(1)
        addr = addr[m.end():].strip()

    m2 = _PREF_PATTERN.match(addr)
    if m2:
        pref = m2.group(1)
        addr = addr[m2.end():].strip()

    return post_code, pref, addr


class ItTrendScraper(StaticCrawler):
    """ITトレンド 法人向けIT製品比較スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["製品名", "対象企業規模"]

    def parse(self, url: str):
        base = url.rstrip("/") + "/"

        # Step 1: /category から全カテゴリ URI を取得
        cat_soup = self.get_soup(f"{base}category")
        if not cat_soup:
            self.logger.error("Category page fetch failed")
            return

        nd = cat_soup.find("script", id="__NEXT_DATA__")
        if not nd:
            self.logger.error("Category page: __NEXT_DATA__ not found")
            return

        category_groups = (
            json.loads(nd.string)["props"]["pageProps"]["data"]
            ["category_groups"]["category_groups"]
        )

        cat_uris = [
            cat["uri"]
            for group in category_groups
            for cat in group.get("categories", [])
            if cat.get("uri")
        ]

        self.total_items = len(cat_uris) * 20  # 推定件数（進捗表示用）
        seen_ids: set[int] = set()

        # Step 2: 各カテゴリの製品を巡回
        for cat_uri in cat_uris:
            cat_page = self.get_soup(f"{base}{cat_uri}")
            if not cat_page:
                continue

            nd2 = cat_page.find("script", id="__NEXT_DATA__")
            if not nd2:
                continue

            try:
                products = (
                    json.loads(nd2.string)["props"]["pageProps"]["data"]
                    ["archiveProductsData"]["products"]
                )
            except (KeyError, json.JSONDecodeError) as e:
                self.logger.warning(f"Category {cat_uri}: parse error — {e}")
                continue

            # Step 3: 各製品の詳細ページを取得して即 yield
            for prod in products:
                prod_id = prod.get("id")
                if not prod_id or prod_id in seen_ids:
                    continue
                seen_ids.add(prod_id)

                detail_url = f"{base}{cat_uri}/{prod_id}"
                detail_soup = self.get_soup(detail_url)
                if not detail_soup:
                    continue

                nd3 = detail_soup.find("script", id="__NEXT_DATA__")
                if not nd3:
                    continue

                try:
                    dc = (
                        json.loads(nd3.string)["props"]["pageProps"]["data"]
                        ["detailContents"]
                    )
                except (KeyError, json.JSONDecodeError) as e:
                    self.logger.warning(f"Product {detail_url}: parse error — {e}")
                    continue

                pf = dc.get("product_fundamentals") or {}
                pc = dc.get("product_company") or {}
                po = dc.get("product_overview") or {}

                post_code, pref, addr = _parse_address(pc.get("address", ""))

                ps = po.get("product_specification") or {}

                hp = _clean(pc.get("url") or pf.get("source_url", ""))

                yield {
                    Schema.URL: detail_url,
                    Schema.NAME: _clean(pc.get("name") or pf.get("company_name", "")),
                    Schema.POST_CODE: post_code,
                    Schema.PREF: pref,
                    Schema.ADDR: addr,
                    Schema.REP_NM: _clean(pc.get("president_name", "")),
                    Schema.CAP: _clean(pc.get("capital", "")),
                    Schema.EMP_NUM: _clean(pc.get("employee_scale", "")),
                    Schema.OPEN_DATE: _clean(pc.get("found_date", "")),
                    Schema.HP: hp,
                    Schema.CAT_SITE: _clean(pf.get("category_name", "")),
                    "製品名": _clean(pf.get("product_name", "")),
                    "対象企業規模": _clean(po.get("employee_scale", "")),
                }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = ItTrendScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://it-trend.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
