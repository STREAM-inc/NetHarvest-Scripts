# scripts/sites/agency_franchise/dairitenboshu.py
"""
代理店本舗 — 代理店募集スクレイパー
対象URL: https://dairitenboshu.com/search/

取得対象 (JSON-LD / Service):
    - 会社名 / HP / 郵便番号 / 都道府県 / 住所

取得対象 (JSON-LD / BreadcrumbList):
    - 大業種 (position=2) / サイト定義業種 (position=3)

取得対象 (会社情報テーブル th/td):
    - 会社名 / 所在地 / 設立日 / 代表者名 / 役職 / 資本金 / 従業員数 / 事業内容

取得フロー:
    一覧ページ (/search/ → /search/page_N) → 詳細URL収集 → 各 /syo/{ID} でデータ取得

実行方法:
    # ローカル直接実行
    python scripts/sites/agency_franchise/dairitenboshu.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id dairitenboshu
"""

import json
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

# プロジェクトルートを sys.path に追加（ローカル直接実行対応）
# scripts/sites/agency_franchise/xxx.py → .parent×4 でプロジェクトルートを取得
_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://dairitenboshu.com"
LIST_URL = "https://dairitenboshu.com/search/"

# カテゴリID一覧（1〜10: IT・情報通信〜その他）
# デフォルトの /search/ ページネーションは全213件しか表示しないため、
# カテゴリ別に全10カテゴリを巡回して全301件を取得する。
_CATEGORIES = list(range(1, 11))

# 代表者の役職パターン（名前から分離する）
_TITLE_PATTERN = re.compile(
    r"^(代表取締役(?:社長)?|取締役|代表|社長|会長|CEO|COO|CFO|CTO|執行役員|専務取締役|常務取締役|専務|常務)"
    r"[\s　]+(.*)"
)


class DairitenboshuScraper(StaticCrawler):
    """代理店本舗スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["お取り扱い商材"]

    def parse(self, url: str):
        """
        全10カテゴリを順番に巡回し、重複除外しながら全案件URLを収集・スクレイピングする。

        背景:
            デフォルト /search/ ページネーションは 213件しか表示しない。
            サイト全体の実件数は 301件であり、残り88件はカテゴリ別ページにのみ存在する。
            カテゴリ別URL /search/category_N (N=1〜10) を巡回することで全件を取得できる。

        URL構造:
            カテゴリ1ページ目 = /search/category_N
            カテゴリNページM = /search/category_N/page_M
        """
        self.total_items = 301  # サイトヘッダー「全301件」から
        scraped_urls: set[str] = set()

        for cat_id in _CATEGORIES:
            cat_base = f"{BASE_URL}/search/category_{cat_id}"
            page = 1
            cat_max_page: int | None = None

            while True:
                list_url = cat_base if page == 1 else f"{cat_base}/page_{page}"
                self.logger.info(
                    "カテゴリ%d page=%d 取得中 (累計: %d件)", cat_id, page, len(scraped_urls)
                )

                try:
                    soup = self.get_soup(list_url)
                except Exception as e:
                    self.logger.warning("一覧ページ取得失敗: %s / %s", list_url, e)
                    break

                # 1ページ目でカテゴリの最終ページ番号を確認
                if page == 1:
                    _, cat_max_page = self._extract_total_and_max_page(soup, cat_id=cat_id)
                    self.logger.info("カテゴリ%d: 最終ページ=%d", cat_id, cat_max_page)

                # カテゴリ横断で重複除外しながらURLを収集
                new_urls = []
                seen_on_page: set[str] = set()
                for a in soup.select("a[href*='/syo/']"):
                    href = a.get("href", "")
                    if not re.search(r"/syo/\d+$", href):
                        continue
                    full_url = urljoin(BASE_URL, href)
                    if full_url not in scraped_urls and full_url not in seen_on_page:
                        new_urls.append(full_url)
                        seen_on_page.add(full_url)

                self.logger.info(
                    "カテゴリ%d page=%d: %d件の新規URL検出", cat_id, page, len(new_urls)
                )

                # 新規URLの詳細ページをスクレイピング
                for detail_url in new_urls:
                    time.sleep(self.DELAY)
                    try:
                        item = self._scrape_detail(detail_url)
                        if item:
                            scraped_urls.add(detail_url)
                            yield item
                    except Exception as e:
                        self.logger.warning("詳細ページ取得失敗: %s / %s", detail_url, e)
                        continue

                # 次ページ判定
                if cat_max_page is not None:
                    if page >= cat_max_page:
                        self.logger.info(
                            "カテゴリ%d 最終ページ(%d)完了", cat_id, cat_max_page
                        )
                        break
                else:
                    if not soup.select_one(f"a[href*='category_{cat_id}/page_{page + 1}']"):
                        self.logger.info("カテゴリ%d 次ページなし", cat_id)
                        break

                page += 1
                time.sleep(self.DELAY)

        self.logger.info("全カテゴリ完了。合計%d件取得。", len(scraped_urls))

    # ──────────────────────────────────────────────
    # ページネーション補助
    # ──────────────────────────────────────────────

    def _extract_total_and_max_page(self, soup, cat_id: int | None = None) -> tuple[int, int]:
        """
        「全N件中X～Y件表示」からの総件数と最終ページ番号を取得する。

        Args:
            soup    : 一覧ページの BeautifulSoup オブジェクト
            cat_id  : カテゴリID（1〜10）。None の場合はデフォルト /search/ 扱い。

        Returns:
            (total_count, max_page) のタプル。取得失敗時は (0, 1)。
        """
        import math

        total = 0
        m = re.search(r"全(\d+)件中", soup.get_text())
        if m:
            total = int(m.group(1))

        # ページネーションリンクから最終ページ番号を取得
        max_page = 1
        if cat_id is not None:
            # カテゴリページ: /search/category_N/page_M
            selector = f"a[href*='/search/category_{cat_id}/page_']"
            pattern = rf"/search/category_{cat_id}/page_(\d+)"
        else:
            # デフォルト検索: /search/page_N
            selector = "a[href*='/search/page_']"
            pattern = r"/search/page_(\d+)"

        for a in soup.select(selector):
            pm = re.search(pattern, a.get("href", ""))
            if pm:
                n = int(pm.group(1))
                if n > max_page:
                    max_page = n

        # ページネーションリンクに最終ページが表示されない場合は件数から計算
        if total and max_page == 1:
            max_page = math.ceil(total / 30)

        return total, max_page

    # ──────────────────────────────────────────────
    # 詳細ページのスクレイピング
    # ──────────────────────────────────────────────

    def _scrape_detail(self, detail_url: str) -> dict | None:
        """詳細ページ1件からデータを取得して dict を返す"""
        self.logger.info("詳細ページ取得: %s", detail_url)
        soup = self.get_soup(detail_url)

        item: dict = {Schema.URL: detail_url}

        # JSON-LD から取得（HP / 住所系 / カテゴリ）
        self._parse_json_ld(soup, item)

        # 会社情報テーブルから取得（会社名 / 設立日 / 代表者 / 資本金 等）
        self._parse_company_table(soup, item)

        if not item.get(Schema.NAME):
            self.logger.warning("会社名が取得できませんでした: %s", detail_url)
            return None

        return item

    # ──────────────────────────────────────────────
    # JSON-LD パース
    # ──────────────────────────────────────────────

    def _parse_json_ld(self, soup, item: dict) -> None:
        """
        <script type="application/ld+json"> からデータを取得する。

        Service ブロック:
            provider.name       → Schema.NAME（会社名テーブル未取得時のフォールバック）
            provider.url        → Schema.HP（dairitenboshu.com 自体は除外）
            provider.address
              .addressRegion    → Schema.PREF
              .postalCode       → Schema.POST_CODE
              .addressLocality  → Schema.ADDR

        BreadcrumbList ブロック:
            position=2 の name  → Schema.CAT_LV1（大業種）
            position=3 の name  → Schema.CAT_SITE（サイト定義業種）
        """
        for script in soup.select("script[type='application/ld+json']"):
            try:
                data = json.loads(script.string or "")
            except (json.JSONDecodeError, TypeError):
                continue

            dtype = data.get("@type", "")

            if dtype == "Service":
                provider = data.get("provider", {})

                # 会社名（テーブルのフォールバック用）
                name = provider.get("name", "").strip()
                if name and not item.get(Schema.NAME):
                    item[Schema.NAME] = name

                # HP（サイト自身のURLは除外）
                hp = provider.get("url", "").strip()
                if hp and "dairitenboshu.com" not in hp and not item.get(Schema.HP):
                    item[Schema.HP] = hp

                # 住所
                addr_data = provider.get("address", {})

                pref = addr_data.get("addressRegion", "").strip()
                if pref and not item.get(Schema.PREF):
                    item[Schema.PREF] = pref

                postal = addr_data.get("postalCode", "").strip()
                if postal and not item.get(Schema.POST_CODE):
                    item[Schema.POST_CODE] = postal

                locality = addr_data.get("addressLocality", "").strip()
                if locality and not item.get(Schema.ADDR):
                    item[Schema.ADDR] = locality

            elif dtype == "BreadcrumbList":
                for crumb in data.get("itemListElement", []):
                    pos = crumb.get("position")
                    name = crumb.get("name", "").strip()
                    if pos == 2 and name and not item.get(Schema.CAT_LV1):
                        item[Schema.CAT_LV1] = name
                    elif pos == 3 and name and not item.get(Schema.CAT_SITE):
                        item[Schema.CAT_SITE] = name

    # ──────────────────────────────────────────────
    # 会社情報テーブル パース
    # ──────────────────────────────────────────────

    def _parse_company_table(self, soup, item: dict) -> None:
        """
        「〇〇の会社情報」h2 直下の <table> から th/td ペアを取得する。

        取得フィールド:
            会社名 / 所在地 / 設立 / 代表者 / 資本金 / 従業員数 / 事業内容 / お取り扱い商材
        """
        # 「会社情報」を含む h2 の直後のテーブルを探す
        table = None
        for h2 in soup.find_all("h2"):
            if "会社情報" in h2.get_text():
                table = h2.find_next("table")
                break

        # フォールバック: ページ内の最初のテーブル
        if table is None:
            table = soup.find("table")

        if table is None:
            return

        for row in table.select("tr"):
            th = row.select_one("th")
            td = row.select_one("td")
            if not th or not td:
                continue

            label = th.get_text(strip=True)
            value = re.sub(r"\s+", " ", td.get_text(separator=" ", strip=True)).strip()

            if not value:
                continue

            if label == "会社名":
                # JSON-LD で既に取得済みでも上書き（テーブルを優先）
                item[Schema.NAME] = value

            elif label == "所在地":
                # JSON-LD で都道府県・住所が設定済みの場合は補完しない
                if not item.get(Schema.ADDR):
                    item[Schema.ADDR] = value

            elif label == "設立":
                item[Schema.OPEN_DATE] = value

            elif label == "代表者":
                # 役職と氏名を分離する
                # 例: "代表取締役 佐野翔太郎" → POS_NM="代表取締役", REP_NM="佐野翔太郎"
                # 例: "青山 仁" → POS_NM なし, REP_NM="青山 仁"
                m = _TITLE_PATTERN.match(value)
                if m:
                    item[Schema.POS_NM] = m.group(1)
                    item[Schema.REP_NM] = m.group(2).strip()
                else:
                    item[Schema.REP_NM] = value

            elif label == "資本金":
                item[Schema.CAP] = value

            elif label == "従業員数":
                item[Schema.EMP_NUM] = value

            elif label == "事業内容":
                item[Schema.LOB] = value

            elif label == "お取り扱い商材":
                item["お取り扱い商材"] = value


# =============================================================================
# ローカル実行用エントリーポイント
# =============================================================================
if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = DairitenboshuScraper()
    scraper.execute(LIST_URL)

    print("\n" + "=" * 40)
    print("実行結果サマリ")
    print("=" * 40)
    print(f"  出力ファイル: {scraper.output_filepath}")
    print(f"  取得件数:     {scraper.item_count}")
    print(f"  観測カラム:   {scraper.observed_columns}")
    print("=" * 40)

    if scraper.output_filepath:
        print("\n CSV 先頭5行:")
        print("-" * 40)
        with open(scraper.output_filepath, encoding="utf-8-sig") as f:
            for i, line in enumerate(f):
                if i >= 6:
                    break
                print(line.rstrip())
