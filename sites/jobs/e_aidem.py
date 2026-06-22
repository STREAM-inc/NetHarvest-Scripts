# scripts/sites/jobs/e_aidem.py
"""
イーアイデム【全国】 (e-aidem.com) — 企業情報スクレイパー

取得対象:
    - 詳細ページの「企業情報」セクション (.companyBox)
        社名 (名称), 企業概要, 本社所在地 (都道府県 / 住所), URL (HP)
    - 詳細ページの「応募情報」セクション (.applicationBox)
        連絡先TEL (TEL)

取得フロー:
    トップページ (?show=1) からエリア (region_id=01〜07) を発見
      → 各エリアの一覧ページ (/aps/list.htm?region_id=NN&page=M) をページ送り
        → 詳細ページ (/aps/*_detail.htm) を 1 件ずつ取得して即 yield
    ※ region_id 01〜07 で全国 (北海道〜九州) を網羅 (総件数 約 120,000 件)。

URL 一貫性 (SSOT = sites.yml):
    parse(url) は引数 url (= https://www.e-aidem.com/?show=1) を唯一のルートとし、
    一覧・詳細 URL はすべて urljoin で url から派生させる。固定 URL はハードコードしない。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/e_aidem.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id e_aidem
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 都道府県の先頭一致用パターン (本社所在地から都道府県を分離する)
_PREF_PATTERN = re.compile(r"^(北海道|東京都|京都府|大阪府|.{2,3}県)")
# 郵便番号除去用
_POSTCODE_PATTERN = re.compile(r"〒?\s*\d{3}-?\d{4}")


class EAidemScraper(StaticCrawler):
    """イーアイデム【全国】 企業情報スクレイパー"""

    DELAY = 1.0  # サーバー負荷軽減（秒）
    EXTRA_COLUMNS = ["企業概要"]  # 事業内容・会社概要 (備考で取得指定済み)

    def parse(self, url):
        """
        トップページ → 各エリア一覧 (ページ送り) → 詳細ページ → 企業情報取得。

        Args:
            url: サイトの正規ルート URL (https://www.e-aidem.com/?show=1)。
                 一覧・詳細 URL はこの url から派生させる。
        """
        region_ids = self._discover_regions(url)
        self.logger.info("対象エリア: %s", region_ids)

        for rid in region_ids:
            yield from self._crawl_region(url, rid)

    # ------------------------------------------------------------------
    def _discover_regions(self, root_url: str) -> list[str]:
        """トップページからエリア ID (region_id) を発見する。

        取得できない場合は全国 7 エリア (01〜07) にフォールバックする。
        """
        region_ids: list[str] = []
        soup = self.get_soup(root_url)
        if soup:
            for a in soup.select("a[href*='list.htm']"):
                m = re.search(r"region_id=(\d+)", a.get("href", ""))
                if m and m.group(1) not in region_ids:
                    region_ids.append(m.group(1))
        # 並びを安定させる (01〜07)
        region_ids = sorted(region_ids)
        return region_ids or [f"{i:02d}" for i in range(1, 8)]

    def _crawl_region(self, root_url: str, region_id: str):
        """1 エリアを全ページ巡回し、詳細を 1 件ずつ取得して即 yield する。"""
        page = 1
        while True:
            list_url = urljoin(
                root_url, f"/aps/list.htm?region_id={region_id}&page={page}"
            )
            self.logger.info("一覧取得: region_id=%s page=%d", region_id, page)
            soup = self.get_soup(list_url)
            if soup is None:
                break  # 最終ページ超過 (404) 等 → このエリア終了

            # 総件数を初回だけ total_items に設定 (ETA 表示用)
            if self.total_items is None:
                m = re.search(r"([\d,]+)\s*件", soup.get_text())
                if m:
                    try:
                        self.total_items = int(m.group(1).replace(",", ""))
                    except ValueError:
                        pass

            detail_urls: list[str] = []
            for a in soup.select("a[href*='_detail.htm']"):
                href = a.get("href", "")
                if "_detail.htm" not in href:
                    continue
                full = urljoin(root_url, href)
                if full not in detail_urls:
                    detail_urls.append(full)

            if not detail_urls:
                break  # 詳細リンクが無くなったら最終ページ

            for detail_url in detail_urls:
                try:
                    item = self._scrape_detail(detail_url)
                    if item:
                        yield item
                except Exception as e:  # noqa: BLE001 — 個別失敗は握りつぶして継続
                    self.logger.warning(
                        "詳細ページ取得失敗: %s (%s)", detail_url, e
                    )
                    continue

            page += 1

    # ------------------------------------------------------------------
    def _scrape_detail(self, detail_url: str) -> dict | None:
        """詳細ページから企業情報と連絡先TELを取得する。"""
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        item = {Schema.URL: detail_url}

        # --- 企業情報 (.companyBox) ---
        company_box = soup.select_one(".companyBox")
        if company_box:
            for row in company_box.select("tr"):
                th = row.select_one("th")
                td = row.select_one("td")
                if not th or not td:
                    continue

                label = th.get_text(strip=True)

                if label == "社名":
                    # <span class="other">「○○の他の求人をみる」を除外
                    other = td.select_one(".other")
                    if other:
                        other.extract()
                    item[Schema.NAME] = td.get_text(strip=True)

                elif label == "本社所在地":
                    addr_text = re.sub(
                        r"\s+", " ", td.get_text(" ", strip=True)
                    ).strip()
                    # 郵便番号を除去
                    addr_text = _POSTCODE_PATTERN.sub("", addr_text).strip()
                    # 先頭の都道府県を分離
                    m = _PREF_PATTERN.match(addr_text)
                    if m:
                        item[Schema.PREF] = m.group(1)
                        addr_text = addr_text[m.end():].strip()
                    item[Schema.ADDR] = addr_text

                elif label == "URL":
                    a_tag = td.select_one("a")
                    if a_tag:
                        item[Schema.HP] = a_tag.get("href", "").strip()

                elif label == "企業概要":
                    item["企業概要"] = td.get_text(" ", strip=True)

        # --- 連絡先TEL (.applicationBox) ---
        app_box = soup.select_one(".applicationBox")
        if app_box:
            for row in app_box.select("tr"):
                th = row.select_one("th")
                if th and "TEL" in th.get_text():
                    td = row.select_one("td")
                    if td:
                        item[Schema.TEL] = td.get_text(strip=True)
                    break

        # 社名が取れなかった場合はスキップ
        if Schema.NAME not in item:
            return None

        return item


# =============================================================================
# ローカル実行用エントリーポイント
# =============================================================================
if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = EAidemScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.e-aidem.com/?show=1")

    print("\n" + "=" * 60)
    print("📊 実行結果サマリ")
    print("=" * 60)
    print(f"  出力ファイル:     {scraper.output_filepath}")
    print(f"  取得件数:         {scraper.item_count}")
    print(f"  観測カラム数:     {len(scraper.observed_columns)}")
    print(f"  観測カラム:       {scraper.observed_columns}")
    print("=" * 60)
