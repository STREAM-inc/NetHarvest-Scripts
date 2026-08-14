"""
日本温泉協会 会員企業一覧 — spa_2

取得対象:
    - https://www.spa.or.jp/company/ (会員企業紹介) に掲載された会員企業 約105社
      企業名 / 都道府県 / 業務内容 / TEL / 公式HP を企業単位で取得する。

    ※ 既存の onsen_kyokai (https://www.spa.or.jp/search_p/) は「温泉地情報」であり
      本クローラーの対象 (会員企業) とは別データ。

取得フロー:
    1. sites.yml の url (= parse() の引数) を GET し、会員企業テーブルを取得
    2. rowspan を展開して 4 列 (都道府県 / 企業名 / 業務内容 / TEL) のグリッドに正規化
       — 都道府県セルは同一県の複数社にまたがる rowspan を持つため、展開しないと列がズレる
    3. ヘッダ行を除く各行を 1 件ずつ即 yield (Pattern B)

    ページネーションは存在しない (全件 1 ページ)。

実行方法:
    # ローカルテスト
    python scripts/sites/service/spa_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id spa_2
"""

import logging
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# ヘッダ行の判定に使う語 (1 列目が「都道府県」なら見出し行)
_HEADER_WORDS = {"都道府県", "企業名", "業務内容", "TEL", "ＴＥＬ"}

# 都道府県セルの表記ゆれ吸収用
_PREF_RE = re.compile(r"(北海道|東京都|(?:大阪|京都)府|.{2,3}県)")


class SpaCompanyCrawler(StaticCrawler):
    """日本温泉協会 会員企業一覧 スクレイパー"""

    SITE_ID = "spa_2"
    DELAY = 1.5
    EXTRA_COLUMNS = []  # 全項目が Schema に対応するため無し

    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)
        if soup is None:
            logger.error("一覧ページを取得できませんでした: %s", url)
            return

        table = self._find_company_table(soup)
        if table is None:
            logger.error("会員企業テーブルが見つかりません: %s", url)
            return

        grid = self._expand_rowspan(table)
        rows = [row for row in grid if not self._is_header(row)]
        self.total_items = len(rows)
        logger.info("会員企業テーブル: %d 件", len(rows))

        for row in rows:
            try:
                item = self._build_item(row, url)
            except Exception as e:  # 1 行の失敗で全体を止めない
                logger.warning("行の解析に失敗しました (スキップ): %s", e)
                continue
            if item:
                yield item

    # ------------------------------------------------------------------
    # テーブル探索
    # ------------------------------------------------------------------
    def _find_company_table(self, soup):
        """「企業名」列を持つテーブルを会員企業テーブルとして選ぶ。"""
        for table in soup.select("table"):
            head = table.find(["tr"])
            if head is None:
                continue
            texts = {c.get_text(strip=True) for c in head.find_all(["th", "td"])}
            if "企業名" in texts:
                return table
        # フォールバック: 本文内の最初のテーブル
        return soup.select_one(".postwrap table") or soup.select_one("table")

    # ------------------------------------------------------------------
    # rowspan 展開
    # ------------------------------------------------------------------
    @staticmethod
    def _expand_rowspan(table) -> list[list]:
        """rowspan / colspan を展開して行ごとのセルリストを返す。

        都道府県セルは同一県の複数社をまたぐ rowspan を持つので、
        展開しないと 2 社目以降で列がひとつズレる (0 列目が企業名になる)。
        """
        grid: list[list] = []
        pending: dict[int, list] = {}  # col -> [残り行数, cell]

        for tr in table.find_all("tr"):
            cells = tr.find_all(["th", "td"], recursive=False) or tr.find_all(["th", "td"])
            placed: dict[int, object] = {}

            # 前の行から繰り越された rowspan セルを先に配置
            for col in sorted(pending):
                rem, cell = pending[col]
                placed[col] = cell
                if rem - 1 <= 0:
                    del pending[col]
                else:
                    pending[col] = [rem - 1, cell]

            col = 0
            for cell in cells:
                while col in placed:
                    col += 1
                try:
                    rowspan = int(cell.get("rowspan") or 1)
                except ValueError:
                    rowspan = 1
                try:
                    colspan = int(cell.get("colspan") or 1)
                except ValueError:
                    colspan = 1
                for _ in range(max(colspan, 1)):
                    while col in placed:
                        col += 1
                    placed[col] = cell
                    if rowspan > 1:
                        pending[col] = [rowspan - 1, cell]
                    col += 1

            if not placed:
                continue
            width = max(placed) + 1
            grid.append([placed.get(i) for i in range(width)])

        return grid

    @staticmethod
    def _is_header(row: list) -> bool:
        texts = [c.get_text(strip=True) if c is not None else "" for c in row]
        return any(t in _HEADER_WORDS for t in texts[:2])

    # ------------------------------------------------------------------
    # 1 行 -> item
    # ------------------------------------------------------------------
    def _build_item(self, row: list, url: str) -> dict | None:
        def cell_text(idx: int) -> str:
            if idx < len(row) and row[idx] is not None:
                return row[idx].get_text(" ", strip=True)
            return ""

        name = cell_text(1)
        if not name:
            return None

        pref_raw = cell_text(0)
        m = _PREF_RE.search(pref_raw)
        pref = m.group(1) if m else pref_raw

        # 企業名セルのリンク = 各社の公式 HP (外部サイト)
        hp = ""
        if len(row) > 1 and row[1] is not None:
            a = row[1].select_one("a[href]")
            if a:
                href = a.get("href", "").strip()
                if href and not href.startswith(("javascript:", "mailto:", "#")):
                    hp = urljoin(url, href)

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.TEL: cell_text(3),
            Schema.LOB: cell_text(2),
            Schema.HP: hp,
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = SpaCompanyCrawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.spa.or.jp/company/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
