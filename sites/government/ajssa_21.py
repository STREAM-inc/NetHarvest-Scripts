"""
一般社団法人 愛知県警備業協会（AJSSA 会員名簿・愛知県）— 会員名簿

取得対象:
    - 愛知県警備業協会の会員企業（警備会社・約518社）
    - 会員番号 / 会社名 / 支部 / 市区町村(住所) / TEL / 業務区分 / ホームページ

取得フロー:
    引数 url (= sites.yml の url, https://aikeikyo.jp/wp_user/wordpress/member_list/) は
    静的な単一ページ。会員一覧は支部ごと（中支部/北東支部/西支部/南支部/三河支部/
    ビルメン支部）に複数の <table> で並ぶ。各テーブルの見出し行 (th) が
    「会員番号 / 会社名 / 支部 / 市区町村 / TEL / 業務区分」で、以降のデータ行 (td) が
    会員 1 社に対応する。会社名セルに <a href> があれば会社ホームページ。
    ページ冒頭の「支部編成」テーブル（th=支部名/支部設置区割）は会員データではないため
    見出しに「会社名」を含むテーブルのみを取得対象とする。
    会員を 1 件取得するごとに即 yield する (Pattern B)。ページネーション無し。

    ※所在地はすべて愛知県内のため PREF は「愛知県」固定。市区町村を ADDR に格納する。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_21.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_21
"""

import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

_PREF = "愛知県"

# 見出しテキスト → Schema/EXTRA へのマッピング用ラベル
_COL_NAME = "会社名"


class Ajssa21(StaticCrawler):
    """一般社団法人 愛知県警備業協会 会員名簿 スクレイパー"""

    DELAY = 1.5
    # 会員番号・支部 はサイト固有の構造化ラベル（Schema に該当なし）
    EXTRA_COLUMNS = ["会員番号", "支部"]

    def parse(self, url: str):
        # 引数 url (= sites.yml の url) を唯一のルートとして取得する
        soup = self.get_soup(url)
        if not soup:
            logger.warning("ページ取得に失敗しました: %s", url)
            return

        # 見出しに「会社名」を含むテーブルのみが会員一覧（支部編成テーブルは除外）
        member_tables = []
        for table in soup.select("table"):
            headers = [th.get_text(strip=True) for th in table.select("tr th")]
            if _COL_NAME in headers:
                member_tables.append(table)

        # 進捗表示のため総件数を事前に集計（ネットワーク不要・単一ページ内）
        total = 0
        for table in member_tables:
            total += sum(1 for tr in table.select("tr") if tr.find("td"))
        self.total_items = total

        for table in member_tables:
            # このテーブルの見出し順を確定（列順はテーブルにより不変だが堅牢に）
            head_cells = table.select_one("tr").select("th")
            col_index = {th.get_text(strip=True): i for i, th in enumerate(head_cells)}

            for tr in table.select("tr"):
                tds = tr.find_all("td")
                if not tds:
                    continue  # 見出し行
                try:
                    item = self._parse_row(tds, col_index, url)
                    if item:
                        yield item
                except Exception as e:  # 個別行のエラーはスキップして継続
                    logger.warning("行の解析に失敗しskip: %s", e)
                    continue

    def _parse_row(self, tds, col_index: dict, source_url: str) -> dict | None:
        def cell(label: str):
            idx = col_index.get(label)
            if idx is None or idx >= len(tds):
                return None
            return tds[idx]

        name_cell = cell(_COL_NAME)
        if name_cell is None:
            return None
        name = self._norm(name_cell.get_text(" ", strip=True))
        if not name:
            return None

        # 会社名セル内リンク = 会社ホームページ（絶対/相対どちらも url 起点で解決）
        a = name_cell.find("a", href=True)
        hp = urljoin(source_url, a["href"].strip()) if a else ""

        def text_of(label: str) -> str:
            c = cell(label)
            return self._norm(c.get_text(" ", strip=True)) if c is not None else ""

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: _PREF,  # 愛知県警備業協会の会員 = 全て愛知県
            Schema.ADDR: text_of("市区町村"),
            Schema.TEL: text_of("TEL"),
            Schema.HP: hp,
            Schema.CAT_SITE: text_of("業務区分"),  # 例: "1号 2号"（法定業務区分の短ラベル）
            "会員番号": text_of("会員番号"),
            "支部": text_of("支部"),
        }

    @staticmethod
    def _norm(text: str) -> str:
        """全角/半角スペース・改行を単一スペースに整形する。"""
        return re.sub(r"[\s　]+", " ", text).strip()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa21()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://aikeikyo.jp/wp_user/wordpress/member_list/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
