"""
一般社団法人 石川県警備業協会（AJSSA 会員名簿・石川県）— 会員名簿

取得対象:
    - 石川県警備業協会の会員企業（警備会社・約73社）
    - 会社名 / 代表者名 / 所在地(住所) / 電話番号 / ホームページ
    ※ページ冒頭の「役員名簿」(役職名・氏名・所属)は会員名簿ではないため対象外。

取得フロー:
    引数 url (= sites.yml の url, .../pages/10/) は goope 系の静的な単一ページ。
    本文は「役員名簿」→「会員名簿」の順で並ぶ。役員名簿は class 無しの素の table、
    会員名簿は `div.dataArea` 内の `table.type007Table` として 2 ブロック(五十音順で
    分割)に分かれる。会員 1 社 = `tr.type007Tr` 1 行で、4 セル構成:
      [0] 会社名（<a href> があれば HP リンク） / [1] 代表者名 / [2] 所在地 / [3] 電話番号
    `.dataArea` 内の `tr.type007Tr` のみを対象にすることで役員名簿(素の table)は自然に
    除外される。会員を 1 件取得するごとに即 yield する (Pattern B)。ページネーション無し。

    ※所在地はすべて石川県内のため PREF は「石川県」固定。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_18.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_18
"""

import logging
import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

_PREF = "石川県"


class Ajssa18(StaticCrawler):
    """一般社団法人 石川県警備業協会 会員名簿 スクレイパー"""

    DELAY = 1.5
    # サイト固有の短い構造化カラムは無し（全て Schema に収まる）。
    EXTRA_COLUMNS = []

    def parse(self, url: str):
        # 引数 url (= sites.yml の url) を唯一のルートとして取得する
        soup = self.get_soup(url)
        if not soup:
            logger.warning("ページ取得に失敗しました: %s", url)
            return

        # 会員名簿は div.dataArea 内の tr.type007Tr。役員名簿(素の table)は含まれない。
        rows = []
        for area in soup.select(".dataArea"):
            rows.extend(area.select("tr.type007Tr"))
        self.total_items = len(rows)

        for tr in rows:
            try:
                item = self._parse_row(tr, url)
                if item:
                    yield item
            except Exception as e:  # 個別行のエラーはスキップして継続
                logger.warning("行の解析に失敗しskip: %s", e)
                continue

    def _parse_row(self, tr, source_url: str) -> dict | None:
        cells = tr.find_all("td", recursive=False)
        if len(cells) < 4:
            return None

        name = self._norm(cells[0].get_text(" ", strip=True))
        if not name:
            return None

        rep_nm = self._norm(cells[1].get_text(" ", strip=True))
        addr = self._norm(cells[2].get_text(" ", strip=True))
        tel = self._norm(cells[3].get_text(" ", strip=True))

        # 会社名セル内にリンクがあれば HP として採用
        a = cells[0].find("a", href=True)
        hp = a["href"].strip() if a else ""

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: _PREF,  # 石川県警備業協会の会員 = 全て石川県
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.REP_NM: rep_nm,
            Schema.HP: hp,
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

    scraper = Ajssa18()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://www.ishikeikyo.or.jp/pages/10/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
