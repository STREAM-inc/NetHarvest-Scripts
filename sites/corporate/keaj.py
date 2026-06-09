"""
在日中国朝鮮族経営者協会 (keaj.org) — 会員企業スクレイパー

取得対象:
    - 会員一覧ページ (/members/) の会員企業（約 38 件）
    - 会社名・業種・代表者名・協会役職

取得フロー:
    /members/ の単一テーブル (table.text-sm) を解析し、各行を 1 件として即 yield する。
    詳細ページ・ページネーションは存在しない（全件が 1 ページの表に収まる）。

実行方法:
    python scripts/sites/corporate/keaj.py
    python bin/run_flow.py --site-id keaj
"""

import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


def _clean(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"[\s　\xa0\r\n]+", " ", str(text)).strip()
    # 値なしプレースホルダ（協会役職列の "—" 等）は空文字に正規化
    if text in {"—", "-", "ー", "−"}:
        return ""
    return text


class KeajScraper(StaticCrawler):
    """在日中国朝鮮族経営者協会 会員企業スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = []  # 全フィールドが Schema に対応するため EXTRA なし

    def parse(self, url: str):
        soup = self.get_soup(url)
        if soup is None:
            return

        # 会員一覧は単一テーブル。ヘッダ: 会社名 / 業種 / 代表者名 / 協会役職
        rows = soup.select("table.text-sm tbody tr")
        if not rows:
            rows = soup.select("table tbody tr")
        self.total_items = len(rows)

        for row in rows:
            try:
                cells = row.find_all("td")
                if len(cells) < 4:
                    continue
                yield {
                    Schema.URL: url,
                    Schema.NAME: _clean(cells[0].get_text(" ", strip=True)),
                    Schema.LOB: _clean(cells[1].get_text(" ", strip=True)),
                    Schema.REP_NM: _clean(cells[2].get_text(" ", strip=True)),
                    Schema.POS_NM: _clean(cells[3].get_text(" ", strip=True)),
                }
            except Exception:
                self.logger.exception("行の解析に失敗: %s", url)
                continue


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = KeajScraper()
    scraper.execute("https://keaj.org/members/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
