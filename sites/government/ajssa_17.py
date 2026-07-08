"""
一般社団法人 富山県警備業協会（AJSSA 会員名簿・富山県）— 会員名簿

取得対象:
    - 富山県警備業協会の全会員企業（1 ページ・約49社）

取得フロー:
    引数 url (= sites.yml の url, .../meibo_kn.html) が会員名簿ページそのもの。
    ページ内に 1 個だけある <table class="yakuinmeibo"> が会員名簿で、1 社 = 1 行・3 列:
      [0] 会員名（社名） / [1] TEL / [2] ホームページ（<a> があれば HP リンク）
    先頭行は見出し（会員名 / ＴＥＬ / ホームページ）なのでスキップする。
    ページネーションは無い。会員を 1 件取得するごとに即 yield する (Pattern B)。

    ※ 会員はすべて富山県内の企業のため PREF は「富山県」固定。所在地・業種などの
      追加カラムはページに存在しない（3 列のみ）。文章（自由記述）カラムも無い。
    ※ ページは Shift_JIS だが StaticCrawler.get_soup が文字コードを自動判定する。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_17.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_17
"""

import logging
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)


class Ajssa17(StaticCrawler):
    """一般社団法人 富山県警備業協会 会員名簿 スクレイパー"""

    DELAY = 1.5
    # サイト固有の追加カラムは無い（社名 / TEL / HP のみ）。
    EXTRA_COLUMNS = []

    def parse(self, url: str):
        # 引数 url (会員名簿ページ) を唯一の基点とする。
        soup = self.get_soup(url)
        if not soup:
            logger.warning("会員名簿ページの取得に失敗: %s", url)
            return

        table = soup.find("table", class_="yakuinmeibo")
        if not table:
            logger.warning("会員名簿テーブルが見つからない: %s", url)
            return

        total = 0
        for row in table.find_all("tr"):
            try:
                item = self._parse_member(row, url)
                if item:
                    total += 1
                    self.total_items = total  # 進捗表示用（累積）
                    yield item
            except Exception as e:  # 個別会員のエラーはスキップして継続
                logger.warning("会員の解析に失敗しskip: %s", e)
                continue

    def _parse_member(self, row, source_url: str) -> dict | None:
        cells = row.find_all(["td", "th"])
        if len(cells) < 3:
            return None

        name = cells[0].get_text(" ", strip=True).replace("　", " ").strip()
        # 見出し行（会員名 / ＴＥＬ / ホームページ）と空行はスキップ
        if not name or name in ("会員名", "会社名"):
            return None

        tel = cells[1].get_text(" ", strip=True).strip()

        # ホームページセル内にリンクがあれば HP として採用（相対 URL は url 起点で解決）
        a = cells[2].find("a", href=True)
        hp = urljoin(source_url, a["href"].strip()) if a else ""

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: "富山県",  # 富山県警備業協会の会員 = 全て富山県
            Schema.TEL: tel,
            Schema.HP: hp,
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa17()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://www.security-toyama.jp/meibo_kn.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
