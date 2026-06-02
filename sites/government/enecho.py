"""
登録小売電気事業者一覧 (資源エネルギー庁 / METI) — enecho

取得対象:
    - 経済産業省 資源エネルギー庁が公表する「登録小売電気事業者一覧」の全事業者。
      登録番号・事業者名・法人番号・本社住所・代表者情報・登録年月日・休止予定期間を取得する。

取得フロー:
    - 単一の静的 HTML ページ (table.ichiran) に全件が掲載されている。
      ページネーション・詳細ページは無い。テーブルの各データ行 (td が 11 個) を 1 レコードとして yield する。
      ヘッダー行 (th のみ / 2 行) は td を持たないため自然に除外される。

注意 (0件問題への対処):
    - 本ページは約220KBと大きく、enecho 側サーバーの応答が遅い。基底クラスの
      既定タイムアウト (20秒) では応答前にタイムアウトし get_soup() が None を返すため、
      parse() が 1件も yield できず「0件」となっていた。
      そのため本スクレイパーで TIMEOUT を延長している (セレクタ自体は正しい)。

実行方法:
    # ローカルテスト
    python scripts/sites/government/enecho.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id enecho
"""

import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 一覧テーブル。全データが直接埋め込まれた単一の静的テーブル。
TABLE_CSS = "table.ichiran"

# データ行は td が 11 セル。0-based のセル位置で各フィールドを取得する。
COL_REG_NO = 0      # 登録番号 (例: A0002)
COL_NAME = 1        # 氏名又は名称
COL_CO_NUM = 2      # 法人番号 (13桁)
COL_PREF = 3        # 住所(本社) 都道府県
COL_ADDR = 4        # 住所(本社) 市区町村以降
COL_POS = 5         # 代表者情報 役職
COL_REP_LAST = 6    # 代表者情報 氏名(姓)
COL_REP_FIRST = 7   # 代表者情報 氏名(名)
COL_REG_DATE = 8    # 登録年月日
COL_SUSPEND_FROM = 9   # 休止予定期間 開始
COL_SUSPEND_TO = 10    # 休止予定期間 終了

EXPECTED_CELLS = 11


class EnechoScraper(StaticCrawler):
    """資源エネルギー庁 登録小売電気事業者一覧 スクレイパー"""

    DELAY = 1.5
    # enecho の一覧ページは大きく応答が遅い。基底既定の 20 秒では取得前に
    # タイムアウトして 0 件になるため、十分に長い値へ延長する。
    TIMEOUT = 120
    EXTRA_COLUMNS = ["登録番号", "登録年月日", "休止予定期間_開始", "休止予定期間_終了"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)
        if soup is None:
            return

        table = soup.select_one(TABLE_CSS)
        if table is None:
            self.logger.warning("一覧テーブル (%s) が見つかりませんでした: %s", TABLE_CSS, url)
            return

        rows = table.find_all("tr")
        # データ行 = td を EXPECTED_CELLS 個以上持つ行。
        # ヘッダーは th のみ (td=0) のため除外され、列が増減しても
        # 先頭から必要セルだけ参照することで取りこぼしを防ぐ。
        data_rows = [r for r in rows if len(r.find_all("td")) >= EXPECTED_CELLS]
        self.total_items = len(data_rows)
        self.logger.info("データ行 %d 件を検出", self.total_items)

        for tr in data_rows:
            try:
                tds = tr.find_all("td")
                cells = [td.get_text(" ", strip=True) for td in tds]

                name = cells[COL_NAME]
                if not name:
                    # 名称が無い行は不正な行としてスキップ
                    continue

                last = cells[COL_REP_LAST]
                first = cells[COL_REP_FIRST]
                rep_name = f"{last} {first}".strip()

                yield {
                    Schema.URL: url,
                    Schema.NAME: name,
                    Schema.CO_NUM: cells[COL_CO_NUM],
                    Schema.PREF: cells[COL_PREF],
                    Schema.ADDR: cells[COL_ADDR],
                    Schema.POS_NM: cells[COL_POS],
                    Schema.REP_NM: rep_name,
                    "登録番号": cells[COL_REG_NO],
                    "登録年月日": cells[COL_REG_DATE],
                    "休止予定期間_開始": cells[COL_SUSPEND_FROM],
                    "休止予定期間_終了": cells[COL_SUSPEND_TO],
                }
            except Exception as e:  # noqa: BLE001 — 個別行のエラーはログして継続
                self.logger.warning("行の解析に失敗しスキップ: %s", e)
                continue


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = EnechoScraper()
    scraper.execute(
        "https://www.enecho.meti.go.jp/category/electricity_and_gas/electric/summary/retailers_list/index.html"
    )

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
