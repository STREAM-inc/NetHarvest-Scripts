"""
木建市場 工務店全国一覧 — https://www.mokken.com/cgi-bin/gyousya/koumuten.cgi

取得対象:
    - 全国47都道府県に登録された工務店の会社概要
      (会社名 / 住所 / 電話番号 / 担当者名 / FAX番号 / 営業時間 / 営業エリア / 定休日)

取得フロー:
    - 都道府県別ページ (?mode=part&part=0〜46 / 地図クリック相当) を 47 ページ巡回する。
      1 都道府県 1 ページで完結し、ページ送りは無い。
    - 各ページ内は工務店ごとに <th>会社名</th> を持つ <table> が並ぶ。
      そのテーブル単位でラベル(th)→値(td)を拾い、1 件ずつ即 yield する。
    - 都道府県名はページ見出し (th[bgcolor='#6699CC']) から取得する。

備考:
    - 「紹介文」は自由記述の文章 (著作権リスク) のため取得しない。
    - 「訪問数」は集計値のため取得しない。
    - SSL 中間証明書が欠落しており requests の証明書検証が失敗するため verify=False。
    - 文字コードは Shift_JIS (Content-Type ヘッダで宣言済み → get_soup が自動判定)。

実行方法:
    python scripts/sites/construction/mokken.py
    docker compose exec worker python /app/bin/run_flow.py --site-id mokken
"""

import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import urllib3

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class MokkenScraper(StaticCrawler):
    """木建市場 工務店全国一覧 スクレイパー"""

    DELAY = 1.5

    # サイト固有カラム (いずれも構造化された短い値。自由記述の文章は含めない)
    EXTRA_COLUMNS = ["担当者名", "FAX番号", "営業エリア"]

    # 都道府県別ページ数 (part=0〜46 の 47 ページ)
    _PART_COUNT = 47

    # th ラベル → 出力先の対応表
    _LABEL_MAP = {
        "住所": Schema.ADDR,
        "電話番号": Schema.TEL,
        "ファックス番号": "FAX番号",
        "営業時間": Schema.TIME,
        "営業エリア": "営業エリア",
        "定休日": Schema.HOLIDAY,
        "担当者": "担当者名",
    }

    def _setup(self):
        # 標準セッションを構築後、欠落した SSL 中間証明書対策で検証を無効化する
        super()._setup()
        self.session.verify = False
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def parse(self, url: str):
        for part in range(self._PART_COUNT):
            page_url = f"{url}?mode=part&part={part}"
            soup = self.get_soup(page_url)
            if soup is None:
                continue

            pref_el = soup.select_one("th[bgcolor='#6699CC']")
            pref = pref_el.get_text(strip=True) if pref_el else ""

            # 会社ごとに <th>会社名</th> を持つテーブルが並ぶ
            for name_th in soup.find_all("th"):
                if name_th.get_text(strip=True) != "会社名":
                    continue
                table = name_th.find_parent("table")
                if table is None:
                    continue

                item = self._parse_company(table, pref, page_url)
                if item and item.get(Schema.NAME):
                    yield item

    def _parse_company(self, table, pref: str, page_url: str) -> dict:
        item = {
            Schema.URL: page_url,
            Schema.PREF: pref,
            Schema.NAME: "",
            Schema.ADDR: "",
            Schema.TEL: "",
            Schema.TIME: "",
            Schema.HOLIDAY: "",
            "担当者名": "",
            "FAX番号": "",
            "営業エリア": "",
        }

        for tr in table.find_all("tr"):
            label_cell = tr.find(["th", "td"])
            if label_cell is None:
                continue
            label = label_cell.get_text(strip=True)

            if label == "会社名":
                item[Schema.NAME] = self._second_cell_text(tr, label_cell)
                continue

            key = self._LABEL_MAP.get(label)
            if key is None:
                # 訪問数・紹介文・修正/削除行などは対象外
                continue
            item[key] = self._second_cell_text(tr, label_cell)

        return item

    @staticmethod
    def _second_cell_text(tr, label_cell) -> str:
        """ラベルセルの次の td (値セル) のテキストを返す。"""
        for cell in tr.find_all(["th", "td"]):
            if cell is label_cell:
                continue
            return cell.get_text(" ", strip=True)
        return ""


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = MokkenScraper()
    # 🔒 sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.mokken.com/cgi-bin/gyousya/koumuten.cgi")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
