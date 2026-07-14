"""
一般社団法人 長崎県警備業協会（AJSSA 会員名簿・長崎県）— 会員名簿

取得対象:
    - 長崎県警備業協会の全会員企業（約106社、1ページ）

取得フロー:
    引数 url (= sites.yml の url,
    https://choukeikyo.or.jp/協会員名簿/) が会員名簿ページ。
    会員名簿は 1 個の <table>（先頭のデータ表）で、先頭 1 行は見出し行。
    データ行は 5 セル:
      [0] 会社名（<a> があれば HP）
      [1] 業務種別（記号列。凡例:
             □-機械警備 ○/〇-施設警備 ◎-交通誘導警備
             ●-運搬警備 △-身辺警備 空-空港保安）
      [2] 所在地（市区町村以降）
      [3] 〒（郵便番号）
      [4] 電話番号
    会員を 1 件取得するごとに即 yield する (Pattern B)。ページネーションは無い。

    ※ 所在地はすべて長崎県内のため PREF は「長崎県」固定。
      業務種別（記号）は短い構造化ラベルのため EXTRA「対応業務」に "/" 連結で格納。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_39.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_39
"""

import logging
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 業務種別の記号 → 号業務の細区分（凡例より）。○/〇 のゆらぎを吸収。
_SERVICE_MARKS = {
    "□": "機械警備",
    "○": "施設警備",
    "〇": "施設警備",
    "◎": "交通誘導警備",
    "●": "運搬警備",
    "△": "身辺警備",
    "空": "空港保安",
}


class Ajssa39(StaticCrawler):
    """一般社団法人 長崎県警備業協会 会員名簿 スクレイパー"""

    DELAY = 1.5
    # 業務種別は記号→短い構造化ラベル → EXTRA。長文プロースは無いため除外なし。
    EXTRA_COLUMNS = ["対応業務"]

    def parse(self, url: str):
        # 引数 url を唯一の基点とする（別 URL はハードコードしない）。
        soup = self.get_soup(url)
        if not soup:
            logger.warning("会員名簿ページの取得に失敗: %s", url)
            return

        # 先頭のデータ表（会員名簿）を採用
        table = soup.find("table")
        if not table:
            logger.warning("会員名簿テーブルが見つからない: %s", url)
            return

        total = 0
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            # 見出し行はセル数 5 未満か、先頭が見出し語 → スキップ
            if len(cells) < 5:
                continue
            try:
                item = self._parse_member(cells, url)
            except Exception as e:  # 個別会員のエラーはスキップして継続
                logger.warning("会員の解析に失敗しskip: %s", e)
                continue
            if item:
                total += 1
                self.total_items = total  # 進捗表示用（累積）
                yield item

    def _parse_member(self, cells, source_url: str) -> dict | None:
        name = cells[0].get_text(strip=True).replace("　", " ").strip()
        # 見出し行（「会社名」）や空行はスキップ
        if not name or name == "会社名":
            return None

        # HP: 会社名セル内の <a>
        hp = ""
        a0 = cells[0].find("a", href=True)
        if a0:
            hp = a0["href"].strip()

        # 業務種別: 記号を対応業務に変換（重複除去・出現順維持）
        services = []
        for ch in cells[1].get_text(strip=True):
            label = _SERVICE_MARKS.get(ch)
            if label and label not in services:
                services.append(label)

        addr = cells[2].get_text(" ", strip=True).replace("　", " ").strip()
        post_code = cells[3].get_text(strip=True)
        tel = cells[4].get_text(strip=True)

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: "長崎県",  # 長崎県警備業協会の会員 = 全て長崎県
            Schema.ADDR: addr,
            Schema.POST_CODE: post_code,
            Schema.TEL: tel,
            Schema.HP: hp,
            "対応業務": "/".join(services),
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa39()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://choukeikyo.or.jp/%e5%8d%94%e4%bc%9a%e5%93%a1%e5%90%8d%e7%b0%bf/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
