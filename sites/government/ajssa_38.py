"""
一般社団法人 佐賀県警備業協会（AJSSA 会員名簿・佐賀県）— 会員名簿

取得対象:
    - 佐賀県警備業協会の全会員企業（約55社、1ページ）

取得フロー:
    引数 url (= sites.yml の url, https://sakeikyo.or.jp/members/) が会員名簿ページ。
    ページ内には表が複数あるが、会員名簿は先頭セルに「名称」見出しを持つ <table>。
    データ行は 6 セル:
      [0] 名称 / [1] 郵便番号 / [2] 所在地 / [3] 電話番号
      [4] 業務（施/交/保/機/貴/ホ の短縮記号を「・」連結） / [5] HP（<a> があれば URL）
    会員を 1 件取得するごとに即 yield する (Pattern B)。ページネーションは無い。

    ※ 所在地はすべて佐賀県内のため PREF は「佐賀県」固定。
      業務欄は短縮記号の構造化ラベルのため EXTRA「業務」にそのまま格納（自由記述プロースは無い）。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_38.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_38
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


class Ajssa38(StaticCrawler):
    """一般社団法人 佐賀県警備業協会 会員名簿 スクレイパー"""

    DELAY = 1.5
    # 業務欄は施/交/保/機/貴/ホ の短縮記号（構造化ラベル）→ EXTRA。長文プロースは無い。
    EXTRA_COLUMNS = ["業務"]

    def parse(self, url: str):
        # 引数 url を唯一の基点とする（別 URL はハードコードしない）。
        soup = self.get_soup(url)
        if not soup:
            logger.warning("会員名簿ページの取得に失敗: %s", url)
            return

        # 「名称」見出しを持つ会員名簿テーブルを採用（会費表など他表を除外）
        table = None
        for t in soup.find_all("table"):
            head = t.find("tr")
            if head and "名称" in head.get_text():
                table = t
                break
        if table is None:
            logger.warning("会員名簿テーブルが見つからない: %s", url)
            return

        total = 0
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 6:
                continue
            name = cells[0].get_text(" ", strip=True).replace("　", " ").strip()
            # 見出し行・空行はスキップ
            if not name or name == "名称":
                continue
            try:
                item = self._parse_member(cells, url, name)
            except Exception as e:  # 個別会員のエラーはスキップして継続
                logger.warning("会員の解析に失敗しskip: %s", e)
                continue
            if item:
                total += 1
                self.total_items = total  # 進捗表示用（累積）
                yield item

    def _parse_member(self, cells, source_url: str, name: str) -> dict:
        post_code = cells[1].get_text(" ", strip=True).replace("　", " ").strip()
        addr = cells[2].get_text(" ", strip=True).replace("　", " ").strip()
        tel = cells[3].get_text(" ", strip=True).replace("　", " ").strip()
        biz = cells[4].get_text("", strip=True).replace("　", "").strip()

        hp = ""
        a = cells[5].find("a", href=True)
        if a:
            hp = a["href"].strip()

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: "佐賀県",  # 佐賀県警備業協会の会員 = 全て佐賀県
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: hp,
            "業務": biz,
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa38()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://sakeikyo.or.jp/members/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
