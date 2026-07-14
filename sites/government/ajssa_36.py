"""
一般社団法人 愛媛県警備業協会（AJSSA 会員名簿・愛媛県）— 加盟員名簿

取得対象:
    - 愛媛県警備業協会の全加盟業者（警備会社・約81社）
    - 会社名 / 所在地(住所) / 電話番号 / 業種(警備業務種別) / ホームページ / 地区

取得フロー:
    引数 url (= sites.yml の url, .../meibo) は goope 系の静的な単一ページ。
    加盟員名簿は地区ごと（松山市地区 / 中予地区 / 東予地区 / 南予地区）に
    `table.table-bordered` として 4 ブロックに分かれる。各テーブルは 4 列:
      [0] 名称（会社ホームページが <a href> で貼られている場合あり）
      [1] 所在地（住所）
      [2] 電話番号
      [3] 業種（数字コード。区切りは , . ・ 等で揺れる）
    先頭行はヘッダ (名称/所在地/電話番号/業種) なのでスキップする。
    地区名は各テーブル直前の見出しテキストから取得する。会員 1 社 = tr 1 行で、
    1 件取得するごとに即 yield する (Pattern B)。ページネーションは無い。

    業種コードの凡例 (ページ内記載):
      １：施設  ２：交通誘導・雑踏  ３：貴重品運搬  ４：身辺  ５：機械

    ※所在地はすべて愛媛県内のため PREF は「愛媛県」固定。
    ※ルート URL は引数 url を唯一の起点とする (SSOT = sites.yml)。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_36.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_36
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

_PREF = "愛媛県"

# 業種の数字コード → 警備業務種別 (ページ内の凡例に準拠)
_BIZ = {
    "1": "施設",
    "2": "交通誘導・雑踏",
    "3": "貴重品運搬",
    "4": "身辺",
    "5": "機械",
}

# 地区見出しの照合 (テーブル直前の見出しテキストから抽出)
_REGION_RE = re.compile(r"(松山市地区|中予地区|東予地区|南予地区)")


class Ajssa36(StaticCrawler):
    """一般社団法人 愛媛県警備業協会 加盟員名簿 スクレイパー"""

    DELAY = 1.5
    # 業種・地区はサイト固有の短い構造化ラベル → EXTRA。長文プロースは無い。
    EXTRA_COLUMNS = ["業種", "地区"]

    def parse(self, url: str):
        # 引数 url (= sites.yml の url) を唯一のルートとして取得する
        soup = self.get_soup(url)
        if not soup:
            logger.warning("ページ取得に失敗しました: %s", url)
            return

        count = 0
        for table in soup.select("table.table-bordered"):
            rows = table.find_all("tr")
            # 先頭セルが「名称」の行はヘッダ → データテーブルのみ処理
            header = rows[0].find_all(["th", "td"]) if rows else []
            if not header or header[0].get_text(strip=True) != "名称":
                continue

            region = self._region_of(table)

            for tr in rows[1:]:
                try:
                    item = self._parse_row(tr, url, region)
                    if item:
                        count += 1
                        self.total_items = count  # 進捗表示用（累積）
                        yield item
                except Exception as e:  # 個別行のエラーはスキップして継続
                    logger.warning("行の解析に失敗しskip: %s", e)
                    continue

    def _region_of(self, table) -> str:
        """テーブル直前の見出しテキストから地区名を取得する。"""
        node = table.find_previous(string=_REGION_RE)
        if node:
            m = _REGION_RE.search(str(node))
            if m:
                return m.group(1)
        return ""

    def _parse_row(self, tr, source_url: str, region: str) -> dict | None:
        cells = tr.find_all(["td", "th"])
        if len(cells) < 4:
            return None

        name = self._norm(cells[0].get_text(" ", strip=True))
        if not name:
            return None

        addr = self._norm(cells[1].get_text(" ", strip=True))
        tel = self._norm(cells[2].get_text(" ", strip=True))

        # 業種: 数字コード列 (区切りは , . ・ 、 / 等で揺れる) を種別名へ展開
        raw = cells[3].get_text(" ", strip=True)
        biz = []
        for code in re.split(r"[,\.・･、／/\s]+", raw):
            code = code.strip()
            if not code:
                continue
            biz.append(_BIZ.get(code, code))

        # 名称セルに会社ホームページのリンクがあれば取得
        a = cells[0].find("a", href=True)
        hp = a["href"].strip() if a else ""

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: _PREF,  # 愛媛県警備業協会の加盟員 = 全て愛媛県
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: hp,
            "業種": "/".join(biz),
            "地区": region,
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

    scraper = Ajssa36()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://www.himekeikyo.or.jp/meibo")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
