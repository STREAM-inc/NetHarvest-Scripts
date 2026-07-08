"""
一般社団法人 兵庫県警備業協会（AJSSA 会員名簿・兵庫県）— 加盟会社一覧

取得対象:
    - 兵庫県警備業協会の加盟会社（約311社）
    - 会社名 / 支部 / 郵便番号 / 都道府県 / 住所 / TEL / 代表者 /
      現在の業務(警備種別) / HP

取得フロー:
    引数 url (= sites.yml の url = /members-list/) は加盟会社一覧の静的
    WordPress ページ。会員一覧は単一の <table> に全件掲載されている。
    ページネーション・詳細ページは無い。

    テーブル構造 (見出し 8 列):
      会社名 / 支部 / 郵便番号 / 所在地 / 電話番号 / 代表者 /
      現在の業務 / ひとこと
      - 会社名セルに HP を持つ会社は <a href> リンクを持つ。
      - 所在地は原則「神戸市…」等の市区町村始まり (兵庫県名は省略)。
        県外本社等で都道府県名始まりの場合はそこから PREF を切り出す。
      - 「現在の業務」は施設警備/交通誘導警備 等の短い構造化ラベル
        → Schema.CAT_SITE。
      - 「ひとこと」は会社が書いた自由記述のキャッチコピー(プロース)の
        ため著作権リスクで取得しない (除外)。

    見出し行 (th 行) を除いた td 行を 1 件ずつ即 yield する (Pattern B)。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_26.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_26
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

_DEFAULT_PREF = "兵庫県"

# 住所先頭の都道府県抽出用 (県外本社の会社向け)
_PREFECTURES = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]

# 見出しキーワード → 列種別の対応
_HEADER_MAP = {
    "会社名": "name",
    "支部": "branch",
    "郵便番号": "post",
    "所在地": "addr",
    "電話番号": "tel",
    "代表者": "rep",
    "現在の業務": "cat",
}


class Ajssa26(StaticCrawler):
    """一般社団法人 兵庫県警備業協会 加盟会社名簿 スクレイパー"""

    DELAY = 1.5
    # 支部はサイト固有の構造化ラベルとして EXTRA。現在の業務(警備種別)は CAT_SITE。
    # 「ひとこと」(自由記述のキャッチコピー) は著作権リスクのため取得しない。
    EXTRA_COLUMNS = ["支部"]

    def parse(self, url: str):
        # 引数 url (= sites.yml の url) を唯一のルートとして取得する
        soup = self.get_soup(url)
        if soup is None:
            logger.warning("ページ取得に失敗しました: %s", url)
            return

        table = soup.find("table")
        if table is None:
            logger.warning("会員テーブルが見つかりません: %s", url)
            return

        col_index, header_row = self._find_header(table)
        if not col_index or "name" not in col_index:
            logger.warning("見出し行を特定できません: %s", url)
            return

        # 見出し行 (th) を除いた td を持つデータ行を 1 件ずつ処理
        rows = [
            tr for tr in table.find_all("tr")
            if tr is not header_row and tr.find("td")
        ]
        self.total_items = len(rows)

        for tr in rows:
            try:
                item = self._parse_row(tr, col_index, url)
                if item:
                    yield item
            except Exception as e:  # 個別会員のエラーはスキップして継続
                logger.warning("会員の解析に失敗しskip: %s", e)
                continue

    def _find_header(self, table):
        """会社名を含む見出し行と、列種別→インデックスの対応を返す。"""
        for tr in table.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            texts = [c.get_text(" ", strip=True) for c in cells]
            if not any("会社名" in t for t in texts):
                continue
            idx = {}
            for i, t in enumerate(texts):
                for kw, key in _HEADER_MAP.items():
                    if kw in t and key not in idx:
                        idx[key] = i
                        break
            if "name" in idx:
                return idx, tr
        return {}, None

    def _parse_row(self, tr, col_index: dict, source_url: str) -> dict | None:
        tds = tr.find_all("td")

        def cell(key: str):
            i = col_index.get(key)
            if i is None or i >= len(tds):
                return None
            return tds[i]

        name_cell = cell("name")
        if name_cell is None:
            return None
        name = self._norm(name_cell.get_text(" ", strip=True))
        if not name:
            return None

        # HP: 会社名セルの <a href> (リンクを持つ会社のみ)
        a = name_cell.find("a", href=True)
        hp = a["href"].strip() if a else ""

        branch = self._cell_text(cell("branch"))
        post = self._cell_text(cell("post"))
        tel = self._cell_text(cell("tel"))
        rep = self._cell_text(cell("rep"))
        cat = self._cell_text(cell("cat"))

        # 住所: 都道府県名で始まればそこから PREF を切り出す。
        # 兵庫県内は市区町村始まりが原則なので既定 PREF=兵庫県。
        pref, addr = _DEFAULT_PREF, self._cell_text(cell("addr"))
        for p in _PREFECTURES:
            if addr.startswith(p):
                pref = p
                addr = addr[len(p):].strip()
                break

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.REP_NM: rep,
            Schema.HP: hp,
            Schema.CAT_SITE: cat,
            "支部": branch,
        }

    @staticmethod
    def _cell_text(cell) -> str:
        return Ajssa26._norm(cell.get_text(" ", strip=True)) if cell else ""

    @staticmethod
    def _norm(text: str) -> str:
        """全角/半角スペース・改行を単一スペースに整形する。"""
        return re.sub(r"[\s　]+", " ", text).strip()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa26()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://hyo-keikyo.or.jp/members-list/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
