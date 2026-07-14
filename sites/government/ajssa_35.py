"""
一般社団法人 徳島県警備業協会（AJSSA 会員名簿・徳島県）— 会員名簿

取得対象:
    - 徳島県警備業協会の全会員企業（警備会社・約53社）
    - 会社名 / 所在地(住所) / 電話番号 / ホームページ / 対応業務（警備区分）

取得フロー:
    引数 url (= sites.yml の url, .../pages/26/) は goope 系の静的な単一ページ。
    会員名簿は `div.dataArea` 内の `table.type007Table` として 3 ブロック（五十音順で
    タブ分割・ad-tab-item）に分かれる。各テーブルは 10 列のマトリクス:
      [0] 社名・所在地・電話番号（複数 <div> 行: 1行目=社名 / 中間=住所 / 末尾=TEL.xxx）
      [1]〜[8] 対応業務のグリッド（施設/保安/空港保安/機械警備/交通誘導/雑踏/
               貴重品運搬/身辺警備、○ / 〇 マークで該当を表す）
      [9] HP（<a href> があれば会社ホームページ）
    各テーブル先頭の 3 行（社名見出し / 号区分 / 業務名見出し）はデータ行ではないので
    先頭セルが空か「社名…」の行としてスキップする。会員 1 社 = tr.type007Tr 1 行で、
    1 件取得するごとに即 yield する (Pattern B)。ページネーションは無い。

    ※所在地はすべて徳島県内のため PREF は「徳島県」固定。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_35.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_35
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

_PREF = "徳島県"

# 対応業務グリッド [1]〜[8] の列 → 警備区分名（ヘッダ行の並び順に対応）
_GYOUSHU = [
    "施設",
    "保安",
    "空港保安",
    "機械警備",
    "交通誘導",
    "雑踏",
    "貴重品運搬",
    "身辺警備",
]
# 該当マークに使われる各種の丸記号（全角/半角/白丸/黒丸の揺れを吸収）
_CIRCLES = set("○〇◯●")


class Ajssa35(StaticCrawler):
    """一般社団法人 徳島県警備業協会 会員名簿 スクレイパー"""

    DELAY = 1.5
    # 対応業務(警備区分)はサイト固有の短い構造化ラベル → EXTRA。長文プロースは無い。
    EXTRA_COLUMNS = ["対応業務"]

    def parse(self, url: str):
        # 引数 url (= sites.yml の url) を唯一のルートとして取得する
        soup = self.get_soup(url)
        if not soup:
            logger.warning("ページ取得に失敗しました: %s", url)
            return

        # 会員名簿は div.dataArea 内の tr.type007Tr（3 ブロック・五十音タブ）
        rows = []
        for area in soup.select("div.dataArea"):
            rows.extend(area.select("tr.type007Tr"))

        count = 0
        for tr in rows:
            try:
                item = self._parse_row(tr, url)
                if item:
                    count += 1
                    self.total_items = count  # 進捗表示用（累積）
                    yield item
            except Exception as e:  # 個別行のエラーはスキップして継続
                logger.warning("行の解析に失敗しskip: %s", e)
                continue

    def _parse_row(self, tr, source_url: str) -> dict | None:
        cells = tr.find_all(["td", "th"], recursive=False)
        if len(cells) < 10:
            return None

        # [0] 社名・所在地・電話番号 を <div> 行ごとに分解
        lines = [self._norm(x) for x in cells[0].stripped_strings]
        lines = [ln for ln in lines if ln]
        if not lines:
            return None

        name = lines[0]
        # 見出し行（社名見出し / 号区分 / 業務名見出し）はスキップ
        if name.startswith("社名") or name in ("1号", "施設"):
            return None

        tel = ""
        addr_parts = []
        for ln in lines[1:]:
            if re.match(r"^TEL", ln, re.I):
                tel = re.sub(r"^TEL[\.\s:：]*", "", ln, flags=re.I).strip()
            else:
                addr_parts.append(ln)
        addr = " ".join(addr_parts)

        # [1]〜[8] 対応業務: 丸記号が入っている列の区分名を採用
        gyoushu = []
        for i, label in enumerate(_GYOUSHU, start=1):
            if set(cells[i].get_text(strip=True)) & _CIRCLES:
                gyoushu.append(label)

        # [9] HP リンク
        a = cells[9].find("a", href=True)
        hp = a["href"].strip() if a else ""

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: _PREF,  # 徳島県警備業協会の会員 = 全て徳島県
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: hp,
            "対応業務": "/".join(gyoushu),
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

    scraper = Ajssa35()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://tokukeikyo.jp/pages/26/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
