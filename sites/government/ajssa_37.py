"""
一般社団法人 高知県警備業協会（AJSSA 会員名簿・高知県）— 会員名簿

取得対象:
    - 高知県警備業協会の全会員企業（約35社、1ページ）

取得フロー:
    引数 url (= sites.yml の url, https://koukeikyo.com/membership_list/) が会員名簿ページ。
    会員名簿は 1 個の <table>（先頭のデータ表）で、先頭 2 行は見出し行。
    データ行は 10 セル:
      [0] 会員名 + 所在地 + 電話番号（<br> 区切りの 3 行、会社名に <a> があれば HP）
      [1] HP リンク（◯ に <a> があれば HP）
      [2]-[9] 業務区分の ◯ マーク:
             施設警備 / 保安警備 / 空港保安警備 / 機械警備 /
             交通誘導警備 / 雑踏警備 / 貴重品運搬警備 / 身辺警備
    会員を 1 件取得するごとに即 yield する (Pattern B)。ページネーションは無い。

    ※ 所在地はすべて高知県内のため PREF は「高知県」固定。
      業務区分（○ マーク）は短い構造化ラベルのため EXTRA「対応業務」に "/" 連結で格納。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_37.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_37
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

# データ行 セル[2]〜[9] に対応する業務区分（警備業法の号業務の細区分）
_SERVICE_LABELS = [
    "施設警備",
    "保安警備",
    "空港保安警備",
    "機械警備",
    "交通誘導警備",
    "雑踏警備",
    "貴重品運搬警備",
    "身辺警備",
]
# ○ 相当の記号（全角・半角ゆらぎを吸収）
_MARK = {"◯", "○", "◎", "●", "〇"}


class Ajssa37(StaticCrawler):
    """一般社団法人 高知県警備業協会 会員名簿 スクレイパー"""

    DELAY = 1.5
    # 業務区分は ○/× の短い構造化ラベル → EXTRA。長文プロースは無いため除外なし。
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
            # 見出し行（会員名/HPリンク/号 見出し）はセル数が 10 未満、または
            # 先頭セルが見出し語 → スキップ
            if len(cells) < 10:
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
        c0 = cells[0]
        # <br> を改行に変換してから行分割
        for br in c0.find_all("br"):
            br.replace_with("\n")
        lines = [
            ln.replace("　", " ").strip()
            for ln in c0.get_text().split("\n")
            if ln.strip()
        ]
        if not lines:
            return None

        name = lines[0]
        # 見出し行（「会員名」）や空行はスキップ
        if not name or name == "会員名":
            return None

        tel = ""
        addr_parts = []
        for ln in lines[1:]:
            if "TEL" in ln or "ＴＥＬ" in ln:
                tel = re.sub(r"^[TＴ][EＥ][LＬ][：:\s]*", "", ln).strip()
            else:
                addr_parts.append(ln)
        addr = " ".join(addr_parts).strip()

        # HP: 会社名セル内の <a> を優先、無ければ HPリンク列（cells[1]）の <a>
        hp = ""
        a0 = c0.find("a", href=True)
        if a0:
            hp = a0["href"].strip()
        else:
            a1 = cells[1].find("a", href=True)
            if a1:
                hp = a1["href"].strip()

        # 業務区分: セル[2]〜[9] の ○ マークを対応業務として収集
        services = []
        for i, label in enumerate(_SERVICE_LABELS):
            idx = i + 2
            if idx < len(cells) and cells[idx].get_text(strip=True) in _MARK:
                services.append(label)

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: "高知県",  # 高知県警備業協会の会員 = 全て高知県
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: hp,
            "対応業務": "/".join(services),
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa37()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://koukeikyo.com/membership_list/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
