"""
倉庫業者登録簿 (国土交通省 登録倉庫事業者棟別リスト) — 登録倉庫事業者クローラー

取得対象:
    国土交通省が公表する「登録倉庫事業者棟別リスト」(Excel/.xlsx) を横断取得する。
    1 行 = 1 倉庫 (棟) で、事業者名・営業所・倉庫名称・所在地・倉庫種類・保管面積等を持つ。

取得フロー:
    起点 URL (sites.yml の url = .xlsx の直リンク) を session.get でダウンロードし、
    Python 標準ライブラリ (zipfile + xml.etree) で解析 (外部パッケージ不要)。ヘッダ行 (『氏名又は名称』を含む行) を検出後、
    データ行を 1 行ずつ即 yield する。詳細ページ (レコード単位の別 URL) は存在せず、
    全項目がこの 1 ファイル内に構造化されて含まれる。

利用規約:
    国土交通省ウェブサイトは「公共データ利用規約 (PDL1.0)」に準拠し、複製・二次利用が
    許可されている (スクレイピングの明示的禁止規定は無し)。出典表示が条件。

備考 (呼び出し指示):
    - フィルタ指示は無し (全件取得)。
    - 自由記述の文章カラムは無い (全カラムが名称・コード・数値・記号)。

実行方法:
    # ローカルテスト
    python scripts/sites/government/mlit.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id mlit
"""

import io
import logging
import re
import sys
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import pandas as pd

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# ○ / 〇 (全角) — 倉庫種類「有」の記号
_MARKS = ("○", "〇", "◯", "●")

# 倉庫種類フラグ列の範囲 (0-indexed, col9〜col25)。ラベルはヘッダ行から動的に取得する。
_TYPE_COL_START = 9
_TYPE_COL_END = 26  # exclusive


class Mlit(StaticCrawler):
    """国土交通省 倉庫業者登録簿 (登録倉庫事業者棟別リスト) スクレイパー"""

    DELAY = 1.5
    CONTINUE_ON_ERROR = True

    # Schema に該当しないサイト固有カラム (全て名称/コード/数値/記号 — 自由記述の文章は無い)
    EXTRA_COLUMNS = [
        "登録番号",        # 登録倉庫事業者の登録番号
        "営業所番号",      # 事業者内の営業所番号
        "営業所名称",      # 営業所名称
        "倉庫番号",        # 営業所内の倉庫番号
        "倉庫名称",        # 倉庫 (棟) 名称
        "倉庫管轄局",      # 北海道運輸局 等
        "倉庫種類",        # ○ の付いた種類ラベルを連結 (例: 「Ｆ１,Ｃ２」)
        "発券区分",        # 発券 / 非発券
        "冷蔵倉庫面積_m3", # 冷蔵倉庫 (m3)
        "普通倉庫面積_m2", # 普通倉庫 (m2)
    ]

    # ------------------------------------------------------------------ #
    # helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _txt(value) -> str:
        """セル値を安全に文字列化する (nan/None は空文字、空白は正規化)。"""
        if value is None:
            return ""
        s = str(value).strip()
        if not s or s.lower() == "nan":
            return ""
        return re.sub(r"\s+", " ", s.replace("　", " ")).strip()

    def _read_xlsx(self, url: str) -> pd.DataFrame | None:
        """.xlsx を取得し DataFrame (ヘッダ無し) を返す。
        openpyxl/calamine 不要 — Python 標準ライブラリのみで解析する。"""
        resp = self.session.get(url, timeout=self.TIMEOUT)
        resp.raise_for_status()
        rows = self._parse_xlsx(resp.content)
        if not rows:
            return None
        return pd.DataFrame(rows)

    @staticmethod
    def _parse_xlsx(content: bytes) -> list:
        """zipfile + xml.etree だけで xlsx を解析し list[list] を返す。"""
        _NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"

        with zipfile.ZipFile(io.BytesIO(content)) as z:
            names = z.namelist()

            # 共有文字列テーブル (sharedStrings.xml)
            shared: list[str] = []
            if "xl/sharedStrings.xml" in names:
                root = ET.parse(z.open("xl/sharedStrings.xml")).getroot()
                for si in root.findall(f".//{{{_NS}}}si"):
                    parts = si.findall(f".//{{{_NS}}}t")
                    shared.append("".join(t.text or "" for t in parts))

            # 1 枚目のシート (sheet1.xml)
            sheet_path = next(
                (n for n in names if re.match(r"xl/worksheets/sheet\d+\.xml", n)),
                None,
            )
            if not sheet_path:
                return []

            root = ET.parse(z.open(sheet_path)).getroot()
            result: list[list] = []
            for row_elem in root.findall(f".//{{{_NS}}}row"):
                sparse: dict[int, object] = {}
                for cell in row_elem.findall(f"{{{_NS}}}c"):
                    ref = cell.get("r", "")
                    col_str = "".join(c for c in ref if c.isalpha()).upper()
                    col_idx = 0
                    for ch in col_str:
                        col_idx = col_idx * 26 + (ord(ch) - ord("A") + 1)
                    col_idx -= 1  # 0-indexed

                    t = cell.get("t", "")
                    v = cell.find(f"{{{_NS}}}v")
                    val: object = None
                    if v is not None and v.text is not None:
                        if t == "s":  # shared string
                            idx = int(v.text)
                            val = shared[idx] if idx < len(shared) else ""
                        elif t == "inlineStr":
                            it = cell.find(f".//{{{_NS}}}t")
                            val = it.text if it is not None else ""
                        else:
                            val = v.text  # 数値はそのまま文字列で受け取る
                    sparse[col_idx] = val

                if sparse:
                    width = max(sparse.keys()) + 1
                    result.append([sparse.get(i) for i in range(width)])

        return result

    @staticmethod
    def _find_header_row(df: pd.DataFrame) -> int | None:
        """『氏名又は名称』を含むヘッダ行のインデックスを探す。"""
        for i in range(min(30, len(df))):
            if any("氏名又は名称" in str(v) for v in df.iloc[i].tolist()):
                return i
        return None

    # ------------------------------------------------------------------ #
    # entry point
    # ------------------------------------------------------------------ #
    def parse(self, url: str):
        # 🔒 引数 url を唯一のルートとして使う (別 URL をハードコードしない)。
        df = self._read_xlsx(url)
        if df is None or df.empty:
            logger.warning("Excel が空 (取得失敗): %s", url)
            return

        header = self._find_header_row(df)
        if header is None:
            logger.warning("ヘッダ行 (『氏名又は名称』) を検出できず: %s", url)
            return

        # 倉庫種類フラグ列のラベル (改行・空白を除去)
        header_cells = df.iloc[header].tolist()
        type_labels = {
            j: self._txt(header_cells[j])
            for j in range(_TYPE_COL_START, min(_TYPE_COL_END, len(header_cells)))
        }

        rows = df.iloc[header + 1:]
        # 事業者名 (col1) が入っている行数を総件数とする
        self.total_items = int(rows[1].notna().sum())

        for _, row in rows.iterrows():
            cells = row.tolist()
            name = self._txt(cells[1]) if len(cells) > 1 else ""
            if not name:
                continue
            try:
                # ○ の付いた倉庫種類ラベルを連結
                types = [
                    type_labels.get(j, "")
                    for j in range(_TYPE_COL_START, min(_TYPE_COL_END, len(cells)))
                    if str(cells[j]).strip() in _MARKS and type_labels.get(j)
                ]

                def _cell(i: int) -> str:
                    return self._txt(cells[i]) if len(cells) > i else ""

                yield {
                    Schema.NAME: name,               # col1 氏名又は名称
                    Schema.PREF: _cell(8),           # col8 都道府県名
                    Schema.ADDR: _cell(27),          # col27 営業所住所 (市区町村以降)
                    Schema.URL: url,
                    "登録番号": _cell(0),
                    "営業所番号": _cell(2),
                    "営業所名称": _cell(3),
                    "倉庫番号": _cell(4),
                    "倉庫名称": _cell(5),
                    "倉庫管轄局": _cell(6),
                    "倉庫種類": ",".join(types),
                    "発券区分": _cell(26),
                    "冷蔵倉庫面積_m3": _cell(28),
                    "普通倉庫面積_m2": _cell(29),
                }
            except Exception as e:  # noqa: BLE001 — 1 行失敗でも他は継続
                logger.warning("行の解析に失敗 (スキップ): %s — %s", name, e)
                continue


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Mlit()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.mlit.go.jp/seisakutokatsu/freight/content/001991758.xlsx")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
