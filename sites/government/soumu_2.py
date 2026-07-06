"""
電気通信事業者 登録・届出一覧 (総務省 情報通信統計データベース) — 登録電気通信事業者一覧クローラー

取得対象:
    総務省 情報通信統計DB「電気通信事業者 登録・届出一覧」ページが公表する
    登録電気通信事業者一覧 (gt010402.xls) の全事業者。

    備考 (呼び出し指示) で指定された CSV 相当の列だけを取得する:
        所管総通局等 / 登録番号 (登録年月日) / 事業者名 / 法人番号 / 代表者 / 提供区域
    ※「提供する電気通信役務」(○ 記号列) と「認定有無」は備考に含まれないため取得しない。

取得フロー:
    起点 URL (sites.yml の url = tsuushin04.html) を唯一のルートとし、urljoin で
    同一ホスト上の登録一覧 .xls (data/gt010402.xls) の絶対 URL を導出してダウンロードし、
    旧 .xls (BIFF) を python-calamine エンジンで解析、1 行ずつ即 yield する。

備考 (呼び出し指示):
    - 取得列は上記 6 項目のみ (自由記述の文章カラムは無し)。提供区域は「全国」「関東地方」等の
      構造化された区域名のため取得対象に含める。
    - フィルタ指示は無し (全件取得)。

実行方法:
    # ローカルテスト
    python scripts/sites/government/soumu_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id soumu_2
"""

import io
import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import pandas as pd

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 「第233号\n(平成16年4月1日)」→ ("第233号", "平成16年4月1日") を分離
_REG_NO_RE = re.compile(r"(第?\s*[0-9０-９]+\s*号)[\s　]*[（(]?\s*(.*?)\s*[)）]?$", re.S)

# 登録一覧のヘッダ行 (col2=事業者名) 上の列インデックス
_COL_SOKAN = 0   # 所管総通局等
_COL_REGNO = 1   # 登録番号 (登録年月日)
_COL_NAME = 2    # 事業者名
_COL_CONUM_LABEL = 3  # 「法人番号」ラベル (merged cell)
_COL_CONUM_VALUE = 4  # 法人番号 値
_COL_REP = 5     # 代表者
_COL_AREA = 6    # 提供区域


class Soumu2(StaticCrawler):
    """総務省 登録電気通信事業者一覧 スクレイパー"""

    # .xls を 1 回ダウンロードして行を yield するだけで、item ごとの HTTP 通信は無い。
    # DELAY は yield ごとに sleep するため、単一ダウンロード型では 0 にする (334件×1.5秒で504になる)。
    DELAY = 0.0
    CONTINUE_ON_ERROR = True

    EXTRA_COLUMNS = [
        "所管総通局等",   # 本省・北海道・関東 等
        "登録番号",       # 第233号
        "登録年月日",     # 平成16年4月1日
        "提供区域",       # 全国・関東地方 等 (区域名)
    ]

    # 起点 URL (tsuushin04.html) の host を基に urljoin で導出する登録一覧 .xls
    _REGISTERED_XLS = "data/gt010402.xls"

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

    @staticmethod
    def _conum(value) -> str:
        """法人番号を 13 桁文字列で返す (calamine は int/float で返すことがある)。"""
        if value is None:
            return ""
        s = str(value).strip()
        if not s or s.lower() == "nan":
            return ""
        # "8011101028104.0" のような float 表記を除去
        if re.fullmatch(r"\d+\.0+", s):
            s = s.split(".")[0]
        return s

    @classmethod
    def _split_reg_no(cls, raw: str) -> tuple[str, str]:
        """『第233号 (平成16年4月1日)』を (登録番号, 登録年月日) に分離。"""
        text = cls._txt(raw)
        if not text:
            return "", ""
        m = _REG_NO_RE.search(text)
        if m:
            return m.group(1).replace(" ", ""), cls._txt(m.group(2))
        return text, ""

    def _read_xls(self, xls_url: str) -> pd.DataFrame | None:
        """旧 .xls を取得し DataFrame (ヘッダ無し) を返す。"""
        # session.get はテストランナーのソフトタイムアウト対象 (get_soup と同経路)
        resp = self.session.get(xls_url, timeout=self.TIMEOUT)
        resp.raise_for_status()
        return pd.read_excel(
            io.BytesIO(resp.content),
            engine="calamine",
            header=None,
            dtype=object,
        )

    @staticmethod
    def _find_header_row(df: pd.DataFrame) -> int | None:
        """『事業者名』を含むヘッダ行のインデックスを探す。"""
        for i in range(min(60, len(df))):
            if any("事業者名" in str(v) for v in df.iloc[i].tolist()):
                return i
        return None

    # ------------------------------------------------------------------ #
    # entry point
    # ------------------------------------------------------------------ #
    def parse(self, url: str):
        # 引数 url を唯一のルートとし、同一ホスト上の絶対パスを urljoin で導出
        xls_url = urljoin(url, self._REGISTERED_XLS)
        try:
            df = self._read_xls(xls_url)
        except Exception as e:  # noqa: BLE001
            logger.error("登録一覧の取得に失敗: %s (%s)", xls_url, e)
            return
        if df is None or df.empty:
            logger.warning("登録一覧が空です: %s", xls_url)
            return

        header_row = self._find_header_row(df)
        if header_row is None:
            logger.error("ヘッダ行 (事業者名) が見つかりません: %s", xls_url)
            return

        # ○ 記号列サブヘッダ (header_row+1) を飛ばし、その次からがデータ行
        start = header_row + 2
        data = df.iloc[start:]
        # 事業者名が入っている行のみが有効レコード (末尾は空行/脚注)
        self.total_items = int((data[_COL_NAME].map(self._txt) != "").sum())

        for _, row in data.iterrows():
            name = self._txt(row[_COL_NAME])
            if not name:
                continue
            try:
                reg_no, reg_date = self._split_reg_no(row[_COL_REGNO])
                yield {
                    Schema.NAME: name,
                    Schema.CO_NUM: self._conum(row[_COL_CONUM_VALUE]),
                    Schema.REP_NM: self._txt(row[_COL_REP]),
                    Schema.URL: xls_url,
                    "所管総通局等": self._txt(row[_COL_SOKAN]),
                    "登録番号": reg_no,
                    "登録年月日": reg_date,
                    "提供区域": self._txt(row[_COL_AREA]),
                }
            except Exception as e:  # noqa: BLE001
                logger.warning("行の解析に失敗 (%s): %s", name, e)
                continue


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Soumu2()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://www.soumu.go.jp/johotsusintokei/field/tsuushin04.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
