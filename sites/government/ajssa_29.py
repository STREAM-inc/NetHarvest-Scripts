"""
一般社団法人 鳥取県警備業協会（AJSSA 会員名簿・鳥取県）— 会員名簿 (PDF)

取得対象:
    - 鳥取県警備業協会の会員 (約39社)
    - 会社名 / 業種(警備種別) / 郵便番号 / 都道府県 / 住所 / TEL / FAX /
      代表者名 / HP / 備考(役職・注記)

取得フロー:
    引数 url (= sites.yml の url = 会員名簿 PDF) が唯一のルート。PDF を
    self.session.get でダウンロードし (get_soup と同経路でソフトタイムアウト対象)、
    pdfplumber で各ページの「名称」ヘッダを持つ表を抽出する。
    会員 1 件は No 列 (先頭列) が入った行を先頭に、代表者・HP 等の続き行を含む
    複数行ブロックで構成される。No 列が埋まった行を検出するたびに直前のブロックを
    1 件として即 yield する (Pattern B)。詳細ページ・ページネーションは無い。

    業種欄は数字/記号コード (1:施設警備 2:交通誘導・雑踏 3:貴重品運搬 4:身辺
    機:機械警備 空:空港保安 ホ:保安業務) で、読みやすいラベルに展開し
    「/」区切りの短い構造化ラベルとして Schema.CAT_SITE へ格納する。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_29.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_29
"""

import io
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

# 業種欄コード → ラベル (末尾の凡例より)
_GYOSHU = {
    "1": "施設警備",
    "2": "交通誘導警備・雑踏警備",
    "3": "貴重品運搬警備",
    "4": "身辺警備",
    "機": "機械警備",
    "空": "空港保安警備",
    "ホ": "保安業務",
}

# URL 判定
_URL_RE = re.compile(r"^https?://", re.I)
# 電話番号 (半角/全角ダッシュ混在) 判定
_TEL_RE = re.compile(r"^[0-9０-９][0-9０-９\-－―‐\s]{6,}$")

# 住所先頭の市区郡から県外を判定 (鳥取県以外がごく一部含まれるため)
_OUT_PREF = {"広島市": "広島県"}


class Ajssa29(StaticCrawler):
    """一般社団法人 鳥取県警備業協会 会員名簿 (PDF) スクレイパー"""

    DELAY = 1.5
    # 業種 = 警備種別の短い構造化ラベル → Schema.CAT_SITE。
    # 郵便番号(〒) は共通スキーマに列が無いためサイト固有の EXTRA 列として出力する。
    # FAX(電話番号)・備考(役職/短い注記) も同様にサイト固有の構造化情報として EXTRA。
    EXTRA_COLUMNS = ["郵便番号", "FAX", "備考"]

    @staticmethod
    def _norm_num(s: str) -> str:
        """電話/郵便番号の全角ダッシュ・全角数字を半角へ正規化する。"""
        if not s:
            return ""
        trans = str.maketrans("０１２３４５６７８９－―‐", "0123456789---")
        return re.sub(r"\s+", "", s.translate(trans))

    @staticmethod
    def _join(s: str) -> str:
        """セル内改行を除去して 1 行に連結する (名称・住所・URL 用)。"""
        return re.sub(r"\s*\n\s*", "", (s or "")).strip()

    def parse(self, url: str):
        # pdfplumber は PDF 解析にのみ必要な重い外部依存。モジュール読込
        # (サイト登録/収集) 時に未導入でも失敗しないよう遅延 import する。
        import pdfplumber

        # session.get はテストランナーのソフトタイムアウト対象 (get_soup と同経路)
        resp = self.session.get(url, timeout=self.TIMEOUT)
        resp.raise_for_status()

        # 全ページから「名称」ヘッダを持つ会員テーブルの行を収集
        rows: list[list[str]] = []
        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            for page in pdf.pages:
                for table in page.extract_tables():
                    if not table:
                        continue
                    header = [(c or "") for c in table[0]]
                    if not any("名称" in c for c in header):
                        continue  # 協会本体の見出しテーブル等はスキップ
                    # ヘッダ 2 行 (名称/… と 代表者/備考) を除いたデータ行
                    rows.extend(table[2:])

        # No 列 (先頭列) が数字の行を境に会員ブロックへ分割
        blocks: list[list[list[str]]] = []
        for row in rows:
            no = (row[0] or "").strip() if row else ""
            if no.isdigit():
                blocks.append([row])
            elif blocks and any((c or "").strip() for c in row):
                blocks[-1].append(row)  # 続き行 (代表者・HP・業種記号 等)
        self.total_items = len(blocks)

        for block in blocks:
            try:
                item = self._parse_block(block, url)
                if item:
                    yield item
            except Exception as e:  # 個別会員のエラーはスキップして継続
                logger.warning("会員の解析に失敗しskip: %s", e)
                continue

    def _parse_block(self, block: list[list[str]], source_url: str) -> dict | None:
        head = block[0]
        # 列レイアウト: 0=No, 1-5=業種, 6=名称, 7=〒, 8=所在地, 9=TEL/代表者/HP, 10=FAX/備考
        name = self._join(head[6])
        if not name:
            return None

        # 業種コードをブロック全行の col1-5 から出現順に収集しラベル展開
        codes: list[str] = []
        for row in block:
            for c in row[1:6]:
                v = (c or "").strip()
                if v and v not in codes:
                    codes.append(v)
        gyoshu = "/".join(_GYOSHU.get(c, c) for c in codes)

        post = self._norm_num(head[7])
        addr = self._join(head[8])

        # 都道府県: 原則 鳥取県。県外市区が住所先頭にあれば補正
        pref = "鳥取県"
        for city, p in _OUT_PREF.items():
            if addr.startswith(city):
                pref = p
                break

        tel = self._norm_num(head[9])
        fax = self._norm_num(head[10])

        # col9 の続き行を代表者 / HP に振り分け、col10 の続き行を備考へ
        rep = ""
        hp = ""
        notes: list[str] = []
        for row in block[1:]:
            v9 = self._join(row[9]) if len(row) > 9 else ""
            if v9:
                if _URL_RE.match(v9):
                    hp = v9
                elif not _TEL_RE.match(v9) and not rep:
                    rep = v9
            v10 = (row[10] or "").strip() if len(row) > 10 else ""
            if v10:
                notes.append(v10)

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.CAT_SITE: gyoshu,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.REP_NM: rep,
            Schema.HP: hp,
            "郵便番号": post,
            "FAX": fax,
            "備考": " ".join(notes),
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa29()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute(
        "http://www.torikeikyo.jp/wordpress/wp-content/uploads/2026/06/HP%E7%94%A8%E4%BC%9A%E5%93%A1%E5%90%8D%E7%B0%BF%EF%BC%88%EF%BC%B28.5%EF%BC%89.pdf"
    )

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
