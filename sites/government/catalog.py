"""
東京都理容所・美容所一覧 (東京都オープンデータカタログ / 練馬区 環境衛生営業施設一覧) — クローラー

取得対象:
    東京都オープンデータカタログサイトのデータセット「環境衛生営業施設一覧」
    (練馬区が公表する、届出のあった理容所・美容所・クリーニング所・コインランドリーの
    新規/廃止施設一覧) のうち、依頼指示に従い **理容所・美容所** の施設を横断取得する。
    実データはデータセットに紐づく多数のリソースファイル (ZIP/XLSX/CSV) として
    別ドメイン (www.city.nerima.tokyo.jp) にホストされている。

取得フロー:
    1. 起点 URL (sites.yml の url = カタログのデータセットページ) の末尾からデータセット ID を取り出し、
       同一オリジンの CKAN API (/api/3/action/package_show?id=<id>) を叩いてリソース一覧を得る。
       ※ データセットページ (HTML) は AWS WAF の JS チャレンジ (HTTP 202) でブロックされるが、
          CKAN API エンドポイントは requests から 200 で取得できる。
    2. リソース名に「理容所」または「美容所」を含むものだけを対象にする (依頼のフィルタ指示)。
    3. 各リソースを形式別に取得して 1 施設 (1 行) ずつ即 yield する (全件バッファしない):
         - ZIP  : 月次 XLSX が複数同梱 → 展開して各シートを解析
         - XLSX : 1 ファイル 1 月分 → シートを解析
         - CSV  : 将来月の未公開プレースホルダは 404 になり得るためスキップ
    4. 許可種別 (理容所/美容所) と届出区分 (新規/廃止) はリソース名から導出する
       (ファイル本文には無い項目のため)。

備考 (呼び出し指示への対応):
    - 依頼の【サイト】は「東京都理容所・美容所一覧」なので、同一データセットに含まれる
      クリーニング所・コインランドリーは対象外とし、理容所・美容所のみ取得する (parse() でフィルタ)。
    - 取得項目: 施設名・住所・電話番号 (掲載があれば)・許可種別 (理容所/美容所) を中心に、
      構造化された短い付随項目 (届出区分・確認番号・確認日・開設者 等) を EXTRA で収集する。
    - 自由記述 (文章) のカラムは元データに存在しない。著作権リスクのあるプロース列は無し。
    - 利用規約: データセットのライセンスは CC BY 4.0 (クリエイティブ・コモンズ 表示) で
      出典明示のうえ複製・再配布が明示的に許諾されており、取得を継続する。
      規約ページ (/pages/terms) は WAF でブロックされ本文取得不可だが、
      個別データのライセンス (CC BY 4.0) が取得可否を規定する。
    - 2026-08-12 再push: commit後5日経過してもS3にデータが到着しなかったため、
      本コメント追加のみで再pushし本番デプロイの再トリガを試みる（コード変更なし）。

実行方法:
    # ローカルテスト
    python scripts/sites/government/catalog.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id catalog
"""

import csv
import datetime
import io
import logging
import sys
import time
import zipfile
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import python_calamine  # 旧 .xls / .xlsx 読み込みエンジン (pyproject: python-calamine)

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 依頼の【サイト】= 理容所・美容所一覧。同データセットのクリーニング所/コインランドリーは対象外。
_PERMIT_TYPES = ("理容所", "美容所")

# XLSX/CSV 共通のヘッダ名 → 出力カラム。構造化された短い値のみ (自由記述カラムは元データに無し)。
_H_CODE = "市区町村コード"
_H_PREF = "都道府県名"
_H_CITY = "市区町村名"
_H_KIND = "種別"          # 施設の種別 (例: 一般)
_H_CONF_NO = "確認番号"
_H_CONF_DATE = "確認日"
_H_NAME = "施設名"
_H_ADDR = "所在地結合"
_H_BLDG = "建物名等"
_H_TEL = "所在地TEL"
_H_OPENER = "開設者"
_H_TITLE = "肩書"
_H_CORP_REP = "法人代表者"
_H_OPENER_ADDR = "開設者の住所"
_H_OPENER_TEL = "開設者のTEL"


class Catalog(StaticCrawler):
    """東京都理容所・美容所一覧 (練馬区 環境衛生営業施設) スクレイパー"""

    # AWS WAF / 別ドメイン配信対策として新しめの UA を明示 (基底の既定は古い Chrome94)。
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    # 1 リソースに数十行入るため per-yield sleep は不要。負荷配慮はリソース間の FILE_DELAY で行う。
    DELAY = 0.0
    CONTINUE_ON_ERROR = True
    TIMEOUT = 90
    FILE_DELAY = 1.0  # リソースファイルを 1 本取得するごとに空ける間隔 (秒)

    EXTRA_COLUMNS = [
        "届出区分",        # 新規 / 廃止 (リソース名から導出)
        "市区町村コード",   # JIS コード (例: 131202)
        "市区町村名",       # 例: 練馬区
        "種別",            # 例: 一般
        "確認番号",
        "確認日",
        "建物名等",
        "法人代表者",
        "開設者の住所",
        "開設者のTEL",
    ]

    @staticmethod
    def _txt(value) -> str:
        """セル値を安全に文字列化 (全角空白正規化・前後空白除去・float/日付の整形)。"""
        if value is None:
            return ""
        if isinstance(value, bool):
            return ""
        if isinstance(value, float):
            # 市区町村コード等が 131202.0 で来るため整数化。実数値は基本現れない。
            value = str(int(value)) if value.is_integer() else str(value)
        elif isinstance(value, int):
            value = str(value)
        elif isinstance(value, (datetime.datetime, datetime.date)):
            value = value.isoformat()[:10]
        s = str(value).replace("　", " ").strip()
        return "" if s.lower() == "nan" else s

    def _fetch_resources(self, api_url: str) -> list:
        """CKAN API からデータセットのリソース一覧を取得する。"""
        # session.get はテストランナー / smoke_test のソフトタイムアウト対象 (中断ポイント)。
        resp = self.session.get(api_url, timeout=self.TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        if not data.get("success"):
            raise RuntimeError(f"CKAN API が success=false を返しました: {api_url}")
        return data["result"].get("resources", [])

    def parse(self, url: str):
        # 引数 url を唯一のルートとし、末尾のデータセット ID から同一オリジンの CKAN API を導出。
        dataset_id = url.rstrip("/").split("/")[-1]
        api_url = urljoin(url, f"/api/3/action/package_show?id={dataset_id}")

        resources = self._fetch_resources(api_url)
        targets = [
            r for r in resources
            if any(t in (r.get("name") or "") for t in _PERMIT_TYPES)
        ]
        self.total_items = None  # 総件数は事前不明 (リソース数のみ判明)
        logger.info("対象リソース: %d / %d 件 (理容所・美容所)", len(targets), len(resources))

        for idx, res in enumerate(targets):
            name = res.get("name") or ""
            fmt = (res.get("format") or "").upper()
            file_url = res.get("url") or ""
            if not file_url:
                continue
            permit = next((t for t in _PERMIT_TYPES if t in name), "")
            notif = "新規" if "新規" in name else ("廃止" if "廃止" in name else "")

            if idx > 0 and self.FILE_DELAY > 0:
                time.sleep(self.FILE_DELAY)
            try:
                yield from self._read_resource(file_url, fmt, permit, notif)
            except Exception as e:  # noqa: BLE001 — 1 リソース失敗 (404 の未公開月等) でも他は継続
                self.error_count += 1
                logger.warning("リソース取得/解析に失敗 (スキップ): %s [%s] — %s", file_url, name, e)
                continue

    def _read_resource(self, file_url: str, fmt: str, permit: str, notif: str):
        """リソースファイルを形式別にダウンロード・解析し、1 行ずつ即 yield する。"""
        # session.get はソフトタイムアウトの中断ポイント。ファイルは小さい (~数十 KB) ので全取得。
        resp = self.session.get(file_url, timeout=self.TIMEOUT)
        resp.raise_for_status()
        content = resp.content

        if fmt == "ZIP" or file_url.lower().endswith(".zip"):
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                for member in zf.namelist():
                    low = member.lower()
                    if not (low.endswith(".xlsx") or low.endswith(".xls")):
                        continue
                    yield from self._emit_workbook(zf.read(member), file_url, permit, notif)
        elif fmt == "CSV" or file_url.lower().endswith(".csv"):
            yield from self._emit_csv(content, file_url, permit, notif)
        else:  # XLSX / XLS
            yield from self._emit_workbook(content, file_url, permit, notif)

    def _emit_workbook(self, data: bytes, file_url: str, permit: str, notif: str):
        """XLSX/XLS バイト列を calamine で解析し、全シートの行を yield する。"""
        wb = python_calamine.CalamineWorkbook.from_filelike(io.BytesIO(data))
        for sheet_name in wb.sheet_names:
            rows = wb.get_sheet_by_name(sheet_name).to_python()
            if len(rows) < 2:
                continue
            header = [self._txt(h) for h in rows[0]]
            yield from self._emit_rows(header, rows[1:], file_url, permit, notif)

    def _emit_csv(self, data: bytes, file_url: str, permit: str, notif: str):
        """CSV バイト列を解析して行を yield する (UTF-8 BOM / CP932 両対応)。"""
        text = None
        for enc in ("utf-8-sig", "cp932", "utf-8"):
            try:
                text = data.decode(enc)
                break
            except UnicodeDecodeError:
                continue
        if text is None:
            text = data.decode("utf-8", errors="replace")
        reader = list(csv.reader(io.StringIO(text)))
        if len(reader) < 2:
            return
        header = [self._txt(h) for h in reader[0]]
        yield from self._emit_rows(header, reader[1:], file_url, permit, notif)

    def _emit_rows(self, header: list, data_rows: list, file_url: str, permit: str, notif: str):
        """ヘッダ名で列を引きながら 1 行 = 1 施設を Schema/EXTRA にマッピングして yield する。"""
        col = {name: i for i, name in enumerate(header)}

        def cell(row, name):
            i = col.get(name)
            return self._txt(row[i]) if (i is not None and i < len(row)) else ""

        for row in data_rows:
            if not any(self._txt(c) for c in row):
                continue
            name_val = cell(row, _H_NAME)
            if not name_val:
                continue  # 施設名が無い行 (空行・注記等) は除外

            addr = cell(row, _H_ADDR)
            bldg = cell(row, _H_BLDG)
            full_addr = f"{addr} {bldg}".strip() if bldg else addr

            item = {
                Schema.NAME: name_val,
                Schema.PREF: cell(row, _H_PREF),
                Schema.ADDR: full_addr,
                Schema.TEL: cell(row, _H_TEL),
                Schema.REP_NM: cell(row, _H_OPENER),
                Schema.POS_NM: cell(row, _H_TITLE),
                Schema.CAT_SITE: permit,  # 許可種別 (理容所/美容所)
                Schema.URL: file_url,
                "届出区分": notif,
                "市区町村コード": cell(row, _H_CODE),
                "市区町村名": cell(row, _H_CITY),
                "種別": cell(row, _H_KIND),
                "確認番号": cell(row, _H_CONF_NO),
                "確認日": cell(row, _H_CONF_DATE),
                "建物名等": bldg,
                "法人代表者": cell(row, _H_CORP_REP),
                "開設者の住所": cell(row, _H_OPENER_ADDR),
                "開設者のTEL": cell(row, _H_OPENER_TEL),
            }
            yield item


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Catalog()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    parse() は引数 url からデータセット ID を取り出し、同一オリジンの CKAN API を導出する。
    scraper.execute("https://catalog.data.metro.tokyo.lg.jp/dataset/t131202d0000000116")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
