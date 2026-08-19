"""
千葉県オープンデータサイト 環境衛生関係施設一覧 (クリーニング所・旅館業) — クローラー

取得対象:
    千葉県オープンデータサイトのデータセット「【千葉県】環境衛生関係施設一覧」
    (datasets/6) に公開されている施設一覧ファイル (XLSX) のうち、
    「クリーニング所」「旅館・ホテル」等の施設一覧を対象に、施設ごとの
    営業者/申請者名・施設名称・所在地・電話番号・業務種別・種別・許可情報を抽出する。

    ※ 同一データセット内の「理容所」「美容所」(および理容所の月次新規施設) は
      既存クローラー (site_id: opendata / government/opendata.py) が担当済みのため、
      本クローラーでは除外する。2 本あわせてデータセット全体を重複なく網羅する。

取得フロー:
    1. 起点 URL (sites.yml の url = データセットページ) を GET し、リソースリンク
       (a.is-resource → /resources/{id}) とそのタイトルを抽出する。
    2. タイトルに「理容所」「美容所」を含まないリソースのみを対象とし、
       各リソースの XLSX を /resource_download/{id} からダウンロードする。
       (URL は起点 url からの urljoin で導出。ハードコードしない)
    3. XLSX は保健所所管区域ごとに複数シート (習志野/市川/松戸…市原の 13 枚) で
       構成される。各シートで「営業者名」「申請者名」「申請者氏名」等のいずれかを
       含むヘッダ行を探索し、以降のデータ行を 1 施設 = 1 レコードとして
       ヘッダ名で照合しながら 1 件ずつ即 yield する (全件バッファしない)。
       ヘッダが見つからないシート (プレースホルダ等) はスキップする。

構造上の注意 (Phase 1 で確認済み):
    - 営業者を表す列名はファイル/シートで揺れる: 「営業者名」(クリーニング所の大半・
      旅館業の市川/香取)、「申請者名」(旅館業の大半)、「申請者氏名」(海匝)。
    - 海匝保健所のシートのみ列構成が異なり、「施設名称１/２」「施設所在地１/２」ではなく
      「施設名称」「施設所在地」の単一列、種別も「種別１」となる。→ 列名候補で解決。
    - 許可情報の列名も業態で異なる: クリーニング所=「検査確認番号/検査確認日」、
      旅館業=「許可番号/許可日」。→ 列名候補で解決。
    - ヘッダ末尾に空セルや「オープンデータ」等の余剰セルが混ざるが、
      ヘッダ名駆動の照合なので影響しない。

備考 (呼び出し指示への対応):
    - 備考「一覧ファイルをダウンロードできます」に従い、データセットページから
      一覧ファイル (XLSX) をダウンロードして中身をレコード化する。
    - 全リソースは千葉県 (県所管保健所) のデータのため PREF は「千葉県」固定。
      所在地に都道府県表記は無いため ADDR は市区町村以下をそのまま格納する。
    - 電話番号が空欄 (全角/半角スペースのみ) の行があるため、空文字に正規化する。
    - 自由記述 (プロース) カラムは元データに存在せず、EXTRA は種別・許可番号/日・
      所管保健所・リソース名の構造化された短い値のみ (著作権リスクの自由記述は無し)。
    - 詳細リンク (www.pref.chiba.lg.jp/eishi/opendata/opendata-kankyoueisei-kankeisisetu.html)
      は 2026-08 時点で 404 (ページ廃止/移設)。データ本体はオープンデータサイト側の
      リソース XLSX に存在するため、取得には影響しない。
    - 利用規約 (/pages/terms) を確認済み。「公共データ利用規約 (第1.0版)」(PDL1.0 /
      政府標準利用規約準拠) が適用され、出典明記のうえ営利・非営利を問わず複製・加工・
      再配布が可能。スクレイピング/クローリングを禁止する条項は無い。→ 取得を継続。

実行方法:
    # ローカルテスト
    python scripts/sites/government/opendata_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id opendata_2
"""

import io
import logging
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from python_calamine import CalamineWorkbook

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 既存クローラー (site_id: opendata) が担当するリソースは除外し、
# それ以外の施設一覧 (クリーニング所・旅館業 等) を対象とする。
_EXCLUDE_RE = re.compile(r"(理容所|美容所)")

# ヘッダ行の判定に使うキー候補 (このいずれかのセルを含む行をヘッダとみなす)
_HEADER_KEYS = ("営業者名", "申請者名", "申請者氏名", "開設者名")

# 列名の揺れ吸収用: 論理名 → 実際の列名候補 (先に見つかったものを採用)
_REP_COLS = _HEADER_KEYS
_NAME_COLS = ("施設名称１", "施設名称")
_NAME2_COLS = ("施設名称２",)
_ADDR_COLS = ("施設所在地１", "施設所在地")
_ADDR2_COLS = ("施設所在地２",)
_TEL_COLS = ("施設電話番号",)
_BIZ_COLS = ("業務種別",)
_TYPE1_COLS = ("クリーニング種別１", "旅館業の種別", "種別１", "種別")
_TYPE2_COLS = ("クリーニング種別２", "種別２")
_SEASON_COLS = ("季節営業",)
_LICNO_COLS = ("検査確認番号", "許可番号")
_LICDATE_COLS = ("検査確認日", "許可日")


class Opendata2(StaticCrawler):
    """千葉県 環境衛生関係施設一覧 (クリーニング所・旅館業) スクレイパー"""

    # 1 施設ごとの sleep は行数 (数千) に対して無駄が大きいので 0。
    # 負荷配慮はリソース XLSX ファイル間の FILE_DELAY で行う。
    DELAY = 0.0
    CONTINUE_ON_ERROR = True
    TIMEOUT = 90  # XLSX ダウンロードに余裕を持たせる

    # リソース XLSX を 1 本取得するごとに空ける間隔 (秒)。
    FILE_DELAY = 1.0

    EXTRA_COLUMNS = ["種別", "種別2", "季節営業", "許可番号", "許可日", "所管保健所", "リソース名"]

    @staticmethod
    def _txt(value) -> str:
        """セル値を安全に文字列化 (前後空白除去・全角空白正規化)。"""
        if value is None:
            return ""
        s = str(value).replace("　", " ").strip()
        return "" if s.lower() == "nan" else s

    def _iter_resources(self, url: str):
        """データセットページから (resource_id, title) を出現順で列挙する。
        理容所/美容所 (既存 site_id: opendata が担当) のリソースは除外する。"""
        soup = self.get_soup(url)
        if soup is None:
            logger.warning("データセットページを取得できませんでした: %s", url)
            return
        for a in soup.select("a.is-resource"):
            href = a.get("href", "")
            m = re.search(r"/resources/(\d+)", href)
            if not m:
                continue
            title = a.get_text(strip=True)
            if _EXCLUDE_RE.search(title):
                logger.info("対象外リソースをスキップ (理容所/美容所): %s", title)
                continue
            yield m.group(1), title

    def parse(self, url: str):
        resources = list(self._iter_resources(url))
        self.total_items = None  # 総件数は事前に不明 (リソース数のみ判明)
        logger.info("対象リソース (クリーニング所・旅館業 等): %d 件", len(resources))

        for idx, (rid, title) in enumerate(resources):
            if idx > 0 and self.FILE_DELAY > 0:
                time.sleep(self.FILE_DELAY)
            # 引数 url を唯一のルートとし、同一オリジンの各 URL を urljoin で導出
            download_url = urljoin(url, f"/resource_download/{rid}")
            resource_url = urljoin(url, f"/resources/{rid}")
            try:
                yield from self._parse_workbook(download_url, resource_url, title)
            except Exception as e:  # noqa: BLE001 — 1 リソース失敗でも他は継続
                self.error_count += 1
                logger.warning("リソースの取得/解析に失敗 (スキップ): %s — %s", download_url, e)
                continue

    def _parse_workbook(self, download_url: str, resource_url: str, title: str):
        """XLSX をダウンロードし、全シートをヘッダ駆動で 1 行ずつ即 yield する。"""
        # session.get はテストランナー / smoke_test のソフトタイムアウト対象。
        resp = self.session.get(download_url, timeout=self.TIMEOUT)
        resp.raise_for_status()
        wb = CalamineWorkbook.from_filelike(io.BytesIO(resp.content))

        for sheet_name in wb.sheet_names:
            rows = wb.get_sheet_by_name(sheet_name).to_python()
            yield from self._parse_sheet(rows, sheet_name, resource_url, title)

    def _parse_sheet(self, rows, sheet_name, resource_url, title):
        """1 シート分の行から、ヘッダ行を探して以降のデータ行を yield する。"""
        header_idx = None
        col = {}
        for i, row in enumerate(rows):
            cells = [self._txt(c) for c in row]
            if any(k in cells for k in _HEADER_KEYS):
                header_idx = i
                col = {name: j for j, name in enumerate(cells) if name}
                break
        if header_idx is None:
            # ヘッダの無いシート (プレースホルダ等) → データ無し
            logger.debug("ヘッダ行が見つからないシートをスキップ: %s", sheet_name)
            return

        def cell(row, *candidates):
            """列名候補を順に試し、最初に見つかった列の値を返す (揺れ吸収)。"""
            for name in candidates:
                j = col.get(name)
                if j is not None and j < len(row):
                    return self._txt(row[j])
            return ""

        def joined(row, first, second):
            parts = [cell(row, *first), cell(row, *second)]
            return " ".join(p for p in parts if p)

        for row in rows[header_idx + 1:]:
            if not any(self._txt(c) for c in row):
                continue

            name = joined(row, _NAME_COLS, _NAME2_COLS)
            addr = joined(row, _ADDR_COLS, _ADDR2_COLS)

            # 名称も所在地も無い行はデータ行でない (脚注等) のでスキップ
            if not name and not addr:
                continue

            yield {
                Schema.NAME: name,
                Schema.REP_NM: cell(row, *_REP_COLS),
                Schema.PREF: "千葉県",
                Schema.ADDR: addr,
                Schema.TEL: cell(row, *_TEL_COLS),
                Schema.CAT_SITE: cell(row, *_BIZ_COLS),
                Schema.URL: resource_url,
                "種別": cell(row, *_TYPE1_COLS),
                "種別2": cell(row, *_TYPE2_COLS),
                "季節営業": cell(row, *_SEASON_COLS),
                "許可番号": cell(row, *_LICNO_COLS),
                "許可日": cell(row, *_LICDATE_COLS),
                "所管保健所": sheet_name,
                "リソース名": title,
            }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Opendata2()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    parse() は引数 url を起点にリソースを列挙し、同一オリジンの XLSX URL を urljoin で導出する。
    scraper.execute("https://opendata.pref.chiba.lg.jp/datasets/6")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
