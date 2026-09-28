"""
Catastro Turístico (エクアドル観光省 観光事業者台帳) — クローラー

取得対象:
    エクアドル観光省 (Ministerio de Turismo) のサービスポータル
    https://servicios.turismo.gob.ec/catastro-turistico/ に掲載されている
    「Descargar catastro de establecimientos」(全国観光事業者台帳 xlsx) から、
    **旅行業 (Agencias de viajes / AGENCIAMIENTO TURÍSTICO)** の事業者だけを
    1 事業者 = 1 行で取得する。

取得フロー:
    1. 起点 URL (sites.yml の url) を GET し、ページ内の .xlsx リンクを走査する。
       同ページには「観光ガイド台帳 (catastro de guías)」の xlsx も並んでいるため、
       リンク文字列/ファイル名に guía(guias/guia) を含むものは除外し、
       establecimiento / consolidado を含むものを事業者台帳として採用する。
       ⚠ ファイル URL は毎月更新される
         (例: /wp-content/uploads/2026/09/Consolidado-Nacional-2026-publico-8.xlsx)。
         URL は決め打ちせず、必ず起点ページのリンクから導出する。
    2. xlsx をダウンロードし、**標準ライブラリのみ (zipfile + ElementTree)** で
       ストリーム解析する。外部 Excel ライブラリ (openpyxl / python-calamine) に
       依存しないため実行環境を選ばない。
    3. ヘッダ行 (1 行目) から列記号 → 列名のマップを作り、データ行を 1 行ずつ判定 →
       条件に合致したものを **即 yield** する (全件バッファしない)。

元データのカラム (2026-09 時点 / 全 11 列・36,017 行):
    RUC / Nombre Comercial / Número de Registro / Actividad / Modalidad /
    Clasificación / Categoría / Razón social (Propietario) /
    Provincia / Cantón / Parroquia / Estado Registro del Establecimiento

フィルタ (呼び出し備考「『Agencias de viajes』区分でフィルタ」への対応):
    Actividad / Modalidad == "AGENCIAMIENTO TURÍSTICO" (= 旅行業) を対象とする。
    この活動区分の内訳 (Clasificación) は 2026-09 時点で
        AGENCIA OPERADORA DE TURISMO      1,252
        AGENCIA DE VIAJES DUAL            1,132
        AGENCIA DE VIAJES INTERNACIONAL     470
        AGENCIA DE VIAJES MAYORISTA         151
    の計 3,005 件で、いずれも旅行業 (agencias de viajes) の免許区分。
    細かい区分は Schema.CAT_NM (細業種) にそのまま出力するので、
    後工程で「AGENCIA DE VIAJES ～」だけに絞り込むこともできる。
    念のため Clasificación 側が AGENCIA で始まる行も拾う (活動名の表記ゆれ対策)。

カラム設計:
    名称        ← Nombre Comercial (商号)。空の場合のみ Razón social で補完
    施設名      ← Nombre Comercial
    法人番号    ← RUC (エクアドルの納税者番号 13 桁)
    代表者名    ← Razón social (Propietario) = 所有者 (個人名または法人名)
    都道府県    ← Provincia (県)
    住所        ← Cantón + Parroquia を連結したもの
                  ※ 公開台帳には**番地レベルの住所が存在しない** (行政区画までの粒度)
    大業種/サイト定義業種 ← Actividad / Modalidad
    細業種/中業種 ← Clasificación (AGENCIA DE VIAJES DUAL 等)
    営業状態    ← Estado Registro del Establecimiento (RATIFICADO / PENDIENTE DE INSPECCIÓN)
    取得URL     ← 起点ページ URL (parse() の引数)
    EXTRA       ← 登録番号 / カテゴリ / 都市 / 教区 / 元ファイル

    ※ 備考で求められた TEL・URL (事業者サイト) は**公開台帳に列自体が存在しない**。
      掲載ページにも「En caso de requerir información ampliada solicítalo al correo
      catastros@produccion.gob.ec」(拡張情報はメールで請求) と明記されており、
      公開データからは取得できないため空欄とする (推測で埋めない)。
      同様に郵便番号・従業員数・資本金・売上・メール・SNS・営業時間も存在しない。

利用規約:
    データセット掲載元 https://datosabiertos.gob.ec/dataset/catastro-turistico-total は
    WAF により直接取得できない (403) ため Wayback Machine 版を確認した。
    ライセンスは「Creative Commons Attribution」(CC BY / 出典表示)、
    出典は Ministerio de Turismo。スクレイピング・自動取得を禁止する条項は無い。
"""

import io
import logging
import re
import sys
import time
import unicodedata
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# SpreadsheetML の名前空間
_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

# セル参照 (例: "AB123") から列記号を取り出す
_CELL_COL_RE = re.compile(r"([A-Z]+)")

# 旅行業 (Agencias de viajes) の判定語。アクセントを除去した大文字で比較する
_AGENCY_ACTIVITY_KEY = "AGENCIAMIENTO"   # ACTIVIDAD: AGENCIAMIENTO TURISTICO
_AGENCY_CLASS_KEY = "AGENCIA"            # CLASIFICACION: AGENCIA DE VIAJES ... / AGENCIA OPERADORA ...

# 元データのヘッダ名 (アクセント除去・大文字化したキー → 論理名)
_HEADER_ALIASES = {
    "RUC": "ruc",
    "NOMBRE COMERCIAL": "nombre",
    "NUMERO DE REGISTRO": "registro",
    "ACTIVIDAD / MODALIDAD": "actividad",
    "ACTIVIDAD/MODALIDAD": "actividad",
    "CLASIFICACION": "clasificacion",
    "CATEGORIA": "categoria",
    "RAZON SOCIAL (PROPIETARIO)": "razon_social",
    "RAZON SOCIAL": "razon_social",
    "PROVINCIA": "provincia",
    "CANTON": "canton",
    "PARROQUIA": "parroquia",
    "ESTADO REGISTRO DEL ESTABLECIMIENTO": "estado",
    "ESTADO": "estado",
}

# xlsx ダウンロードの最大試行回数 (無限リトライ禁止)
_MAX_DOWNLOAD_ATTEMPTS = 3


class CatastroTuristico(StaticCrawler):
    """エクアドル観光省 観光事業者台帳 (旅行業) スクレイパー"""

    # 通信は起点ページ 1 回 + xlsx 1 本のみ。基盤は yield ごとに DELAY 秒待つため 0 とする。
    DELAY = 0.0
    ITEM_DELAY = 0.0
    CONTINUE_ON_ERROR = True
    TIMEOUT = 120

    EXTRA_COLUMNS = [
        "登録番号",
        "カテゴリ",
        "都市",
        "教区",
        "元ファイル",
    ]

    # ------------------------------------------------------------------ utils

    @staticmethod
    def _txt(value) -> str:
        """セル値を安全に文字列化する (改行・連続空白を整理)。"""
        if value is None:
            return ""
        s = str(value).replace("\r", " ").replace("\n", " ")
        return re.sub(r"\s+", " ", s).strip()

    @classmethod
    def _key(cls, value: str) -> str:
        """アクセントを除去し大文字化した比較用キーを返す (TURÍSTICO → TURISTICO)。"""
        s = unicodedata.normalize("NFD", cls._txt(value))
        s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
        return s.upper()

    # ----------------------------------------------------------- xlsx URL 解決

    def _find_xlsx_url(self, url: str) -> str:
        """起点ページから事業者台帳 xlsx の URL を導出する (ガイド台帳は除外)。"""
        soup = self.get_soup(url)
        if soup is None:
            raise RuntimeError(f"起点ページを取得できませんでした: {url}")

        best = ""
        for a in soup.select("a[href]"):
            href = a.get("href", "").strip()
            if ".xlsx" not in href.lower():
                continue
            # 引数 url を唯一のルートとして相対 URL を解決する
            absolute = urljoin(url, href)
            label = self._key(a.get_text(" ", strip=True)) + " " + self._key(absolute)
            if "GUIA" in label:      # 観光ガイド台帳は対象外
                continue
            if "ESTABLECIMIENTO" in label or "CONSOLIDADO" in label:
                return absolute
            best = best or absolute  # 文言が変わった場合の保険

        if best:
            logger.warning("台帳リンクの文言が想定外です。最初の xlsx を使用します: %s", best)
            return best
        raise RuntimeError(f"事業者台帳 (xlsx) のリンクが見つかりませんでした: {url}")

    def _download(self, xlsx_url: str) -> bytes:
        """xlsx をダウンロードする。上限回数まで再試行し、全失敗なら例外を送出する。"""
        last_error: Exception | None = None
        for attempt in range(_MAX_DOWNLOAD_ATTEMPTS):
            try:
                logger.info("台帳ダウンロード中 (%d/%d): %s",
                            attempt + 1, _MAX_DOWNLOAD_ATTEMPTS, xlsx_url)
                resp = self.session.get(xlsx_url, timeout=self.TIMEOUT)
                resp.raise_for_status()
                content = resp.content
                if not content.startswith(b"PK"):  # xlsx は ZIP (PK) で始まる
                    raise RuntimeError("xlsx ではないレスポンスを受信しました")
                return content
            except Exception as e:  # noqa: BLE001 — 上限付きで再試行し、最後は raise する
                last_error = e
                logger.warning("ダウンロード失敗 (%d/%d): %s", attempt + 1, _MAX_DOWNLOAD_ATTEMPTS, e)
                if attempt < _MAX_DOWNLOAD_ATTEMPTS - 1:
                    time.sleep(min(2 ** attempt, 8))
        raise RuntimeError(f"台帳のダウンロードに失敗しました: {xlsx_url} — {last_error}")

    # --------------------------------------------------------- xlsx ストリーム解析

    @staticmethod
    def _shared_strings(zf: zipfile.ZipFile) -> list[str]:
        """sharedStrings.xml を読み、共有文字列テーブルを返す。"""
        if "xl/sharedStrings.xml" not in zf.namelist():
            return []
        strings: list[str] = []
        with zf.open("xl/sharedStrings.xml") as f:
            for _, el in ET.iterparse(f, events=("end",)):
                if el.tag == _NS + "si":
                    strings.append("".join(t.text or "" for t in el.iter(_NS + "t")))
                    el.clear()
        return strings

    @staticmethod
    def _sheet_path(zf: zipfile.ZipFile) -> str:
        """先頭シートの XML パスを返す (台帳は 1 シート構成)。"""
        sheets = sorted(n for n in zf.namelist() if n.startswith("xl/worksheets/sheet"))
        if not sheets:
            raise RuntimeError("xlsx 内にワークシートが見つかりません")
        return sheets[0]

    def _iter_rows(self, content: bytes):
        """xlsx の先頭シートを 1 行ずつ {論理名: 値} で列挙する (全件バッファしない)。"""
        zf = zipfile.ZipFile(io.BytesIO(content))
        shared = self._shared_strings(zf)
        header: dict[str, str] | None = None   # 列記号 → 論理名

        with zf.open(self._sheet_path(zf)) as f:
            for _, el in ET.iterparse(f, events=("end",)):
                if el.tag != _NS + "row":
                    continue
                cells: dict[str, str] = {}
                for c in el.findall(_NS + "c"):
                    ref = c.get("r") or ""
                    m = _CELL_COL_RE.match(ref)
                    if not m:
                        continue
                    cells[m.group(1)] = self._cell_value(c, shared)
                el.clear()

                if not any(cells.values()):
                    continue

                if header is None:
                    header = {
                        col: _HEADER_ALIASES[self._key(val)]
                        for col, val in cells.items()
                        if self._key(val) in _HEADER_ALIASES
                    }
                    unknown = [val for col, val in cells.items() if col not in header and val]
                    if unknown:
                        logger.warning("未対応の列を検出しました: %s", " / ".join(unknown))
                    if "nombre" not in header.values():
                        # ヘッダを特定できない = 書式変更。黙って 0 件にせず失敗させる
                        raise RuntimeError(f"台帳のヘッダ行を解釈できません: {list(cells.values())}")
                    continue

                yield {name: cells.get(col, "") for col, name in header.items()}

    @staticmethod
    def _cell_value(cell, shared: list[str]) -> str:
        """セル要素から表示値を取り出す (共有文字列 / インライン文字列 / 数値)。"""
        ctype = cell.get("t")
        if ctype == "inlineStr":
            node = cell.find(_NS + "is")
            return "".join(t.text or "" for t in node.iter(_NS + "t")) if node is not None else ""
        v = cell.find(_NS + "v")
        if v is None or v.text is None:
            return ""
        if ctype == "s":
            try:
                return shared[int(v.text)]
            except (ValueError, IndexError):
                return ""
        return v.text

    # ------------------------------------------------------------------ 判定

    @classmethod
    def _is_travel_agency(cls, row: dict) -> bool:
        """旅行業 (Agencias de viajes) の行かどうかを判定する。"""
        actividad = cls._key(row.get("actividad", ""))
        clasificacion = cls._key(row.get("clasificacion", ""))
        return _AGENCY_ACTIVITY_KEY in actividad or clasificacion.startswith(_AGENCY_CLASS_KEY)

    # ------------------------------------------------------------------ parse

    def parse(self, url: str):
        """起点 URL から台帳 xlsx を取得し、旅行業の事業者を 1 件ずつ yield する。"""
        xlsx_url = self._find_xlsx_url(url)
        filename = xlsx_url.rsplit("/", 1)[-1]
        logger.info("台帳ファイル: %s", filename)

        content = self._download(xlsx_url)
        # 総件数は台帳を読み切るまで確定しない (旅行業のみ抽出するため)
        self.total_items = None

        count = 0
        for row in self._iter_rows(content):
            name = self._txt(row.get("nombre", "")) or self._txt(row.get("razon_social", ""))
            if not name:
                continue
            if not self._is_travel_agency(row):
                continue

            canton = self._txt(row.get("canton", ""))
            parroquia = self._txt(row.get("parroquia", ""))
            addr = " ".join(p for p in (canton, parroquia) if p)

            item = {
                Schema.NAME: name,
                Schema.FAC_NAME: self._txt(row.get("nombre", "")),
                Schema.CO_NUM: self._txt(row.get("ruc", "")),
                Schema.REP_NM: self._txt(row.get("razon_social", "")),
                Schema.PREF: self._txt(row.get("provincia", "")),
                Schema.ADDR: addr,
                Schema.CAT_LV1: self._txt(row.get("actividad", "")),
                Schema.CAT_LV2: self._txt(row.get("clasificacion", "")),
                Schema.CAT_NM: self._txt(row.get("clasificacion", "")),
                Schema.CAT_SITE: self._txt(row.get("actividad", "")),
                Schema.STS_NM: self._txt(row.get("estado", "")),
                Schema.URL: url,
                "登録番号": self._txt(row.get("registro", "")),
                "カテゴリ": self._txt(row.get("categoria", "")),
                "都市": canton,
                "教区": parroquia,
                "元ファイル": filename,
            }
            count += 1
            yield item

        logger.info("旅行業として抽出した件数: %d", count)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = CatastroTuristico()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    parse() は引数 url を起点に xlsx の URL を導出する。
    scraper.execute("https://servicios.turismo.gob.ec/catastro-turistico/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
