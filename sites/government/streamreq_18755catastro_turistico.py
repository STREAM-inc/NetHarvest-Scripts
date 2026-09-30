"""
【STREAMREQ-18755】Catastro Turístico オープンデータ (エクアドル観光省) — 旅行業クローラー

取得対象:
    Ministerio de Turismo (エクアドル観光省) が公表する
    Catastro Turístico Nacional (全国観光事業者台帳) のうち、
    区分「Agencias de viajes / Operadora」= 活動区分 AGENCIAMIENTO TURÍSTICO の
    事業者だけを 1 事業者 = 1 行で取得する。

データの所在 (重要):
    sites.yml の正規 URL はエクアドル政府オープンデータポータルのデータセットページ
    https://datosabiertos.gob.ec/dataset/catastro-turistico-total 。
    ただし datosabiertos.gob.ec は 2026-09 時点で全パスが Apache レベルの
    403 Forbidden を返す (トップ / robots.txt / CKAN API `/api/3/action/package_show`
    のいずれも 403。ブラウザ相当ヘッダでも、外部プロキシ経由でも同じ 403 のため
    本環境固有の IP ブロックではなくポータル側の遮断)。

    一方、当該データセットのメタデータ (Wayback の 2024-07-08 スナップショットで確認)
    には配布元として
        Fuente        : Ministerio de Turismo
        Página web    : https://servicios.turismo.gob.ec/catastro-turistico/
        Formato       : XLSX
        Licencia de uso: Creative Commons Attribution
    と明記されており、実ファイル (XLSX) は観光省のサービスポータル側が
    認証不要・WAF 無しで配信している。

    そこで本クローラーは
      - 正規 URL (引数 url) … 起点。CKAN API / データセットページからの
                              リソース解決を最初に試み、Schema.URL (取得元URL) に使う
      - 観光省ポータル       … 上記が 403 等で解決できない場合の実ファイル取得元
                              (データセット自身が "Página web" として宣言している配布元)
    という二段構えにしている。どちらの経路で取得したかは EXTRA「取得経路」に残す。

取得フロー:
    1. 引数 url から CKAN の package_show API URL を組み立てて叩き、XLSX リソースの
       URL を解決する (成功すればそれを使う)。失敗したらデータセットページの HTML から
       .xlsx リンクを探す。それも駄目なら観光省ポータルへフォールバックする。
    2. 観光省ポータル経由の場合、ページ内の .xlsx リンクを走査する。同ページには
       「観光ガイド台帳 (catastro de guías)」の xlsx も並んでいるため、リンク文字列 /
       ファイル名に guía(guias/guia) を含むものは除外し、establecimiento /
       consolidado を含むものを事業者台帳として採用する。
       ⚠ ファイル URL は毎月更新される
         (例: /wp-content/uploads/2026/09/Consolidado-Nacional-2026-publico-8.xlsx)。
         URL は決め打ちせず、必ずページ上のリンクから導出する。
    3. xlsx をダウンロードし、**標準ライブラリのみ (zipfile + ElementTree)** で
       ストリーム解析する。外部 Excel ライブラリ (openpyxl / python-calamine) に
       依存しないため実行環境を選ばない。
    4. ヘッダ行から列記号 → 列名のマップを作り、データ行を 1 行ずつ判定 →
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
    の計 3,005 件で、いずれも旅行業 (agencias de viajes / operadora) の免許区分。
    細かい区分は Schema.CAT_NM (細業種) にそのまま出力する。
    活動名の表記ゆれ対策として Clasificación が AGENCIA で始まる行も拾う。

カラム設計 (参考スクレイピングカラムとの対応):
    会社名      → Schema.NAME     ← Nombre Comercial (空なら Razón social で補完)
    住所        → Schema.ADDR     ← Cantón + Parroquia (Dirección 列がある配布物では
                                    そちらを優先)
    都市        → EXTRA「都市」   ← Cantón
    国          → EXTRA「国」     ← "エクアドル" 固定 (備考の指示どおり)
    区分(作業用)→ Schema.CAT_NM / CAT_LV2 ← Clasificación (AGENCIA DE VIAJES DUAL 等)
    TEL         → Schema.TEL + EXTRA「TEL(国際表記)」(+593 表記)
    URL         → Schema.WEBSITE  ← 事業者サイト
    取得元URL   → Schema.URL      ← 起点 URL (parse() の引数)

    ※ TEL / 事業者 URL / メール / 住所(番地) は 2026-09 時点の公開配布物
      (Consolidado Nacional xlsx) には**列自体が存在しない**。配布ページにも
      「En caso de requerir información ampliada solicítalo al correo
      catastros@produccion.gob.ec」(拡張情報はメールで請求) と明記されている。
      ただし配布物の列構成は更新のたびに変わりうるため、Dirección / Teléfono /
      Correo / Página web に相当する列名は**エイリアスとして実装済み**で、
      列が現れた時点で自動的に埋まる (現行データでは空欄)。推測では埋めない。

名寄せ:
    海外所在の事業者のため STX 名寄せは不可 (対象外)。名寄せ工程は設けない。

利用規約:
    datosabiertos.gob.ec はエクアドル政府公式オープンデータポータル。
    当該データセットのライセンスは Creative Commons Attribution で、
    スクレイピング・機械的取得を禁止する条項は無い
    (robots.txt は 403 のため取得不可 = 遮断されており記述を確認できない)。

実行方法:
    # ローカルテスト
    python scripts/sites/government/streamreq_18755catastro_turistico.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id streamreq_18755catastro_turistico
"""

import io
import json
import logging
import re
import sys
import time
import unicodedata
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

# SpreadsheetML の名前空間
_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

# セル参照 (例: "AB123") から列記号を取り出す
_CELL_COL_RE = re.compile(r"([A-Z]+)")

# 旅行業 (Agencias de viajes) の判定語。アクセントを除去した大文字で比較する
_AGENCY_ACTIVITY_KEY = "AGENCIAMIENTO"   # ACTIVIDAD: AGENCIAMIENTO TURISTICO
_AGENCY_CLASS_KEY = "AGENCIA"            # CLASIFICACION: AGENCIA DE VIAJES ... / AGENCIA OPERADORA ...

# 元データのヘッダ名 (アクセント除去・大文字化したキー → 論理名)
# Dirección / Teléfono / Correo / Página web は現行配布物には無いが、
# 列構成が変わったときに自動で拾えるようエイリアスを用意しておく。
_HEADER_ALIASES = {
    "RUC": "ruc",
    "NOMBRE COMERCIAL": "nombre",
    "NUMERO DE REGISTRO": "registro",
    "ACTIVIDAD / MODALIDAD": "actividad",
    "ACTIVIDAD/MODALIDAD": "actividad",
    "ACTIVIDAD": "actividad",
    "CLASIFICACION": "clasificacion",
    "CATEGORIA": "categoria",
    "RAZON SOCIAL (PROPIETARIO)": "razon_social",
    "RAZON SOCIAL": "razon_social",
    "PROVINCIA": "provincia",
    "CANTON": "canton",
    "PARROQUIA": "parroquia",
    "ESTADO REGISTRO DEL ESTABLECIMIENTO": "estado",
    "ESTADO": "estado",
    "DIRECCION": "direccion",
    "DIRECCION DEL ESTABLECIMIENTO": "direccion",
    "TELEFONO": "telefono",
    "TELEFONO DEL ESTABLECIMIENTO": "telefono",
    "CELULAR": "telefono",
    "CORREO": "email",
    "CORREO ELECTRONICO": "email",
    "EMAIL": "email",
    "PAGINA WEB": "web",
    "SITIO WEB": "web",
    "WEB": "web",
}

# データセットが "Página web" (配布元) として宣言している観光省ポータル。
# 正規 URL (CKAN 側) が 403 等で解決できないときのフォールバック先。
_PUBLISHER_PAGE = "https://servicios.turismo.gob.ec/catastro-turistico/"

# エクアドルの国番号 (備考: TEL は +593 を含む国際表記に正規化する)
_COUNTRY_CALLING_CODE = "593"
_COUNTRY_NAME_JA = "エクアドル"

# ダウンロード / 取得の最大試行回数 (無限リトライ・無限再帰は禁止)
_MAX_DOWNLOAD_ATTEMPTS = 3


class Streamreq18755CatastroTuristico(StaticCrawler):
    """エクアドル観光省 Catastro Turístico (旅行業) スクレイパー"""

    # 通信は起点ページ (+フォールバック 1 ページ) と xlsx 1 本のみ。
    # 基盤は yield ごとに DELAY 秒待つため、待機はページ取得側に寄せて 0 とする。
    DELAY = 0.0
    ITEM_DELAY = 0.0
    CONTINUE_ON_ERROR = True
    TIMEOUT = 120

    EXTRA_COLUMNS = [
        "国",
        "都市",
        "教区",
        "登録番号",
        "カテゴリ",
        "TEL(国際表記)",
        "取得経路",
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

    @classmethod
    def _intl_tel(cls, value: str) -> str:
        """エクアドルの電話番号を +593 を含む国際表記に整える。

        例: "022345678" → "+593 2 2345678" / "0991234567" → "+593 99 1234567"
        判別できない場合は数字を整形した上で +593 を前置する。
        """
        raw = cls._txt(value)
        if not raw:
            return ""
        digits = re.sub(r"\D", "", raw)
        if not digits:
            return ""
        if digits.startswith("00" + _COUNTRY_CALLING_CODE):
            digits = digits[2 + len(_COUNTRY_CALLING_CODE):]
        elif digits.startswith(_COUNTRY_CALLING_CODE) and len(digits) > 9:
            digits = digits[len(_COUNTRY_CALLING_CODE):]
        digits = digits.lstrip("0")          # 国内プレフィックス 0 を除去
        if not digits:
            return ""
        if digits.startswith("9") and len(digits) >= 9:   # 携帯: 9x xxxxxxx
            return f"+{_COUNTRY_CALLING_CODE} {digits[:2]} {digits[2:]}"
        return f"+{_COUNTRY_CALLING_CODE} {digits[:1]} {digits[1:]}"

    # ----------------------------------------------------------- xlsx URL 解決

    def _xlsx_from_ckan_api(self, url: str) -> str:
        """引数 url から CKAN package_show API を組み立て、XLSX リソース URL を返す。

        datosabiertos.gob.ec は現在全パス 403 のため通常は空文字を返すが、
        遮断が解除された場合はこちらが優先される (正規 URL からの解決)。
        """
        parsed = urlparse(url)
        slug = parsed.path.rstrip("/").rsplit("/", 1)[-1]
        if not slug:
            return ""
        api_url = urljoin(url, f"/api/3/action/package_show?id={slug}")
        try:
            resp = self.session.get(api_url, timeout=self.TIMEOUT)
            resp.raise_for_status()
            payload = json.loads(resp.text)
        except Exception as e:  # noqa: BLE001 — 403/JSON 不正はフォールバック対象
            logger.info("CKAN API からリソースを解決できません (%s): %s", api_url, e)
            return ""

        resources = (payload.get("result") or {}).get("resources") or []
        for res in resources:
            fmt = self._key(res.get("format", ""))
            res_url = self._txt(res.get("url", ""))
            if not res_url:
                continue
            if fmt in ("XLSX", "XLS") or res_url.lower().endswith((".xlsx", ".xls")):
                return urljoin(url, res_url)
        logger.info("CKAN API に XLSX リソースがありません: %s", api_url)
        return ""

    def _xlsx_from_page(self, page_url: str) -> str:
        """HTML ページ内の .xlsx リンクから事業者台帳の URL を導出する。

        観光ガイド台帳 (catastro de guías) の xlsx も同じページに並ぶため除外する。
        見つからない場合は空文字を返す (呼び出し側がフォールバックを判断する)。
        """
        soup = self.get_soup(page_url)
        if soup is None:
            logger.info("ページを取得できませんでした: %s", page_url)
            return ""

        fallback = ""
        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            if ".xlsx" not in href.lower() and ".xls" not in href.lower():
                continue
            absolute = urljoin(page_url, href)   # 相対 URL は当該ページ基準で解決
            label = self._key(a.get_text(" ", strip=True)) + " " + self._key(absolute)
            if "GUIA" in label:      # 観光ガイド台帳は対象外
                continue
            if "ESTABLECIMIENTO" in label or "CONSOLIDADO" in label or "CATASTRO" in label:
                return absolute
            fallback = fallback or absolute      # 文言が変わった場合の保険

        if fallback:
            logger.warning("台帳リンクの文言が想定外です。最初の xlsx を使用します: %s", fallback)
        return fallback

    def _resolve_xlsx(self, url: str) -> tuple[str, str]:
        """台帳 xlsx の URL と取得経路を解決する。

        優先順位:
            1. 正規 URL の CKAN API (package_show)
            2. 正規 URL のデータセットページ HTML
            3. データセットが配布元として宣言している観光省ポータル
        """
        xlsx_url = self._xlsx_from_ckan_api(url)
        if xlsx_url:
            return xlsx_url, "オープンデータポータル (CKAN API)"

        xlsx_url = self._xlsx_from_page(url)
        if xlsx_url:
            return xlsx_url, "オープンデータポータル (データセットページ)"

        logger.warning(
            "正規 URL から台帳を解決できませんでした (ポータルが 403 の可能性)。"
            "データセットが配布元として宣言している観光省ポータルへフォールバックします: %s",
            _PUBLISHER_PAGE,
        )
        xlsx_url = self._xlsx_from_page(_PUBLISHER_PAGE)
        if xlsx_url:
            return xlsx_url, "観光省ポータル (配布元)"

        raise RuntimeError(
            f"事業者台帳 (xlsx) のリンクを解決できませんでした: {url} / {_PUBLISHER_PAGE}"
        )

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
                logger.warning("ダウンロード失敗 (%d/%d): %s",
                               attempt + 1, _MAX_DOWNLOAD_ATTEMPTS, e)
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

    def _iter_rows(self, content: bytes) -> Generator[dict, None, None]:
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

    # ------------------------------------------------------------------ 判定

    @classmethod
    def _is_travel_agency(cls, row: dict) -> bool:
        """旅行業 (Agencias de viajes / Operadora) の行かどうかを判定する。"""
        actividad = cls._key(row.get("actividad", ""))
        clasificacion = cls._key(row.get("clasificacion", ""))
        return _AGENCY_ACTIVITY_KEY in actividad or clasificacion.startswith(_AGENCY_CLASS_KEY)

    # ------------------------------------------------------------------ parse

    def parse(self, url: str) -> Generator[dict, None, None]:
        """起点 URL から台帳 xlsx を解決・取得し、旅行業の事業者を 1 件ずつ yield する。"""
        xlsx_url, route = self._resolve_xlsx(url)
        filename = xlsx_url.rsplit("/", 1)[-1]
        logger.info("台帳ファイル: %s (経路: %s)", filename, route)

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
            # 番地レベルの住所列がある配布物ではそれを優先し、無ければ行政区画で代替する
            addr = self._txt(row.get("direccion", "")) or " ".join(
                p for p in (canton, parroquia) if p
            )
            tel = self._txt(row.get("telefono", ""))

            item = {
                Schema.NAME: name,
                Schema.FAC_NAME: self._txt(row.get("nombre", "")),
                Schema.CO_NUM: self._txt(row.get("ruc", "")),
                Schema.REP_NM: self._txt(row.get("razon_social", "")),
                Schema.PREF: self._txt(row.get("provincia", "")),
                Schema.ADDR: addr,
                Schema.TEL: tel,
                Schema.EMAIL: self._txt(row.get("email", "")),
                Schema.WEBSITE: self._txt(row.get("web", "")),
                Schema.CAT_LV1: self._txt(row.get("actividad", "")),
                Schema.CAT_LV2: self._txt(row.get("clasificacion", "")),
                Schema.CAT_NM: self._txt(row.get("clasificacion", "")),
                Schema.CAT_SITE: self._txt(row.get("actividad", "")),
                Schema.STS_NM: self._txt(row.get("estado", "")),
                Schema.URL: url,
                "国": _COUNTRY_NAME_JA,
                "都市": canton,
                "教区": parroquia,
                "登録番号": self._txt(row.get("registro", "")),
                "カテゴリ": self._txt(row.get("categoria", "")),
                "TEL(国際表記)": self._intl_tel(tel),
                "取得経路": route,
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

    scraper = Streamreq18755CatastroTuristico()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    parse() は引数 url を起点に台帳 xlsx の URL を解決する。
    scraper.execute("https://datosabiertos.gob.ec/dataset/catastro-turistico-total")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
