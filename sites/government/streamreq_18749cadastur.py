"""
【STREAMREQ-18749】CADASTUR オープンデータ (ブラジル観光省) — 旅行代理店 (Agências de Turismo)

取得対象:
    Ministério do Turismo (ブラジル観光省) が運用する観光サービス提供者登録制度
    CADASTUR の公開データのうち、区分「Prestadores de Serviços Turísticos -
    Agência de Turismo」(旅行代理店・法人) の全国登録簿。
    最新四半期スナップショットで 57,000 件超 (2026年第2四半期時点 57,723 件)。

データの所在 (重要):
    sites.yml の正規 URL はブラジル連邦政府オープンデータポータルの
    データセットページ (https://dados.gov.br/dados/conjuntos-dados/cadastur-03)。
    ただし dados.gov.br 本体は Vue の SPA シェルのみを返し、その裏 API
    (/dados/api/publico/...) は API キー必須で未認証だと 401 になる。
    dados.gov.br は観光省の CKAN (dados.turismo.gov.br) をハーベストしている
    ミラーであり、実ファイル (XLSX) は観光省 CKAN 側が認証不要で配信している。
    そのため本クローラーは
      - 正規 URL      … 起点の到達確認 + Schema.URL (取得元URL) として使用
      - 観光省 CKAN   … 実データ (最新四半期ファイル) の取得元
    という二段構えにしている。CKAN のホストとデータセット slug のみ定数化し、
    リソース URL は package_show の応答から動的に解決する (URL 決め打ちしない)。

取得フロー:
    1. 正規 URL (引数 url) を GET して起点の生存を確認する。
    2. 観光省 CKAN の package_show API でデータセット
       "agencia-de-turismo" のリソース一覧を取得する。
    3. XLSX / XLS / CSV のうち last_modified が最新のリソースを 1 本選び、
       そのファイルをダウンロードする (四半期ごとの全件スナップショット)。
    4. 1 行 = 1 事業者として読み進め、絞り込み条件に合致した行を即 yield する
       (全件バッファしない)。

絞り込み (課題の【取得方法】指示への対応):
    全国 57,723 件をそのまま ⑫HP巡回 に流すと工数が破綻するため、課題指示に従って
    母集団を事前縮小する。採用した条件は OR 結合の 2 本:
      (A) 社名 / 商号 / Website / E-mail に日本関連キーワードを含む (全国対象)
          … Japão/Japan/Tóquio/Nippon/Oriente/Sakura/JTB 等
      (B) 日系コミュニティ集積地の市区に所在する
          … サンパウロ / リオデジャネイロ / クリチバ / ロンドリーナ / マリンガ /
             カンポグランデ / ベレン / スザノ / モジダスクルーゼス / バストス 等
          ただし大都市 3 市 (São Paulo / Rio de Janeiro / Curitiba) は単独で
          11,068 件あり指示の「数百〜数千件規模」を超えるため、⑫HP巡回 の実効性が
          ある「Website 記載あり」のレコードに限定する (4,529 件)。
    どの条件で拾ったかは EXTRA カラム「抽出条件」に残す。

備考への対応:
    - 「国」カラムは "ブラジル" 固定。
    - TEL は +55 を含む国際表記に正規化し、EXTRA「TEL(国際表記)」に格納する。
      Schema.TEL はフレームワークの正規化で数字とハイフン以外が除去されるため、
      同じ番号をハイフン区切りの国別番号付き (例: 55-11-3875-6656) で入れる。
    - STX 名寄せは行わない (海外事業者のため対象外)。
    - 「Atividades Obrigatórias」「Atividades Opcionais」は 1 セル数百字の自由記述
      (法令上の業務内容の文章) のため、著作権リスク回避のルールに従い取得しない。

利用規約:
    dados.gov.br / dados.turismo.gov.br はブラジル連邦政府の公式オープンデータ
    ポータル。当該データセットのライセンスは Open Data Commons Open Database
    License (ODbL) で、スクレイピング・機械的取得を禁止する条項は無い。

実行方法:
    # ローカルテスト
    python scripts/sites/government/streamreq_18749cadastur.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id streamreq_18749cadastur
"""

import csv
import datetime
import io
import logging
import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

# --- 実データの所在 (dados.gov.br 裏 API は API キー必須のためミラー元を使う) ---
_CKAN_ORIGIN = "https://dados.turismo.gov.br"
_CKAN_DATASET = "agencia-de-turismo"  # "Prestadores de serviços turísticos - Agência de Turismo"
_DATA_FORMATS = ("XLSX", "XLS", "CSV")

# --- 絞り込み (A): 日本関連キーワード (アクセント除去・大文字化して部分一致) ---
_JP_KEYWORDS = (
    "JAPAO", "JAPAN", "JAPON", "JAPONESA", "JAPONES",
    "TOQUIO", "TOKYO", "TOKIO", "NIPPON", "NIPON", "NIHON",
    "NIKKEI", "NIKKEY", "ORIENTE", "ORIENT", "SAKURA", "FUJI",
    "OSAKA", "KYOTO", "QUIOTO", "SAMURAI", "YAMATO", "MIYAKO",
    "JTB", "ASIA", "ASIAN",
)

# --- 絞り込み (B-1): 日系コミュニティ集積地 (中小都市。Website 有無を問わず採用) ---
_HUB_CITIES = frozenset({
    ("PR", "LONDRINA"), ("PR", "MARINGA"), ("PR", "ASSAI"), ("PR", "URAI"),
    ("PR", "CORNELIO PROCOPIO"), ("PR", "ROLANDIA"), ("PR", "APUCARANA"),
    ("SP", "SUZANO"), ("SP", "MOGI DAS CRUZES"), ("SP", "MARILIA"),
    ("SP", "PRESIDENTE PRUDENTE"), ("SP", "REGISTRO"), ("SP", "BASTOS"),
    ("SP", "ATIBAIA"), ("SP", "SANTOS"), ("SP", "GUARULHOS"),
    ("SP", "SAO BERNARDO DO CAMPO"), ("SP", "ITAPECERICA DA SERRA"),
    ("SP", "PROMISSAO"), ("SP", "ALVARES MACHADO"), ("SP", "PEREIRA BARRETO"),
    ("SP", "PIRACICABA"), ("SP", "CAMPINAS"),
    ("MS", "CAMPO GRANDE"), ("MS", "DOURADOS"),
    ("PA", "BELEM"), ("PA", "TOME ACU"),
    ("AM", "MANAUS"),
})

# --- 絞り込み (B-2): 大都市。件数が多いため Website 記載ありに限定する ---
_METRO_CITIES = frozenset({
    ("SP", "SAO PAULO"), ("RJ", "RIO DE JANEIRO"), ("PR", "CURITIBA"),
})

_COUNTRY = "ブラジル"
_MAX_ATTEMPTS = 3
_EMPTY_VALUES = {"", "-", "--", "0", "N/A", "NAO POSSUI", "NÃO POSSUI"}


def _ascii_upper(value) -> str:
    """アクセント記号を落として大文字化し、比較しやすい形にそろえる。"""
    text = unicodedata.normalize("NFD", str(value or ""))
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.upper().replace("-", " ")
    return " ".join(text.split())


def _clean(value) -> str:
    """セル値を文字列化し、空値プレースホルダを空文字にそろえる。"""
    if value is None:
        return ""
    if isinstance(value, datetime.datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, datetime.date):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    text = str(value).strip()
    if text.upper() in _EMPTY_VALUES:
        return ""
    return text


def _to_international_tel(raw: str) -> tuple[str, str]:
    """ブラジルの市外局番付き番号を国際表記へ整える。

    Args:
        raw: 例 "(19)3875-6656"

    Returns:
        (Schema.TEL 用のハイフン区切り表記, EXTRA 用の +55 表記)。
        数字が取れない場合は ("", "")。
    """
    digits = re.sub(r"\D", "", raw or "")
    if len(digits) < 8:
        return "", ""
    # 先頭に国番号 55 が二重で付いているデータがあるため 1 度だけ剥がす
    if len(digits) > 11 and digits.startswith("55"):
        digits = digits[2:]
    if len(digits) in (10, 11):  # DDD(2桁) + 加入者番号(8 or 9桁)
        ddd, subscriber = digits[:2], digits[2:]
    else:                        # DDD 欠落データはそのまま加入者番号扱い
        ddd, subscriber = "", digits
    if ddd:
        return f"55-{ddd}-{subscriber}", f"+55 {ddd} {subscriber}"
    return f"55-{subscriber}", f"+55 {subscriber}"


def _normalize_website(raw: str) -> str:
    """スキームの無い Website 値に http:// を補う (大小文字は原データのまま)。"""
    site = (raw or "").strip()
    if not site or site.upper() in _EMPTY_VALUES:
        return ""
    if not re.match(r"^https?://", site, re.I):
        if "." not in site:
            return ""
        site = "http://" + site
    return site


class Streamreq18749Cadastur(StaticCrawler):
    """CADASTUR「Agência de Turismo」オープンデータ (ブラジル観光省) クローラー。"""

    # ファイルを 1 本落として全件展開する構成のため、アイテム単位の待機は不要。
    DELAY: float = 1.0
    ITEM_DELAY: float = 0.0
    TIMEOUT = 180

    EXTRA_COLUMNS = [
        "CNPJ",
        "商号(Nome Fantasia)",
        "国",
        "州(UF)",
        "都市(Município)",
        "TEL(国際表記)",
        "代表電話(国際表記)",
        "メールアドレス(代表)",
        "事業所区分",
        "法人形態",
        "企業規模",
        "CNAE",
        "活動状況",
        "登録カテゴリ",
        "観光セグメント",
        "従業員有無",
        "証明書番号",
        "証明書有効期限",
        "抽出条件",
        "データ提供期",
        "データ出典URL",
    ]

    # =========================================================================
    # メイン
    # =========================================================================
    def parse(self, url: str) -> Generator[dict, None, None]:
        """正規 URL を起点に最新四半期ファイルを取得し、絞り込み後の 1 件ずつを yield する。"""
        # 1. 正規 URL (sites.yml の url) の生存確認。SPA シェルのため中身は使わない。
        self._ping_canonical(url)

        # 2. 観光省 CKAN から最新リソースを解決してダウンロード
        resource = self._latest_resource()
        logger.info(
            "最新リソース: %s (%s) %s", resource["name"], resource["format"], resource["url"]
        )
        payload = self._download(resource["url"])

        # 3. 行を読みながら絞り込み → 即 yield
        rows = self._iter_rows(payload, resource["format"])
        try:
            header = next(rows)
        except StopIteration:
            logger.warning("データ行が 1 行もありません: %s", resource["url"])
            return
        index = {_ascii_upper(name): pos for pos, name in enumerate(header)}

        total = 0
        matched = 0
        for row in rows:
            total += 1
            record = self._build_record(row, index)
            if not record:
                continue
            reason = self._match_reason(record)
            if not reason:
                continue
            matched += 1
            yield self._to_item(url, record, reason, resource)

        logger.info("読み込み %d 件 / 抽出 %d 件", total, matched)

    # =========================================================================
    # 取得
    # =========================================================================
    def _ping_canonical(self, url: str) -> None:
        """起点 URL の到達確認。失敗しても CKAN 側の取得は続行する。"""
        try:
            response = self.session.get(url, timeout=self.TIMEOUT)
            logger.info("起点 URL 確認: %s (HTTP %s)", url, response.status_code)
        except Exception as e:  # noqa: BLE001 — 到達確認の失敗は致命的ではない
            logger.warning("起点 URL に到達できませんでした (処理は継続): %s — %s", url, e)

    def _latest_resource(self) -> dict:
        """package_show から最新 (last_modified 最大) のデータファイルを 1 本選ぶ。"""
        api = f"{_CKAN_ORIGIN}/api/3/action/package_show?id={_CKAN_DATASET}"
        payload = self._get_json(api)
        resources = payload.get("result", {}).get("resources", [])

        candidates = [
            r for r in resources
            if (r.get("format") or "").upper() in _DATA_FORMATS and r.get("url")
        ]
        if not candidates:
            raise RuntimeError(f"データファイルのリソースが見つかりません: {api}")

        def sort_key(r: dict) -> tuple:
            stamp = r.get("last_modified") or r.get("created") or ""
            return (stamp, r.get("position") or 0)

        latest = max(candidates, key=sort_key)
        return {
            "name": latest.get("name") or "",
            "format": (latest.get("format") or "").upper(),
            "url": latest["url"],
        }

    def _get_json(self, api_url: str) -> dict:
        """CKAN API を上限付きリトライで叩き、JSON を返す。"""
        for attempt in range(_MAX_ATTEMPTS):
            try:
                response = self.session.get(api_url, timeout=self.TIMEOUT)
                response.raise_for_status()
                return response.json()
            except Exception as e:  # noqa: BLE001 — 次の試行へ回す
                logger.warning("CKAN API 取得失敗 (%d/%d): %s — %s",
                               attempt + 1, _MAX_ATTEMPTS, api_url, e)
                if attempt < _MAX_ATTEMPTS - 1:
                    time.sleep(min(2 ** attempt, 8))
        raise RuntimeError(f"CKAN API を取得できませんでした: {api_url}")

    def _download(self, file_url: str) -> bytes:
        """データファイル本体を上限付きリトライでダウンロードする。"""
        for attempt in range(_MAX_ATTEMPTS):
            try:
                response = self.session.get(file_url, timeout=self.TIMEOUT)
                response.raise_for_status()
                logger.info("ダウンロード完了: %d bytes", len(response.content))
                return response.content
            except Exception as e:  # noqa: BLE001 — 次の試行へ回す
                logger.warning("ファイル取得失敗 (%d/%d): %s — %s",
                               attempt + 1, _MAX_ATTEMPTS, file_url, e)
                if attempt < _MAX_ATTEMPTS - 1:
                    time.sleep(min(2 ** attempt, 8))
        raise RuntimeError(f"データファイルを取得できませんでした: {file_url}")

    # =========================================================================
    # パース
    # =========================================================================
    def _iter_rows(self, payload: bytes, fmt: str) -> Generator[list, None, None]:
        """XLSX/XLS は calamine、CSV は csv モジュールで 1 行ずつ返す。"""
        if fmt == "CSV":
            text = payload.decode("utf-8-sig", errors="replace")
            for row in csv.reader(io.StringIO(text), delimiter=";" if text.count(";") > text.count(",") else ","):
                yield row
            return

        from python_calamine import CalamineWorkbook

        workbook = CalamineWorkbook.from_filelike(io.BytesIO(payload))
        sheet = workbook.get_sheet_by_index(0)
        yield from sheet.iter_rows()

    def _build_record(self, row: list, index: dict) -> dict | None:
        """列名で値を引ける dict に変換する。名称が取れない行は捨てる。"""
        def cell(*labels: str) -> str:
            for label in labels:
                pos = index.get(_ascii_upper(label))
                if pos is not None and pos < len(row):
                    value = _clean(row[pos])
                    if value:
                        return value
            return ""

        name = cell("Nome da Pessoa Jurídica") or cell("Nome Fantasia")
        if not name:
            return None

        return {
            "name": name,
            "fantasia": cell("Nome Fantasia"),
            "cnpj": cell("Número de Inscrição do CNPJ"),
            "uf": cell("UF").upper(),
            "city": cell("Município"),
            "addr": cell("Endereço Completo Comercial", "Endereço Completo Receita Federal"),
            "tel": cell("Telefone Comercial", "Telefone Institucional"),
            "tel_inst": cell("Telefone Institucional"),
            "email": cell("E-mail Comercial", "E-mail Institucional"),
            "email_inst": cell("E-mail Institucional"),
            "website": cell("Website"),
            "rep": cell("Nome do Responsável"),
            "opened": cell("Data de Abertura"),
            "atividade": cell("Atividade Turística"),
            "situacao": cell("Situação Cadastral"),
            "situacao_ativ": cell("Situação da Atividade"),
            "estab": cell("Tipo de Estabelecimento"),
            "natureza": cell("Natureza Jurídica"),
            "porte": cell("Porte"),
            "cnae": cell("CNAE(S) relacionados à atividade"),
            "categoria": cell("Categoria de Atuação"),
            "segmentos": cell("Segmentos Turísticos"),
            "empregado": cell("Possui Empregado?"),
            "cert_no": cell("Número do Certificado"),
            "cert_val": cell("Validade do Certificado"),
        }

    # =========================================================================
    # 絞り込み
    # =========================================================================
    def _match_reason(self, record: dict) -> str:
        """課題指示の事前縮小条件に合致するか判定し、合致理由のラベルを返す。"""
        haystack = " ".join((
            _ascii_upper(record["name"]),
            _ascii_upper(record["fantasia"]),
            _ascii_upper(record["website"]),
            _ascii_upper(record["email"]),
        ))
        if any(keyword in haystack for keyword in _JP_KEYWORDS):
            return "日本関連キーワード"

        city_key = (record["uf"], _ascii_upper(record["city"]))
        if city_key in _HUB_CITIES:
            return "日系コミュニティ集積地"
        if city_key in _METRO_CITIES and _normalize_website(record["website"]):
            return "主要都市(Website有)"
        return ""

    # =========================================================================
    # 出力
    # =========================================================================
    def _to_item(self, url: str, record: dict, reason: str, resource: dict) -> dict:
        """1 レコードを Schema + EXTRA の dict に組み立てる。"""
        tel_plain, tel_intl = _to_international_tel(record["tel"])
        _, tel_inst_intl = _to_international_tel(record["tel_inst"])

        return {
            Schema.URL: url,
            Schema.NAME: record["name"],
            Schema.ADDR: record["addr"],
            Schema.TEL: tel_plain,
            Schema.EMAIL: record["email"].lower(),
            Schema.HP: _normalize_website(record["website"]),
            Schema.REP_NM: record["rep"],
            Schema.OPEN_DATE: record["opened"],
            Schema.CAT_SITE: record["atividade"],
            Schema.STS_NM: record["situacao"],
            "CNPJ": record["cnpj"],
            "商号(Nome Fantasia)": record["fantasia"],
            "国": _COUNTRY,
            "州(UF)": record["uf"],
            "都市(Município)": record["city"],
            "TEL(国際表記)": tel_intl,
            "代表電話(国際表記)": tel_inst_intl,
            "メールアドレス(代表)": record["email_inst"].lower(),
            "事業所区分": record["estab"],
            "法人形態": record["natureza"],
            "企業規模": record["porte"],
            "CNAE": record["cnae"],
            "活動状況": record["situacao_ativ"],
            "登録カテゴリ": record["categoria"],
            "観光セグメント": record["segmentos"],
            "従業員有無": record["empregado"],
            "証明書番号": record["cert_no"],
            "証明書有効期限": record["cert_val"],
            "抽出条件": reason,
            "データ提供期": resource["name"],
            "データ出典URL": resource["url"],
        }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    scraper = Streamreq18749Cadastur()
    scraper.execute("https://dados.gov.br/dados/conjuntos-dados/cadastur-03")
