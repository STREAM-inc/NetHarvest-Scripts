"""
【STREAMREQ-18751】RNT オープンデータ (コロンビア) — streamreq_18751rnt

取得対象:
    コロンビア商工観光省 (MinCIT / Ministerio de Comercio, Industria y Turismo) が
    政府オープンデータポータル datos.gov.co (Socrata 基盤) で公開している
    「Registro Nacional de Turismo - RNT」(観光事業者国家登録簿) データセット
    (リソース ID: thwd-ivmp)。

    データセット全体は 679,548 行あるが、これは 1 事業者 × 1 年 (ano) の
    年次スナップショットが積み上がったものである (2019〜2026)。

取得フロー (SODA API / SoQL):
    1. 引数 url (= sites.yml の url = SODA JSON エンドポイント) を唯一の起点にする。
       すべてのクエリはこの url にクエリパラメータを付けて発行する。
    2. `$select=max(ano)` で最新スナップショット年を動的に取得する
       (年をハードコードすると翌年の更新で 0 件になるため)。
    3. 絞り込み条件 (下記「母集団の事前縮小」) を $where に組み立て、
       `$order=codigo_rnt,municipio` + `$limit=50` + `$offset=N` でページングする。
    4. 1 行取得するごとに即 yield する (全件バッファしない / 最初の 1 件は数秒で出る)。
       返却件数が $limit 未満になったページで終了する。

母集団の事前縮小 (依頼備考の指示):
    「ACTIVO かつ categoria = AGENCIAS DE VIAJES」は 107,404 行 (実測) あり、
    後続の⑬HP巡回の対象としては大きすぎる。備考の指示に従い、以下で
    数千件規模に事前縮小する (実測 5,497 件 / 最新年 2026 時点):

        estado_rnt = "ACTIVO"                      … 登録が有効なもののみ
        categoria  = "AGENCIAS DE VIAJES"          … 旅行代理店区分のみ
        ano        = max(ano)                      … 最新スナップショット年のみ
                                                     (= 現存登録。年跨ぎ重複も解消される)
        かつ (主要都市 in (ボゴタ / メデジン / カリ)
              または 会社名キーワード (JAPON/JAPAN/TOKIO/NIPPON/ORIENTE 等) に合致)

    備考では「会社名キーワード *または* 主要都市」とあるため、両方を OR で束ねて
    主要都市以外に所在する日本・東洋関連事業者を取りこぼさないようにしている。
    条件は下記のクラス定数 (_ACTIVE_CATEGORIA / _MAJOR_MUNICIPIOS / _NAME_KEYWORDS)
    を書き換えるだけで調整できる。

データセットのカラム (全 14 列。これが公開されている全項目):
    codigo_rnt / estado_rnt / razon_social_establecimiento / departamento / cod_dpto /
    municipio / cod_mun / nit / categoria / sub_categoria / habitaciones / camas /
    num_emp1 / ano

備考 (依頼指示の反映):
    - 住所・TEL・メールアドレス・URL のカラムは本データセットに存在しない。
      したがって Schema.ADDR / TEL / EMAIL / HP は出力しない (後工程の⑬HP巡回で補完する)。
      TEL の +57 国際表記への正規化も、TEL を取得する⑬HP巡回側の工程で行う。
    - 国カラムは指示どおり「コロンビア」固定。
    - コロンビアの事業者のため Schema.PREF (日本の都道府県) は使わず、
      departamento (州) / municipio (都市) は EXTRA カラムに入れる。
    - 「区分」は Schema.CAT_SITE (サイト定義業種) に categoria を、
      EXTRA「小区分」に sub_categoria を入れる。
    - habitaciones / camas (客室数・ベッド数) は宿泊業向けの項目で、
      AGENCIAS DE VIAJES では常に 0 のため取得しない。
    - 自由記述 (プロース) のカラムはデータセットに一切存在しないため、
      著作権リスクによる除外対象は無い。
    - STX 名寄せは海外事業者のため不可 (依頼備考どおり名寄せ工程は設けない)。

利用規約 / ライセンス:
    データセットのメタデータ上のライセンスは
    "Creative Commons Attribution | Share Alike 4.0 International" (CC BY-SA 4.0)。
    コロンビア政府公式オープンデータポータルによる公式公開で、
    スクレイピング / 機械的取得を禁止する条項は無い。SODA API は公開 API であり
    アプリケーショントークン無しでもアクセス可能 (レート制限のみ)。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/streamreq_18751rnt.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id streamreq_18751rnt
"""

import logging
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urlencode

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import requests

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)


class Streamreq18751Rnt(StaticCrawler):
    """コロンビア RNT (観光事業者国家登録簿) オープンデータ スクレイパー"""

    # SODA API は JSON を返すだけなのでブラウザ UA は不要だが、
    # 既定 UA のままで 200 が返ることを確認済み。
    TIMEOUT = 30
    DELAY = 0.0          # 待機はページ (API) 取得側に寄せる
    ITEM_DELAY = 0       # 1 リクエストで最大 50 件 yield するため item 単位の待機は無効化
    CONTINUE_ON_ERROR = True

    # --- 絞り込み条件 (依頼備考「母集団の事前縮小」) -------------------------
    _ACTIVE_ESTADO = "ACTIVO"
    _ACTIVE_CATEGORIA = "AGENCIAS DE VIAJES"

    # 主要都市 (municipio の実データ表記)
    _MAJOR_MUNICIPIOS = (
        "BOGOTA D.C.",   # ボゴタ
        "MEDELLIN",      # メデジン
        "CALI",          # カリ
    )

    # 会社名キーワード (razon_social_establecimiento を大文字化して部分一致)
    # ※ スペイン語のアクセント付き綴り (JAPÓN) は upper() でも保持されるため別途列挙する
    _NAME_KEYWORDS = (
        "JAPON", "JAPÓN", "JAPAN", "TOKIO", "TOKYO", "NIPPON", "NIHON",
        "OSAKA", "KYOTO", "SAKURA", "SAMURAI", "ASIA", "ORIENTE", "ORIENTAL",
    )

    # --- ページング ---------------------------------------------------------
    PAGE_SIZE = 50        # read timeout を避けるため小さめ (SODA の既定上限は 50000)
    MAX_PAGES = 400       # 安全弁 (50 × 400 = 20,000 件)
    MAX_ATTEMPTS = 3      # 1 リクエストあたりのリトライ上限
    REQUEST_INTERVAL = 0.3  # API への連続アクセス間隔 (秒)

    # 国は依頼指示により固定値
    COUNTRY = "コロンビア"

    EXTRA_COLUMNS = [
        "国",
        "州",
        "都市",
        "NIT",
        "RNT登録番号",
        "登録状態",
        "小区分",
        "データ年",
    ]

    # ------------------------------------------------------------------ utils
    def _get_json(self, url: str, params: dict) -> list:
        """SODA API を叩いて JSON (list) を返す。上限付きリトライ、全滅で raise。"""
        last_error: Exception | None = None
        for attempt in range(self.MAX_ATTEMPTS):
            try:
                response = self.session.get(url, params=params, timeout=self.TIMEOUT)
                response.raise_for_status()
                data = response.json()
                if not isinstance(data, list):
                    # SoQL エラーは dict {"message": ..., "errorCode": ...} で返る
                    raise ValueError(f"SODA API エラー応答: {data}")
                return data
            except (requests.exceptions.RequestException, ValueError) as e:
                last_error = e
                logger.warning(
                    "API 取得失敗 (%d/%d): %s — %s",
                    attempt + 1, self.MAX_ATTEMPTS, params.get("$offset", ""), e,
                )
                if attempt < self.MAX_ATTEMPTS - 1:
                    time.sleep(min(2 ** attempt, 8))
        raise RuntimeError(f"SODA API の取得に {self.MAX_ATTEMPTS} 回失敗しました: {last_error}")

    def _latest_ano(self, url: str) -> str:
        """データセット内の最新スナップショット年を取得する。"""
        rows = self._get_json(url, {"$select": "max(ano)"})
        ano = (rows[0].get("max_ano") if rows else "") or ""
        if not ano:
            raise RuntimeError("最新年 (max(ano)) を取得できませんでした")
        logger.info("最新スナップショット年: %s", ano)
        return str(ano)

    def _build_where(self, ano: str) -> str:
        """依頼備考の事前縮小条件を SoQL の $where 文字列に組み立てる。"""
        municipios = ",".join(f'"{m}"' for m in self._MAJOR_MUNICIPIOS)
        keywords = " OR ".join(
            f'upper(razon_social_establecimiento) like "%{kw}%"'
            for kw in self._NAME_KEYWORDS
        )
        return (
            f'estado_rnt="{self._ACTIVE_ESTADO}" '
            f'AND categoria="{self._ACTIVE_CATEGORIA}" '
            f'AND ano="{ano}" '
            f'AND (municipio in({municipios}) OR ({keywords}))'
        )

    def _count(self, url: str, where: str) -> int:
        """絞り込み後の総件数 (課題コメント用の実測値) を取得する。"""
        try:
            rows = self._get_json(url, {"$select": "count(*)", "$where": where})
            return int(rows[0].get("count", 0)) if rows else 0
        except RuntimeError as e:
            logger.warning("件数の取得に失敗 (続行): %s", e)
            return 0

    def _source_url(self, url: str, row: dict) -> str:
        """1 レコードを再取得できる取得元 URL を組み立てる (url からの派生のみ)。"""
        query = urlencode({
            "codigo_rnt": row.get("codigo_rnt", ""),
            "ano": row.get("ano", ""),
        })
        return f"{url}?{query}"

    # ------------------------------------------------------------------ parse
    def parse(self, url: str) -> Generator[dict, None, None]:
        """SODA API をページングし、1 レコードずつ即 yield する。"""
        ano = self._latest_ano(url)
        where = self._build_where(ano)

        total = self._count(url, where)
        if total:
            self.total_items = total
            logger.info("事前縮小後の対象件数: %s 件 (年=%s)", total, ano)

        seen: set[str] = set()
        for page in range(self.MAX_PAGES):
            params = {
                "$where": where,
                # ページング中に順序が揺れないよう安定ソートキーを指定する
                "$order": "codigo_rnt,municipio",
                "$limit": self.PAGE_SIZE,
                "$offset": page * self.PAGE_SIZE,
            }
            rows = self._get_json(url, params)
            if not rows:
                logger.info("空ページに到達したため終了します (offset=%d)", page * self.PAGE_SIZE)
                break

            for row in rows:
                name = (row.get("razon_social_establecimiento") or "").strip()
                if not name:
                    continue

                # 同一年の重複は無い想定だが、念のため RNT 登録番号で一意化する
                codigo = (row.get("codigo_rnt") or "").strip()
                key = codigo or name
                if key in seen:
                    continue
                seen.add(key)

                yield {
                    Schema.URL: self._source_url(url, row),
                    Schema.NAME: name,
                    # 従業員数 (num_emp1) はデータセットに含まれる数少ない属性値
                    Schema.EMP_NUM: (row.get("num_emp1") or "").strip(),
                    # 「区分」= categoria (サイト定義業種)
                    Schema.CAT_SITE: (row.get("categoria") or "").strip(),
                    "国": self.COUNTRY,
                    "州": (row.get("departamento") or "").strip(),
                    "都市": (row.get("municipio") or "").strip(),
                    "NIT": (row.get("nit") or "").strip(),
                    "RNT登録番号": codigo,
                    "登録状態": (row.get("estado_rnt") or "").strip(),
                    "小区分": (row.get("sub_categoria") or "").strip(),
                    "データ年": (row.get("ano") or "").strip(),
                }

            if len(rows) < self.PAGE_SIZE:
                logger.info("最終ページに到達しました (取得 %d 件)", len(rows))
                break

            time.sleep(self.REQUEST_INTERVAL)
        else:
            logger.warning("MAX_PAGES (%d) に到達したため打ち切りました", self.MAX_PAGES)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    scraper = Streamreq18751Rnt()
    scraper.execute("https://www.datos.gov.co/resource/thwd-ivmp.json")
