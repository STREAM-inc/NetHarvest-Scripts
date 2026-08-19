"""
札幌市オープンデータサイト「札幌市内の環境衛生営業施設一覧」クローラー
— 旅館 / クリーニング所 / 公衆浴場 / 興行場 / コインランドリー 編

取得対象:
    札幌市 CKAN (ckan.pf-sapporo.jp) が公開する「札幌市内の環境衛生営業施設一覧」
    データセット (全 7 CSV リソース) のうち、**理容所・美容所を除く 5 業態** を取得する。
        - 旅館 (ホテル・旅館等)
        - クリーニング所
        - 公衆浴場 (普通浴場・サウナ等)
        - 興行場 (映画館・演劇場等)
        - コインランドリー
    ※ 理容所・美容所は既存クローラー scripts/sites/government/ckan.py が担当しており、
      重複取得を避けるため本クローラーでは除外する (千葉県 opendata / opendata_2 と同じ分担方式)。

取得フロー:
    1. 起点 URL (= sites.yml の url = データセットページ) の末尾からデータセット slug を取り出し、
       同一オリジンの CKAN package_show API (/api/3/action/package_show?id={slug}) を GET する。
       → リソース名にファイル年月が入る (例「令和8年（2026年）6月末現在」) ため、
         CSV URL をハードコードせず毎回 API から解決する。
    2. resources から format=CSV かつリソース名が対象 5 業態のものだけを抽出する。
    3. 各 CSV (UTF-8 BOM / カンマ区切り) をストリーミング取得し、1 行 (= 1 施設) ずつ即 yield する
       (全件バッファしない → 最初の 1 件が数秒以内に yield される)。

CSV の構造 (2026-08 時点で全 5 リソース共通):
    業種区分, 施設名称, 都道府県, 施設所在地, 施設ﾋﾞﾙ名, 施設TEL,
    開設者名, 開設者住所, 開設者ﾋﾞﾙ名, 開設者TEL, [客室数, 定員 ← 旅館のみ], 許可年月日, 前月許可
    - 客室数 / 定員 は旅館リソースにのみ存在する (他業態では空文字)。
    - 施設TEL / 開設者名 等は未登録 (空欄) の行がある。これは出典データ側の欠落。

件数 (2026-08-19 時点、ヘッダ除く):
    旅館 645 / クリーニング所 749 / 公衆浴場 280 / 興行場 53 / コインランドリー 256 = 計 1,983 件

ライセンス:
    クリエイティブ・コモンズ 表示 4.0 国際 (CC BY 4.0)
    https://creativecommons.org/licenses/by/4.0/deed.ja
    → 出典表示のうえ自由に再利用可能。スクレイピング/機械取得を禁じる条項は無い。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ckan_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ckan_2
"""

import csv
import io
import json
import logging
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

# 対象業態: リソース名に含まれるキーワード → 出力用の業態ラベル
# (理容所・美容所は既存 site_id: ckan が担当するため含めない)
_TARGET_GENRES = {
    "旅館": "旅館",
    "クリーニング": "クリーニング所",
    "公衆浴場": "公衆浴場",
    "興行場": "興行場",
    "コインランドリー": "コインランドリー",
}

# CSV ヘッダ名 → EXTRA_COLUMNS のカラム名
# (いずれも構造化された短い値: 名称 / 住所 / TEL / 数値 / 日付。自由記述プロースは無い)
_EXTRA_MAP = {
    "施設ﾋﾞﾙ名": "施設ビル名",
    "開設者住所": "開設者住所",
    "開設者ﾋﾞﾙ名": "開設者ビル名",
    "開設者TEL": "開設者TEL",
    "客室数": "客室数",
    "定員": "定員",
    "許可年月日": "許可年月日",
    "前月許可": "前月許可",
}

_GENRE_COLUMN = "業態"  # リソース単位の業態ラベル (業種区分より粒度が粗い上位分類)


class Ckan2(StaticCrawler):
    """札幌市 CKAN 環境衛生営業施設一覧 (旅館・クリーニング所・公衆浴場・興行場・コインランドリー)"""

    DELAY = 0.0  # per-yield sleep は数千行に対して重すぎるため 0。負荷配慮は CSV 間の sleep で行う
    CONTINUE_ON_ERROR = True
    TIMEOUT = 90  # CSV ストリーミング開始待ちに余裕を持たせる

    EXTRA_COLUMNS = [_GENRE_COLUMN] + list(dict.fromkeys(_EXTRA_MAP.values()))

    _RESOURCE_SLEEP = 1.0  # リソース (CSV) 切り替え時のインターバル秒

    @staticmethod
    def _txt(value) -> str:
        """セル値を安全に文字列化 (前後空白除去・全角空白を半角化)。"""
        if value is None:
            return ""
        s = str(value).replace("　", " ").strip()
        return "" if s.lower() == "nan" else s

    @classmethod
    def _genre_of(cls, resource_name: str) -> str | None:
        """リソース名から対象業態ラベルを判定する (対象外なら None)。"""
        for keyword, label in _TARGET_GENRES.items():
            if keyword in resource_name:
                return label
        return None

    def _fetch_resources(self, url: str) -> list[tuple[str, str, str]]:
        """起点 URL から CKAN package_show API を叩き、対象 CSV リソースを列挙する。

        Returns:
            [(業態ラベル, リソース名, CSV URL), ...]
        """
        slug = urlparse(url).path.rstrip("/").split("/")[-1]
        api_url = urljoin(url, f"/api/3/action/package_show?id={slug}")
        logger.info("CKAN API を取得: %s", api_url)

        # session.get はテストランナー / smoke_test のソフトタイムアウト対象 (get_soup と同経路)
        resp = self.session.get(api_url, timeout=self.TIMEOUT)
        resp.raise_for_status()
        payload = json.loads(resp.text)
        if not payload.get("success"):
            raise RuntimeError(f"CKAN API が success=false を返しました: {api_url}")

        resources = payload.get("result", {}).get("resources", [])
        targets: list[tuple[str, str, str]] = []
        for res in resources:
            name = self._txt(res.get("name"))
            fmt = self._txt(res.get("format")).upper()
            csv_url = self._txt(res.get("url"))
            if fmt != "CSV" or not csv_url:
                continue
            genre = self._genre_of(name)
            if genre is None:
                continue  # 理容所・美容所 (site_id: ckan が担当) 等は除外
            targets.append((genre, name, csv_url))

        logger.info(
            "対象リソース: %d 件 / 全 %d 件 (理容所・美容所は除外)", len(targets), len(resources)
        )
        if not targets:
            raise RuntimeError(f"対象業態の CSV リソースが見つかりませんでした: {api_url}")
        return targets

    def parse(self, url: str):
        """CKAN API でリソースを解決し、CSV を 1 行ずつ即 yield する。"""
        targets = self._fetch_resources(url)
        self.total_items = None  # 総件数は CSV を読み終えるまで確定しないため未設定

        for index, (genre, name, csv_url) in enumerate(targets):
            if index:
                time.sleep(self._RESOURCE_SLEEP)
            logger.info("CSV を取得 [%s]: %s (%s)", genre, name, csv_url)
            try:
                yield from self._stream_csv(genre, csv_url)
            except Exception as e:  # noqa: BLE001 — 1 リソース失敗でも他業態は継続する
                self.error_count += 1
                logger.warning("CSV の取得/解析に失敗 (スキップ): %s — %s", csv_url, e)
                continue

    def _stream_csv(self, genre: str, csv_url: str):
        """CSV をストリーミング取得し、1 行ずつ Schema にマッピングして即 yield する。"""
        # stream=True でヘッダ受信後すぐ制御が返るため、最初の 1 行を数秒以内に yield できる
        resp = self.session.get(csv_url, stream=True, timeout=self.TIMEOUT)
        resp.raise_for_status()
        resp.raw.decode_content = True

        # BOM 除去 + 引用符内改行の正しい処理のため TextIOWrapper 経由で csv.reader に渡す
        text_stream = io.TextIOWrapper(resp.raw, encoding="utf-8-sig", errors="replace", newline="")
        reader = csv.reader(text_stream)

        try:
            header = [self._txt(h) for h in next(reader)]
        except StopIteration:
            logger.warning("CSV が空です: %s", csv_url)
            return
        col = {h: i for i, h in enumerate(header)}

        def cell(row: list[str], name: str) -> str:
            i = col.get(name)
            return self._txt(row[i]) if (i is not None and i < len(row)) else ""

        for row in reader:
            if not any(c.strip() for c in row):
                continue  # 空行スキップ
            name = cell(row, "施設名称")
            if not name:
                continue  # 施設名称が無い行 (末尾の注記行等) はレコードとして扱わない

            item = {
                Schema.NAME: name,
                Schema.PREF: cell(row, "都道府県"),
                Schema.ADDR: cell(row, "施設所在地"),
                Schema.TEL: cell(row, "施設TEL"),
                Schema.REP_NM: cell(row, "開設者名"),
                Schema.CAT_SITE: cell(row, "業種区分"),
                Schema.OPEN_DATE: cell(row, "許可年月日"),
                Schema.URL: csv_url,
                _GENRE_COLUMN: genre,
            }
            for csv_name, out_name in _EXTRA_MAP.items():
                item[out_name] = cell(row, csv_name)
            yield item


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ckan2()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    parse() は引数 url からデータセット slug を取り出し、同一オリジンの CKAN API / CSV を導出する。
    scraper.execute("https://ckan.pf-sapporo.jp/dataset/sapporo_environmental_hygiene_services")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
