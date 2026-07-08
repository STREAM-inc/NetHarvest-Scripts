"""
食品関係営業許可 (食品衛生申請等システム / 厚生労働省 i2fas) — 営業許可・届出情報オープンデータ クローラー

取得対象:
    厚生労働省「食品衛生申請等システム」が公表する、前月末までの
    食品等事業者の営業許可・届出情報 (オープンデータ) を全自治体分横断取得する。
    自治体 (都道府県・保健所設置市・特別区) 単位で 1 CSV が公開されており、
    起点ページに列挙された 157 自治体の CSV をすべてダウンロードする。

取得フロー:
    1. 起点 URL (sites.yml の url = オープンデータダウンロードページ) を GET し、
       ページ内の `actionlink_a_n{自治体コード5桁}` を抽出して自治体コード一覧を得る。
    2. 各自治体コードから CSV の絶対 URL を url からの相対で導出:
         {origin}/faspub/page/opendatadownload.jsp?param={code}_food_business_all.csv
    3. CSV (UTF-8 BOM / カンマ区切り / ダブルクォート) をストリーミング取得し、
       1 行 (= 1 施設) ずつ即 yield する (全件バッファしない)。

備考 (呼び出し指示への対応):
    - 各 CSV の列は指示どおり 25 列 (自治体コード〜備考)。すべてヘッダ名で照合する。
    - 「許可条件」「備考」は最大 200 字超の自由記述になり得るため、著作権リスクを避けて
      EXTRA_COLUMNS から除外している (明示的な取得許可が無いため)。
    - フィルタ指示 (地域限定・期間限定 等) は無いため全自治体・全行を取得する。
    - 並列ワーカーは使わず、自治体 CSV を逐次ダウンロードする (行政サーバへの負荷配慮)。
    - 利用規約 (/termsofuse.htm) を確認済み。当該規約はシステム利用者・利用行政庁向けで、
      公表オープンデータの取得を明示的に禁止していないため取得を継続する。

実行方法:
    # ローカルテスト
    python scripts/sites/government/i2fas.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id i2fas
"""

import csv
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

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 起点ページ内の各自治体ダウンロードリンク: name="actionlink_a_n01000" 等 (5 桁の自治体コード)
_CODE_RE = re.compile(r"actionlink_a_n(\d{5})")

# CSV ヘッダ名 → EXTRA_COLUMNS のカラム名 (構造化された短い値のみ。自由記述は除外)
_EXTRA_MAP = {
    "自治体コード": "自治体コード",
    "行番号": "行番号",
    "市区町村名": "市区町村名",
    "業態": "業態",
    "営業施設方書": "営業施設方書",
    "緯度": "緯度",
    "経度": "経度",
    "法人名": "法人名",
    "法人住所": "法人住所",
    "許可番号": "許可番号",
    "初回許可年月日": "初回許可年月日",
    "許可年月日": "許可年月日",
    "許可開始日": "許可開始日",
    "許可満了日": "許可満了日",
    "廃業年月日": "廃業年月日",
    "申請区分": "申請区分",
    # 「許可条件」「備考」は自由記述 (最大 200 字超) のため著作権リスク回避で意図的に除外
}


class I2fas(StaticCrawler):
    """食品衛生申請等システム 営業許可・届出情報オープンデータ スクレイパー"""

    # per-yield sleep は行数 (数百万) に対して致命的なので 0。負荷配慮は自治体 CSV 間の sleep で行う。
    DELAY = 0.0
    CONTINUE_ON_ERROR = True
    TIMEOUT = 90  # 大きい自治体 (北海道 ~16MB) のストリーミング開始待ち用に余裕を持たせる

    # 自治体 CSV を 1 本ダウンロードするごとに空ける間隔 (秒)。逐次取得・負荷配慮。
    FILE_DELAY = 1.0

    EXTRA_COLUMNS = list(dict.fromkeys(_EXTRA_MAP.values()))

    @staticmethod
    def _txt(value) -> str:
        """セル値を安全に文字列化 (前後空白除去・全角空白正規化)。"""
        if value is None:
            return ""
        s = str(value).replace("　", " ").strip()
        return "" if s.lower() == "nan" else s

    def _iter_codes(self, url: str):
        """起点ページから自治体コード (5 桁) を出現順・重複除去で列挙する。"""
        soup = self.get_soup(url)
        if soup is None:
            logger.warning("起点ページを取得できませんでした: %s", url)
            return
        html = str(soup)
        seen = set()
        for code in _CODE_RE.findall(html):
            if code not in seen:
                seen.add(code)
                yield code

    def parse(self, url: str):
        codes = list(self._iter_codes(url))
        self.total_items = None  # 総件数は事前に不明 (自治体数のみ判明)
        logger.info("対象自治体 CSV: %d 件", len(codes))

        for idx, code in enumerate(codes):
            # 引数 url を唯一のルートとし、同一オリジン上の CSV 絶対 URL を urljoin で導出
            csv_url = urljoin(url, f"/faspub/page/opendatadownload.jsp?param={code}_food_business_all.csv")
            if idx > 0 and self.FILE_DELAY > 0:
                time.sleep(self.FILE_DELAY)
            try:
                yield from self._stream_csv(csv_url)
            except Exception as e:  # noqa: BLE001 — 1 自治体失敗でも他は継続
                self.error_count += 1
                logger.warning("自治体 CSV の取得/解析に失敗 (スキップ): %s — %s", csv_url, e)
                continue

    def _stream_csv(self, csv_url: str):
        """自治体 CSV をストリーミング取得し、1 行ずつマッピングして即 yield する。"""
        # session.get はテストランナー / smoke_test のソフトタイムアウト対象 (get_soup と同経路)。
        # stream=True でヘッダ受信後すぐ制御が返り、最初の 1 行を速やかに yield できる。
        resp = self.session.get(csv_url, stream=True, timeout=self.TIMEOUT)
        resp.raise_for_status()
        resp.raw.decode_content = True  # gzip 等が来ても生ストリームで展開する

        # BOM 除去 + 引用符内の改行を正しく扱うため TextIOWrapper 経由で csv.reader に渡す
        text_stream = io.TextIOWrapper(resp.raw, encoding="utf-8-sig", errors="replace", newline="")
        reader = csv.reader(text_stream)

        try:
            header = next(reader)
        except StopIteration:
            return
        header = [self._txt(h) for h in header]
        col = {name: i for i, name in enumerate(header)}

        def cell(row, name):
            i = col.get(name)
            return self._txt(row[i]) if (i is not None and i < len(row)) else ""

        for row in reader:
            if not any(c.strip() for c in row):
                continue
            item = {
                Schema.NAME: cell(row, "営業施設名称、屋号又は商号"),
                Schema.NAME_KANA: cell(row, "営業施設名称、屋号又は商号（フリガナ）"),
                Schema.PREF: cell(row, "都道府県名"),
                Schema.ADDR: cell(row, "営業施設所在地"),
                Schema.TEL: cell(row, "営業施設電話番号"),
                Schema.CO_NUM: cell(row, "法人番号"),
                Schema.CAT_SITE: cell(row, "営業の種類"),
                Schema.URL: csv_url,
            }
            for csv_name, out_name in _EXTRA_MAP.items():
                item[out_name] = cell(row, csv_name)
            yield item


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = I2fas()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    parse() は引数 url を起点に自治体コードを抽出し、同一オリジンの CSV URL を urljoin で導出する。
    scraper.execute(
        "https://i2fas.mhlw.go.jp/faspub/IO_S010303.do?_errCheck=false&_searched=false"
        "&_sessionId=16CF4AE107F25ABF9C0720C37B29B24E&method=a_menu_o01Action&param="
        "&_focus=actionlink_a_menu_o01&_posx=0&_posy=0&_rowidx=0&_language="
        "&_timezoneOffset=-540&_timezoneId=Asia%2FTokyo&_downloadId=&_status="
        "&_labelMapArchive=&_wfinfo=&_wfinfo_RefParams="
        "&_ActionHistoryList%5B0%5D.action=%2FIO_S010303"
    )

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
