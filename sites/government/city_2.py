"""
川崎市 理容所・美容所一覧 (環境衛生関係営業施設一覧 オープンデータ) — クローラー

取得対象:
    川崎市が公表する「環境衛生関係営業に関する情報 (オープンデータ)」ページのうち、
    指示 (備考) に従い **理容所** と **美容所** の施設一覧 CSV のみを取得する。
    クリーニング所・公衆浴場・旅館業・興行場など他業態の CSV は対象外 (ダウンロードしない)。

取得フロー:
    1. 起点 URL (sites.yml の url = オープンデータ一覧ページ) を GET し、
       アンカーのうちリンク文言に「理容所」「美容所」を含む .csv リンクだけを抽出する
       (他業態の .csv は文言で除外)。ファイル名には公表年月が埋め込まれ年次更新で
       変わるため、ファイル名を決め打ちせずページ上のリンクから毎回導出する。
    2. 各 CSV を url からの相対で urljoin して取得。川崎市 CSV は UTF-8 (BOM 付き) /
       カンマ区切り。理容所/美容所の区別はダウンロード元リンク文言から付与する。
    3. 各 CSV をヘッダ名で照合し、1 行 (= 1 施設) ずつ即 yield する (全件バッファしない)。

備考 (呼び出し指示への対応):
    - 対象は理容所・美容所のみ。リンク文言で他業態 (クリーニング所等) を除外。
    - 施設名称・住所・電話番号を抽出。すべて構造化された短い値のみを採用し、
      自由記述の文章カラムは元データに無い (EXTRA も短いラベル/コード/数値のみ)。
    - 電話番号は元データの掲載値をそのまま採用する (掲載が無い行は空文字)。
      市外局番の推測付与はデータ捏造になるため行わない。
    - 都道府県は全件川崎市 (= 神奈川県) のため PREF を「神奈川県」固定とする。
      施設所在地は行政区始まり (例: 川崎区…) のため住所には「川崎市」を前置して補完する。
    - 利用規約: 本ページは川崎市が「オープンデータ」として公表しており、川崎市ウェブサイトの
      利用規約はスクレイピング/クローリングを明示的に禁止していない (二次利用可) ため取得を継続。

実行方法:
    # ローカルテスト
    python scripts/sites/government/city_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id city_2
"""

import csv
import io
import logging
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 対象業態 (これ以外の業種のリンクは除外)
_TARGET_KEYWORDS = ("理容所", "美容所")

# CSV ヘッダ名 → EXTRA_COLUMNS 名 (構造化された短い値のみ。自由記述は無し)
_EXTRA_MAP = {
    "開設者名": "開設者名",
    "開設者住所": "開設者住所",
    "確認日": "確認日",
    "確認番号": "確認番号",
    "（構造設備の概要）営業所面積（㎡）": "営業所面積(㎡)",
}


class City2(StaticCrawler):
    """川崎市 理容所・美容所一覧 スクレイパー"""

    DELAY = 1.0
    CONTINUE_ON_ERROR = True
    TIMEOUT = 90

    EXTRA_COLUMNS = list(dict.fromkeys(_EXTRA_MAP.values()))

    @staticmethod
    def _txt(value) -> str:
        """セル値を安全に文字列化 (前後空白除去・全角空白正規化)。"""
        if value is None:
            return ""
        s = str(value).replace("　", " ").strip()
        return "" if s.lower() == "nan" else s

    def _iter_csv_links(self, url: str):
        """起点ページから理容所・美容所の .csv リンクを (csv_url, 業種) で列挙する。"""
        soup = self.get_soup(url)
        if soup is None:
            logger.warning("起点ページを取得できませんでした: %s", url)
            return
        seen = set()
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if ".csv" not in href.lower():
                continue
            text = a.get_text(strip=True)
            gyoushu = next((k for k in _TARGET_KEYWORDS if k in text), None)
            if gyoushu is None:
                continue
            # 引数 url を唯一のルートとし、同一サイト上の絶対 URL を urljoin で導出
            csv_url = urljoin(url, href)
            if csv_url not in seen:
                seen.add(csv_url)
                yield csv_url, gyoushu

    def parse(self, url: str):
        links = list(self._iter_csv_links(url))
        self.total_items = None  # 総件数は事前に不明 (CSV 展開後に判明)
        logger.info("対象 CSV: %d 件 (%s)", len(links), [u for u, _ in links])

        for csv_url, gyoushu in links:
            try:
                yield from self._stream_csv(csv_url, gyoushu)
            except Exception as e:  # noqa: BLE001 — 1 CSV 失敗でも他は継続
                self.error_count += 1
                logger.warning("CSV の取得/解析に失敗 (スキップ): %s — %s", csv_url, e)
                continue

    def _stream_csv(self, csv_url: str, gyoushu: str):
        """CSV をダウンロードし、1 行 (= 1 施設) ずつ即 yield する。"""
        # session.get はテストランナー / smoke_test のソフトタイムアウト対象 (get_soup と同経路)
        resp = self.session.get(csv_url, timeout=self.TIMEOUT)
        resp.raise_for_status()
        # 川崎市 CSV は UTF-8 (BOM 付き) / カンマ区切り
        text = resp.content.decode("utf-8-sig")

        reader = csv.reader(io.StringIO(text))
        try:
            header = [self._txt(h) for h in next(reader)]
        except StopIteration:
            return
        col = {name: i for i, name in enumerate(header)}

        def cell(row, name):
            i = col.get(name)
            return self._txt(row[i]) if (i is not None and i < len(row)) else ""

        for row in reader:
            if not any(c.strip() for c in row):
                continue

            name = cell(row, "施設名称")
            if not name:
                continue

            # 住所は行政区始まり (川崎区…) のため「川崎市」を前置して補完。方書があれば付与。
            addr = cell(row, "施設所在地")
            hosho = cell(row, "施設方書")
            if addr:
                addr = f"川崎市{addr}"
                if hosho:
                    addr = f"{addr} {hosho}"

            item = {
                Schema.NAME: name,
                Schema.PREF: "神奈川県",
                Schema.ADDR: addr,
                Schema.TEL: cell(row, "施設電話番号"),
                Schema.REP_NM: cell(row, "代表者名"),
                Schema.CAT_SITE: gyoushu,
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

    scraper = City2()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    parse() は引数 url を起点にページ内の .csv リンクを抽出し、urljoin で CSV URL を導出する。
    scraper.execute("https://www.city.kawasaki.jp/350/page/0000120745.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
