"""
横浜市 理容所・美容所施設一覧 (環境衛生関係営業施設一覧 オープンデータ) — クローラー

取得対象:
    横浜市が公表する「環境衛生関係営業施設一覧」オープンデータのうち、
    指示 (備考) に従い **理容所** と **美容所** の施設一覧のみを取得する。
    クリーニング所・旅館業・公衆浴場・興行場など他業態は対象外 (ダウンロードしない)。

取得フロー:
    1. 起点 URL (sites.yml の url = オープンデータ一覧ページ) を GET し、
       アンカーのうちリンク文言に「理容所」「美容所」を含む .zip リンクだけを抽出する
       (他業態の .zip は文言で除外)。ファイル名には公表年月日が埋め込まれ年次更新で
       変わるため、ファイル名を決め打ちせずページ上のリンクから毎回導出する。
    2. 各 ZIP を url からの相対で urljoin して取得。ZIP 内は行政区ごとの CSV
       (UTF-16 / タブ区切り / 先頭に BOM) が複数格納されている。
    3. 各 CSV をヘッダ名で照合し、1 行 (= 1 施設) ずつ即 yield する (全件バッファしない)。

備考 (呼び出し指示への対応):
    - 対象は理容所・美容所のみ。業種列 (業種) が「理容所」「美容所」以外の行は念のため除外。
    - 施設名称・住所・電話番号を抽出。すべて構造化された短い値のみを採用し、
      自由記述の文章カラムは無い (著作権リスク回避のため EXTRA も短いラベル/コードのみ)。
    - 電話番号は元データ (Excel 由来) の数字列をそのまま採用する。7 桁 (市外局番 045 省略の
      横浜市内番号) や、先頭 0 が欠落した携帯番号等が混在するが、市外局番の推測付与は
      データ捏造になるため行わず、掲載値をそのまま出力する。
    - 都道府県は全件横浜市 (= 神奈川県) のため PREF を「神奈川県」固定とする。
    - 利用規約: 本ページは横浜市が「オープンデータ」として公表しており、横浜市ウェブサイトの
      利用規約はスクレイピング/クローリングを明示的に禁止していない (二次利用可) ため取得を継続。

実行方法:
    # ローカルテスト
    python scripts/sites/government/city.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id city
"""

import csv
import io
import logging
import sys
import zipfile
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 対象業態 (これ以外の業種列の行は除外)
_TARGET_KEYWORDS = ("理容所", "美容所")

# CSV ヘッダ名 → EXTRA_COLUMNS 名 (構造化された短い値のみ。自由記述は無し)
_EXTRA_MAP = {
    "台帳番号": "台帳番号",
    "許可番号": "許可番号",
    "申請者法人名称": "申請者法人名称",
    "詳細業種": "詳細業種",
    "許可等年月日": "許可等年月日",
}


class City(StaticCrawler):
    """横浜市 理容所・美容所施設一覧 スクレイパー"""

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

    def _iter_zip_urls(self, url: str):
        """起点ページから理容所・美容所の .zip リンクを出現順・重複除去で列挙する。"""
        soup = self.get_soup(url)
        if soup is None:
            logger.warning("起点ページを取得できませんでした: %s", url)
            return
        seen = set()
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if not href.lower().endswith(".zip"):
                continue
            text = a.get_text(strip=True)
            if not any(k in text for k in _TARGET_KEYWORDS):
                continue
            # 引数 url を唯一のルートとし、同一サイト上の絶対 URL を urljoin で導出
            zip_url = urljoin(url, href)
            if zip_url not in seen:
                seen.add(zip_url)
                yield zip_url

    def parse(self, url: str):
        zip_urls = list(self._iter_zip_urls(url))
        self.total_items = None  # 総件数は事前に不明 (ZIP 展開後に判明)
        logger.info("対象 ZIP: %d 件 (%s)", len(zip_urls), zip_urls)

        for zip_url in zip_urls:
            try:
                yield from self._stream_zip(zip_url)
            except Exception as e:  # noqa: BLE001 — 1 ZIP 失敗でも他は継続
                self.error_count += 1
                logger.warning("ZIP の取得/解析に失敗 (スキップ): %s — %s", zip_url, e)
                continue

    def _stream_zip(self, zip_url: str):
        """ZIP をダウンロードし、内包する行政区別 CSV を 1 行ずつ即 yield する。"""
        # session.get はテストランナー / smoke_test のソフトタイムアウト対象 (get_soup と同経路)
        resp = self.session.get(zip_url, timeout=self.TIMEOUT)
        resp.raise_for_status()

        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        for member in zf.namelist():
            if not member.lower().endswith(".csv"):
                continue
            try:
                # 横浜市 CSV は UTF-16 (BOM 付き) / タブ区切り
                text = zf.read(member).decode("utf-16")
            except Exception as e:  # noqa: BLE001
                logger.warning("CSV デコード失敗 (スキップ): %s — %s", member, e)
                continue
            yield from self._parse_csv(text, zip_url)

    def _parse_csv(self, text: str, zip_url: str):
        reader = csv.reader(io.StringIO(text), delimiter="\t")
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
            # 業種フィルタ: 理容所・美容所以外は除外 (備考の対象外業態を弾く)
            gyoushu = cell(row, "業種")
            if gyoushu and not any(k in gyoushu for k in _TARGET_KEYWORDS):
                continue

            name = cell(row, "施設名称")
            name2 = cell(row, "施設名称２")
            if name2:
                name = f"{name} {name2}".strip()
            if not name:
                continue

            item = {
                Schema.NAME: name,
                Schema.PREF: "神奈川県",
                Schema.ADDR: cell(row, "施設所在地"),
                Schema.TEL: cell(row, "施設電話番号"),
                Schema.REP_NM: cell(row, "申請者氏名"),
                Schema.POS_NM: cell(row, "申請者役職"),
                Schema.CAT_SITE: gyoushu,
                Schema.URL: zip_url,
            }
            for csv_name, out_name in _EXTRA_MAP.items():
                item[out_name] = cell(row, csv_name)
            yield item


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = City()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    parse() は引数 url を起点にページ内の .zip リンクを抽出し、urljoin で ZIP URL を導出する。
    scraper.execute(
        "https://www.city.yokohama.lg.jp/kurashi/sumai-kurashi/seikatsu/kaiteki/kankyodata.html"
    )

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
