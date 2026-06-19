"""
国税庁法人番号公表サイト — 全国法人番号一括取得

運営: 国税庁 (National Tax Agency)
ポータルURL: https://www.houjin-bangou.nta.go.jp/

取得対象:
    - 法人番号公表データ (全件 CSV Shift-JIS、前月末時点の最新情報)
    - 法人番号 / 商号又は名称 / 名称カナ / 住所 / 郵便番号 /
      法人種別 / 指定年月日 / 英語名称・住所 等の構造化情報

取得フロー:
    1. /download/zenken/ を GET → CSRF トークンおよび CSV Shift-JIS
       都道府県別ファイル番号をページから動的取得
    2. 都道府県ごとに POST でZIPをダウンロード (全国単一ファイル 238MB は除外)
    3. ZIP 内 CSV (CP932) を 1 行ずつ解析して即 yield
       (東京など分割ファイルも各 ZIP を順次処理)

設計メモ:
    - ダウンロードは POST フォーム (CSRF トークン必須)。トークンはセッション内で再利用可能。
    - session.post() はテストランナーのソフトタイムアウト対象外のため、
      大きいファイル (東京 分割1 = 40MB 程度) は test-run で時間がかかる場合がある。
    - DELAY=0.0 は ZIP 内 CSV の行 yield に対する待機を0にするため。
      都道府県間の待機は _download_and_parse() 内で time.sleep(1.0) を実施。
    - 自由記述の長文フィールドは存在しないため著作権リスクは低い (構造化データのみ)。
    - 英語名称・住所は任意登録のため空の行が多い (官公庁・地方公共団体に多い)。

実行方法:
    # ローカルテスト
    python scripts/sites/government/houjin_bangou.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id houjin_bangou
"""

import csv
import io
import re
import sys
import time
import zipfile
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 法人種別コード → 名称マッピング (リソース定義書より)
_TYPE_MAP = {
    "101": "国の機関",
    "201": "地方公共団体",
    "301": "株式会社",
    "302": "有限会社",
    "303": "合名会社",
    "304": "合資会社",
    "305": "合同会社",
    "399": "その他の設立登記法人",
    "401": "外国会社等",
    "499": "その他",
}

# ダウンロードフォームのパス
_LIST_PATH = "download/zenken/"
_DOWNLOAD_PATH = "download/zenken/index.html"

# CSRF トークン input 名の識別パターン
_TOKEN_RE = re.compile(r"CNSFWTokenProcessor", re.IGNORECASE)

# 都道府県別ファイルを格納するセクションの ID
_SJIS_SECTION_ID = "csv-sjis"

# 郵便番号整形 (7桁数字 → XXX-XXXX)
_ZIP_RE = re.compile(r"^\d{7}$")


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", s.replace("　", " ")).strip()


class HoujinBangouScraper(StaticCrawler):
    """国税庁法人番号公表サイト スクレイパー (全国法人番号 CSV Shift-JIS)"""

    # ZIP 内 CSV の行 yield ごとに待機しない (都道府県間は明示的 sleep を使用)
    DELAY = 0.0
    # 大きいZIPファイル対応 (東京 分割1 = 40MB 程度)
    TIMEOUT = 120

    EXTRA_COLUMNS = [
        "法人種別",          # 例: "株式会社" / "国の機関" (構造化ラベル)
        "法人種別コード",     # 例: "301" (数値コード)
        "最終更新年月日",     # 例: "2024-03-01"
        "閉鎖年月日",         # 例: "2020-03-15" (廃業・閉鎖時のみ。活動中は空)
        "英語名称",           # 例: "Tottori Summary Court" (任意登録。空の場合あり)
        "英語住所",           # 例: "Tottori, 2-223, Higashimachi, Tottori shi" (任意登録)
    ]

    # CSRF トークンの input name (初回 GET 時にページから動的取得)
    _token_name: str = ""
    _token_val: str = ""

    def parse(self, url: str):
        list_url = urljoin(url, _LIST_PATH)
        soup = self.get_soup(list_url)
        if soup is None:
            self.logger.error("ダウンロードページの取得に失敗しました: %s", list_url)
            return

        # CSRF トークンを取得
        token_input = soup.find("input", {"name": _TOKEN_RE})
        if not token_input:
            self.logger.error("CSRF トークンが見つかりません。処理を中断します。")
            return
        self._token_name = token_input["name"]
        self._token_val = token_input["value"]
        self.logger.info("CSRF トークン取得完了")

        # CSV Shift-JIS セクションのファイル番号を動的取得
        file_numbers = self._extract_sjis_file_numbers(soup)
        self.logger.info("取得対象ファイル数: %d 都道府県・地域", len(file_numbers))
        self.total_items = len(file_numbers)  # ファイル数で進捗管理

        download_url = urljoin(url, _DOWNLOAD_PATH)

        for idx, file_no in enumerate(file_numbers):
            self.logger.info(
                "[%d/%d] ファイル番号 %s をダウンロード中",
                idx + 1, len(file_numbers), file_no,
            )
            yield from self._download_and_parse(download_url, file_no, url)
            if idx < len(file_numbers) - 1:
                time.sleep(1.0)  # サーバー負荷軽減

    # ------------------------------------------------------------------
    # ファイル番号の抽出 (CSV Shift-JIS セクションのみ)
    # ------------------------------------------------------------------

    def _extract_sjis_file_numbers(self, soup) -> list:
        """CSV Shift-JIS セクションの都道府県別ファイル番号リストを返す。
        全国一括ファイル (238MB) はサイズ大のため除外する。

        ページ構造:
          <h2 id="csv-sjis">CSV形式・Shift_JIS</h2>
          <table>... (SJIS ファイルのみのテーブル) ...</table>
          <h2 id="csv-unicode">CSV形式・Unicode</h2>
          ...
        id は <h2> タグ自体に付与されているため、
        BeautifulSoup の find(id=...) ではセクション全体を取れない。
        ページ HTML の文字位置でセクション区間を切り出す。
        """
        page_html = str(soup)
        m_sjis = re.search(r'id=["\']csv-sjis["\']', page_html)
        m_unicode = re.search(r'id=["\']csv-unicode["\']', page_html)

        if m_sjis and m_unicode:
            section = page_html[m_sjis.start():m_unicode.start()]
        else:
            self.logger.warning(
                "SJIS セクション境界 (id=csv-sjis / id=csv-unicode) が見つかりません。"
                " 全ファイル番号の前 1/3 を使用します。"
            )
            all_nums = re.findall(r"doDownload\((\d+)\)", page_html)
            # 3 フォーマット (SJIS / Unicode / XML) の先頭 1/3 が SJIS に相当
            return all_nums[1 : len(all_nums) // 3]  # [0] は全国ファイルなので除外

        file_nums = []
        for m in re.finditer(r"doDownload\((\d+)\)", section):
            file_no = m.group(1)
            # 前後 200 文字のコンテキストから「全国」ラベルを検出してスキップ
            ctx_start = max(0, m.start() - 200)
            ctx_text = re.sub(r"<[^>]+>", " ", section[ctx_start : m.start()])
            if re.search(r"全国", ctx_text):
                self.logger.info(
                    "全国ファイル (番号=%s) はサイズ大のためスキップ", file_no
                )
                continue
            file_nums.append(file_no)

        return file_nums

    # ------------------------------------------------------------------
    # ZIP ダウンロード & CSV 解析
    # ------------------------------------------------------------------

    def _download_and_parse(self, download_url: str, file_no: str, source_url: str):
        """指定ファイル番号の ZIP を POST でダウンロードし CSV 行を yield する。"""
        try:
            resp = self.session.post(
                download_url,
                data={
                    self._token_name: self._token_val,
                    "event": "download",
                    "selDlFileNo": file_no,
                },
                timeout=self.TIMEOUT,
            )
            resp.raise_for_status()
        except Exception as e:
            self.logger.warning("ZIP ダウンロード失敗 (file_no=%s): %s", file_no, e)
            return

        if resp.content[:4] != b"PK\x03\x04":
            self.logger.warning(
                "ZIP 以外のレスポンス (file_no=%s): %s", file_no, resp.content[:80]
            )
            return

        try:
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                if not csv_names:
                    self.logger.warning("ZIP 内に CSV が見つかりません (file_no=%s)", file_no)
                    return
                for csv_name in csv_names:
                    self.logger.info("  CSV 解析: %s", csv_name)
                    with zf.open(csv_name) as fp:
                        text = io.TextIOWrapper(fp, encoding="cp932", newline="")
                        reader = csv.reader(text)
                        count = 0
                        for row in reader:
                            if len(row) < 16:
                                continue
                            try:
                                item = self._build_item(row, source_url)
                            except Exception as ex:
                                self.logger.warning("行のパースに失敗 (スキップ): %s", ex)
                                continue
                            if item is None:
                                continue
                            count += 1
                            yield item
                        self.logger.info("  → %d 件 yield (file_no=%s)", count, file_no)
        except Exception as e:
            self.logger.warning("ZIP 解析失敗 (file_no=%s): %s", file_no, e)

    # ------------------------------------------------------------------
    # 1 行 → レコード
    # ------------------------------------------------------------------

    def _build_item(self, row: list, source_url: str) -> dict | None:
        """CSV 1 行をレコード dict に変換する。

        CSV 列定義 (30列、ヘッダ行なし):
          [0]  一連番号
          [1]  法人番号 (13桁)
          [2]  処理区分
          [3]  訂正区分
          [4]  最終更新年月日
          [5]  指定年月日 (法人番号が指定された日)
          [6]  商号又は名称
          [7]  商号又は名称イメージID
          [8]  法人種別コード
          [9]  国内所在地（都道府県）
          [10] 国内所在地（市区町村）
          [11] 国内所在地（丁目番地等）
          [12] 国内所在地イメージID
          [13] 都道府県コード
          [14] 市区町村コード
          [15] 郵便番号 (7桁)
          [16] 国外所在地 (国内法人は空)
          [17] 国外所在地イメージID
          [18] 登記記録の閉鎖等年月日
          [19] 登記記録の閉鎖等事由
          [20] 承継先法人番号
          [21] 変更事由の詳細
          [22] 変更年月日
          [23] 国内外区分 (1=国内, 2=国外)
          [24] 英語名称
          [25] 英語所在地（都道府県）
          [26] 英語所在地（市区町村以降）
          [27] 英語所在地イメージID
          [28] 名称カナ
          [29] (フラグ)
        """
        co_num = _clean(row[1])
        name = _clean(row[6])
        if not name:
            return None  # 名称は必須

        pref = _clean(row[9])
        city = _clean(row[10])
        street = _clean(row[11])
        foreign_addr = _clean(row[16]) if len(row) > 16 else ""

        # 住所の組み立て (国内: 都道府県+市区町村+丁目番地 / 国外: 国外所在地)
        if pref:
            addr_parts = [p for p in [pref, city, street] if p]
            full_addr = "".join(addr_parts)
        else:
            full_addr = foreign_addr

        # 郵便番号整形 (7桁 → XXX-XXXX)
        zip_raw = _clean(row[15])
        zip_fmt = f"{zip_raw[:3]}-{zip_raw[3:]}" if _ZIP_RE.match(zip_raw) else zip_raw

        # 英語住所 (都道府県 + 市区町村以降を結合)
        en_pref = _clean(row[25]) if len(row) > 25 else ""
        en_city = _clean(row[26]) if len(row) > 26 else ""
        en_addr_parts = [p for p in [en_pref, en_city] if p]
        en_addr = ", ".join(en_addr_parts)

        # 法人種別
        type_code = _clean(row[8])
        type_name = _TYPE_MAP.get(type_code, type_code)

        # 詳細ページ URL (法人番号から導出)
        detail_url = urljoin(source_url, f"henkorireki-johoto.html?selHoujinNo={co_num}")

        return {
            Schema.CO_NUM: co_num,
            Schema.NAME: name,
            Schema.NAME_KANA: _clean(row[28]) if len(row) > 28 else "",
            Schema.PREF: pref,
            Schema.ADDR: full_addr,
            Schema.POST_CODE: zip_fmt,
            Schema.OPEN_DATE: _clean(row[5]),
            Schema.URL: detail_url,
            # --- EXTRA ---
            "法人種別": type_name,
            "法人種別コード": type_code,
            "最終更新年月日": _clean(row[4]),
            "閉鎖年月日": _clean(row[18]) if len(row) > 18 else "",
            "英語名称": _clean(row[24]) if len(row) > 24 else "",
            "英語住所": en_addr,
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = HoujinBangouScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、
    #    ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://www.houjin-bangou.nta.go.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
