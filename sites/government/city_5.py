"""
川崎市 環境衛生関係営業施設一覧 — クローラー

取得対象:
    川崎市が「【全市】環境衛生関係営業に関する情報（オープンデータ）」として
    公表する CSV を **ページ上にある全ファイル** 取得する。備考の指示
    「URLに一覧データのファイルがあります。ダウンロードしてください」に
    業態の限定が無いため、業態フィルタは掛けず全施設を対象とする。

    ページ上のファイル (2026-08 時点 6 本 / 実データ 3,197 行):
        理容所 (557) / 美容所 (1,763) / クリーニング所 (540) /
        公衆浴場 (183) / 旅館業 (115) / 興行場 (39)
    ※ ファイル名に公表年月 (例: 01riyoujo202607.csv) が埋め込まれ毎月更新される
      ため、URL は決め打ちせず起点ページのリンクから毎回導出する。
      施設一覧は前月末時点の内容が毎月 15 日までに更新される。

取得フロー:
    1. 起点 URL (sites.yml の url) を GET し、`.csv` で終わるアンカーを
       **出現順** に列挙する。リンク文言
       「理容所一覧（令和8年7月末時点）(CSV, 91.39KB)」から
       「データ種別」(理容所) と「公表時点」(令和8年7月末時点) を分離する。
       ファイルサイズ表記 "(CSV, 91.39KB)" は公表時点と誤認しないよう除外する。
    2. 各 CSV を引数 url からの相対で urljoin して取得 (href は "../cmsfiles/…" 形式)。
       文字コードは UTF-8 (BOM 付き) だが、将来の作成者差異に備えて
       utf-8-sig → cp932 → utf-16 の順にフォールバックする。
    3. ヘッダ名で照合し、1 行 (= 1 施設) ずつ即 yield する (全件バッファしない)。
       ⚠ ヘッダ名は業態ごとに揺れる (作成部署が同一でも法令用語が異なるため)。
         決め打ちだと列が丸ごと欠落するので別名を正規化してから照合する
         (_canon_header):
           確認日 / 許可日                       → 許可日
           確認番号 / 許可番号                   → 許可番号
           開設者名 / 営業者名                   → 営業者名   (理美容は「開設者」)
           開設者住所 / 営業者住所               → 営業者住所
           開設者方書 / 営業者方書               → 営業者方書
           （構造設備の概要）営業所面積（㎡） /
           （建物）営業所延面積（㎡） /
           （建物）延面積（㎡）                  → 営業所面積  (「面積」を含む列)
           （客室）合計客室数（室）              → 客室数      (「客室数」を含む列)
         未知のヘッダ名は WARNING で通知する (列追加への追従用)。

カラム設計 (取得可能な項目はすべて出力する):
    名称        ← 施設名称
    施設名      ← 施設名称 (同上。GBP 系カラムにも入れる)
    都道府県    ← 「神奈川県」固定 (全件川崎市内のため)
    住所        ← 「川崎市」+ 施設所在地 (+ 施設方書)。元データの施設所在地は
                  「川崎区宮本町４‐１２」のように行政区始まりで市名を含まないため補う。
                  例外的に「移動」(移動理容・美容) のような行政区で始まらない値は
                  そのまま出力する (市名を付けると誤った住所になるため)。
    TEL         ← 施設電話番号 (掲載値そのまま)
    代表者名    ← 代表者名の氏名部分。法人の場合のみ記載があるため、
                  空欄 (= 個人事業主) のときは営業者名 (開設者名) を採用する。
    役職        ← 代表者名の役職部分 (例:「代表取締役　吉田　徹」→ 代表取締役)
    サイト定義業種・ジャンル ← データ種別 (理容所 / 美容所 / クリーニング所 /
                              公衆浴場 / 旅館業 / 興行場)
    細業種      ← 施設（種別） (クリーニング業: 一般/取次店、旅館業: 旅館・ホテル営業/
                  簡易宿所営業 など。他業態は元データに列が無いため空欄)
    設立年月日  ← 適合確認日 / 許可日を和暦→西暦 (YYYY-MM-DD) 変換したもの
    EXTRA       ← 公表時点/行政区/施設方書/許可日(和暦原文)/許可番号/営業者名/
                  営業者住所/営業者方書/施設種別/営業所面積（㎡）/客室数（室）/元ファイル
    ※ 郵便番号・法人番号・資本金・売上・従業員数・事業内容・FAX・メール・HP・SNS・
      営業時間・定休日は元データに存在しないため空欄 (推測で埋めない)。

備考 (呼び出し指示への対応):
    - 「一覧データのファイルをダウンロード」= ページ上の CSV 全 6 本を取得する。
      業態の限定指示は無いためフィルタ無し。
    - EXTRA は許可番号・日付・面積・客室数など **構造化された短い値のみ**。
      自由記述の文章カラムは元データに存在しない (著作権リスク回避)。
    - 利用規約: 本データはクリエイティブ・コモンズライセンス下で提供される
      オープンデータ (https://www.city.kawasaki.jp/shisei/category/51-7-4-0-0-0-0-0-0-0.html)。
      川崎市ホームページのサイトポリシー
      (https://www.city.kawasaki.jp/main/site_policy/0000000025.html) にも
      スクレイピング/クローリングの禁止条項は無いため取得を継続する。
    - DELAY=0: 通信は起点ページ 1 + CSV 6 の計 7 回のみで、基盤は 1 件 yield ごとに
      DELAY 秒スリープする実装のため、DELAY>0 だと 3,197 件で無駄に長時間化する。

実行方法:
    # ローカルテスト
    python scripts/sites/government/city_5.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id city_5
"""

import csv
import io
import logging
import re
import sys
import unicodedata
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# CSV ヘッダ名の表記ゆれ → 正規名。業態 (根拠法令) ごとに列名が揺れるため吸収する。
_HEADER_ALIASES = {
    "確認日": "許可日",
    "許可日": "許可日",
    "確認番号": "許可番号",
    "許可番号": "許可番号",
    "開設者名": "営業者名",
    "営業者名": "営業者名",
    "開設者住所": "営業者住所",
    "営業者住所": "営業者住所",
    "開設者方書": "営業者方書",
    "営業者方書": "営業者方書",
    "施設（種別）": "施設種別",
}

# 正規化後のヘッダ名 → EXTRA_COLUMNS 名 (構造化された短い値のみ。自由記述プロースは無し)
_EXTRA_MAP = {
    "施設方書": "施設方書",
    "許可日": "許可日(和暦)",
    "許可番号": "許可番号",
    "営業者名": "営業者名",
    "営業者住所": "営業者住所",
    "営業者方書": "営業者方書",
    "施設種別": "施設種別",
    "営業所面積": "営業所面積（㎡）",
    "客室数": "客室数（室）",
}

# Schema 側で直接使うヘッダ (未知ヘッダ検出用のホワイトリストにも使う)
_SCHEMA_HEADERS = {"施設名称", "施設所在地", "施設電話番号", "代表者名"}

# 施設所在地の先頭にある行政区 (川崎市は市名が省略され行政区始まりで記載される)
_WARD_RE = re.compile(r"^(川崎区|幸区|中原区|高津区|宮前区|多摩区|麻生区)")

# 代表者名「代表取締役　吉田　徹」→ (役職, 氏名)。㈹ は代表者の略記。
_POSITION_RE = re.compile(
    r"^(㈹|代表理事|代表取締役社長|代表取締役|取締役社長|代表社員|代表役員|代表者|"
    r"理事長|会長|社長|取締役|執行役|代表|支配人|園長|校長|館長|組合長|理事)\s*"
)

# 和暦 → 西暦 (元年 = 1 年目)
_ERA_BASE = {"令和": 2018, "平成": 1988, "昭和": 1925, "大正": 1911, "明治": 1867}
_WAREKI_RE = re.compile(r"(令和|平成|昭和|大正|明治)\s*(\d+|元)\s*年\s*(\d+)\s*月\s*(\d+)\s*日")
_SEIREKI_RE = re.compile(r"(\d{4})\D+(\d{1,2})\D+(\d{1,2})")


class City5(StaticCrawler):
    """川崎市 環境衛生関係営業施設一覧スクレイパー"""

    # 通信は起点 1 + CSV 6 の計 7 回のみ。基盤は yield ごとに DELAY 秒待つため 0 とする。
    DELAY = 0.0
    CONTINUE_ON_ERROR = True
    TIMEOUT = 90

    EXTRA_COLUMNS = ["公表時点", "行政区"] + list(dict.fromkeys(_EXTRA_MAP.values())) + ["元ファイル"]

    # 未知ヘッダの通知は 1 回だけ出す。
    # __init__ のオーバーライドは基盤で禁止されているため prepare() で初期化する。
    _unknown_headers: set = None

    def prepare(self):
        self._unknown_headers = set()

    @staticmethod
    def _txt(value) -> str:
        """セル値を安全に文字列化 (改行・全角空白・連続空白を整理)。

        許可番号は元データに改行が入る (例: "川崎市指令\\n衛環第224号") ため潰す。
        """
        if value is None:
            return ""
        s = str(value).replace("\r", " ").replace("\n", " ").replace("　", " ")
        return re.sub(r"\s+", " ", s).strip()

    @classmethod
    def _canon_header(cls, name: str) -> str:
        """ヘッダ名の表記ゆれを正規名に寄せる (BOM/空白除去 + 別名変換)。"""
        s = name.replace("﻿", "").replace("　", "").strip()
        if s in _HEADER_ALIASES:
            return _HEADER_ALIASES[s]
        # 面積・客室数は「（構造設備の概要）営業所面積（㎡）」等、括弧付きで業態ごとに異なる
        if "客室数" in s:
            return "客室数"
        if "面積" in s:
            return "営業所面積"
        return s

    @staticmethod
    def _to_iso_date(value: str) -> str:
        """和暦 (例: 平成25年2月18日) / 西暦文字列を YYYY-MM-DD に変換する。失敗時は空文字。"""
        if not value:
            return ""
        s = unicodedata.normalize("NFKC", value)
        m = _WAREKI_RE.search(s)
        if m:
            era, yy, mm, dd = m.groups()
            year = _ERA_BASE[era] + (1 if yy == "元" else int(yy))
            return f"{year:04d}-{int(mm):02d}-{int(dd):02d}"
        m = _SEIREKI_RE.search(s)
        if m:
            y, mm, dd = m.groups()
            return f"{int(y):04d}-{int(mm):02d}-{int(dd):02d}"
        return ""

    @classmethod
    def _split_representative(cls, rep: str, operator: str) -> tuple[str, str]:
        """代表者名を (役職, 氏名) に分解する。

        法人の場合のみ「代表取締役　吉田　徹」形式で記載され、個人事業主は空欄。
        空欄のときは営業者名 (= 開設者本人の氏名) を代表者名として採る。
        """
        rep = cls._txt(rep)
        if not rep:
            return "", cls._txt(operator)
        m = _POSITION_RE.match(rep)
        if m:
            pos = "代表者" if m.group(1) == "㈹" else m.group(1)
            return pos, rep[m.end():].strip()
        return "", rep

    @classmethod
    def _build_address(cls, addr: str, hogaki: str) -> str:
        """施設所在地 (行政区始まり) に市名を補い、方書 (建物名・部屋番号) を連結する。"""
        addr = cls._txt(addr)
        if not addr:
            return ""
        # 「移動」(移動理容・移動美容) のように行政区で始まらない値には市名を付けない
        full = f"川崎市{addr}" if _WARD_RE.match(addr) else addr
        hogaki = cls._txt(hogaki)
        return f"{full} {hogaki}".strip() if hogaki else full

    def _iter_csv_links(self, url: str):
        """起点ページから .csv リンクを出現順・重複除去で列挙する。

        Yields:
            tuple[str, str, str]: (CSV の絶対 URL, データ種別, 公表時点)
        """
        soup = self.get_soup(url)
        if soup is None:
            logger.warning("起点ページを取得できませんでした: %s", url)
            return
        seen = set()
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if not href.lower().split("?")[0].endswith(".csv"):
                continue
            # 引数 url を唯一のルートとし、相対パス (../cmsfiles/…) を urljoin で解決する
            csv_url = urljoin(url, href)
            if csv_url in seen:
                continue
            seen.add(csv_url)
            # リンク文言 "理容所一覧（令和8年7月末時点）(CSV, 91.39KB)" を
            # 種別 / 公表時点 / ファイルサイズ に分解する
            raw = self._txt(a.get_text(" ", strip=True))
            parens = [
                p.strip()
                for p in re.findall(r"[（(]([^）)]*)[）)]", raw)
                # "(CSV, 91.39KB)" はファイルサイズ表記なので公表時点と誤認しない
                if "CSV" not in p.upper() and "ファイル" not in p and "KB" not in p.upper()
            ]
            label = re.sub(r"[（(][^）)]*[）)]", "", raw).strip()
            # "理容所一覧" → "理容所" (末尾の「一覧」は種別名ではない)
            label = re.sub(r"一覧$", "", label) or Path(href).stem
            yield csv_url, label, parens[0] if parens else ""

    def parse(self, url: str):
        links = list(self._iter_csv_links(url))
        self.total_items = None  # 総件数は CSV 取得後に判明するため事前には不明
        logger.info("対象 CSV: %d 件", len(links))

        for csv_url, label, published in links:
            try:
                yield from self._stream_csv(csv_url, label, published)
            except Exception as e:  # noqa: BLE001 — 1 ファイル失敗でも他は継続
                self.error_count += 1
                logger.warning("CSV の取得/解析に失敗 (スキップ): %s — %s", csv_url, e)
                continue

    def _stream_csv(self, csv_url: str, label: str, published: str):
        """CSV をダウンロードし、1 行ずつ即 yield する (全件バッファしない)。"""
        # session.get はテストランナー / smoke_test のソフトタイムアウト対象 (get_soup と同経路)
        resp = self.session.get(csv_url, timeout=self.TIMEOUT)
        resp.raise_for_status()
        text = self._decode_csv(resp.content, csv_url)
        if text is None:
            return
        yield from self._parse_csv(text, csv_url, label, published)

    @staticmethod
    def _decode_csv(raw: bytes, name: str) -> str | None:
        """CSV バイト列をデコードする。実データは UTF-8 (BOM) だが将来差異に備え候補を試す。"""
        if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
            candidates = ("utf-16", "utf-8-sig", "cp932")
        else:
            candidates = ("utf-8-sig", "cp932", "utf-16")
        for enc in candidates:
            try:
                return raw.decode(enc)
            except UnicodeDecodeError:
                continue
        logger.warning("CSV デコード失敗 (スキップ): %s", name)
        return None

    def _parse_csv(self, text: str, csv_url: str, label: str, published: str):
        reader = csv.reader(io.StringIO(text))
        try:
            header = [self._canon_header(h) for h in next(reader)]
        except StopIteration:
            return
        col = {name: i for i, name in enumerate(header)}

        # 取りこぼし検知: 既知マッピングに無いヘッダがあれば通知する (列追加への追従用)
        if self._unknown_headers is None:  # prepare() を経ない呼び出しへの保険
            self._unknown_headers = set()
        for h in header:
            if h and h not in _EXTRA_MAP and h not in _SCHEMA_HEADERS:
                if h not in self._unknown_headers:
                    self._unknown_headers.add(h)
                    logger.warning("未対応の CSV カラムを検出しました (%s): %s", csv_url, h)

        def cell(row, name):
            i = col.get(name)
            return self._txt(row[i]) if (i is not None and i < len(row)) else ""

        filename = Path(csv_url).name
        for row in reader:
            if not any(c.strip() for c in row):
                continue

            name = cell(row, "施設名称")
            if not name:
                continue

            addr_raw = cell(row, "施設所在地")
            ward_m = _WARD_RE.match(addr_raw)
            pos, rep = self._split_representative(cell(row, "代表者名"), cell(row, "営業者名"))

            item = {
                Schema.NAME: name,
                Schema.FAC_NAME: name,
                Schema.PREF: "神奈川県",
                Schema.ADDR: self._build_address(addr_raw, cell(row, "施設方書")),
                Schema.TEL: cell(row, "施設電話番号"),
                Schema.REP_NM: rep,
                Schema.POS_NM: pos,
                Schema.CAT_SITE: label,
                Schema.CAT_NM: cell(row, "施設種別"),
                Schema.OPEN_DATE: self._to_iso_date(cell(row, "許可日")),
                Schema.URL: csv_url,
                "公表時点": published,
                "行政区": ward_m.group(1) if ward_m else "",
                "元ファイル": filename,
            }
            for csv_name, out_name in _EXTRA_MAP.items():
                item[out_name] = cell(row, csv_name)
            yield item


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = City5()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    parse() は引数 url を起点にページ内の .csv リンクを抽出し、urljoin で CSV URL を導出する。
    scraper.execute("https://www.city.kawasaki.jp/350/page/0000120745.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
