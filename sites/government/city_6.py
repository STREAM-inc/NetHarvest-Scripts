"""
水戸市オープンデータサイト 環境衛生関係施設一覧 — クローラー

取得対象:
    水戸市が「環境衛生関係施設一覧」としてオープンデータ公開している CSV を
    **ページ上にある全ファイル** 取得する。備考の指示
    「一覧ファイルをダウンロードできます」に業態の限定が無いため、
    業態フィルタは掛けず全施設を対象とする。

    ページ上のファイル (2026-08 時点 9 本 / 実データ 1,494 行):
        理容所 (258) / 美容所 (821) / クリーニング所・一般 (48) /
        クリーニング所・取次店 (69) / 無店舗取次店 (7) / 旅館業 (95) /
        公衆浴場 (52) / 興行場 (15) / 特定建築物 (130)
    ※ CSV は /uploaded/attachment/{ID}.csv 形式で、更新のたびに添付 ID が
      振り直される (公表時点はリンク文言に埋め込まれる) ため、URL は
      決め打ちせず起点ページのリンクから毎回導出する。

取得フロー:
    1. 起点 URL (sites.yml の url) を GET し、`.csv` で終わるアンカーを
       **出現順** に列挙する。リンク文言
       「1 施設一覧（理容所）（令和８年７月２日現在） [その他のファイル／23KB]」
       の括弧から「データ種別」(理容所) と「公表時点」(令和８年７月２日現在)
       を分離する。ファイルサイズ表記 [その他のファイル／23KB] は角括弧なので
       括弧抽出には掛からない。
    2. 各 CSV を引数 url からの相対で urljoin して取得 (href は "/uploaded/…" 形式)。
       文字コードは **Shift_JIS (cp932)**。将来の作成者差異に備えて
       cp932 → utf-8-sig → utf-16 の順にフォールバックする。
    3. ヘッダ名で照合し、1 行 (= 1 施設) ずつ即 yield する (全件バッファしない)。
       ⚠ ヘッダ名は業態ごとに 4 パターンに揺れる。決め打ちだと列が丸ごと
         欠落するので別名を正規化してから照合する (_canon_header):
           屋号                                        → 施設名称
           施設所在地 / 所在地_住所                    → 施設所在地
           施設電話番号 / 所在地_電話番号 /
           営業者電話番号                              → 電話番号
           許可番号 / 許可番号(帳票用)                 → 許可番号
           許可決裁年月日                              → 許可日
           営業者氏名 / 営業者_氏名 / 所有者_氏名      → 営業者名
           代表者氏名 / 代表者_氏名                    → 代表者名
           営業の種類 / 営業種別                       → 施設種別
           総客室数 / 総定員                           → そのまま EXTRA
         実際のヘッダ 4 パターン:
           A (理容所/美容所/クリーニング一般/取次店/公衆浴場/興行場):
              屋号,施設所在地,施設電話番号,許可番号,許可決裁年月日,営業者氏名,代表者氏名
           B (無店舗取次店): 屋号,施設所在地,営業者氏名,営業者電話番号,代表者氏名
              ※ 許可番号・許可日の列が無い (元データに存在しない) ため空欄
           C (旅館業): 所在地_住所,屋号,所在地_電話番号,営業者_氏名,代表者_氏名,
                       許可番号(帳票用),許可決裁年月日,営業の種類,総客室数,総定員
           D (特定建築物): 営業種別,屋号,所在地_住所,所在地_電話番号,
                           許可決裁年月日,所有者_氏名,代表者_氏名
         未知のヘッダ名は WARNING で通知する (列追加への追従用)。

カラム設計 (取得可能な項目はすべて出力する):
    名称        ← 屋号
    施設名      ← 屋号 (同上。GBP 系カラムにも入れる)
    都道府県    ← 「茨城県」固定 (全件水戸市内のため)
    住所        ← 施設所在地 / 所在地_住所 (元データが「水戸市…」で始まるためそのまま)
    TEL         ← 施設電話番号 / 所在地_電話番号 / 営業者電話番号 (掲載値そのまま。
                  市外局番無しの「21-3931」形式も原文のまま出力する)
    代表者名    ← 代表者氏名の氏名部分。法人の場合のみ記載があるため、
                  空欄 (= 個人事業主) のときは営業者氏名を採用する。
    役職        ← 代表者氏名の役職部分 (例:「代表取締役　榊原　厳典」→ 代表取締役)
    サイト定義業種・ジャンル ← データ種別 (理容所 / 美容所 / クリーニング所・一般 /
                  クリーニング所・取次店 / 無店舗取次店 / 旅館業 / 公衆浴場 /
                  興行場 / 特定建築物)
    細業種      ← 営業の種類 (旅館・ホテル営業 / 簡易宿所営業) または
                  営業種別 (特定建築物(店舗) 等)。他業態は元データに列が無いため空欄。
    設立年月日  ← 許可決裁年月日を和暦/西暦 → YYYY-MM-DD 変換したもの
    EXTRA       ← 公表時点/許可番号/許可決裁年月日(原文)/営業者氏名/総客室数/総定員/元ファイル
    ※ 名称_カナ・郵便番号・法人番号・資本金・売上・従業員数・事業内容・FAX・
      メール・HP・SNS・営業時間・定休日は元データに存在しないため空欄 (推測で埋めない)。

備考 (呼び出し指示への対応):
    - 「一覧ファイルをダウンロードできます」= ページ上の CSV 全 9 本を取得する。
      業態の限定指示は無いためフィルタ無し。
    - EXTRA は許可番号・日付・客室数など **構造化された短い値のみ**。
      自由記述の文章カラムは元データに存在しない (著作権リスク回避)。
    - 利用規約: 水戸市「リンク・著作権・免責事項」
      (https://www.city.mito.lg.jp/page/18265.html) を確認。リンクは原則フリー、
      著作権は著作権法の一般原則を述べるのみで、スクレイピング / クローリング /
      機械的なデータ取得を禁止する条項は無い。本データは
      「水戸市オープンデータライブラリ」(https://www.city.mito.lg.jp/site/open-data/)
      で公開されるオープンデータのため取得を継続する。
    - DELAY=0: 通信は起点ページ 1 + CSV 9 の計 10 回のみで、基盤は 1 件 yield ごとに
      DELAY 秒スリープする実装のため、DELAY>0 だと 1,494 件で無駄に長時間化する。

実行方法:
    # ローカルテスト
    python scripts/sites/government/city_6.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id city_6
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

# CSV ヘッダ名の表記ゆれ → 正規名。業態 (根拠法令 / 作成担当) ごとに列名が揺れるため吸収する。
_HEADER_ALIASES = {
    "屋号": "施設名称",
    "施設所在地": "施設所在地",
    "所在地_住所": "施設所在地",
    "施設電話番号": "電話番号",
    "所在地_電話番号": "電話番号",
    "営業者電話番号": "電話番号",
    "許可番号": "許可番号",
    "許可番号(帳票用)": "許可番号",
    "許可決裁年月日": "許可日",
    "営業者氏名": "営業者名",
    "営業者_氏名": "営業者名",
    "所有者_氏名": "営業者名",
    "代表者氏名": "代表者名",
    "代表者_氏名": "代表者名",
    "営業の種類": "施設種別",
    "営業種別": "施設種別",
    "総客室数": "総客室数",
    "総定員": "総定員",
}

# 正規化後のヘッダ名 → EXTRA_COLUMNS 名 (構造化された短い値のみ。自由記述プロースは無し)
_EXTRA_MAP = {
    "許可番号": "許可番号",
    "許可日": "許可決裁年月日(原文)",
    "営業者名": "営業者氏名",
    "総客室数": "総客室数",
    "総定員": "総定員",
}

# Schema 側で直接使うヘッダ (未知ヘッダ検出用のホワイトリストにも使う)
_SCHEMA_HEADERS = {"施設名称", "施設所在地", "電話番号", "代表者名", "施設種別"}

# 代表者氏名「代表取締役　榊原　厳典」→ (役職, 氏名)。（代）は代表者の略記。
_POSITION_RE = re.compile(
    r"^(（代）|\(代\)|㈹|水戸市長|市長|知事|理事長|会長|"
    r"代表取締役社長|代表取締役|取締役社長|代表社員|代表役員|代表理事|代表者|"
    r"社長|専務|常務|取締役|執行役|代表|支配人|園長|校長|館長|所長|組合長|理事)\s*"
)

# 和暦 → 西暦 (元年 = 1 年目)
_ERA_BASE = {"令和": 2018, "平成": 1988, "昭和": 1925, "大正": 1911, "明治": 1867}
_WAREKI_RE = re.compile(r"(令和|平成|昭和|大正|明治)\s*(\d+|元)\s*年\s*(\d+)\s*月\s*(\d+)\s*日")
_SEIREKI_RE = re.compile(r"(\d{4})\D+(\d{1,2})\D+(\d{1,2})")


class City6(StaticCrawler):
    """水戸市 環境衛生関係施設一覧スクレイパー"""

    # 通信は起点 1 + CSV 9 の計 10 回のみ。基盤は yield ごとに DELAY 秒待つため 0 とする。
    DELAY = 0.0
    CONTINUE_ON_ERROR = True
    TIMEOUT = 90

    EXTRA_COLUMNS = ["公表時点"] + list(dict.fromkeys(_EXTRA_MAP.values())) + ["元ファイル"]

    # 未知ヘッダの通知は 1 回だけ出す。
    # __init__ のオーバーライドは基盤で禁止されているため prepare() で初期化する。
    _unknown_headers: set = None

    def prepare(self):
        self._unknown_headers = set()

    @staticmethod
    def _txt(value) -> str:
        """セル値を安全に文字列化 (改行・全角空白・連続空白を整理)。"""
        if value is None:
            return ""
        s = str(value).replace("\r", " ").replace("\n", " ").replace("　", " ")
        s = s.replace(" ", " ")
        return re.sub(r"\s+", " ", s).strip()

    @classmethod
    def _canon_header(cls, name: str) -> str:
        """ヘッダ名の表記ゆれを正規名に寄せる (BOM/空白除去 + 別名変換)。"""
        s = name.replace("﻿", "").replace("　", "").strip()
        return _HEADER_ALIASES.get(s, s)

    @staticmethod
    def _to_iso_date(value: str) -> str:
        """和暦 (例: 平成6年4月4日) / 西暦 (例: 2026/6/1) を YYYY-MM-DD に変換。失敗時は空文字。"""
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
        """代表者氏名を (役職, 氏名) に分解する。

        法人の場合のみ「代表取締役　榊原　厳典」形式で記載され、個人事業主は空欄。
        空欄のときは営業者氏名 (= 事業主本人の氏名) を代表者名として採る。
        """
        rep = cls._txt(rep)
        if not rep:
            return "", cls._txt(operator)
        m = _POSITION_RE.match(rep)
        if m:
            token = m.group(1)
            pos = "代表者" if token in ("（代）", "(代)", "㈹") else token
            return pos, rep[m.end():].strip()
        return "", rep

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
            # 引数 url を唯一のルートとし、相対/絶対パスを urljoin で解決する
            csv_url = urljoin(url, href)
            if csv_url in seen:
                continue
            seen.add(csv_url)
            # リンク文言 "1 施設一覧（理容所）（令和８年７月２日現在） [その他のファイル／23KB]"
            # を 種別 / 公表時点 に分解する (ファイルサイズは角括弧なので混入しない)
            raw = self._txt(a.get_text(" ", strip=True))
            parens = [p.strip() for p in re.findall(r"[（(]([^）)]*)[）)]", raw) if p.strip()]
            label = parens[0] if parens else ""
            if not label:
                # 括弧が無い形式に変わった場合は先頭の連番と「施設一覧」を除いた文言で代用
                label = re.sub(r"\[[^\]]*\]", "", raw)
                label = re.sub(r"^\s*\d+\s*", "", label).replace("施設一覧", "").strip()
                label = label or Path(href).stem
            published = parens[1] if len(parens) > 1 else ""
            yield csv_url, label, published

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
        """CSV バイト列をデコードする。実データは cp932 だが将来差異に備え候補を試す。"""
        if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
            candidates = ("utf-16", "utf-8-sig", "cp932")
        elif raw[:3] == b"\xef\xbb\xbf":
            candidates = ("utf-8-sig", "cp932", "utf-16")
        else:
            candidates = ("cp932", "utf-8-sig", "utf-16")
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

            pos, rep = self._split_representative(cell(row, "代表者名"), cell(row, "営業者名"))

            item = {
                Schema.NAME: name,
                Schema.FAC_NAME: name,
                Schema.PREF: "茨城県",
                Schema.ADDR: cell(row, "施設所在地"),
                Schema.TEL: cell(row, "電話番号"),
                Schema.REP_NM: rep,
                Schema.POS_NM: pos,
                Schema.CAT_SITE: label,
                Schema.CAT_NM: cell(row, "施設種別"),
                Schema.OPEN_DATE: self._to_iso_date(cell(row, "許可日")),
                Schema.URL: csv_url,
                "公表時点": published,
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

    scraper = City6()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    parse() は引数 url を起点にページ内の .csv リンクを抽出し、urljoin で CSV URL を導出する。
    scraper.execute("https://www.city.mito.lg.jp/site/open-data/4496.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
