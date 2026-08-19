"""
横浜市 環境衛生関係施設一覧【全業態】 — クローラー

取得対象:
    横浜市が「環境衛生関係施設一覧」として公表するオープンデータ ZIP を
    **ページ上にある全ファイル** 取得する。備考の指示「一覧ファイルのダウンロードが
    できます。ファイルのダウンロードをお願いします。」に業態の限定が無いため、
    業態フィルタは掛けず全施設を対象とする。

    ページ上のファイル (2026-08 時点 12 本 / 実データ約 22,600 行):
        理容所 / 美容所 / クリーニング所 / 旅館業 / 公衆浴場 / 興行場 (各 4/1 現在)
        新規施設一覧 (月次・4〜7月分)
        特定建築物施設一覧 / 受水槽施設一覧 (各 4/1 現在)
    ※ 既存 site_id `city` は同ページの理容所・美容所のみを対象とする別クローラー。
      本クローラー (city_4) は全業態・全カラムを網羅する。

取得フロー:
    1. 起点 URL (sites.yml の url) を GET し、`.zip` で終わるアンカーを
       **出現順** に列挙する (ファイル名に公表年月日が埋め込まれ年次/月次で変わるため
       決め打ちせずページ上のリンクから毎回導出)。リンク文言から
       「データ種別」(例: 理容所施設一覧) と「公表時点」(例: 令和８年４月１日現在) を分離する。
    2. 各 ZIP を引数 url からの相対で urljoin して取得。ZIP 内は行政区ごとの CSV が
       複数格納されている。文字コード・区切り文字は **ファイルごとに不統一** で、
       4/1 現在の一覧は UTF-16(BOM 付き)/タブ区切りだが、月次「新規施設一覧」の ZIP には
       CP932/カンマ区切り・UTF-8(BOM)/カンマ区切りの CSV が混在する (作成者依存)。
       そのため BOM 判定 → utf-16 / utf-8-sig / cp932 の順に試し、区切り文字は
       ヘッダ行のタブ数とカンマ数の多寡で判定する。
    3. 各 CSV をヘッダ名で照合し、1 行 (= 1 施設) ずつ即 yield する (全件バッファしない)。
       ヘッダは 2 系統あり、どちらもヘッダ名駆動で対応する:
         (a) 営業施設系 (理容所〜興行場・新規):
             台帳番号/許可番号/申請者法人名称/申請者役職/申請者氏名/施設所在地/
             施設名称/施設名称２/施設電話番号/業種/詳細業種/許可等年月日
         (b) 建築物系 (特定建築物): 台帳番号/施設所在地/施設名称/施設名称２/用途/延べ面積
             (受水槽): 台帳番号/施設所在地/施設名称/施設名称２/区分/用途/有効容量/
                       設置場所/設置形態/材質
       ⚠ 許可日のヘッダ名は作成者依存で「許可等年月日」「許可年月日」「許可年等月日」(誤記)
         の 3 種が混在する。1 名決め打ちだと一部行政区の許可日が丸ごと欠落するため、
         別名を正規化してから照合する (_canon_header)。未知のヘッダ名は WARNING で通知する。
    4. 「新規施設一覧」は 4/1 現在の一覧と重複する施設を含むため、
       (台帳番号, 業種, 施設名称, 施設所在地) で重複排除する。

カラム設計 (取得可能な項目はすべて出力する):
    名称        ← 施設名称 (+ 施設名称２ が屋号/支店表記の場合は連結)
    名称_カナ   ← 施設名称２ が「（…）」で括られたカナ表記の場合のみ (半角カナは全角へ)
    施設名      ← 施設名称 (連結前の素の施設名)
    都道府県    ← 「神奈川県」固定 (全件横浜市内のため)
    住所        ← 施設所在地
    TEL         ← 施設電話番号 (掲載値そのまま。下記「電話番号」参照)
    代表者名    ← 申請者氏名 / 役職 ← 申請者役職
    サイト定義業種・ジャンル ← 業種 (建築物系は業種列が無いためデータ種別で代替)
    細業種      ← 詳細業種
    設立年月日  ← 許可(等)年月日を和暦→西暦 (YYYY-MM-DD) 変換したもの
    EXTRA       ← データ種別/公表時点/行政区/台帳番号/許可番号/申請者法人名称/施設名称２/
                  許可年月日(和暦原文)/用途/延べ面積/受水槽区分/有効容量/設置場所/
                  設置形態/材質/元ファイル
    ※ 郵便番号・法人番号・資本金・売上・従業員数・事業内容・FAX・メール・HP・SNS は
      元データに存在しないため空欄 (推測で埋めない)。

備考 (呼び出し指示への対応):
    - 業態の限定指示は無いためフィルタ無し (全 ZIP・全行を対象)。
    - 電話番号は元データ (Excel 由来) の数字列をそのまま採用する。7 桁 (市外局番 045 が
      省略された横浜市内番号) や先頭 0 が欠落した携帯番号が混在するが、市外局番の推測付与は
      データ捏造になるため行わず掲載値をそのまま出力する。建築物系ファイルには
      電話番号列が無いため空欄。
    - EXTRA は台帳番号・許可番号・日付・用途・容量・材質など**構造化された短い値のみ**。
      自由記述の文章カラムは元データに存在しない (著作権リスク回避)。
    - 利用規約: 横浜市サイトポリシー (https://www.city.yokohama.lg.jp/aboutweb/sitepolicy.html)
      にスクレイピング/クローリングの禁止条項は無く、「数値データ、簡単な表・グラフ等は
      著作権による保護の対象ではありませんので、自由に利用できます」と明記されているため
      取得を継続する。

実行方法:
    # ローカルテスト
    python scripts/sites/government/city_4.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id city_4
"""

import csv
import io
import logging
import re
import sys
import unicodedata
import zipfile
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# CSV ヘッダ名の表記ゆれ → 正規名。作成者 (行政区) ごとに列名が揺れるため吸収する。
_HEADER_ALIASES = {
    "許可等年月日": "許可年月日",
    "許可年等月日": "許可年月日",  # 原データの誤記 (南区・興行場 等)
    "許可年月日": "許可年月日",
}

# 正規化後のヘッダ名 → EXTRA_COLUMNS 名 (構造化された短い値のみ。自由記述プロースは無し)
_EXTRA_MAP = {
    "台帳番号": "台帳番号",
    "許可番号": "許可番号",
    "申請者法人名称": "申請者法人名称",
    "施設名称２": "施設名称２",
    "許可年月日": "許可年月日",
    "用途": "用途",
    "延べ面積": "延べ面積",
    "区分": "受水槽区分",
    "有効容量": "有効容量",
    "設置場所": "設置場所",
    "設置形態": "設置形態",
    "材質": "材質",
}

# Schema 側で直接使うヘッダ (未知ヘッダ検出用のホワイトリストにも使う)
_SCHEMA_HEADERS = {
    "施設名称", "施設所在地", "施設電話番号", "申請者氏名", "申請者役職", "業種", "詳細業種",
}

# 施設所在地から行政区を切り出す (例: 横浜市保土ケ谷区川辺町… → 保土ケ谷区)
_WARD_RE = re.compile(r"横浜市\s*([^\s0-9０-９]{1,8}?区)")

# 和暦 → 西暦 (元年 = 1 年目)
_ERA_BASE = {"令和": 2018, "平成": 1988, "昭和": 1925, "大正": 1911, "明治": 1867}
_WAREKI_RE = re.compile(r"(令和|平成|昭和|大正|明治)\s*(\d+|元)\s*年\s*(\d+)\s*月\s*(\d+)\s*日")
_SEIREKI_RE = re.compile(r"(\d{4})\D+(\d{1,2})\D+(\d{1,2})")

# 施設名称２ が「（…）」で括られた読み仮名かどうかの判定 (半角カナ・長音・中黒を許容)
_PAREN_RE = re.compile(r"^[（(](.+)[）)]$")
_KANA_RE = re.compile(r"^[ァ-ヶーｦ-ﾟ・･\s　]+$")


class City4(StaticCrawler):
    """横浜市 環境衛生関係施設一覧【全業態】スクレイパー"""

    DELAY = 1.0
    CONTINUE_ON_ERROR = True
    TIMEOUT = 90

    EXTRA_COLUMNS = ["データ種別", "公表時点", "行政区"] + list(dict.fromkeys(_EXTRA_MAP.values())) + ["元ファイル"]

    # 未知ヘッダの通知は 1 回だけ出す (行政区 CSV が 200 本以上あるため)。
    # __init__ のオーバーライドは基盤で禁止されているため prepare() で初期化する。
    _unknown_headers: set = None

    def prepare(self):
        self._unknown_headers = set()

    @staticmethod
    def _txt(value) -> str:
        """セル値を安全に文字列化 (前後空白除去・全角空白正規化)。"""
        if value is None:
            return ""
        s = str(value).replace("　", " ").strip()
        return "" if s.lower() == "nan" else s

    @staticmethod
    def _canon_header(name: str) -> str:
        """ヘッダ名の表記ゆれを正規名に寄せる (BOM/空白除去 + 別名変換)。"""
        s = name.replace("﻿", "").replace("　", "").strip()
        if s in _HEADER_ALIASES:
            return _HEADER_ALIASES[s]
        # 想定外の誤記 (例: 許可年 月日) も許可日として拾う
        if re.fullmatch(r"許可.*年.*月.*日", s):
            return "許可年月日"
        return s

    @staticmethod
    def _to_iso_date(value: str) -> str:
        """和暦 (例: 平成2年1月4日) / 西暦文字列を YYYY-MM-DD に変換する。失敗時は空文字。"""
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

    @staticmethod
    def _split_name2(name2: str, as_kana: bool) -> tuple[str, str]:
        """施設名称２ を (名称に連結する屋号/支店名, 読み仮名) に振り分ける。

        営業施設系 (理容所〜興行場・新規) では「（ヘアーサロン カリーノ）」のように
        括弧付きのカナが屋号の読み仮名として入る慣習のため名称_カナ に採る。
        建築物系 (特定建築物・受水槽) の括弧書きは旧称・別棟名であることが多く
        読み仮名と断定できないため EXTRA (施設名称２) にのみ残す (as_kana=False)。
        「横浜桜木町店」のような素の文字列は名称の一部 (支店名・別館名) として扱う。
        """
        if not name2:
            return "", ""
        m = _PAREN_RE.match(name2)
        if m:
            inner = m.group(1).strip()
            if as_kana and _KANA_RE.match(inner):
                # 半角カナ (ﾐｯﾄﾞﾀｳﾝ) が混じるため全角へ寄せる
                return "", re.sub(r"\s+", " ", unicodedata.normalize("NFKC", inner)).strip()
            # 括弧付きだがカナでない (英字別表記など) → 名称の別表記として保持のみ
            return "", ""
        return name2, ""

    def _iter_zip_links(self, url: str):
        """起点ページから .zip リンクを出現順・重複除去で列挙する。

        Yields:
            tuple[str, str, str]: (ZIP の絶対 URL, データ種別, 公表時点)
        """
        soup = self.get_soup(url)
        if soup is None:
            logger.warning("起点ページを取得できませんでした: %s", url)
            return
        seen = set()
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if not href.lower().endswith(".zip"):
                continue
            # 引数 url を唯一のルートとし、同一サイト上の絶対 URL を urljoin で導出
            zip_url = urljoin(url, href)
            if zip_url in seen:
                continue
            seen.add(zip_url)
            # リンク文言 "理容所施設一覧（令和８年４月１日現在）（ファイル：87KB）" を
            # 種別 / 公表時点 / ファイルサイズ に分解する
            raw = self._txt(a.get_text(" ", strip=True))
            parens = [p for p in re.findall(r"[（(]([^）)]*)[）)]", raw) if "ファイル" not in p]
            label = re.sub(r"[（(][^）)]*[）)]", "", raw).strip() or Path(href).stem
            yield zip_url, label, parens[0].strip() if parens else ""

    def parse(self, url: str):
        links = list(self._iter_zip_links(url))
        self.total_items = None  # 総件数は ZIP 展開後に判明するため事前には不明
        logger.info("対象 ZIP: %d 件", len(links))

        seen_keys: set[tuple[str, str, str, str]] = set()
        for zip_url, label, published in links:
            try:
                yield from self._stream_zip(zip_url, label, published, seen_keys)
            except Exception as e:  # noqa: BLE001 — 1 ZIP 失敗でも他は継続
                self.error_count += 1
                logger.warning("ZIP の取得/解析に失敗 (スキップ): %s — %s", zip_url, e)
                continue

    def _stream_zip(self, zip_url: str, label: str, published: str, seen_keys: set):
        """ZIP をダウンロードし、内包する行政区別 CSV を 1 行ずつ即 yield する。"""
        # session.get はテストランナー / smoke_test のソフトタイムアウト対象 (get_soup と同経路)
        resp = self.session.get(zip_url, timeout=self.TIMEOUT)
        resp.raise_for_status()

        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        for member in zf.namelist():
            if not member.lower().endswith(".csv"):
                continue
            text = self._decode_csv(zf.read(member), member)
            if text is None:
                continue
            yield from self._parse_csv(text, zip_url, label, published, member, seen_keys)

    @staticmethod
    def _decode_csv(raw: bytes, member: str) -> str | None:
        """CSV バイト列をデコードする。ZIP 内で文字コードが不統一なため候補を順に試す。

        4/1 現在の一覧は UTF-16 (BOM 付き) だが、月次「新規施設一覧」には CP932 や
        UTF-8 (BOM) の CSV が混在する。UTF-16 決め打ちだと後者を取りこぼす。
        """
        if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
            candidates = ("utf-16", "utf-8-sig", "cp932")
        else:
            candidates = ("utf-8-sig", "cp932", "utf-16")
        for enc in candidates:
            try:
                return raw.decode(enc)
            except UnicodeDecodeError:
                continue
        logger.warning("CSV デコード失敗 (スキップ): %s", member)
        return None

    def _parse_csv(self, text: str, zip_url: str, label: str, published: str,
                   member: str, seen_keys: set):
        # 区切り文字も不統一 (UTF-16 はタブ / CP932・UTF-8 はカンマ) のためヘッダ行で判定
        head = text.splitlines()[0] if text else ""
        delimiter = "\t" if head.count("\t") >= head.count(",") else ","
        reader = csv.reader(io.StringIO(text), delimiter=delimiter)
        try:
            header = [self._canon_header(h) for h in next(reader)]
        except StopIteration:
            return
        col = {name: i for i, name in enumerate(header)}

        # 取りこぼし検知: 既知マッピングに無いヘッダがあれば通知する (列追加への追従用)
        if self._unknown_headers is None:  # prepare() を経ない呼び出しへの保険
            self._unknown_headers = set()
        unknown = [h for h in header if h and h not in _EXTRA_MAP and h not in _SCHEMA_HEADERS]
        for h in unknown:
            if h not in self._unknown_headers:
                self._unknown_headers.add(h)
                logger.warning("未対応の CSV カラムを検出しました (%s): %s", member, h)

        def cell(row, name):
            i = col.get(name)
            return self._txt(row[i]) if (i is not None and i < len(row)) else ""

        for row in reader:
            if not any(c.strip() for c in row):
                continue

            base_name = cell(row, "施設名称")
            name2 = cell(row, "施設名称２")
            # 業種列を持つのは営業施設系のみ (建築物系は括弧書きを読み仮名扱いしない)
            suffix, kana = self._split_name2(name2, as_kana="業種" in col)
            name = f"{base_name} {suffix}".strip() if suffix else base_name
            if not name:
                continue

            addr = cell(row, "施設所在地")
            # 業種列を持たない建築物系 (特定建築物/受水槽) はデータ種別ラベルで代替
            gyoushu = cell(row, "業種") or label
            # 新規施設一覧は 4/1 現在の一覧と重複するため台帳番号+業種+名称+住所で排除
            key = (cell(row, "台帳番号"), gyoushu, name, addr)
            if key in seen_keys:
                continue
            seen_keys.add(key)

            ward_m = _WARD_RE.search(addr)

            item = {
                Schema.NAME: name,
                Schema.NAME_KANA: kana,
                Schema.FAC_NAME: base_name,
                Schema.PREF: "神奈川県",
                Schema.ADDR: addr,
                Schema.TEL: cell(row, "施設電話番号"),
                Schema.REP_NM: cell(row, "申請者氏名"),
                Schema.POS_NM: cell(row, "申請者役職"),
                Schema.CAT_SITE: gyoushu,
                Schema.CAT_NM: cell(row, "詳細業種"),
                Schema.OPEN_DATE: self._to_iso_date(cell(row, "許可年月日")),
                Schema.URL: zip_url,
                "データ種別": label,
                "公表時点": published,
                "行政区": ward_m.group(1) if ward_m else "",
                "元ファイル": Path(member).name,
            }
            for csv_name, out_name in _EXTRA_MAP.items():
                item[out_name] = cell(row, csv_name)
            yield item


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = City4()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    parse() は引数 url を起点にページ内の .zip リンクを抽出し、urljoin で ZIP URL を導出する。
    scraper.execute(
        "https://www.city.yokohama.lg.jp/kurashi/sumai-kurashi/seikatsu/kaiteki/kankyodata.html"
    )

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
