"""
京都市オープンデータ「理容所・美容所・クリーニング所の施設一覧について」(データセット 00530) — クローラー

取得対象:
    京都市オープンデータポータル (KYOTO OPEN DATA) のデータセット 00530 に
    格納されている **Excel 形式の施設一覧ファイル** から、京都市内で営業中の
    理容所 / 美容所 / クリーニング所を 1 施設 = 1 行で取得する。

    データセットには 200 件のリソース (2026-08 時点) が登録されているが、内訳は
        (A) 「〇〇所営業施設一覧(令和X年3月末時点)」 … 年度末時点の **全件一覧**
        (B) 「〇〇所令和X年Y月新規施設一覧」         … その月の **新規開設分のみ**
    であり、それぞれ Excel と PDF の 2 形式で重複掲載されている。
    そのまま全ファイルを読むと、過去年度の (A) に含まれる **廃業済み施設** まで
    取り込んでしまう。そこで業態ごとに次のルールで対象を絞る:

        業態 (理容所/美容所/クリーニング所) ごとに
          1. 最新の (A) 全件一覧 (更新日時が最大のもの) を採用する
          2. その (A) より **更新日時が新しい** (B) 新規一覧を追加で採用する

    これにより「最新の年度末スナップショット + それ以降に新規開設された施設」=
    現時点の営業施設一覧が得られる。2026-08 時点の対象は
        理容所   … 令和8年3月末時点 (1,000 行) + 令和8年4/5/6月新規
        美容所   … 令和8年3月末時点 (行政区別 11 シート/約 3,946 行) + 令和8年4/5/6月新規
        クリーニング所 … 令和8年3月末時点 (799 行) + 令和8年5/6月新規
    で、合計およそ 5,800 件。

    PDF 版は Excel と同一内容 (ページ上の説明文いわく「エクセルデータは一部文字化けを
    起こしておりますので、ＰＤＦデータを併せて掲載しています」) だが、構造化された
    表として扱えるのは Excel のため Excel 側を採用する。まれに外字由来の文字化けが
    残る点は **元データ側の制約** であり、当クローラーでは補正しない (推測で書き換えない)。

取得フロー:
    1. 起点 URL (sites.yml の url) を GET。リソース一覧は 1 ページ 50 件 × 4 ページで、
       ページ送りは `#pageMenu` の `./?page=N`。引数 url から urljoin で導出する。
    2. 各リソースブロック (`table.resultTable`) から
         - タイトル      … `.resultRight1 a` のテキスト
         - リソース URL  … `.resultRight1 a[href]` (../../resource/?id=NNNN)
         - ダウンロード名 … `form input[name=upload_file]` の value
         - ファイル形式/更新日時 … `.resultRight2` のテキスト「ファイル形式：xlsx ｜ 更新：YYYY-MM-DD hh:mm:ss」
       を抽出する。xlsx 以外 (pdf) は捨てる。
    3. 上記ルールで対象ファイルを選別し、**全件一覧を先に** ダウンロードする
       (1 ファイル目で数千件 yield できるため、最初の 1 件が数秒以内に出る)。
    4. ダウンロードは resource ページへの POST
       (`download=Download` / `upload_file=<保存ファイル名>` / `upload_url=`)。
       GET ではファイル本体が返らないため POST 必須。
    5. python-calamine で読み、**全シート** を走査する。
       ⚠ 美容所の全件一覧のみ行政区ごとにシートが分かれている (北区/上京区/…/伏見区)。
         先頭シートだけ読むと 3,946 件中 260 件しか取れないので必ず全シートを回す。
       ⚠ 1 行目はタイトル行 (例:「【北区】美容所施設一覧（令和８年３月末時点）」) で、
         ヘッダは 2 行目。ただし決め打ちせず「施設名称」を含む行をヘッダとして探索する。
    6. 1 行 (= 1 施設) ずつ即 yield する (全件バッファしない)。
       同一施設が 全件一覧と新規一覧の双方に載る可能性があるため
       (業態, 施設名称, 施設所在地) で重複除去する。

カラム設計 (元データの 5 列をすべて出力する):
    名称        ← 施設名称
    施設名      ← 施設名称 (同上)
    都道府県    ← 「京都府」固定 (全件京都市内のため)
    住所        ← 施設所在地 (元データが「京都市北区…」と市名込みのためそのまま)
    代表者名    ← 申請者氏名 の氏名部分。法人申請の場合
                  (例:「有限会社１５１Ａ　取締役　岡村　真一」) は法人名・役職を切り離す。
    役職        ← 申請者氏名 の役職部分 (例: 取締役 / 代表取締役)
    サイト定義業種・ジャンル ← 業態 (理容所 / 美容所 / クリーニング所)
    細業種      ← 業態 (同上)
    設立年月日  ← 検査確認日 (和暦略記 S29.07.22 形式) を西暦 YYYY-MM-DD に変換したもの
    取得URL     ← 元リソースのページ URL (https://data.city.kyoto.lg.jp/resource/?id=NNNN)
    EXTRA       ← 検査確認番号 / 検査確認日(和暦) / 申請者氏名(原文) / 申請法人名 /
                  行政区 / 公表時点 / 掲載区分 / 元ファイル
    ※ 郵便番号・TEL・法人番号・資本金・売上・従業員数・事業内容・メール・HP・SNS・
      営業時間・定休日は元データに存在しないため空欄 (推測で埋めない)。
      特に **電話番号は本データセットに一切含まれない**。

備考 (呼び出し指示への対応):
    - 備考「一覧ファイルをダウンロードできます」= データセット内の Excel 一覧ファイルを
      ダウンロードして中身を取得する、と解釈した。業態の限定指示は無いため 3 業態すべてを対象とする。
    - EXTRA は確認番号・日付・氏名・区名など **構造化された短い値のみ**。
      自由記述の文章カラムは元データに存在しない (著作権リスク回避)。
    - 利用規約: 本データセットのライセンスは CC-BY 4.0 (表示)、著作権者は京都市。
      「京都市オープンデータ利用規約（第３版）」
      (https://data.city.kyoto.lg.jp/contents.php?category=0) を確認したが、
      スクレイピング/クローリング/自動取得を禁止する条項は無い (第1〜7条とも
      著作権・ライセンス表示に関する定めのみ)。よって取得を継続する。
      成果物公表時は『京都市オープンデータ』を利用した旨の明記が必要。
    - DELAY=0: 通信はリソース一覧 4 ページ + Excel 約 10 本の計 15 回程度のみ。
      基盤は 1 件 yield ごとに DELAY 秒スリープするため、DELAY>0 だと
      約 5,800 件で無駄に長時間化する。

実行方法:
    # ローカルテスト
    python scripts/sites/government/data_3.py

    # スモークテスト
    python bin/smoke_test.py scripts/sites/government/data_3.py \
        "https://data.city.kyoto.lg.jp/dataset/00530/" --limit 3 --timeout 90

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id data_3
"""

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

from python_calamine import CalamineWorkbook

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# リソースタイトルから業態を判定する (「クリーニング所」を先に見る: 部分一致の衝突は無いが順序を固定)
_GENRES = ("理容所", "美容所", "クリーニング所")

# 「ファイル形式：xlsx ｜ 更新：2026-04-17 17:11:36」
_META_RE = re.compile(r"ファイル形式：\s*(\S+?)\s*[｜|]\s*更新：\s*([\d\-: ]+)")

# resource ページ URL から ID を取り出す
_RESOURCE_ID_RE = re.compile(r"[?&]id=(\d+)")

# タイトル中の公表時点「令和8年3月末時点」「令和８年６月新規」など (全角数字も含む)
_PUBLISHED_RE = re.compile(r"((?:令和|平成)\s*[0-9０-９元]+\s*年(?:\s*[0-9０-９]+\s*月)?[^）)（(]*?(?:時点|新規))")

# 検査確認日の和暦略記 (例: S29.07.22 / R08.06.08 / H10.4.1)
_ABBR_DATE_RE = re.compile(r"^([MTSHR])\s*(\d{1,2})\D+(\d{1,2})\D+(\d{1,2})")
_ABBR_ERA_BASE = {"M": 1867, "T": 1911, "S": 1925, "H": 1988, "R": 2018}
# 念のため和暦フル表記・西暦表記にも対応する (将来の書式変更への保険)
_WAREKI_RE = re.compile(r"(令和|平成|昭和|大正|明治)\s*(\d+|元)\s*年\s*(\d+)\s*月\s*(\d+)\s*日")
_ERA_BASE = {"令和": 2018, "平成": 1988, "昭和": 1925, "大正": 1911, "明治": 1867}
_SEIREKI_RE = re.compile(r"^(\d{4})\D+(\d{1,2})\D+(\d{1,2})")

# 申請者氏名に含まれる法人格 (法人申請の場合のみ付く)
_CORP_KEYWORD_RE = re.compile(
    r"(株式会社|有限会社|合同会社|合資会社|合名会社|一般社団法人|一般財団法人|"
    r"公益社団法人|公益財団法人|社会医療法人|医療法人|社会福祉法人|学校法人|宗教法人|"
    r"特定非営利活動法人|事業協同組合|協同組合|生活協同組合|農業協同組合|"
    r"独立行政法人|法人)"
)
# 役職。法人申請は「<法人名> <役職> <氏名>」の並びで、法人名と役職の間に
# 全角空白が入るため「先頭一致」では切り出せない。文字列中を検索して分割する。
_POSITION_RE = re.compile(
    r"(代表取締役社長|代表取締役|取締役社長|代表理事|代表社員|代表役員|代表者|"
    r"理事長|組合長|会長|社長|取締役|執行役員|執行役|支配人|園長|館長|代表)"
)

# 元データのヘッダ名 → EXTRA_COLUMNS 名 (構造化された短い値のみ。自由記述プロースは無い)
_EXTRA_MAP = {
    "検査確認番号": "検査確認番号",
    "検査確認日": "検査確認日(和暦)",
    "申請者氏名": "申請者氏名",
}

# ヘッダ行の判定に使う必須ラベル
_HEADER_KEY = "施設名称"


class Data3(StaticCrawler):
    """京都市オープンデータ 理容所・美容所・クリーニング所 施設一覧スクレイパー"""

    # 通信は一覧 4 ページ + Excel 約 10 本のみ。基盤は yield ごとに DELAY 秒待つため 0 とする。
    DELAY = 0.0
    CONTINUE_ON_ERROR = True
    TIMEOUT = 90

    EXTRA_COLUMNS = list(_EXTRA_MAP.values()) + [
        "申請法人名",
        "行政区",
        "公表時点",
        "掲載区分",
        "元ファイル",
    ]

    # __init__ のオーバーライドは基盤で禁止されているため prepare() で初期化する
    _seen: set = None

    def prepare(self):
        self._seen = set()

    # ------------------------------------------------------------------ utils

    @staticmethod
    def _txt(value) -> str:
        """セル値/ノードテキストを安全に文字列化する (全角空白・連続空白・改行を整理)。"""
        if value is None:
            return ""
        s = str(value).replace("\r", " ").replace("\n", " ").replace("　", " ")
        return re.sub(r"\s+", " ", s).strip()

    @classmethod
    def _to_iso_date(cls, value: str) -> str:
        """検査確認日を YYYY-MM-DD に変換する。変換できない場合は空文字。

        実データは和暦略記 (S29.07.22 / R08.06.08)。将来書式が変わっても拾えるよう
        和暦フル表記・西暦表記にもフォールバックする。
        """
        s = unicodedata.normalize("NFKC", cls._txt(value))
        if not s:
            return ""
        m = _ABBR_DATE_RE.match(s)
        if m:
            era, yy, mm, dd = m.groups()
            return f"{_ABBR_ERA_BASE[era] + int(yy):04d}-{int(mm):02d}-{int(dd):02d}"
        m = _WAREKI_RE.search(s)
        if m:
            era, yy, mm, dd = m.groups()
            year = _ERA_BASE[era] + (1 if yy == "元" else int(yy))
            return f"{year:04d}-{int(mm):02d}-{int(dd):02d}"
        m = _SEIREKI_RE.match(s)
        if m:
            y, mm, dd = m.groups()
            return f"{int(y):04d}-{int(mm):02d}-{int(dd):02d}"
        return ""

    @classmethod
    def _split_applicant(cls, applicant: str) -> tuple[str, str, str]:
        """申請者氏名を (法人名, 役職, 氏名) に分解する。

        個人事業主は氏名のみ (例:「大谷 和久」) なので ("", "", 氏名) を返す。
        法人申請は「株式会社ＦＡＮ ＳＴＹＬＥ 代表取締役 松田 誠」のように
        <法人名> <役職> <氏名> の並びで、法人名の途中に空白が入る
        (元データは全角空白区切り) ため、役職語を文字列中から検索して分割する。
        役職語が無い法人申請 (例:「株式会社〇〇」のみ) は氏名を空欄とする。
        """
        s = cls._txt(applicant)
        if not s:
            return "", "", ""
        m = _POSITION_RE.search(s)
        if m:
            corp = s[:m.start()].strip()
            return corp, m.group(1), s[m.end():].strip()
        if _CORP_KEYWORD_RE.search(s):
            # 役職の記載が無い法人申請。氏名は元データに無いため空欄にする
            return s, "", ""
        return "", "", s

    @classmethod
    def _extract_published(cls, title: str) -> str:
        """リソースタイトルから公表時点 (令和8年3月末時点 / 令和８年６月新規) を取り出す。"""
        m = _PUBLISHED_RE.search(cls._txt(title))
        return m.group(1).strip() if m else ""

    @staticmethod
    def _genre_of(title: str) -> str:
        for g in _GENRES:
            if g in title:
                return g
        return ""

    # ------------------------------------------------------- resource listing

    def _iter_resources(self, url: str):
        """起点 URL からリソース一覧を全ページ走査して列挙する。

        Yields:
            dict: {id, title, page_url, upload_file, filetype, updated}
        """
        seen_pages = {url}
        pending = [url]
        while pending:
            page_url = pending.pop(0)
            soup = self.get_soup(page_url)
            if soup is None:
                logger.warning("リソース一覧ページを取得できませんでした: %s", page_url)
                continue

            for table in soup.select("table.resultTable"):
                link = table.select_one(".resultRight1 a[href]")
                if link is None:
                    continue
                title = self._txt(link.get_text(" ", strip=True))
                # 引数 url を唯一のルートとし、相対パス (../../resource/?id=…) を urljoin で解決する
                resource_url = urljoin(page_url, link.get("href", ""))
                id_m = _RESOURCE_ID_RE.search(resource_url)
                if not id_m:
                    continue

                upload_input = table.select_one("form input[name='upload_file']")
                upload_file = upload_input.get("value", "") if upload_input else ""

                meta = self._txt(table.select_one(".resultRight2").get_text(" ", strip=True)) \
                    if table.select_one(".resultRight2") else ""
                meta_m = _META_RE.search(meta)
                filetype = meta_m.group(1).lower() if meta_m else ""
                updated = meta_m.group(2).strip() if meta_m else ""

                yield {
                    "id": id_m.group(1),
                    "title": title,
                    "page_url": resource_url,
                    "upload_file": upload_file,
                    "filetype": filetype,
                    "updated": updated,
                }

            # ページ送り (./?page=N) を発見順に追加する
            for a in soup.select("#pageMenu a[href]"):
                nxt = urljoin(page_url, a.get("href", ""))
                if nxt not in seen_pages:
                    seen_pages.add(nxt)
                    pending.append(nxt)

    @classmethod
    def _select_targets(cls, resources: list[dict]) -> list[dict]:
        """業態ごとに「最新の全件一覧 + それより新しい新規一覧」を選ぶ。

        Returns:
            list[dict]: 取得対象。全件一覧を先頭に並べる (最初の 1 件を早く yield するため)。
        """
        by_genre: dict[str, dict[str, list[dict]]] = {
            g: {"full": [], "new": []} for g in _GENRES
        }
        for r in resources:
            if r["filetype"] != "xlsx" or not r["upload_file"]:
                continue
            genre = cls._genre_of(r["title"])
            if not genre:
                continue
            title = r["title"]
            if "時点" in title:          # 「〇〇所営業施設一覧(令和X年3月末時点)」= 全件
                by_genre[genre]["full"].append(r)
            elif "新規" in title:        # 「〇〇所令和X年Y月新規施設一覧」= 差分
                by_genre[genre]["new"].append(r)

        fulls: list[dict] = []
        news: list[dict] = []
        for genre in _GENRES:
            bucket = by_genre[genre]
            if not bucket["full"]:
                # 全件一覧が見つからない場合は差分のみでも取得する (取りこぼしより過少を選ばない)
                logger.warning("%s の全件一覧が見つかりません。新規一覧のみ取得します。", genre)
                for r in bucket["new"]:
                    r["kind"] = "新規一覧"
                news.extend(bucket["new"])
                continue
            latest = max(bucket["full"], key=lambda r: r["updated"])
            latest["kind"] = "全件一覧"
            latest["genre"] = genre
            fulls.append(latest)
            for r in bucket["new"]:
                # 全件一覧のスナップショットより後に公表された新規分だけを足す
                if r["updated"] > latest["updated"]:
                    r["kind"] = "新規一覧"
                    r["genre"] = genre
                    news.append(r)

        for r in fulls + news:
            r.setdefault("genre", cls._genre_of(r["title"]))
        # 全件一覧 → 新規一覧の順。新規一覧は新しいものから。
        news.sort(key=lambda r: r["updated"], reverse=True)
        return fulls + news

    # ------------------------------------------------------------- xlsx 読み込み

    def _download(self, resource: dict) -> bytes | None:
        """リソースページへ POST して Excel の実体を取得する (GET では本体が返らない)。"""
        payload = {
            "download": "Download",
            "upload_file": resource["upload_file"],
            "upload_url": "",
        }
        resp = self.session.post(resource["page_url"], data=payload, timeout=self.TIMEOUT)
        resp.raise_for_status()
        content = resp.content
        # 失敗時は HTML が返るため簡易チェック (xlsx は ZIP なので PK で始まる)
        if not content.startswith(b"PK"):
            logger.warning("Excel ではないレスポンスを受信 (スキップ): %s", resource["title"])
            return None
        return content

    def _iter_rows(self, content: bytes, resource: dict):
        """xlsx の全シートを走査し、(行データ dict, シート名) を列挙する。

        ⚠ 美容所の全件一覧のみ行政区ごとにシートが分かれる。先頭シートだけ読むと大半を落とす。
        """
        workbook = CalamineWorkbook.from_filelike(io.BytesIO(content))
        for sheet_name in workbook.sheet_names:
            rows = workbook.get_sheet_by_name(sheet_name).to_python()
            header: list[str] | None = None
            col: dict[str, int] = {}
            for row in rows:
                values = [self._txt(c) for c in row]
                if header is None:
                    # 1 行目はタイトル行。「施設名称」を含む行をヘッダとして探す
                    if _HEADER_KEY in values:
                        header = values
                        col = {name: i for i, name in enumerate(values) if name}
                        for name in values:
                            if name and name not in _EXTRA_MAP and name not in ("施設名称", "施設所在地"):
                                logger.warning(
                                    "未対応の Excel カラムを検出しました (%s / %s): %s",
                                    resource["title"], sheet_name, name,
                                )
                    continue
                if not any(values):
                    continue
                yield {name: (values[i] if i < len(values) else "") for name, i in col.items()}, sheet_name

    # ------------------------------------------------------------------ parse

    def parse(self, url: str):
        resources = list(self._iter_resources(url))
        logger.info("データセット内リソース: %d 件", len(resources))

        targets = self._select_targets(resources)
        if not targets:
            logger.warning("取得対象の Excel が見つかりませんでした: %s", url)
            return
        logger.info(
            "取得対象 Excel: %d 本 — %s",
            len(targets), " / ".join(f"{t['title']}" for t in targets),
        )
        # 総件数は Excel を開くまで不明
        self.total_items = None

        if self._seen is None:  # prepare() を経ない呼び出しへの保険
            self._seen = set()

        for resource in targets:
            try:
                yield from self._parse_resource(resource)
            except Exception as e:  # noqa: BLE001 — 1 ファイル失敗でも他は継続する
                self.error_count += 1
                logger.warning("Excel の取得/解析に失敗 (スキップ): %s — %s", resource["title"], e)
                continue

    def _parse_resource(self, resource: dict):
        """1 ファイル分をダウンロードし、1 行ずつ即 yield する (全件バッファしない)。"""
        content = self._download(resource)
        if content is None:
            return

        genre = resource.get("genre") or self._genre_of(resource["title"])
        published = self._extract_published(resource["title"])
        filename = resource["upload_file"]

        for row, sheet_name in self._iter_rows(content, resource):
            name = row.get("施設名称", "")
            if not name or name == "施設名称":
                continue
            addr = row.get("施設所在地", "")

            key = (genre, name, addr)
            if key in self._seen:
                continue
            self._seen.add(key)

            applicant = row.get("申請者氏名", "")
            corp, position, person = self._split_applicant(applicant)

            item = {
                Schema.NAME: name,
                Schema.FAC_NAME: name,
                Schema.PREF: "京都府",
                Schema.ADDR: addr,
                Schema.REP_NM: person,
                Schema.POS_NM: position,
                Schema.CAT_SITE: genre,
                Schema.CAT_NM: genre,
                Schema.OPEN_DATE: self._to_iso_date(row.get("検査確認日", "")),
                Schema.URL: resource["page_url"],
                "申請法人名": corp,
                # 行政区は美容所の全件一覧のみシート名で分かれる。それ以外は住所から拾う
                "行政区": sheet_name if sheet_name.endswith("区") else self._ward_from_addr(addr),
                "公表時点": published,
                "掲載区分": resource.get("kind", ""),
                "元ファイル": filename,
            }
            for src_name, out_name in _EXTRA_MAP.items():
                item[out_name] = row.get(src_name, "")
            yield item

    @staticmethod
    def _ward_from_addr(addr: str) -> str:
        """住所 (京都市北区…) から行政区を取り出す。取れない場合は空文字。"""
        m = re.match(r"^京都市([^\s]{1,4}?区)", addr)
        return m.group(1) if m else ""


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Data3()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    parse() は引数 url を起点にリソース一覧/ページ送り/Excel URL をすべて導出する。
    scraper.execute("https://data.city.kyoto.lg.jp/dataset/00530/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
