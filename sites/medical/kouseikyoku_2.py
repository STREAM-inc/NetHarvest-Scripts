"""
地方厚生局(歯科・新規指定) — 関東信越10都県 + 東海北陸4県の保険医療機関(歯科)指定状況

取得対象:
    - 関東信越厚生局 管轄10都県 (茨城・栃木・群馬・埼玉・千葉・東京・神奈川・新潟・山梨・長野)
      「保険医療機関・保険薬局の指定等一覧」ページの 歯科（ZIP）(例 shitei_shika_r0810.zip)
    - 東海北陸厚生局 管轄6県のうち 岐阜・静岡・愛知・三重 の4県のみ
      「1．コード内容別医療機関一覧表」ページの コード内容別医療機関一覧表（歯科）ZIP
      ※富山県・石川県は依頼仕様により対象外 (ZIP に同梱されるが除外する)
    - 合計14都県を1つの CSV に統合する

取得フロー:
    1. 引数 url (関東信越「指定等一覧」ページ) を取得し、歯科（ZIP）リンクを特定
       (shitei_shika_rYYMM.zip のうち最新月。医科/薬局/併設は除外)
    2. ZIP 内の都県別 xlsx を1件ずつストリーミング解析して即 yield
    3. 同一オリジンの東海北陸「保険医療機関等情報」ページ (_TOKAI_INDEX_PATH) を取得し、
       「コード内容別医療機関一覧表」ページ → 歯科 ZIP を辿る
    4. ZIP 内の県別 xlsx のうち 岐阜/静岡/愛知/三重 のみ解析して即 yield

Excel のブロック構造 (1レコード = 2〜5行):
    head : [0]連番 [1]医療機関コード [2]名称 [3]〒+住所 [4]TEL [5]開設者氏名
           [6]管理者氏名 [7]指定年月日 [8]診療科名(病院は病床数) [9]医療機関種別
    sub1 : [1]記号番号 [4]常勤数 [7]登録理由(新規/移動 等) [8]診療科名(病院のみ) [9]現存フラグ
    sub2 : [4]歯科医師数 [7]指定期間始
    sub3+: 非常勤 等

⚠ 「指定年月日」と「指定期間始」の区別 (過去の事故の再発防止):
    - 指定年月日 … head[7]。初回指定日 (開業日相当)。一度決まったら変わらない
    - 指定期間始 … sub2[7]。保険医療機関の指定は6年ごとに更新されるため、
                   更新のたびに書き換わる「現在の指定期間の開始日」。開業日ではない
    この2つは別カラムとして両方保持し、絶対に混同しない。

備考 (フィルタ):
    - 歯科のファイル (歯科／コード内容別医療機関一覧表（歯科）) のみを取得対象とし、
      医科単体・薬局・医科ベースの「歯科併設」一覧は取得しない
    - 歯科の一覧には歯科診療所に加え歯科病院も含まれる。区別できるよう
      「医療機関種別」(診療所/病院) をそのまま保持する
    - 日付は和暦 (昭51. 11. 1) で記載されるため YYYY-MM-DD に正規化する

利用規約:
    厚生労働省ホームページのコンテンツは「公共データ利用規約(第1.0版)」(PDL1.0) 準拠。
    出典記載を条件に利用可能で、スクレイピングを禁止する条項は無い。
    出典: 厚生労働省ホームページ (https://kouseikyoku.mhlw.go.jp/)

実行方法:
    # ローカルテスト
    python scripts/sites/medical/kouseikyoku_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id kouseikyoku_2
"""

import io
import itertools
import logging
import re
import sys
import time
import zipfile
from pathlib import Path
from urllib.parse import urljoin
from xml.etree.ElementTree import iterparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

# --- 東海北陸厚生局 ---------------------------------------------------------
# 引数 url (関東信越) からはリンクで辿れないため、同一オリジン上のパスを定数で保持する。
# 実アクセス先は urljoin(url, _TOKAI_INDEX_PATH) で組み立てる (ホストは url 由来)。
_TOKAI_INDEX_PATH = "/tokaihokuriku/gyomu/gyomu/hoken_kikan/shitei.html"
# 東海北陸の対象県 (富山県・石川県は依頼仕様により対象外)
_TOKAI_TARGET_PREFS = {"岐阜県", "静岡県", "愛知県", "三重県"}

_KANTO_PREFS = [
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県",
    "東京都", "神奈川県", "新潟県", "山梨県", "長野県",
]
_TOKAI_PREFS = ["富山県", "石川県", "岐阜県", "静岡県", "愛知県", "三重県"]
_ALL_PREFS = _KANTO_PREFS + _TOKAI_PREFS

# ファイル名は「…（歯科）茨城r0810.xlsx」「2610（愛知歯科）…」のように接尾辞なしの県名表記。
# 長い表記を先に試すため (県名フル表記 → 短縮表記) の順で並べる。
_PREF_ALIASES: list[tuple[str, str]] = (
    [(p, p) for p in _ALL_PREFS]
    + [(p.rstrip("都道府県"), p) for p in _ALL_PREFS]
)
_PREF_IN_BRACKET = re.compile(r"[\[［]\s*(" + "|".join(_ALL_PREFS) + r")\s*[\]］]")

_XLSX_EXT = (".xlsx", ".xlsm")
# 関東信越の歯科一括 ZIP (shitei_shika_r0810.zip)。併設 (shikaheisetsu) は別物なので除外する
_KANTO_SHIKA_ZIP = re.compile(r"shitei_shika_r(\d+)(?:-\d+)?\.zip$", re.I)
_SHIKA_RE = re.compile(r"歯\s*科|shika|sika")
_HEISETSU_RE = re.compile(r"併\s*設|heisetsu|heisetu")
_IKA_YAKKYOKU_RE = re.compile(r"医\s*科|薬\s*局|ika|yakkyoku")
_CODE_ICHIRAN_RE = re.compile(r"コード内容別医療機関一覧表")

_POST_RE = re.compile(r"〒\s*([0-9０-９]{3})\s*[-－―‐ー]\s*([0-9０-９]{4})")
# 現存区分 (col9 のうち現存フラグに該当する値。残りは医療機関種別)
_STATUS_RE = re.compile(r"^(現存|休止|停止|廃止)$")
# col8 のうち病床数の行 (「一般    1,052」「精神  53」「一般（感染）」「   1」)。
# 残りが診療科名 (「歯 矯歯 小歯」等)
_BED_RE = re.compile(r"^(?:一般|精神|療養|感染|結核|[\d,\s]+$)")
# 和暦日付 (昭51. 11. 1 / 令元. 5. 1)
_WAREKI_RE = re.compile(r"^([明大昭平令])\s*(元|\d+)\s*\.\s*(\d+)\s*\.\s*(\d+)")
_ERA_BASE = {"明": 1868, "大": 1912, "昭": 1926, "平": 1989, "令": 2019}

_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
_MAX_ATTEMPTS = 3


class Kouseikyoku2(StaticCrawler):
    """地方厚生局(歯科・新規指定) スクレイパー"""

    DELAY = 0.0
    ITEM_DELAY = 0.0        # 数万件規模のため1件ごとの待機は行わない (待機はファイル取得側)
    FETCH_DELAY = 1.0       # ページ/ファイル取得ごとの待機秒数
    TIMEOUT = 90            # 数MB の ZIP をダウンロードするため長め

    EXTRA_COLUMNS = [
        "医療機関コード",
        "指定年月日",
        "指定期間始",
        "登録理由",
        "管理者氏名",
        "現存フラグ",
        "診療科名",
        "医療機関種別",
        "管轄厚生局",
    ]

    def prepare(self):
        """都県別の取得件数カウンタを初期化する。"""
        self._pref_counts: dict[str, int] = {}

    # ------------------------------------------------------------------ parse
    def parse(self, url: str):
        """関東信越 → 東海北陸 の順に歯科 ZIP を辿り、1レコードずつ yield する。"""
        if not hasattr(self, "_pref_counts"):
            self._pref_counts = {}

        yield from self._crawl_kanto(url)
        yield from self._crawl_tokai(url)

        self._log_pref_coverage()

    # ---------------------------------------------------------- 関東信越厚生局
    def _crawl_kanto(self, url: str):
        """引数 url (指定等一覧ページ) から歯科（ZIP）を特定して解析する。"""
        try:
            soup = self.get_soup(url)
            if soup is None:
                logger.warning("関東信越: ページ取得失敗 %s", url)
                return
            zip_url = self._pick_kanto_shika_zip(soup, url)
            if not zip_url:
                logger.warning("関東信越: 歯科（ZIP）リンクが見つかりません %s", url)
                return
            logger.info("関東信越厚生局 歯科ZIP: %s", zip_url)
            yield from self._parse_archive(
                zip_url, url, "関東信越厚生局", allowed=set(_KANTO_PREFS),
            )
        except Exception as e:  # noqa: BLE001 — 片方の局の失敗で全体を止めない
            if not self.CONTINUE_ON_ERROR:
                raise
            self.error_count += 1
            logger.warning("関東信越: 処理中にエラー (スキップ) — %s", e)

    def _pick_kanto_shika_zip(self, soup, page_url: str) -> str:
        """shitei_shika_rYYMM.zip のうち最新月のものを選ぶ。"""
        candidates: list[tuple[int, str]] = []
        for a in soup.select("a[href]"):
            href = a.get("href") or ""
            base = href.split("?")[0].rsplit("/", 1)[-1]
            if _HEISETSU_RE.search(base):
                continue
            m = _KANTO_SHIKA_ZIP.search(base)
            if m:
                candidates.append((int(m.group(1)), urljoin(page_url, href)))
        if candidates:
            return max(candidates, key=lambda x: x[0])[1]

        # ファイル名が変わった場合に備えたラベルベースのフォールバック
        for a in soup.select("a[href]"):
            href = a.get("href") or ""
            if not href.split("?")[0].lower().endswith(".zip"):
                continue
            label = a.get_text(" ", strip=True)
            if _SHIKA_RE.search(label) and not _HEISETSU_RE.search(label):
                return urljoin(page_url, href)
        return ""

    # ---------------------------------------------------------- 東海北陸厚生局
    def _crawl_tokai(self, url: str):
        """東海北陸の「コード内容別医療機関一覧表」→ 歯科 ZIP を辿る (対象4県のみ)。"""
        try:
            index_url = urljoin(url, _TOKAI_INDEX_PATH)
            soup = self.get_soup(index_url)
            if soup is None:
                logger.warning("東海北陸: 索引ページ取得失敗 %s", index_url)
                return

            code_url = ""
            for a in soup.select("a[href]"):
                if _CODE_ICHIRAN_RE.search(a.get_text(" ", strip=True)):
                    code_url = urljoin(index_url, a.get("href") or "")
                    break
            if not code_url:
                logger.warning("東海北陸: コード内容別医療機関一覧表のリンクが見つかりません")
                return

            code_soup = self.get_soup(code_url)
            if code_soup is None:
                logger.warning("東海北陸: 一覧表ページ取得失敗 %s", code_url)
                return

            zip_url = ""
            for a in code_soup.select("a[href]"):
                href = a.get("href") or ""
                if not href.split("?")[0].lower().endswith(".zip"):
                    continue
                label = a.get_text(" ", strip=True)
                if _HEISETSU_RE.search(label) or _IKA_YAKKYOKU_RE.search(label):
                    continue
                if _SHIKA_RE.search(label):
                    zip_url = urljoin(code_url, href)
                    break
            if not zip_url:
                logger.warning("東海北陸: 歯科 ZIP が見つかりません %s", code_url)
                return

            logger.info("東海北陸厚生局 歯科ZIP: %s", zip_url)
            yield from self._parse_archive(
                zip_url, code_url, "東海北陸厚生局", allowed=set(_TOKAI_TARGET_PREFS),
            )
        except Exception as e:  # noqa: BLE001
            if not self.CONTINUE_ON_ERROR:
                raise
            self.error_count += 1
            logger.warning("東海北陸: 処理中にエラー (スキップ) — %s", e)

    # ------------------------------------------------------- ダウンロード & 展開
    def _download(self, file_url: str) -> bytes | None:
        """バイナリを取得する。上限付きリトライ (無限ループ禁止)。"""
        last_err: Exception | None = None
        for attempt in range(_MAX_ATTEMPTS):
            try:
                logger.info("ダウンロード中: %s", file_url)
                resp = self.session.get(file_url, timeout=self.TIMEOUT)
                resp.raise_for_status()
                if self.FETCH_DELAY:
                    time.sleep(self.FETCH_DELAY)
                return resp.content
            except Exception as e:  # noqa: BLE001
                last_err = e
                logger.warning(
                    "ダウンロード失敗 (%d/%d): %s — %s",
                    attempt + 1, _MAX_ATTEMPTS, file_url, e,
                )
                if attempt < _MAX_ATTEMPTS - 1:
                    time.sleep(min(2 ** attempt, 8))

        if not self.CONTINUE_ON_ERROR:
            raise RuntimeError(f"ダウンロードに{_MAX_ATTEMPTS}回失敗: {file_url}") from last_err
        self.error_count += 1
        return None

    def _parse_archive(self, zip_url: str, page_url: str, bureau: str, allowed: set[str]):
        """歯科 ZIP をダウンロードし、県別 xlsx を1件ずつ解析して即 yield する。"""
        blob = self._download(zip_url)
        if not blob:
            return
        try:
            zf = zipfile.ZipFile(io.BytesIO(blob))
        except zipfile.BadZipFile:
            logger.warning("ZIP として展開できません: %s", zip_url)
            self.error_count += 1
            return

        members = []
        for info in zf.infolist():
            if info.is_dir():
                continue
            base = info.filename.replace("\\", "/").rsplit("/", 1)[-1]
            if base.startswith("~$") or not base.lower().endswith(_XLSX_EXT):
                continue
            if _HEISETSU_RE.search(base):
                continue
            members.append((base, info))

        if not members:
            logger.warning("ZIP 内に歯科 Excel がありません: %s", zip_url)
            return

        for base, info in members:
            pref = self._pref_from_filename(base)
            if pref and pref not in allowed:
                logger.info("%s: 対象外のためスキップ (%s)", bureau, base)
                continue
            logger.info("%s: 解析開始 %s (%s)", bureau, base, pref or "県名不明")
            yield from self._parse_workbook(zf.read(info), base, pref, page_url, bureau, allowed)

    # ------------------------------------------------------------ Excel パース
    def _parse_workbook(self, blob: bytes, source_name: str, pref: str,
                        page_url: str, bureau: str, allowed: set[str]):
        """xlsx (zip+XML) を標準ライブラリでストリーミング解析する。"""
        try:
            wb = zipfile.ZipFile(io.BytesIO(blob))
        except zipfile.BadZipFile:
            logger.warning("Excel として展開できません: %s", source_name)
            self.error_count += 1
            return

        sst = self._shared_strings(wb)
        sheets = sorted(
            (n for n in wb.namelist()
             if re.fullmatch(r"xl/worksheets/sheet\d+\.xml", n)),
            key=lambda n: int(re.search(r"(\d+)\.xml$", n).group(1)),
        )
        if not sheets:
            logger.warning("ワークシートが見つかりません: %s", source_name)
            return

        for sheet in sheets:
            rows = self._sheet_rows(wb, sheet, sst)
            # 先頭数行に「[愛知県]」表記がある局があるため、バッファして県名を解決する
            head_rows: list[list[str]] = []
            for row in rows:
                head_rows.append(row)
                if len(head_rows) >= 8:
                    break
            sheet_pref = pref or self._pref_from_rows(head_rows)
            if sheet_pref and sheet_pref not in allowed:
                logger.info("%s: 対象外の県のためスキップ (%s)", bureau, sheet_pref)
                continue

            stream = itertools.chain(head_rows, rows)
            yield from self._records(stream, sheet_pref, page_url, bureau)

    @staticmethod
    def _shared_strings(zf: zipfile.ZipFile) -> list[str]:
        try:
            data = zf.read("xl/sharedStrings.xml")
        except KeyError:
            return []
        out: list[str] = []
        for _, el in iterparse(io.BytesIO(data), events=("end",)):
            if el.tag == _NS + "si":
                out.append("".join(t.text or "" for t in el.iter(_NS + "t")))
                el.clear()
        return out

    @staticmethod
    def _col_index(ref: str) -> int:
        n = 0
        for ch in ref:
            if ch.isalpha():
                n = n * 26 + (ord(ch.upper()) - 64)
            else:
                break
        return n - 1

    def _sheet_rows(self, zf: zipfile.ZipFile, sheet: str, sst: list[str]):
        """シートを行 (文字列リスト) のジェネレータとして返す。"""
        data = zf.read(sheet)
        for _, el in iterparse(io.BytesIO(data), events=("end",)):
            if el.tag != _NS + "row":
                continue
            values: dict[int, str] = {}
            for c in el.findall(_NS + "c"):
                idx = self._col_index(c.get("r") or "")
                if idx < 0:
                    continue
                ctype = c.get("t")
                if ctype == "s":
                    v = c.find(_NS + "v")
                    text = sst[int(v.text)] if (v is not None and v.text) else ""
                elif ctype == "inlineStr":
                    isel = c.find(_NS + "is")
                    text = "".join(t.text or "" for t in isel.iter(_NS + "t")) if isel is not None else ""
                else:
                    v = c.find(_NS + "v")
                    text = v.text if (v is not None and v.text) else ""
                if text:
                    values[idx] = text
            width = max(values) + 1 if values else 0
            row = [values.get(i, "") for i in range(width)]
            el.clear()
            yield row

    @staticmethod
    def _pref_from_rows(rows: list[list[str]]) -> str:
        """シート冒頭の「[愛知県]」表記から都道府県を取得する。"""
        for row in rows:
            for cell in row:
                m = _PREF_IN_BRACKET.search(cell)
                if m:
                    return m.group(1)
        return ""

    @staticmethod
    def _pref_from_filename(name: str) -> str:
        """ファイル名中の県名 (「茨城」「東京」等の短縮表記含む) から都道府県を取得する。"""
        base = name.replace("\\", "/").rsplit("/", 1)[-1]
        for alias, pref in _PREF_ALIASES:
            if alias and alias in base:
                return pref
        return ""

    # --------------------------------------------------------- レコード組み立て
    def _records(self, rows, pref: str, page_url: str, bureau: str):
        """1レコード = 2〜5行のブロックをパースして dict を yield する。"""
        block: list[list[str]] = []
        for row in rows:
            head = self._cell(row, 0)
            if head.isdigit() and self._cell(row, 2):
                if block:
                    item = self._build(block, pref, page_url, bureau)
                    if item:
                        yield item
                block = [row]
            elif block:
                block.append(row)
        if block:
            item = self._build(block, pref, page_url, bureau)
            if item:
                yield item

    @staticmethod
    def _cell(row: list[str], idx: int) -> str:
        if idx >= len(row):
            return ""
        return str(row[idx]).replace("　", " ").strip()

    def _build(self, block: list[list[str]], pref: str, page_url: str, bureau: str) -> dict | None:
        head = block[0]
        name = self._cell(head, 2)
        if not name:
            return None

        # --- 住所 (〒NNN-NNNN が先頭に連結されている) ---
        raw_addr = self._cell(head, 3).replace("\n", "")
        post_code = ""
        m = _POST_RE.search(raw_addr)
        if m:
            post_code = "{}-{}".format(
                self._to_ascii_digits(m.group(1)), self._to_ascii_digits(m.group(2))
            )
            addr = raw_addr[m.end():].strip()
        else:
            addr = raw_addr.lstrip("〒").strip()
        # 住所に都道府県が含まれないため補完する
        if addr and pref and not addr.startswith(pref):
            addr = pref + addr

        # --- 2行目以降の col7: 日付なら「指定期間始」、日付でなければ「登録理由」 ---
        reason, period_start = "", ""
        for row in block[1:]:
            val = self._cell(row, 7)
            if not val:
                continue
            if _WAREKI_RE.match(val):
                period_start = period_start or val
            else:
                reason = reason or val

        # --- col8 / col9 はブロック内で縦に積まれる ---
        #   col9: 医療機関種別 (診療所 / 病院 / 特定機能+病院 …) + 現存フラグ (現存/休止)
        #   col8: 病床区分と病床数 (病院のみ) + 診療科名 (歯 矯歯 小歯 …)
        status, kinds, depts = "", [], []
        for row in block:
            v9 = self._cell(row, 9)
            if v9:
                if _STATUS_RE.match(v9):
                    status = status or v9
                else:
                    kinds.append(v9)
            v8 = self._cell(row, 8)
            if v8 and not _BED_RE.match(v8) and v8 not in depts:
                depts.append(v8)

        kind = "".join(kinds)
        dept = " ".join(depts)

        self._pref_counts[pref or "(不明)"] = self._pref_counts.get(pref or "(不明)", 0) + 1

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: self._cell(head, 4),
            Schema.REP_NM: self._cell(head, 5),      # 開設者氏名
            Schema.CAT_SITE: "歯科",
            Schema.URL: page_url,
            "医療機関コード": self._cell(head, 1),
            # ⚠ 初回指定日 (開業日相当)。6年ごとに書き換わる「指定期間始」とは別物
            "指定年月日": self._to_iso_date(self._cell(head, 7)),
            # ⚠ 6年ごとの指定更新における現在の指定期間の開始日。開業日ではない
            "指定期間始": self._to_iso_date(period_start),
            "登録理由": reason,
            "管理者氏名": self._cell(head, 6),
            "現存フラグ": status,
            "診療科名": dept,
            "医療機関種別": kind,
            "管轄厚生局": bureau,
        }

    @staticmethod
    def _to_ascii_digits(text: str) -> str:
        return text.translate(str.maketrans("０１２３４５６７８９", "0123456789"))

    @classmethod
    def _to_iso_date(cls, text: str) -> str:
        """和暦 (昭51. 11. 1 / 令元. 5. 1) を YYYY-MM-DD に正規化する。"""
        if not text:
            return ""
        m = _WAREKI_RE.match(cls._to_ascii_digits(text).strip())
        if not m:
            return ""
        era, year, month, day = m.groups()
        y = 1 if year == "元" else int(year)
        try:
            return "{:04d}-{:02d}-{:02d}".format(
                _ERA_BASE[era] + y - 1, int(month), int(day)
            )
        except (KeyError, ValueError):
            return ""

    # ------------------------------------------------------------ 網羅性チェック
    def _log_pref_coverage(self):
        """対象14都県の取得件数をログ出力し、欠落県を警告する。"""
        targets = _KANTO_PREFS + sorted(_TOKAI_TARGET_PREFS)
        total = sum(self._pref_counts.values())
        logger.info("=== 都県別 取得件数 (合計 %d 件) ===", total)
        for pref in targets:
            logger.info("  %s: %d 件", pref, self._pref_counts.get(pref, 0))
        missing = [p for p in targets if not self._pref_counts.get(p)]
        if missing:
            logger.warning("取得0件の都県 (%d): %s", len(missing), "、".join(missing))
        unknown = self._pref_counts.get("(不明)", 0)
        if unknown:
            logger.warning("都道府県を特定できなかったレコード: %d 件", unknown)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Kouseikyoku2()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://kouseikyoku.mhlw.go.jp/kantoshinetsu/chousa/shitei.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
