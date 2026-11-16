"""
地方厚生局(医科・指定一覧・全国) — 全8地方厚生(支)局 47都道府県の保険医療機関(医科)指定一覧

取得対象:
    地方厚生(支)局が毎月公表している「コード内容別医療機関一覧表」(指定一覧・全件) のうち
    医科 のファイルだけを取得し、47都道府県分を1つの CSV に統合する。
    (同じ作りで 歯科 = kouseikyoku_shika / 薬局 = kouseikyoku_yakkyoku がある。
     違いは _CATEGORY と クラス名 だけ)

    局               掲載ページ                                             Excel の置き方
    北海道厚生局     /hokkaido/.../code_ichiran.html                        xlsx 直リンク (医科は病院/診療所の2本)
    東北厚生局       /tohoku/.../itiran.html                                6県まとめ xlsx (県ごとにシート)
    関東信越厚生局   /kantoshinetsu/chousa/shitei.html                      区分ごとの ZIP (都県別 xlsx)
    東海北陸厚生局   /tokaihokuriku/.../shitei.html → コード内容別一覧表    区分ごとの ZIP (県別 xlsx)
    近畿厚生局       /kinki/tyousa/shinkishitei.html                        区分ごとの ZIP (府県別 xlsx)
    中国四国厚生局   /chugokushikoku/chousaka/iryoukikanshitei.html         区分ごとの ZIP (県別 xlsx)
    四国厚生支局     /shikoku/.../shitei/index.html                         区分ごとの ZIP (県別 xlsx)
    九州厚生局       /kyushu/.../index_00004.html → コード内容別一覧表      県ごとの ZIP (医科/歯科/薬局の3本入り)

取得フロー:
    1. 引数 url (地方厚生(支)局トップ) と同一オリジンの各局掲載ページを取得
    2. ページ内の「コード内容別医療機関一覧表」の区画にある xlsx / ZIP リンクだけを集める
       (同じページに並ぶ 新規指定・廃止・施設基準・訪問看護 等の表や、過去月の版は対象外)
    3. リンク名・ファイル名から明らかに別区分 (歯科/薬局) や併設と分かるものは取得しない
    4. xlsx を1件ずつストリーミング解析し、シート冒頭の
       「[令和 8年11月 1日現在　医科　現存/休止]」で区分を確定してから即 yield

区分の確定方法 (ファイル名に頼らない):
    全局のファイルは同じシステムの出力で、シート冒頭に必ず区分が書かれている。
    北海道・四国・九州はリンク名やファイル名から区分が分からない (または混在する) ため、
    最終的な判定は必ずシート冒頭の表記で行う。

「併設」ファイルを取得しない理由:
    関東信越・東北・近畿には「医科（歯科併設）」「歯科（医科併設）」のファイルが別にあるが、
    これは本体の一覧の部分集合 (歯科併設 ⊂ 医科、医科併設 ⊂ 歯科) で、取り込むと重複する。
    併設先のコードは本体側の2行目 (括弧書き) にも載っているので「併設医療機関コード」で保持する。

Excel のブロック構造 (1レコード = 2〜7行):
    head : [0]項番 [1]医療機関コード [2]名称 [3]〒+住所 [4]TEL [5]開設者氏名
           [6]管理者氏名 [7]指定年月日 [8]病床数 or 診療科名 [9]医療機関種別
    sub1 : [1](併設医療機関コード) [4]常勤数 [7]登録理由(新規/交代/組織変更 等) [8]診療科名 [9]現存フラグ
    sub2 : [4]医師数 [7]指定期間始
    sub3+: 非常勤 等

⚠ 「指定年月日」と「指定期間始」の区別:
    - 指定年月日 … head[7]。初回指定日 (開業日相当)。一度決まったら変わらない
    - 指定期間始 … sub2[7]。指定は6年ごとに更新されるため、更新のたびに書き換わる
                   「現在の指定期間の開始日」。開業日ではない
    この2つは別カラムとして両方保持し、混同しない。

備考:
    - 一覧は「現存/休止」の機関のみ (廃止済みは載らない)。「現存フラグ」で区別できる
    - 医科には病院と診療所の両方が含まれる。「医療機関種別」で区別できる
    - 住所に都道府県名が入っていないため補完する
    - 日付は和暦 (昭51. 11. 1) で記載されるため YYYY-MM-DD に正規化する
    - 同じ (都道府県, 医療機関コード) が2回出た場合は2件目以降を捨てる

利用規約:
    厚生労働省ホームページのコンテンツは「公共データ利用規約(第1.0版)」(PDL1.0) 準拠。
    出典記載を条件に利用可能で、スクレイピングを禁止する条項は無い。
    出典: 厚生労働省ホームページ (https://kouseikyoku.mhlw.go.jp/)

実行方法:
    # ローカルテスト
    python scripts/sites/medical/kouseikyoku_ika.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id kouseikyoku_ika
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

# このスクリプトが取得する区分。歯科版・薬局版との違いはここ (とクラス名) だけ。
_CATEGORY = "医科"

# --- 取得先 -----------------------------------------------------------------
# 引数 url (地方厚生(支)局トップ) からは一覧ページへリンクで辿れないため、同一オリジン上の
# パスを定数で保持する。実アクセス先は urljoin(url, path) で組み立てる (ホストは url 由来)。
# hop=True の局は、パスの先が索引ページなので「コード内容別医療機関一覧表」のリンクを1回辿る
# (辿った先のファイル名が番号だけで、URL が変わりやすいため)。
_BUREAUS: list[dict] = [
    {
        "name": "北海道厚生局",
        "path": "/hokkaido/gyomu/gyomu/hoken_kikan/code_ichiran.html",
        "hop": False,
        "prefs": ["北海道"],
    },
    {
        "name": "東北厚生局",
        "path": "/tohoku/gyomu/gyomu/hoken_kikan/itiran.html",
        "hop": False,
        "prefs": ["青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県"],
    },
    {
        "name": "関東信越厚生局",
        "path": "/kantoshinetsu/chousa/shitei.html",
        "hop": False,
        "prefs": ["茨城県", "栃木県", "群馬県", "埼玉県", "千葉県",
                  "東京都", "神奈川県", "新潟県", "山梨県", "長野県"],
    },
    {
        "name": "東海北陸厚生局",
        "path": "/tokaihokuriku/gyomu/gyomu/hoken_kikan/shitei.html",
        "hop": True,
        "prefs": ["富山県", "石川県", "岐阜県", "静岡県", "愛知県", "三重県"],
    },
    {
        "name": "近畿厚生局",
        "path": "/kinki/tyousa/shinkishitei.html",
        "hop": False,
        "prefs": ["福井県", "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県"],
    },
    {
        "name": "中国四国厚生局",
        "path": "/chugokushikoku/chousaka/iryoukikanshitei.html",
        "hop": False,
        "prefs": ["鳥取県", "島根県", "岡山県", "広島県", "山口県"],
    },
    {
        "name": "四国厚生支局",
        "path": "/shikoku/gyomu/gyomu/hoken_kikan/shitei/index.html",
        "hop": False,
        "prefs": ["徳島県", "香川県", "愛媛県", "高知県"],
    },
    {
        "name": "九州厚生局",
        "path": "/kyushu/gyomu/gyomu/hoken_kikan/index_00004.html",
        "hop": True,
        "prefs": ["福岡県", "佐賀県", "長崎県", "熊本県",
                  "大分県", "宮崎県", "鹿児島県", "沖縄県"],
    },
]
_ALL_PREFS: list[str] = [p for b in _BUREAUS for p in b["prefs"]]

# --- ページ内の区画判定 -------------------------------------------------------
# この文言が出たところから集め始める
_START_RE = re.compile(r"コード内容別医療機関一覧")
# ファイルを1つ以上集めた後にこの文言が出たら、別の表に入ったとみなして打ち切る
_STOP_RE = re.compile(
    r"新規指定|新規登録|廃止|辞退|取消|診療科目変更|届出受理|施設基準|訪問看護|受領委任"
)
# 「令和8年11月1日現在」。九州は過去月の版が同じページに続くので、日付が変わったら打ち切る
_ASOF_RE = re.compile(r"(令和|平成)\s*(元|[0-9０-９]+)\s*年\s*([0-9０-９]+)\s*月\s*([0-9０-９]+)\s*日\s*現在")

_FILE_EXT = (".xlsx", ".xlsm", ".zip")
_XLSX_EXT = (".xlsx", ".xlsm")

# --- 区分の判定 ---------------------------------------------------------------
# シート冒頭「[令和 8年11月 1日現在　医科　現存/休止]」の区分 (併設を先に試す)
_SHEET_CATEGORY_RE = re.compile(r"現在[\s　]+(医科併設|歯科併設|医科|歯科|薬局)[\s　]")
# リンク名・ファイル名からの下見用 (これで別区分と分かれば取得・解析を省く)
_JP_HINTS = (("医科", re.compile(r"医\s*科")),
             ("歯科", re.compile(r"歯\s*科")),
             ("薬局", re.compile(r"薬\s*局")))
_ROMAJI_HINTS = {"ika": "医科", "shika": "歯科", "sika": "歯科", "yakkyoku": "薬局"}
_HEISETSU_JP_RE = re.compile(r"併\s*設")
_HEISETSU_ROMAJI_RE = re.compile(r"heisetsu|heisetu")

_PREF_IN_BRACKET = re.compile(r"[\[［]\s*(" + "|".join(_ALL_PREFS) + r")\s*[\]］]")

_POST_RE = re.compile(r"〒\s*([0-9０-９]{3})\s*[-－―‐ー]\s*([0-9０-９]{4})")
# 現存区分 (col9 のうち現存フラグに該当する値。残りは医療機関種別)
_STATUS_RE = re.compile(r"^(現存|休止|停止|廃止)$")
# col8 のうち病床数の行 (「一般　　    324」「療養　　    198」「一般（感染）  4」「   1」)。
# 残りが診療科名 (「内　呼内　循環器内科」等)。「精神科、心療内科」のような診療科名を
# 誤って落とさないよう、病床区分の後ろは括弧書きと数字だけを許す。
_BED_RE = re.compile(
    r"^(?:(?:一般|精神|療養|感染|結核|介護)(?:[（(][^）)]*[）)])?[\s　]*[\d,]*|[\d,\s　]+)$"
)
# 併設医療機関コード 「(01,3196,7)」。区切りは局によって「,」「-」と表記が揺れる
_SUB_CODE_RE = re.compile(r"^[（(]\s*([0-9０-９][0-9０-９,，\-－‐]*)\s*[）)]$")
# 和暦日付 (昭51. 11. 1 / 令元. 5. 1)
_WAREKI_RE = re.compile(r"^([明大昭平令])\s*(元|\d+)\s*\.\s*(\d+)\s*\.\s*(\d+)")
_ERA_BASE = {"明": 1868, "大": 1912, "昭": 1926, "平": 1989, "令": 2019,
             "明治": 1868, "大正": 1912, "昭和": 1926, "平成": 1989, "令和": 2019}

_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
_MAX_ATTEMPTS = 3
_HEAD_ROWS = 12     # 区分・県名を読むために先読みする行数


class KouseikyokuIka(StaticCrawler):
    """地方厚生局(医科・指定一覧・全国) スクレイパー"""

    DELAY = 0.0
    ITEM_DELAY = 0.0        # 数万〜十万件規模のため1件ごとの待機は行わない (待機はファイル取得側)
    FETCH_DELAY = 1.0       # ページ/ファイル取得ごとの待機秒数
    TIMEOUT = 120           # 数MB の ZIP をダウンロードするため長め

    EXTRA_COLUMNS = [
        "医療機関コード",
        "併設医療機関コード",
        "指定年月日",
        "指定期間始",
        "登録理由",
        "管理者氏名",
        "現存フラグ",
        "診療科名",
        "医療機関種別",
        "管轄厚生局",
        "基準日",
    ]

    def prepare(self):
        """都道府県別の取得件数カウンタと重複チェック用の集合を初期化する。"""
        self._pref_counts: dict[str, int] = {}
        self._seen: set[tuple[str, str]] = set()
        self._dup_count = 0

    # ------------------------------------------------------------------ parse
    def parse(self, url: str):
        """全8局を順に回り、対象区分のレコードを1件ずつ yield する。"""
        if not hasattr(self, "_pref_counts"):
            self.prepare()

        for bureau in _BUREAUS:
            try:
                yield from self._crawl_bureau(url, bureau)
            except Exception as e:  # noqa: BLE001 — 1局の失敗で全体を止めない
                if not self.CONTINUE_ON_ERROR:
                    raise
                self.error_count += 1
                logger.warning("%s: 処理中にエラー (スキップ) — %s", bureau["name"], e)

        self._log_pref_coverage()

    # ------------------------------------------------------------- 局ごとの処理
    def _crawl_bureau(self, url: str, bureau: dict):
        name = bureau["name"]
        page_url = urljoin(url, bureau["path"])
        soup = self.get_soup(page_url)
        if soup is None:
            logger.warning("%s: ページ取得失敗 %s", name, page_url)
            return
        if self.FETCH_DELAY:
            time.sleep(self.FETCH_DELAY)

        if bureau["hop"]:
            hop_url = self._find_code_ichiran_link(soup, page_url)
            if hop_url:
                hop_soup = self.get_soup(hop_url)
                if self.FETCH_DELAY:
                    time.sleep(self.FETCH_DELAY)
                if hop_soup is not None:
                    soup, page_url = hop_soup, hop_url
                else:
                    logger.warning("%s: 一覧表ページ取得失敗 %s", name, hop_url)
            else:
                logger.warning("%s: コード内容別医療機関一覧表のリンクが見つかりません %s",
                               name, page_url)

        files = self._collect_files(soup, page_url)
        if not files:
            logger.warning("%s: コード内容別医療機関一覧表の Excel/ZIP が見つかりません %s",
                           name, page_url)
            return

        for file_url, label in files:
            base = file_url.split("?")[0].rsplit("/", 1)[-1]
            if not self._may_be_target(label + " " + base):
                logger.info("%s: 対象外のため取得しない (%s / %s)", name, label, base)
                continue
            blob = self._download(file_url)
            if not blob:
                continue
            if base.lower().endswith(".zip"):
                yield from self._parse_archive(blob, file_url, page_url, bureau)
            else:
                yield from self._parse_workbook(blob, base, page_url, bureau)

    @staticmethod
    def _find_code_ichiran_link(soup, page_url: str) -> str:
        """索引ページから「コード内容別医療機関一覧表」ページへのリンクを探す。"""
        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href or href.startswith("#"):
                continue
            if href.split("?")[0].lower().endswith(_FILE_EXT + (".pdf",)):
                continue
            if _START_RE.search(a.get_text(" ", strip=True)):
                return urljoin(page_url, href)
        return ""

    @staticmethod
    def _collect_files(soup, page_url: str) -> list[tuple[str, str]]:
        """「コード内容別医療機関一覧表」の区画にある xlsx / ZIP リンクを文書順に集める。

        同じページに新規指定・廃止・施設基準などの表や過去月の版が並ぶ局があるため、
        見出しの文言と「◯年◯月◯日現在」の日付で区画の終わりを判定する。
        """
        files: list[tuple[str, str]] = []
        seen: set[str] = set()
        started = False
        asof = ""
        for node in soup.descendants:
            node_name = getattr(node, "name", None)
            if node_name is None:
                text = str(node).strip()
                if not text:
                    continue
                if not started:
                    if _START_RE.search(text):
                        started = True
                    else:
                        continue
                m = _ASOF_RE.search(text)
                if m:
                    key = "".join(m.groups())
                    if not asof:
                        asof = key
                    elif key != asof and files:
                        break
                if files and _STOP_RE.search(text):
                    break
            elif node_name == "a" and started:
                href = (node.get("href") or "").strip()
                if not href.split("?")[0].lower().endswith(_FILE_EXT):
                    continue
                file_url = urljoin(page_url, href)
                if file_url in seen:
                    continue
                seen.add(file_url)
                files.append((file_url, node.get_text(" ", strip=True)))
        return files

    @staticmethod
    def _may_be_target(text: str) -> bool:
        """リンク名・ファイル名から、対象区分の可能性があるかを判定する (下見)。

        併設 → False。別区分だけが書かれている → False。何も書かれていない → True
        (その場合はシート冒頭の表記で確定する)。
        """
        lower = text.lower()
        if _HEISETSU_JP_RE.search(text) or _HEISETSU_ROMAJI_RE.search(lower):
            return False
        cats = {cat for cat, rx in _JP_HINTS if rx.search(text)}
        for token in re.findall(r"[a-z]+", lower):
            if token in _ROMAJI_HINTS:
                cats.add(_ROMAJI_HINTS[token])
        return not cats or _CATEGORY in cats

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

    @staticmethod
    def _member_name(info: zipfile.ZipInfo) -> str:
        """ZIP 内のファイル名を返す。UTF-8 フラグが無い場合は CP932 として読み直す。"""
        name = info.filename
        if not info.flag_bits & 0x800:
            try:
                name = name.encode("cp437").decode("cp932")
            except (UnicodeEncodeError, UnicodeDecodeError):
                pass
        return name.replace("\\", "/")

    def _parse_archive(self, blob: bytes, zip_url: str, page_url: str, bureau: dict):
        """ZIP 内の xlsx を1件ずつ解析して即 yield する。"""
        try:
            zf = zipfile.ZipFile(io.BytesIO(blob))
        except zipfile.BadZipFile:
            logger.warning("ZIP として展開できません: %s", zip_url)
            self.error_count += 1
            return

        found = False
        for info in zf.infolist():
            if info.is_dir():
                continue
            base = self._member_name(info).rsplit("/", 1)[-1]
            if base.startswith("~$") or not base.lower().endswith(_XLSX_EXT):
                continue
            found = True
            if not self._may_be_target(base):
                logger.info("%s: 対象外のため解析しない (%s)", bureau["name"], base)
                continue
            yield from self._parse_workbook(zf.read(info), base, page_url, bureau)
        if not found:
            logger.warning("ZIP 内に Excel がありません: %s", zip_url)

    # ------------------------------------------------------------ Excel パース
    def _parse_workbook(self, blob: bytes, source_name: str, page_url: str, bureau: dict):
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
            head_rows = list(itertools.islice(rows, _HEAD_ROWS))

            category = self._category_from_rows(head_rows)
            if category != _CATEGORY:
                if category:
                    logger.info("%s: 区分が%sのため対象外 (%s)", bureau["name"], category, source_name)
                else:
                    # 冒頭の表記が読めないシートは、区分を保証できないので取り込まない
                    logger.warning("%s: シート冒頭から区分を判定できません (スキップ) %s",
                                   bureau["name"], source_name)
                continue

            pref = (self._pref_from_rows(head_rows)
                    or self._pref_from_filename(source_name, bureau["prefs"])
                    or (bureau["prefs"][0] if len(bureau["prefs"]) == 1 else ""))
            if pref and pref not in bureau["prefs"]:
                logger.warning("%s: 管轄外の都道府県が含まれています (%s / %s)",
                               bureau["name"], pref, source_name)
            asof = self._asof_from_rows(head_rows)
            logger.info("%s: 解析開始 %s (%s / %s現在)",
                        bureau["name"], source_name, pref or "県名不明", asof or "基準日不明")

            stream = itertools.chain(head_rows, rows)
            yield from self._records(stream, pref, page_url, bureau["name"], asof)

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

    # ------------------------------------------------------ シート冒頭の読み取り
    @staticmethod
    def _category_from_rows(rows: list[list[str]]) -> str:
        """シート冒頭の「[…現在　医科　現存/休止]」から区分を取得する。"""
        for row in rows:
            for cell in row:
                m = _SHEET_CATEGORY_RE.search(cell)
                if m:
                    return m.group(1)
        return ""

    @staticmethod
    def _pref_from_rows(rows: list[list[str]]) -> str:
        """シート冒頭の「[愛知県]」表記から都道府県を取得する。"""
        for row in rows:
            for cell in row:
                m = _PREF_IN_BRACKET.search(cell)
                if m:
                    return m.group(1)
        return ""

    @classmethod
    def _asof_from_rows(cls, rows: list[list[str]]) -> str:
        """シート冒頭の「令和 8年11月 1日現在」を YYYY-MM-DD で返す。"""
        for row in rows:
            for cell in row:
                m = _ASOF_RE.search(cell)
                if not m:
                    continue
                era, year, month, day = (cls._to_ascii_digits(g) for g in m.groups())
                try:
                    y = 1 if year == "元" else int(year)
                    return "{:04d}-{:02d}-{:02d}".format(
                        _ERA_BASE[era] + y - 1, int(month), int(day)
                    )
                except (KeyError, ValueError):
                    return ""
        return ""

    @staticmethod
    def _pref_from_filename(name: str, prefs: list[str]) -> str:
        """ファイル名中の県名 (「茨城」「東京」等の短縮表記含む) から都道府県を取得する。

        冒頭に県名が書かれない関東信越のファイル用。候補はその局の管轄都道府県に限る。
        """
        base = name.replace("\\", "/").rsplit("/", 1)[-1]
        for pref in prefs:                      # 「東京都」を「京都」より先に当てる
            if pref in base:
                return pref
        for pref in prefs:
            short = pref if pref == "北海道" else pref[:-1]
            if short in base:
                return pref
        return ""

    # --------------------------------------------------------- レコード組み立て
    def _records(self, rows, pref: str, page_url: str, bureau_name: str, asof: str):
        """1レコード = 2〜7行のブロックをパースして dict を yield する。"""
        block: list[list[str]] = []
        for row in rows:
            head = self._cell(row, 0)
            if head.isdigit() and self._cell(row, 2):
                if block:
                    item = self._build(block, pref, page_url, bureau_name, asof)
                    if item:
                        yield item
                block = [row]
            elif block:
                block.append(row)
        if block:
            item = self._build(block, pref, page_url, bureau_name, asof)
            if item:
                yield item

    @staticmethod
    def _cell(row: list[str], idx: int) -> str:
        if idx >= len(row):
            return ""
        return str(row[idx]).replace("　", " ").strip()

    def _build(self, block: list[list[str]], pref: str, page_url: str,
               bureau_name: str, asof: str) -> dict | None:
        head = block[0]
        name = self._cell(head, 2)
        if not name:
            return None

        code = self._cell(head, 1)
        key = (pref, code)
        if code and key in self._seen:
            self._dup_count += 1
            return None
        self._seen.add(key)

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

        # --- 2行目以降 ---
        #   col1: (併設医療機関コード)
        #   col7: 日付なら「指定期間始」、日付でなければ「登録理由」
        sub_code, reason, period_start = "", "", ""
        for row in block[1:]:
            m = _SUB_CODE_RE.match(self._cell(row, 1))
            if m and not sub_code:
                sub_code = (self._to_ascii_digits(m.group(1))
                            .replace("，", ",").replace("－", "-").replace("‐", "-"))
            val = self._cell(row, 7)
            if not val:
                continue
            if _WAREKI_RE.match(val):
                period_start = period_start or val
            else:
                reason = reason or val

        # --- col8 / col9 はブロック内で縦に積まれる ---
        #   col9: 医療機関種別 (診療所 / 病院 / 療養病床 / 薬局 …) + 現存フラグ (現存/休止)
        #   col8: 病床区分と病床数 (病院のみ) + 診療科名 (内 呼内 … / 調剤)
        status, kinds, depts = "", [], []
        for row in block:
            v9 = self._cell(row, 9)
            if v9:
                if _STATUS_RE.match(v9):
                    status = status or v9
                elif v9 not in kinds:
                    kinds.append(v9)
            v8 = self._cell(row, 8)
            if v8 and not _BED_RE.match(v8) and v8 not in depts:
                depts.append(v8)

        self._pref_counts[pref or "(不明)"] = self._pref_counts.get(pref or "(不明)", 0) + 1

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: self._cell(head, 4),
            Schema.REP_NM: self._cell(head, 5),      # 開設者氏名
            Schema.CAT_SITE: _CATEGORY,
            Schema.URL: page_url,
            "医療機関コード": code,
            "併設医療機関コード": sub_code,
            # ⚠ 初回指定日 (開業日相当)。6年ごとに書き換わる「指定期間始」とは別物
            "指定年月日": self._to_iso_date(self._cell(head, 7)),
            # ⚠ 6年ごとの指定更新における現在の指定期間の開始日。開業日ではない
            "指定期間始": self._to_iso_date(period_start),
            "登録理由": reason,
            "管理者氏名": self._cell(head, 6),
            "現存フラグ": status,
            "診療科名": " ".join(depts),
            "医療機関種別": " ".join(kinds),
            "管轄厚生局": bureau_name,
            "基準日": asof,
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
        """47都道府県の取得件数をログ出力し、欠落している都道府県を警告する。"""
        total = sum(self._pref_counts.values())
        logger.info("=== %s 都道府県別 取得件数 (合計 %d 件) ===", _CATEGORY, total)
        for pref in _ALL_PREFS:
            logger.info("  %s: %d 件", pref, self._pref_counts.get(pref, 0))
        missing = [p for p in _ALL_PREFS if not self._pref_counts.get(p)]
        if missing:
            logger.warning("取得0件の都道府県 (%d): %s", len(missing), "、".join(missing))
        unknown = self._pref_counts.get("(不明)", 0)
        if unknown:
            logger.warning("都道府県を特定できなかったレコード: %d 件", unknown)
        if self._dup_count:
            logger.warning("重複のため捨てたレコード: %d 件", self._dup_count)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = KouseikyokuIka()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://kouseikyoku.mhlw.go.jp/index.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
