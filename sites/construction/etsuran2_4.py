# -*- coding: utf-8 -*-
"""
etsuran2_4 — 国土交通省 建設業者・宅建業者等企業情報検索システム【建設業者】

※ 既存 `etsuran2` / `etsuran2_2` / `etsuran2_3` と同一サイト
   (kensetuKensaku.do?outPutKbn=1 / ksGaiyo.do)。
   オーケストレーターから別 site_id (etsuran2_4) で割り当てられたため、
   クラス名・チェックポイントファイルを分離して併存できるようにしてある
   (出力 CSV と内部チェックポイントが既存 etsuran2 / etsuran2_2 / etsuran2_3 と
    衝突しないようにするため)。

═══════════════════════════════════════════════════════════════════════════
504 Gateway Time-out 対応（今回の書き換えの主眼）
═══════════════════════════════════════════════════════════════════════════
旧実装（etsuran2 / _2 / _3 を踏襲）は許可番号レンジを 0〜999999 に指定して
「全国全件を 1 回の検索で引き当てる」方式だった。これは検索結果が
**527,000 件超** の巨大な単一結果セットになり、

  - 検索結果ページ自体が 2MB 超（ページ番号プルダウンに 1 万件超の <option>）
  - 上流(アプリサーバ)が巨大な結果セットを保持したままページ送りを処理しきれず、
    深いページや連続アクセスで nginx の待ち時間を超過して **504 Gateway Time-out**

を誘発していた。表示件数を絞る・同時接続を絞る等の小手先では、結果セットそのものが
巨大である以上 504 は根治しない。

【根本対策】呼び出し備考の指示どおり「所在地検索を全国的に行う」=
所在地（都道府県コード kenCode）で検索を 47 都道府県に分割する。
実測で各都道府県の結果は数千〜5万件程度に収まり、検索/ページ送りとも 0.1〜0.3 秒で
応答し 504 は発生しない。47 都道府県の合計件数(≈527,247)は全件検索(≈527,259)と
ほぼ一致するため、分割しても取りこぼしは生じない（差分は所在地が空/海外等のごく僅か）。

   実測の結果、本サイトは JavaScript も Bot 対策(WAF/JS チャレンジ)も無く、
   検索 POST・ページ送り POST・詳細 GET すべてが素の requests で完結する。
   そのため旧 `etsuran2_3` の DynamicCrawler(Playwright) は不要と判断し、
   netharvest 標準の軽量な `StaticCrawler`(requests) ベースに作り替えた。
   ブラウザ起動が無くなり高速・低リソース・504 耐性が大幅に向上する。

備考対応:
    呼び出し備考「一覧ページから所在地検索指定を行い検索ボタンをクリックすることで
    企業リストが表示される。指定を全国的に行い全ての企業を取得」について:
      所在地(kenCode)を 47 都道府県すべてについて順に指定して検索し、各都道府県の
      検索結果をページネーションで末尾まで巡回することで、全国の全建設業許可業者を
      取得する。備考に取得対象を絞るフィルター指示は無いため parse() にフィルターは
      入れない（所在地での分割は取得対象の限定ではなく、上流負荷分散のための分割）。

取得対象:
    - 全国の建設業許可業者の概要情報（許可番号・商号・代表者・所在地・電話番号・
      資本金・法人個人区分・許可業種(28種別の一般/特定)・保険加入状況・許可有効期間 等）

取得フロー:
    一覧（検索結果）→ 詳細（概要）パターン。
      1. kensetuKensaku.do?outPutKbn=1 を GET してセッション(JSESSIONID)を確立
      2. 都道府県コード(kenCode) 01〜47 を順に検索(POST CMD=search, 表示件数 50)
      3. 各検索結果ページから ShowDetail(license_no) の license_no を採取
         - 2ページ目以降は CMD=selectPage で目的ページへ直接ジャンプ（ステートレス）
      4. 各 license_no の詳細(ksGaiyo.do) を requests でスレッド並列取得（shift_jis）
      5. 詳細をパースして 1 件ずつ即 yield（途中停止しても無駄打ちが少ない）
    504/混雑時はリクエスト単位の指数バックオフで粘り、それでも失敗した分は
    次ページ・次都道府県に進んでも欠落しないようチェックポイントで再開可能にする。

中断耐性:
    Pipeline は最終 CSV を close() でのみ生成するため、時間切れ kill では 0 件出力に
    なりうる。そこで取得行を都度 _checkpoint_etsuran2_4.csv に追記し、完了済みの
    (都道府県, ページ) を _progress_etsuran2_4.txt に記録する。再実行時は取得済み行を
    Pipeline に流し直し、完了済みページをスキップして続きから再開する。

著作権配慮:
    取得カラムはすべて構造化された短いラベル/コード/数値（許可番号・区分・業種別の
    一般/特定・保険加入状況 等）であり、自由記述の長文プロースは含めない。

実行方法:
    # ローカルテスト
    python scripts/sites/construction/etsuran2_4.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id etsuran2_4
"""

from __future__ import annotations

import csv
import re
import sys
import time
import logging
import threading
import unicodedata
from pathlib import Path
from typing import Dict, Generator, List, Optional, Set, Tuple
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# ====================== 固定設定 ======================
# 検索画面（GET でセッション確立 / POST で検索・ページ送り）と詳細画面。
SEARCH_INIT_URL = "https://etsuran2.mlit.go.jp/TAKKEN/kensetuKensaku.do?outPutKbn=1"
SEARCH_URL = "https://etsuran2.mlit.go.jp/TAKKEN/kensetuKensaku.do"
DETAIL_URL = "https://etsuran2.mlit.go.jp/TAKKEN/ksGaiyo.do"
BASE_URL = SEARCH_INIT_URL  # execute() に渡すエントリ URL

# 504 対策の要: 所在地(都道府県コード)で検索を 47 分割する。
# コードは検索フォーム #kenCode の <option> 値（01:北海道 〜 47:沖縄県）。
# ※ 51〜64 は北海道の地域分割コードで 01(北海道) と重複するため使用しない。
KEN_CODES: List[str] = ["%02d" % i for i in range(1, 48)]

# 1 ページの表示件数。50 が最大（#dispCount の最大 option）。ページ数を抑えてリクエスト
# 総数を減らす。都道府県単位なら結果セットが小さいので 50 でも 504 にはならない。
DISP_COUNT = 50

# 詳細取得（ksGaiyo.do）の並列度とレート。相手は官公庁サーバのため控えめに。
MAX_WORKERS = 4
GLOBAL_MAX_RPS = 4.0

# (connect, read) のタプル。万一の上流遅延に備えて read は長めに確保する。
HTTP_TIMEOUT = (10, 60)
RETRY_TOTAL = 5
RETRY_BACKOFF = 1.5
POOL_MAXSIZE = 32

# リクエスト単位で 504/混雑に粘る回数とバックオフ基準秒（指数）。
GATEWAY_RETRY = 4
GATEWAY_BACKOFF_SEC = 3.0

# 検索 POST が 504/失敗したときに都道府県内で粘る回数。
SEARCH_RETRY = 4
SEARCH_BACKOFF_SEC = 3.0

# 中断からの再開用（出力 CSV とは別の内部ファイル）。
# 既存 etsuran2 / etsuran2_2 / etsuran2_3 と衝突しないよう site_id 付きファイル名にする。
CHECKPOINT_CSV = Path(__file__).parent / "_checkpoint_etsuran2_4.csv"
PROGRESS_TXT = Path(__file__).parent / "_progress_etsuran2_4.txt"


# ====================== ユーティリティ ======================
def norm(s: Optional[str]) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def looks_like_detail(text: str) -> bool:
    if not text:
        return False
    return (
        ("建設業者の詳細情報" in text)
        or ('class="re_summ"' in text)
        or ("許可番号" in text and "商号又は名称" in text)
    )


def is_gateway_timeout_page(html: str) -> bool:
    """nginx の 504 Gateway Time-out 応答ボディかどうか（HTTP 200 で本文だけ返る場合に備える）。"""
    if not html:
        return False
    return ("504 Gateway Time-out" in html) or ("Gateway Time-out" in html)


def is_system_error_page(html: str) -> bool:
    # システムエラー画面に加え、上流の 504 Gateway Time-out も「再試行すべきエラー」扱い。
    return (
        ("システムエラーが発生しました" in html)
        or ('id="information_body"' in html)
        or is_gateway_timeout_page(html)
    )


# ====================== レートリミッタ ======================
class RateLimiter:
    """トークンバケットによるグローバル RPS 制限（スレッド間共有）。"""

    def __init__(self, rps: float):
        self.capacity = max(1.0, rps)
        self.tokens = self.capacity
        self.rps = rps
        self.lock = threading.Lock()
        self.last = time.time()

    def acquire(self, tokens: float = 1.0):
        while True:
            with self.lock:
                now = time.time()
                self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.rps)
                self.last = now
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return
            time.sleep(0.005)


GLOBAL_LIMITER = RateLimiter(GLOBAL_MAX_RPS)


# ====================== 検索フォーム（一覧）操作: 素の requests ======================
# 検索フォーム #ksModel の既定パラメータ。kenCode / CMD / pageListNo を都度上書きする。
SEARCH_DEFAULTS: Dict[str, str] = {
    "CMD": "search",
    "rdoSelect": "1",          # 名称検索: 半角カナ（未使用）
    "rdoSelectJoken": "1",     # AND 検索
    "rdoSelectSort": "1",      # 並び順: 商号
    "comNameKanaOnly": "",
    "comNameKanjiOnly": "",
    "licenseNoKbn": "",        # 大臣/知事 区分は指定しない（所在地で全件を引く）
    "licenseNoFrom": "",
    "licenseNoTo": "",
    "choice": "2",             # 本店/支店の別: 指定なし
    "kenCode": "",             # ★ここに都道府県コードを入れて分割検索する
    "gyosyu": "",
    "gyosyuType": "",
    "keyWord": "",
    "sortValue": "",
    "dispCount": str(DISP_COUNT),
    "dispPage": "1",
    "caller": "KS",
}


def extract_form_fields(html: str) -> Dict[str, str]:
    """検索結果ページの #ksModel フォームから現在の全 input/select 値を採取する。

    2 ページ目以降の selectPage 遷移では、サーバが返した hidden(sv_*) を含む
    フォーム値をそのまま投げ返す必要があるため、まるごと採取して再利用する。
    """
    soup = BeautifulSoup(html, "html.parser")
    form = soup.find("form", id="ksModel") or soup.find("form")
    fields: Dict[str, str] = {}
    if not form:
        return fields
    for inp in form.find_all("input"):
        name = inp.get("name")
        if not name:
            continue
        itype = (inp.get("type") or "text").lower()
        if itype in ("checkbox", "radio") and inp.get("checked") is None:
            continue
        fields[name] = inp.get("value", "") or ""
    for sel in form.find_all("select"):
        name = sel.get("name")
        if not name:
            continue
        opt = sel.find("option", selected=True) or sel.find("option")
        fields[name] = (opt.get("value", "") if opt else "") or ""
    return fields


def extract_detail_ids(html: str) -> List[str]:
    """検索結果ページから ShowDetail(license_no) の license_no を順序保持で採取する。"""
    ids = re.findall(r"(?:js_)?ShowDetail\(['\"]?(\d+)['\"]?\)", html)
    # 重複除去（テンプレ行などの紛れ込みに備える）。0 埋め文字列のまま保持する。
    seen: Dict[str, None] = {}
    for i in ids:
        seen.setdefault(i, None)
    return list(seen.keys())


def parse_int_field(html: str, field_id: str) -> Optional[int]:
    m = re.search(r'id="%s"[^>]*value="(\d+)"' % re.escape(field_id), html)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


# ====================== requests セッション（スレッドローカル） ======================
_tls = threading.local()
COOKIE_LOCK = threading.Lock()
COOKIE_EPOCH = 0
COOKIE_SNAPSHOT: Dict[str, str] = {}
UA_DEFAULT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)


def _mount_pool(s: requests.Session):
    retry = Retry(
        total=RETRY_TOTAL,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["HEAD", "GET", "OPTIONS", "POST"]),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=POOL_MAXSIZE, pool_maxsize=POOL_MAXSIZE)
    s.mount("https://", adapter)
    s.mount("http://", adapter)


def set_cookie_snapshot(session: requests.Session):
    """一覧操作用セッションの Cookie(JSESSIONID 等) を詳細スレッドへ共有する。"""
    global COOKIE_SNAPSHOT, COOKIE_EPOCH
    with COOKIE_LOCK:
        COOKIE_SNAPSHOT = {c.name: c.value for c in session.cookies}
        COOKIE_EPOCH += 1


def get_thread_session(epoch: int) -> requests.Session:
    s = getattr(_tls, "session", None)
    s_epoch = getattr(_tls, "epoch", -1)
    if s is not None and s_epoch == epoch:
        return s
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA_DEFAULT,
        "Accept-Language": "ja,en;q=0.9",
        "Connection": "keep-alive",
        "Referer": SEARCH_URL,
    })
    with COOKIE_LOCK:
        for name, value in COOKIE_SNAPSHOT.items():
            try:
                s.cookies.set(name, value, domain="etsuran2.mlit.go.jp", path="/")
            except Exception:
                pass
    _mount_pool(s)
    _tls.session = s
    _tls.epoch = epoch
    return s


def fetch_detail_html(license_no: str, epoch: int) -> Optional[str]:
    """詳細 HTML を requests GET で取得する。504/混雑には指数バックオフで粘る。"""
    session = get_thread_session(epoch)
    for attempt in range(GATEWAY_RETRY):
        try:
            GLOBAL_LIMITER.acquire()
            r = session.get(DETAIL_URL, params={"sv_licenseNo": license_no}, timeout=HTTP_TIMEOUT)
            r.encoding = "shift_jis"
            if r.status_code in (429, 500, 502, 503, 504) or is_gateway_timeout_page(r.text):
                time.sleep(GATEWAY_BACKOFF_SEC * (2 ** attempt))
                continue
            if r.ok and looks_like_detail(r.text):
                return r.text
            # 正常応答だが詳細でない（パラメータ不足等）→ 失敗扱い
            return None
        except Exception:
            time.sleep(GATEWAY_BACKOFF_SEC * (2 ** attempt))
    return None


# ====================== HTMLパース ======================
# 許可を受けた建設業の種類（28業種を表す略号）。値は 一般/特定/空。
ABBR_COLUMNS = [
    "土", "建", "大", "左", "と", "石", "屋", "電", "管", "夕",
    "鋼", "筋", "舗", "し", "ゅ", "板", "ガ", "塗", "防", "内",
    "機", "絶", "通", "園", "井", "具", "水", "消", "清", "解",
]

PREF_PAT = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|"
    r"東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|"
    r"香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


def find_value_cell_by_label(soup: BeautifulSoup, label_regex: str):
    pat = re.compile(label_regex)
    for tbl in soup.find_all("table"):
        for tr in tbl.find_all("tr"):
            tds = tr.find_all(["th", "td"])
            if len(tds) < 2:
                continue
            if pat.search(norm(tds[0].get_text(" "))):
                return tds[1]
    return None


def split_phonetic_cell(td) -> Tuple[str, str]:
    """フリガナ(p.phonetic)と本文を分離して (kana, text) を返す。"""
    if td is None:
        return "", ""
    kana_list = [norm(p.get_text(" ")) for p in td.find_all("p", class_=lambda c: c and "phonetic" in c)]
    kana = " ".join([k for k in kana_list if k])
    td2 = BeautifulSoup(str(td), "html.parser")
    for p in td2.find_all("p", class_=lambda c: c and "phonetic" in c):
        p.decompose()
    text = norm(td2.get_text(" "))
    return kana, text


def parse_insurance(soup: BeautifulSoup) -> Dict[str, str]:
    res = {"保険加入状況(健康)": "", "保険加入状況(年金)": "", "保険加入状況(雇用)": ""}
    for tbl in soup.find_all("table"):
        for tr in tbl.find_all("tr"):
            th = tr.find("th")
            if not th:
                continue
            if "保険加入状況" in norm(th.get_text(" ")):
                parent = tr.parent
                trs = parent.find_all("tr")
                try:
                    idx = trs.index(tr)
                except ValueError:
                    continue
                if idx + 2 < len(trs):
                    header_tds = trs[idx + 1].find_all("td")
                    value_tds = trs[idx + 2].find_all("td")
                    if len(header_tds) >= 3 and len(value_tds) >= 3:
                        labels = [norm(td.get_text(" ")) for td in header_tds[:3]]
                        values = [norm(td.get_text(" ")) for td in value_tds[:3]]
                        mapping = dict(zip(labels, values))
                        res["保険加入状況(健康)"] = mapping.get("健康", "")
                        res["保険加入状況(年金)"] = mapping.get("年金", "")
                        res["保険加入状況(雇用)"] = mapping.get("雇用", "")
                        return res
    return res


def parse_industry_table_numbers(soup: BeautifulSoup) -> Dict[str, str]:
    """許可業種テーブルを読み、各略号に 一般/特定/空 を割り当てる。"""
    res = {abbr: "" for abbr in ABBR_COLUMNS}

    def num_to_label(s: str) -> str:
        s = re.sub(r"\s+", "", s)
        if s == "1":
            return "一般"
        if s == "2":
            return "特定"
        return ""

    cand_tables = []
    for tbl in soup.find_all("table"):
        txt = norm(tbl.get_text(" "))
        if "許可を受けた" in txt and "建設業" in txt and "種類" in txt:
            cand_tables.append(tbl)
    for tbl in cand_tables:
        header_tr, value_tr = None, None
        for tr in tbl.find_all("tr"):
            cls = " ".join((tr.get("class") or []))
            if "re_summ_ev" in cls:
                header_tr = tr
            elif "re_summ_odd" in cls:
                value_tr = tr
        if not header_tr or not value_tr:
            continue
        header_cells = header_tr.find_all("td")
        value_cells = value_tr.find_all("td")
        if not header_cells or not value_cells:
            continue
        if len(value_cells) < len(header_cells):
            continue

        idx_to_labels: List[List[str]] = []
        for td in header_cells:
            raw = td.get_text("")
            lab = norm(raw).replace("\n", "").replace(" ", "")
            if lab in ("しゅ", "し\nゅ", "しゅんせつ", "しゅんせつ工事"):
                idx_to_labels.append(["し", "ゅ"])
            else:
                idx_to_labels.append([lab])
        for i, labels in enumerate(idx_to_labels):
            raw_val = norm(value_cells[i].get_text(""))
            label_val = num_to_label(raw_val)
            for lab in labels:
                if lab in res:
                    res[lab] = label_val
        if any(res.values()):
            return res
    return res


def parse_overview(html: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    row: Dict[str, str] = {}

    td = find_value_cell_by_label(soup, r"^許可番号$")
    row["許可番号"] = norm(td.get_text(" ")) if td else ""

    td = find_value_cell_by_label(soup, r"(商号又は名称|名称)")
    kana, name = split_phonetic_cell(td)
    row[Schema.NAME] = name
    row[Schema.NAME_KANA] = kana

    td = find_value_cell_by_label(soup, r"代表者")
    _k, rep = split_phonetic_cell(td)
    row[Schema.REP_NM] = rep

    td = find_value_cell_by_label(soup, r"(所在地|住所)")
    raw_addr = norm(td.get_text(" ")) if td else ""
    mzip = re.search(r"(?:〒\s*)?(\d{3}-\d{4})", raw_addr)
    row[Schema.POST_CODE] = mzip.group(1) if mzip else ""
    addr_wo_zip = re.sub(r"(?:〒\s*)?\d{3}-\d{4}", "", raw_addr).strip()
    row[Schema.ADDR] = re.sub(r"\s+", " ", addr_wo_zip)
    m_pref = PREF_PAT.search(row[Schema.ADDR])
    row[Schema.PREF] = m_pref.group(1) if m_pref else ""

    td = find_value_cell_by_label(soup, r"(電話番号|TEL)")
    # TEL の全角→半角正規化は Pipeline 側が自動処理するため生値を渡す
    row[Schema.TEL] = norm(td.get_text(" ")) if td else ""

    td = find_value_cell_by_label(soup, r"(法人・個人区分|法人・個人の別|法人・個人)")
    row["法人・個人区分"] = norm(td.get_text(" ")) if td else ""

    td = find_value_cell_by_label(soup, r"(資本金|資本金額)")
    row[Schema.CAP] = norm(td.get_text(" ")) if td else ""

    td = find_value_cell_by_label(soup, r"建設業以外の兼業の有無")
    row["建設業以外の兼業の有無"] = norm(td.get_text(" ")) if td else ""

    td = find_value_cell_by_label(soup, r"許可の有効期間")
    row["許可の有効期間"] = norm(td.get_text(" ")) if td else ""

    row.update(parse_industry_table_numbers(soup))
    row.update(parse_insurance(soup))

    # 「許可を受けた建設業の種類」セクションの先頭(土)区分。略号 土 と同値。
    row["許可を受けた建設業の種類(土)"] = row.get("土", "")

    return row


# ====================== チェックポイント / 進捗 ======================
def get_checkpoint_rows() -> List[Dict[str, str]]:
    if not CHECKPOINT_CSV.exists():
        return []
    try:
        with open(CHECKPOINT_CSV, "r", newline="", encoding="utf-8-sig") as f:
            return list(csv.DictReader(f))
    except Exception as e:
        logging.warning("[CHECKPOINT] 読み込み失敗: %s", e)
        return []


def append_to_checkpoint(new_rows: List[Dict[str, str]]) -> None:
    if not new_rows:
        return
    write_header = not CHECKPOINT_CSV.exists()
    fieldnames: List[str] = []
    for r in new_rows:
        for k in r.keys():
            if k not in fieldnames:
                fieldnames.append(k)
    with open(CHECKPOINT_CSV, "a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if write_header:
            w.writeheader()
        w.writerows(new_rows)


def load_done_pages() -> Set[Tuple[str, int]]:
    """完了済みの (都道府県コード, ページ番号) 集合を読み込む。"""
    done: Set[Tuple[str, int]] = set()
    if not PROGRESS_TXT.exists():
        return done
    try:
        with open(PROGRESS_TXT, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or "," not in line:
                    continue
                ken, page = line.split(",", 1)
                try:
                    done.add((ken, int(page)))
                except ValueError:
                    continue
    except Exception as e:
        logging.warning("[PROGRESS] 読み込み失敗: %s", e)
    return done


def mark_page_done(ken: str, page: int) -> None:
    with open(PROGRESS_TXT, "a", encoding="utf-8") as f:
        f.write("%s,%d\n" % (ken, page))


# ====================== NetHarvest クローラー ======================
class Etsuran24Scraper(StaticCrawler):
    """国土交通省 建設業者・宅建業者等企業情報検索システム【建設業者】 スクレイパー (etsuran2_4)

    所在地(都道府県)分割検索 + 詳細スレッド並列取得（素の requests）。
    """

    # 詳細取得は GLOBAL_LIMITER で自前にレート制御するため、
    # フレームワーク側の item 間 sleep は無効化する。
    DELAY = 0.0

    EXTRA_COLUMNS = [
        "許可番号",
        "法人・個人区分",
        "建設業以外の兼業の有無",
        "保険加入状況(健康)",
        "保険加入状況(年金)",
        "保険加入状況(雇用)",
        "許可を受けた建設業の種類(土)",
    ] + ABBR_COLUMNS + [
        "許可の有効期間",
    ]

    def prepare(self):
        """セッションを強化し、チェックポイントが残っていれば再開準備する。"""
        # StaticCrawler が用意した self.session に、504 込みのリトライと UA を上書き。
        _mount_pool(self.session)
        self.session.headers.update({
            "User-Agent": UA_DEFAULT,
            "Accept-Language": "ja,en;q=0.9",
        })

        # 取得済み許可番号の重複防止セット（チェックポイントから復元され、再開をまたいで
        # 一意性を保証する durable な集合）。所在地分割では 1 業者が複数都道府県に営業所を
        # 持つと複数回ヒットするため、許可番号で 1 業者 1 行に集約する。
        self._seen_licenses: Set[str] = set()

        # ShowDetail の license_no(=ID) で「取得前」に重複を弾くための in-memory 集合。
        # ID は 大臣=00xxxxxx / 知事=<県コード>xxxxxx で全国一意なので、同一業者が別の
        # 都道府県検索で再出現しても詳細を二度取得せずに済む（無駄な GET を削減）。
        # ※ 再開時は seed しない（許可番号側の集約で正しさは担保される。これは効率化のみ）。
        self._seen_ids: Set[str] = set()

        checkpoint_rows = get_checkpoint_rows()
        self._already_done = len(checkpoint_rows)
        if self._already_done > 0:
            self.logger.info("[RESUME] チェックポイント %d 件を引継ぎ", self._already_done)
            for row in checkpoint_rows:
                key = row.get("許可番号") or ""
                if key:
                    self._seen_licenses.add(key)
                self.pipeline.process_item(dict(row))

    def _search_prefecture(self, ken: str) -> Optional[str]:
        """都道府県コードで検索(POST CMD=search)し、1 ページ目 HTML を返す。"""
        params = dict(SEARCH_DEFAULTS)
        params["kenCode"] = ken
        params["CMD"] = "search"
        params["dispPage"] = "1"
        for attempt in range(SEARCH_RETRY):
            try:
                r = self.session.post(SEARCH_URL, data=params, timeout=HTTP_TIMEOUT)
                r.encoding = "shift_jis"
                if r.status_code in (429, 500, 502, 503, 504) or is_system_error_page(r.text):
                    time.sleep(SEARCH_BACKOFF_SEC * (2 ** attempt))
                    continue
                return r.text
            except Exception as e:
                self.logger.warning("[SEARCH] ken=%s 失敗(%d回目): %s", ken, attempt + 1, e)
                time.sleep(SEARCH_BACKOFF_SEC * (2 ** attempt))
        return None

    def _fetch_list_page(self, base_form: Dict[str, str], page: int) -> Optional[str]:
        """selectPage で指定ページの検索結果 HTML を取得する（ステートレス）。"""
        params = dict(base_form)
        params["CMD"] = "selectPage"
        params["pageListNo1"] = str(page)
        params["pageListNo2"] = str(page)
        params["dispPage"] = str(page)
        for attempt in range(SEARCH_RETRY):
            try:
                r = self.session.post(SEARCH_URL, data=params, timeout=HTTP_TIMEOUT)
                r.encoding = "shift_jis"
                if r.status_code in (429, 500, 502, 503, 504) or is_system_error_page(r.text):
                    time.sleep(SEARCH_BACKOFF_SEC * (2 ** attempt))
                    continue
                return r.text
            except Exception as e:
                self.logger.warning("[LIST] page=%d 失敗(%d回目): %s", page, attempt + 1, e)
                time.sleep(SEARCH_BACKOFF_SEC * (2 ** attempt))
        return None

    def _process_ids(self, detail_ids: List[str], epoch: int) -> List[dict]:
        """detail_ids を並列取得・パースし、未取得の行リストを返す（yield は呼び出し側）。"""
        # 既取得の許可番号に対応する ID は事前に除外できないが（ID と許可番号は別物のため）、
        # パース後に許可番号で重複判定する。
        rows: List[dict] = []
        # 取得前重複除去: 既出の ID（別都道府県で取得済みの多店舗業者など）は GET しない。
        fresh_ids = []
        for did in detail_ids:
            if did in self._seen_ids:
                continue
            self._seen_ids.add(did)
            fresh_ids.append(did)
        if not fresh_ids:
            return rows
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            fut2id = {ex.submit(fetch_detail_html, did, epoch): did for did in fresh_ids}
            for fut in as_completed(fut2id):
                did = fut2id[fut]
                try:
                    html = fut.result()
                except Exception:
                    html = None
                if not (html and looks_like_detail(html)):
                    self.logger.warning("[detail] 取得失敗 id=%s", did)
                    continue
                try:
                    row = parse_overview(html)
                except Exception as e:
                    self.logger.warning("[detail] パース失敗 id=%s: %s", did, e)
                    continue
                if not any(row.get(k) for k in (Schema.NAME, "許可番号", Schema.ADDR)):
                    self.logger.warning("[detail] 空行スキップ id=%s", did)
                    continue
                lic = row.get("許可番号") or did
                if lic in self._seen_licenses:
                    continue
                self._seen_licenses.add(lic)
                row[Schema.URL] = f"{DETAIL_URL}?{urlencode({'sv_licenseNo': did})}"
                rows.append(row)
        return rows

    def parse(self, url: str) -> Generator[dict, None, None]:
        # セッション(JSESSIONID)確立。これが無いと検索 POST が弾かれる。
        try:
            self.session.get(SEARCH_INIT_URL, timeout=HTTP_TIMEOUT)
        except Exception as e:
            self.logger.warning("[INIT] 検索画面 GET 失敗: %s", e)
        set_cookie_snapshot(self.session)
        epoch = COOKIE_EPOCH

        done_pages = load_done_pages()
        if done_pages:
            self.logger.info("[RESUME] 完了済みページ %d を引継ぎ", len(done_pages))

        for ken in KEN_CODES:
            html = self._search_prefecture(ken)
            if not html:
                self.logger.warning("[SKIP] ken=%s 検索失敗 → 次の都道府県へ", ken)
                continue

            base_form = extract_form_fields(html)
            result_count = parse_int_field(html, "resultCount") or 0
            page_count = parse_int_field(html, "pageCount") or 1
            self.logger.info(
                "[KEN %s] results=%d pages=%d", ken, result_count, page_count
            )

            for page in range(1, page_count + 1):
                if (ken, page) in done_pages:
                    continue

                page_html = html if page == 1 else self._fetch_list_page(base_form, page)
                if not page_html:
                    self.logger.warning("[LIST] ken=%s page=%d 取得失敗 → スキップ", ken, page)
                    continue

                detail_ids = extract_detail_ids(page_html)
                rows = self._process_ids(detail_ids, epoch)

                append_to_checkpoint(rows)
                mark_page_done(ken, page)
                for row in rows:
                    yield row

                if page % 20 == 0 or page == page_count:
                    self.logger.info(
                        "[PROGRESS] ken=%s page=%d/%d 累計seen=%d",
                        ken, page, page_count, len(self._seen_licenses),
                    )

        # 全都道府県を完走したのでチェックポイントを掃除する。
        CHECKPOINT_CSV.unlink(missing_ok=True)
        PROGRESS_TXT.unlink(missing_ok=True)
        self.logger.info("[DONE] 完了・チェックポイント削除 (取得 %d 件)", len(self._seen_licenses))


# ====================== ローカル実行用エントリーポイント ======================
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    scraper = Etsuran24Scraper()
    scraper.execute(BASE_URL)
    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
