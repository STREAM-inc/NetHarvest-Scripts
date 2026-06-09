# -*- coding: utf-8 -*-
"""
etsuran2_5 — 国土交通省 建設業者・宅建業者等企業情報検索システム【マンション管理業者】

※ 既存 `etsuran2` / `etsuran2_2` / `etsuran2_3` / `etsuran2_4` と同一サイト系
   (etsuran2.mlit.go.jp/TAKKEN/) だが、対象が異なる。
   - etsuran2 系 (_1〜_4): 建設業者 (kensetuKensaku.do / ksGaiyo.do)
   - 本モジュール (_5)   : マンション管理業者 (mansionKensaku.do / msGaiyo.do)
   オーケストレーターから別 site_id (etsuran2_5) で割り当てられたため、
   クラス名・チェックポイントファイルを分離して併存できるようにしてある
   (出力 CSV と内部チェックポイントが既存 etsuran2 系と衝突しないようにするため)。

═══════════════════════════════════════════════════════════════════════════
建設業者(_4)との設計差分
═══════════════════════════════════════════════════════════════════════════
建設業者は全国 527,000 件超という巨大な単一結果セットになり 504 を誘発するため
所在地(都道府県コード)で 47 分割して検索していた。
本サイト(マンション管理業者)は **全国全件でも 2,575 件 / 52 ページ** と小さく、
ページ番号プルダウンも軽量で 504 のリスクが無い(実測で検索/ページ送りとも 0.1〜0.3 秒)。
そのため所在地分割は不要と判断し、**全国 1 回の検索 + ページネーション巡回** に簡素化した。
全国検索は各登録業者を 1 回ずつ返すため重複も発生しないが、念のため登録番号で
1 業者 1 行に集約する(防御的)。

   実測の結果、本サイトは JavaScript も Bot 対策(WAF/JS チャレンジ)も無く、
   検索 POST・ページ送り POST・詳細 POST すべてが素の requests で完結する。
   そのため netharvest 標準の軽量な `StaticCrawler`(requests) ベースで実装する
   (Selenium / Playwright は使わない)。

詳細取得の差分:
   建設業者は詳細を GET(?sv_licenseNo=) で取得できたが、マンション管理業者は
   一覧の js_ShowDetail(strCompanySeq) が「フォームの companySeq を埋めて
   msGaiyo.do へ POST submit」する方式。実測で **POST {"companySeq": <seq>}** の
   最小ペイロード(セッション Cookie 付き)だけで詳細 HTML が返ることを確認済み。

取得対象:
    - 全国のマンション管理業者の概要情報(登録番号・商号・代表者・主たる事務所の
      所在地・資本金・基準資産額・法人個人区分・登録有効期間・最初の登録年月日)。
    - 本サイトには電話番号/郵便番号/業種テーブル/保険加入状況の掲載は無い。

取得フロー:
    一覧(検索結果)→ 詳細(概要)パターン。
      1. mansionKensaku.do?outPutKbn=1 を GET してセッション(JSESSIONID)を確立
      2. 全国検索(POST CMD=search, kenCode="", 表示件数 50)で 1 ページ目を取得
      3. 各検索結果ページから ShowDetail(companySeq) の companySeq を採取
         - 2ページ目以降は CMD=selectPage で目的ページへジャンプ
           (結果ページの sv_* hidden を含むフォームをそのまま投げ返す必要がある)
      4. 各 companySeq の詳細(msGaiyo.do) を requests POST でスレッド並列取得(shift_jis)
      5. 詳細をパースして 1 件ずつ即 yield(途中停止しても無駄打ちが少ない Pattern B)
    504/混雑時はリクエスト単位の指数バックオフで粘る。

中断耐性:
    Pipeline は最終 CSV を close() でのみ生成するため、時間切れ kill では 0 件出力に
    なりうる。そこで取得行を都度 _checkpoint_etsuran2_5.csv に追記し、完了済みページを
    _progress_etsuran2_5.txt に記録する。再実行時は取得済み行を Pipeline に流し直し、
    完了済みページをスキップして続きから再開する。

著作権配慮:
    取得カラムはすべて構造化された短いラベル/コード/数値/日付(登録番号・区分・
    資本金・基準資産額・有効期間 等)であり、自由記述の長文プロースは含めない。

実行方法:
    # ローカルテスト
    python scripts/sites/construction/etsuran2_5.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id etsuran2_5
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
# 検索画面(GET でセッション確立 / POST で検索・ページ送り)と詳細画面。
SEARCH_INIT_URL = "https://etsuran2.mlit.go.jp/TAKKEN/mansionKensaku.do?outPutKbn=1"
SEARCH_URL = "https://etsuran2.mlit.go.jp/TAKKEN/mansionKensaku.do"
DETAIL_URL = "https://etsuran2.mlit.go.jp/TAKKEN/msGaiyo.do"
BASE_URL = SEARCH_INIT_URL  # execute() に渡すエントリ URL

# 1 ページの表示件数。50 が最大(#dispCount の最大 option)。全国でも 52 ページなので
# 50 でリクエスト総数を抑える(結果セットが小さく 504 にはならない)。
DISP_COUNT = 50

# 詳細取得(msGaiyo.do)の並列度とレート。相手は官公庁サーバのため控えめに。
MAX_WORKERS = 4
GLOBAL_MAX_RPS = 4.0

# (connect, read) のタプル。万一の上流遅延に備えて read は長めに確保する。
HTTP_TIMEOUT = (10, 60)
RETRY_TOTAL = 5
RETRY_BACKOFF = 1.5
POOL_MAXSIZE = 32

# リクエスト単位で 504/混雑に粘る回数とバックオフ基準秒(指数)。
GATEWAY_RETRY = 4
GATEWAY_BACKOFF_SEC = 3.0

# 検索/ページ送り POST が 504/失敗したときに粘る回数。
SEARCH_RETRY = 4
SEARCH_BACKOFF_SEC = 3.0

# 中断からの再開用(出力 CSV とは別の内部ファイル)。
# 既存 etsuran2 系と衝突しないよう site_id 付きファイル名にする。
CHECKPOINT_CSV = Path(__file__).parent / "_checkpoint_etsuran2_5.csv"
PROGRESS_TXT = Path(__file__).parent / "_progress_etsuran2_5.txt"


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
        ('class="re_summ"' in text)
        or ("登録番号" in text and "商号又は名称" in text)
        or ("登録番号" in text and "基準資産額" in text)
    )


def is_gateway_timeout_page(html: str) -> bool:
    """nginx の 504 Gateway Time-out 応答ボディかどうか(HTTP 200 で本文だけ返る場合に備える)。"""
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
    """トークンバケットによるグローバル RPS 制限(スレッド間共有)。"""

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


# ====================== 検索フォーム(一覧)操作: 素の requests ======================
# 検索フォーム #msModel の既定パラメータ。CMD / pageListNo を都度上書きする。
# kenCode="" = 全国全件(マンション管理業者は全国でも 2,575 件と小さいので分割不要)。
SEARCH_DEFAULTS: Dict[str, str] = {
    "CMD": "search",
    "rdoSelect": "1",          # 名称検索: 半角カナ(未使用)
    "rdoSelectJoken": "1",     # AND 検索
    "rdoSelectSort": "1",      # 並び順: 登録番号
    "comNameKanaOnly": "",
    "comNameKanjiOnly": "",
    "licenseNoKbn": "",        # 大臣/知事 区分は指定しない(全国全件を引く)
    "licenseNoFrom": "",
    "licenseNoTo": "",
    "choice": "2",             # 本店/支店の別: 指定なし
    "kenCode": "",             # 全国(空)
    "sortValue": "",
    "dispCount": str(DISP_COUNT),
    "dispPage": "1",
    "caller": "MS",
    "companySeq": "",
}


def extract_form_fields(html: str) -> Dict[str, str]:
    """検索結果ページの #msModel フォームから現在の全 input/select 値を採取する。

    2 ページ目以降の selectPage 遷移では、サーバが返した hidden(sv_*) を含む
    フォーム値をそのまま投げ返す必要があるため、まるごと採取して再利用する。
    """
    soup = BeautifulSoup(html, "html.parser")
    form = soup.find("form", id="msModel") or soup.find("form")
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
    """検索結果ページから ShowDetail(companySeq) の companySeq を順序保持で採取する。"""
    ids = re.findall(r"(?:js_)?ShowDetail\(['\"]?(\d+)['\"]?\)", html)
    # 重複除去(同一ページにテンプレ行などが紛れ込む場合に備える)。
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


# ====================== requests セッション(スレッドローカル) ======================
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


def fetch_detail_html(company_seq: str, epoch: int) -> Optional[str]:
    """詳細 HTML を requests POST で取得する。504/混雑には指数バックオフで粘る。

    js_ShowDetail(seq) はフォームの companySeq を埋めて msGaiyo.do へ POST submit する。
    実測で最小ペイロード {"companySeq": seq}(セッション Cookie 付き)で詳細が返る。
    """
    session = get_thread_session(epoch)
    for attempt in range(GATEWAY_RETRY):
        try:
            GLOBAL_LIMITER.acquire()
            r = session.post(DETAIL_URL, data={"companySeq": company_seq}, timeout=HTTP_TIMEOUT)
            r.encoding = "shift_jis"
            if r.status_code in (429, 500, 502, 503, 504) or is_gateway_timeout_page(r.text):
                time.sleep(GATEWAY_BACKOFF_SEC * (2 ** attempt))
                continue
            if r.ok and looks_like_detail(r.text):
                return r.text
            # 正常応答だが詳細でない(パラメータ不足/セッション切れ等)→ 失敗扱い
            return None
        except Exception:
            time.sleep(GATEWAY_BACKOFF_SEC * (2 ** attempt))
    return None


# ====================== HTMLパース ======================
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


def parse_overview(html: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    row: Dict[str, str] = {}

    td = find_value_cell_by_label(soup, r"^登録番号$")
    row["登録番号"] = norm(td.get_text(" ")) if td else ""

    td = find_value_cell_by_label(soup, r"登録の有効期間")
    row["登録の有効期間"] = norm(td.get_text(" ")) if td else ""

    td = find_value_cell_by_label(soup, r"(法人・個人の別|法人・個人区分|法人・個人)")
    row["法人・個人区分"] = norm(td.get_text(" ")) if td else ""

    td = find_value_cell_by_label(soup, r"最初の登録年月日")
    row["最初の登録年月日"] = norm(td.get_text(" ")) if td else ""

    td = find_value_cell_by_label(soup, r"(商号又は名称|名称)")
    kana, name = split_phonetic_cell(td)
    row[Schema.NAME] = name
    row[Schema.NAME_KANA] = kana

    td = find_value_cell_by_label(soup, r"代表者")
    _k, rep = split_phonetic_cell(td)
    row[Schema.REP_NM] = rep

    td = find_value_cell_by_label(soup, r"(主たる事務所|所在地|住所)")
    raw_addr = norm(td.get_text(" ")) if td else ""
    # 本サイトの住所には郵便番号の掲載は無いが、念のため検出して分離する。
    mzip = re.search(r"(?:〒\s*)?(\d{3}-\d{4})", raw_addr)
    row[Schema.POST_CODE] = mzip.group(1) if mzip else ""
    addr_wo_zip = re.sub(r"(?:〒\s*)?\d{3}-\d{4}", "", raw_addr).strip()
    row[Schema.ADDR] = re.sub(r"\s+", " ", addr_wo_zip)
    m_pref = PREF_PAT.search(row[Schema.ADDR])
    row[Schema.PREF] = m_pref.group(1) if m_pref else ""

    td = find_value_cell_by_label(soup, r"(資本金|資本金額)")
    row[Schema.CAP] = norm(td.get_text(" ")) if td else ""

    td = find_value_cell_by_label(soup, r"基準資産額")
    row["基準資産額"] = norm(td.get_text(" ")) if td else ""

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


def load_done_pages() -> Set[int]:
    """完了済みページ番号の集合を読み込む。"""
    done: Set[int] = set()
    if not PROGRESS_TXT.exists():
        return done
    try:
        with open(PROGRESS_TXT, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    done.add(int(line))
                except ValueError:
                    continue
    except Exception as e:
        logging.warning("[PROGRESS] 読み込み失敗: %s", e)
    return done


def mark_page_done(page: int) -> None:
    with open(PROGRESS_TXT, "a", encoding="utf-8") as f:
        f.write("%d\n" % page)


# ====================== NetHarvest クローラー ======================
class Etsuran25Scraper(StaticCrawler):
    """国土交通省 建設業者・宅建業者等企業情報検索システム【マンション管理業者】 スクレイパー (etsuran2_5)

    全国 1 回の検索 + ページネーション巡回 + 詳細スレッド並列取得(素の requests)。
    """

    # 詳細取得は GLOBAL_LIMITER で自前にレート制御するため、
    # フレームワーク側の item 間 sleep は無効化する。
    DELAY = 0.0

    EXTRA_COLUMNS = [
        "登録番号",
        "登録の有効期間",
        "法人・個人区分",
        "最初の登録年月日",
        "基準資産額",
    ]

    def prepare(self):
        """セッションを強化し、チェックポイントが残っていれば再開準備する。"""
        # StaticCrawler が用意した self.session に、504 込みのリトライと UA を上書き。
        _mount_pool(self.session)
        self.session.headers.update({
            "User-Agent": UA_DEFAULT,
            "Accept-Language": "ja,en;q=0.9",
        })

        # 取得済み登録番号の重複防止セット(チェックポイントから復元され、再開をまたいで
        # 一意性を保証する)。全国検索は通常 1 業者 1 行だが防御的に集約する。
        self._seen_licenses: Set[str] = set()

        # ShowDetail の companySeq(=ID) で「取得前」に重複を弾く in-memory 集合。
        self._seen_ids: Set[str] = set()

        checkpoint_rows = get_checkpoint_rows()
        self._already_done = len(checkpoint_rows)
        if self._already_done > 0:
            self.logger.info("[RESUME] チェックポイント %d 件を引継ぎ", self._already_done)
            for row in checkpoint_rows:
                key = row.get("登録番号") or ""
                if key:
                    self._seen_licenses.add(key)
                self.pipeline.process_item(dict(row))

    def _search_nationwide(self) -> Optional[str]:
        """全国検索(POST CMD=search, kenCode="")し、1 ページ目 HTML を返す。"""
        params = dict(SEARCH_DEFAULTS)
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
                self.logger.warning("[SEARCH] 全国検索 失敗(%d回目): %s", attempt + 1, e)
                time.sleep(SEARCH_BACKOFF_SEC * (2 ** attempt))
        return None

    def _fetch_list_page(self, base_form: Dict[str, str], page: int) -> Optional[str]:
        """selectPage で指定ページの検索結果 HTML を取得する。

        結果ページの sv_* hidden を含むフォーム(base_form)に CMD=selectPage と
        目的ページ番号を上書きして投げ返す(これが無いと 1 ページ目に戻ってしまう)。
        """
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
        """detail_ids を並列取得・パースし、未取得の行リストを返す(yield は呼び出し側)。"""
        rows: List[dict] = []
        # 取得前重複除去: 既出の companySeq は POST しない。
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
                if not any(row.get(k) for k in (Schema.NAME, "登録番号", Schema.ADDR)):
                    self.logger.warning("[detail] 空行スキップ id=%s", did)
                    continue
                lic = row.get("登録番号") or did
                if lic in self._seen_licenses:
                    continue
                self._seen_licenses.add(lic)
                row[Schema.URL] = f"{DETAIL_URL}?{urlencode({'companySeq': did})}"
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

        html = self._search_nationwide()
        if not html:
            self.logger.error("[FATAL] 全国検索に失敗。0 件で終了。")
            return

        base_form = extract_form_fields(html)
        result_count = parse_int_field(html, "resultCount") or 0
        page_count = parse_int_field(html, "pageCount") or 1
        self.total_items = result_count
        self.logger.info("[SETUP] results=%d pages=%d", result_count, page_count)

        for page in range(1, page_count + 1):
            if page in done_pages:
                continue

            page_html = html if page == 1 else self._fetch_list_page(base_form, page)
            if not page_html:
                self.logger.warning("[LIST] page=%d 取得失敗 → スキップ", page)
                continue

            detail_ids = extract_detail_ids(page_html)
            rows = self._process_ids(detail_ids, epoch)

            append_to_checkpoint(rows)
            mark_page_done(page)
            for row in rows:
                yield row

            if page % 10 == 0 or page == page_count:
                self.logger.info(
                    "[PROGRESS] page=%d/%d 累計seen=%d", page, page_count, len(self._seen_licenses)
                )

        # 全ページを完走したのでチェックポイントを掃除する。
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
    scraper = Etsuran25Scraper()
    scraper.execute(BASE_URL)
    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
