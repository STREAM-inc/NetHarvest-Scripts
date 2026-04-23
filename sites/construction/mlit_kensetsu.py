# -*- coding: utf-8 -*-
"""
MLIT 建設業者・宅建業者等企業情報検索システム（建設業）
爆速版：パイプライン並列HTTP取得 + 最小Selenium

・一覧のページングとhidden採取のみSelenium（画像/スタイル/フォント無効）
・詳細は requests 直叩きをスレッド並列で実行（接続プール + 自動リトライ）
・429/5xxや空ページ連続はクールダウン→再入場
・直叩きで不十分なIDは最後にSeleniumフォールバック
"""

from __future__ import annotations
import csv
import os
import re
import sys
import time
import logging
import datetime as dt
import threading
from pathlib import Path
from typing import Dict, Generator, List, Optional, Tuple
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# ====================== 固定設定 ======================
BASE_URL = "https://etsuran2.mlit.go.jp/TAKKEN/kensetuKensaku.do?outPutKbn=1"
DETAIL_URL = "https://etsuran2.mlit.go.jp/TAKKEN/ksGaiyo.do"

HEADLESS = True
WAIT_SEC = 20
PAGE_NAV_WAIT = 50
ACTION_SLEEP = 0.15
DETAIL_SLEEP = 0.10

MAX_WORKERS = 16
GLOBAL_MAX_RPS = 14.0
COOLDOWN_SEC = 60
EMPTY_PAGE_THRES = 3
HTTP_TIMEOUT = 10
RETRY_TOTAL = 5
POOL_MAXSIZE = 64

START_RAW = int(os.getenv("START_RAW") or 1)
END_RAW = int(os.getenv("END_RAW") or 10**12)

CHECKPOINT_CSV = Path(__file__).parent / "_checkpoint_mlit_kensetsu.csv"
MAX_AUTO_RESTART = 10
RESTART_WAIT_SEC = 30

# ====================== ユーティリティ ======================
import unicodedata


def norm(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def looks_like_detail(text: str) -> bool:
    if not text:
        return False
    return ("建設業者の詳細情報" in text) or ('class="re_summ"' in text) or ("許可番号" in text and "商号又は名称" in text)


def tel_to_digits_text(raw: str) -> str:
    digits = re.sub(r"[^0-9]", "", str(raw or ""))
    return f"'{digits}" if digits else ""


# ====================== レートリミッタ ======================
class RateLimiter:
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

# ====================== Selenium（一覧のみ） ======================
def make_driver(headless: bool) -> webdriver.Chrome:
    opts = ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
        opts.add_argument("--disable-gpu")
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 2,
        "profile.managed_default_content_settings.fonts": 2,
        "profile.default_content_setting_values.notifications": 2,
    }
    opts.add_experimental_option("prefs", prefs)
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--disable-features=TranslateUI,BackForwardCache,PaintHolding")
    opts.add_argument("--window-size=1200,1600")
    opts.add_argument("--lang=ja-JP")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(15)
    driver.implicitly_wait(0)
    return driver


def wait_dom_complete(drv: webdriver.Chrome):
    WebDriverWait(drv, WAIT_SEC).until(lambda d: d.execute_script("return document.readyState") == "complete")


def is_system_error_page(html: str) -> bool:
    return ("システムエラーが発生しました" in html) or ('id="information_body"' in html)


def open_search_and_submit(drv: webdriver.Chrome):
    backoff = 1.0
    for attempt in range(6):
        drv.get(BASE_URL)
        wait_dom_complete(drv)
        time.sleep(ACTION_SLEEP)
        if is_system_error_page(drv.page_source):
            time.sleep(backoff)
            backoff = min(backoff * 2, 10)
            continue
        break

    def set_value(selector):
        els = drv.find_elements(By.CSS_SELECTOR, selector)
        if els:
            el = els[0]
            try:
                el.clear()
            except Exception:
                pass
            el.send_keys("")
            return el
        return None

    try:
        f = set_value("#licenseNoFrom") or set_value("input[name*='licenseNoFrom']")
        t = set_value("#licenseNoTo") or set_value("input[name*='licenseNoTo']")
        if f:
            f.send_keys("0")
        if t:
            t.send_keys("999999")
    except Exception:
        pass
    try:
        disp = drv.find_element(By.ID, "dispCount")
        drv.execute_script("arguments[0].value='50'", disp)
    except Exception:
        sels = drv.find_elements(By.XPATH, "//select[contains(@id,'disp') or contains(@name,'disp')]")
        if sels:
            drv.execute_script("arguments[0].value='50'", sels[0])
    time.sleep(ACTION_SLEEP)
    try:
        drv.execute_script("if (typeof js_Search==='function') js_Search('0');")
    except Exception:
        pass
    try:
        btn = drv.find_element(
            By.XPATH, "//input[@type='button' or @type='submit'][@value='検索' or contains(@onclick,'Search')]"
        )
        btn.click()
    except Exception:
        pass

    WebDriverWait(drv, WAIT_SEC).until(lambda d: "TAKKEN" in d.current_url)
    time.sleep(ACTION_SLEEP)
    if is_system_error_page(drv.page_source):
        drv.get(BASE_URL)
        wait_dom_complete(drv)
        time.sleep(ACTION_SLEEP)
        open_search_and_submit(drv)


def get_total_pages(drv: webdriver.Chrome) -> int:
    try:
        el = drv.find_element(By.ID, "pageCount")
        return max(1, int(el.get_attribute("value").strip()))
    except Exception:
        pass
    try:
        opts = drv.find_elements(By.CSS_SELECTOR, "#pageListNo1 option")
        return max(1, len(opts))
    except Exception:
        return 1


def go_to_page(drv: webdriver.Chrome, page_1based: int, prev_first_id: str = "") -> None:
    idx0 = max(0, page_1based - 1)
    try:
        drv.execute_script("var s=document.getElementById('pageListNo1'); if(s){s.selectedIndex=%d}" % idx0)
        drv.execute_script("if (typeof js_PageSearch==='function') js_PageSearch('1');")
    except Exception:
        try:
            a = drv.find_element(By.XPATH, f"//a[normalize-space(text())='{page_1based}']")
            a.click()
        except Exception:
            pass

    deadline = time.time() + PAGE_NAV_WAIT
    while time.time() < deadline:
        ids = extract_detail_ids_on_page(drv)
        if ids and (not prev_first_id or ids[0] != prev_first_id):
            break
        time.sleep(0.5)


def extract_detail_ids_on_page(drv: webdriver.Chrome) -> List[str]:
    html = drv.page_source
    ids = re.findall(r"(?:js_)?ShowDetail\(['\"]?(\d+)['\"]?\)", html)
    return list(dict.fromkeys(ids))


def harvest_hidden_params(drv: webdriver.Chrome) -> Dict[str, str]:
    soup = BeautifulSoup(drv.page_source, "lxml")
    params: Dict[str, str] = {}
    for inp in soup.find_all("input", attrs={"type": "hidden"}):
        name = inp.get("name") or inp.get("id")
        if not name:
            continue
        params[name] = inp.get("value") or ""
    params.setdefault("sv_dispCount", "50")
    params.setdefault("sv_rdoSelect", "1")
    params.setdefault("sv_choice", "2")
    params["_current_list_url"] = drv.current_url
    return params


# ====================== requests セッション（スレッドローカル） ======================
_tls = threading.local()
COOKIE_EPOCH = 0
COOKIE_LOCK = threading.Lock()
COOKIE_SNAPSHOT = []
UA_SNAPSHOT = ""


def _mount_pool(s: requests.Session):
    retry = Retry(
        total=RETRY_TOTAL,
        backoff_factor=0.4,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["HEAD", "GET", "OPTIONS", "POST"]),
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=POOL_MAXSIZE, pool_maxsize=POOL_MAXSIZE)
    s.mount("https://", adapter)
    s.mount("http://", adapter)


def update_cookie_snapshot_from_driver(drv: webdriver.Chrome):
    global COOKIE_SNAPSHOT, UA_SNAPSHOT, COOKIE_EPOCH
    with COOKIE_LOCK:
        COOKIE_SNAPSHOT = drv.get_cookies()
        UA_SNAPSHOT = drv.execute_script("return navigator.userAgent;")
        COOKIE_EPOCH += 1


def get_thread_session(epoch: int) -> requests.Session:
    s = getattr(_tls, "session", None)
    s_epoch = getattr(_tls, "epoch", -1)
    if s is not None and s_epoch == epoch:
        return s
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA_SNAPSHOT or "Mozilla/5.0",
        "Accept-Language": "ja,en;q=0.9",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Upgrade-Insecure-Requests": "1",
    })
    for c in COOKIE_SNAPSHOT:
        try:
            s.cookies.set(c["name"], c["value"], domain=c.get("domain"), path=c.get("path", "/"))
        except Exception:
            pass
    _mount_pool(s)
    _tls.session = s
    _tls.epoch = epoch
    return s


# ====================== 詳細HTML取得 ======================
def fetch_detail_html_fast(base_params: Dict[str, str], license_no: str, epoch: int) -> Optional[str]:
    session = get_thread_session(epoch)
    referer = base_params.get("_current_list_url", BASE_URL)
    session.headers.update({"Referer": referer})

    try:
        GLOBAL_LIMITER.acquire()
        r = session.get(DETAIL_URL, params={"sv_licenseNo": license_no}, timeout=HTTP_TIMEOUT)
        r.encoding = "shift_jis"
        if r.ok and looks_like_detail(r.text):
            return r.text
    except Exception:
        pass

    params = dict(base_params)
    params["sv_licenseNo"] = license_no
    params.pop("_current_list_url", None)
    try:
        GLOBAL_LIMITER.acquire()
        r = session.get(DETAIL_URL, params=params, timeout=HTTP_TIMEOUT)
        r.encoding = "shift_jis"
        if r.ok and looks_like_detail(r.text):
            return r.text
    except Exception:
        pass

    try:
        GLOBAL_LIMITER.acquire()
        r = session.post(DETAIL_URL, data=params, timeout=HTTP_TIMEOUT)
        r.encoding = "shift_jis"
        if r.ok and looks_like_detail(r.text):
            return r.text
    except Exception:
        pass

    return None


# ====================== HTMLパース ======================
ABBR_COLUMNS = [
    "土", "建", "大", "左", "と", "石", "屋", "電", "管", "夕",
    "鋼", "筋", "舗", "し", "ゅ", "板", "ガ", "塗", "防", "内",
    "機", "絶", "通", "園", "井", "具", "水", "消", "清", "解",
]

PREF_PAT = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
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
    if td is None:
        return "", ""
    kana_list = [norm(p.get_text(" ")) for p in td.find_all("p", class_=lambda c: c and "phonetic" in c)]
    kana = " ".join([k for k in kana_list if k])
    td2 = BeautifulSoup(str(td), "lxml")
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
    soup = BeautifulSoup(html, "lxml")
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
    row["_tel_raw"] = norm(td.get_text(" ")) if td else ""

    td = find_value_cell_by_label(soup, r"(法人・個人区分|法人・個人の別|法人・個人)")
    row["法人・個人区分"] = norm(td.get_text(" ")) if td else ""

    td = find_value_cell_by_label(soup, r"(資本金|資本金額)")
    row[Schema.CAP] = norm(td.get_text(" ")) if td else ""

    td = find_value_cell_by_label(soup, r"建設業以外の兼業の有無")
    row["建設業以外の兼業の有無"] = norm(td.get_text(" ")) if td else ""

    td = find_value_cell_by_label(soup, r"許可の有効期間")
    row["許可の有効期間"] = norm(td.get_text(" ")) if td else ""

    inds = parse_industry_table_numbers(soup)
    row.update(inds)
    row["許可を受けた建設業の種類(土)"] = row.get("土", "")

    ins = parse_insurance(soup)
    row.update(ins)

    return row


# ====================== チェックポイント ======================
def get_checkpoint_rows() -> List[Dict[str, str]]:
    if not CHECKPOINT_CSV.exists():
        return []
    try:
        with open(CHECKPOINT_CSV, "r", newline="", encoding="utf-8-sig") as f:
            return list(csv.DictReader(f))
    except Exception as e:
        logging.warning(f"[CHECKPOINT] 読み込み失敗: {e}")
        return []


def append_to_checkpoint(new_rows: List[Dict[str, str]]) -> None:
    if not new_rows:
        return
    write_header = not CHECKPOINT_CSV.exists()
    with open(CHECKPOINT_CSV, "a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(new_rows[0].keys()))
        if write_header:
            w.writeheader()
        w.writerows(new_rows)


# ====================== NetHarvestクローラー ======================
from concurrent.futures import ThreadPoolExecutor, as_completed


class MlitKensetsuScraper(StaticCrawler):
    """国土交通省 建設業者・宅建業者等企業情報検索システム（建設業）"""

    EXTRA_COLUMNS = [
        "許可番号",
        "法人・個人区分",
        "建設業以外の兼業の有無",
        "保険加入状況(健康)",
        "保険加入状況(年金)",
        "保険加入状況(雇用)",
        "許可を受けた建設業の種類(土)",
        "許可の有効期間",
    ] + ABBR_COLUMNS

    def prepare(self):
        """チェックポイントが残っていれば読み込んで再開する。"""
        checkpoint_rows = get_checkpoint_rows()
        self._already_done = len(checkpoint_rows)
        if self._already_done > 0:
            self.logger.info("[RESUME] チェックポイント %d 件を引継ぎ", self._already_done)
            for row in checkpoint_rows:
                self.pipeline.process_item(dict(row))
        else:
            self._already_done = 0

    def parse(self, url: str) -> Generator[dict, None, None]:
        already_done = getattr(self, "_already_done", 0)
        drv = make_driver(HEADLESS)
        failed_for_fallback: List[Tuple[str, Dict[str, str]]] = []
        global_index = (START_RAW - 1) + already_done

        try:
            open_search_and_submit(drv)
            update_cookie_snapshot_from_driver(drv)
            try:
                cur_page = int(drv.find_element(By.ID, "dispPage").get_attribute("value"))
            except Exception:
                cur_page = 1
            total_pages = get_total_pages(drv)

            try:
                total_results = int(drv.find_element(By.ID, "resultCount").get_attribute("value"))
            except Exception:
                total_results = total_pages * 50
            self.logger.info("[SETUP] pages=%d results≈%d (range %d..%d)", total_pages, total_results, START_RAW, END_RAW)

            empty_pages = 0
            prev_first_id = ""

            if already_done > 0:
                skip_to_page = max(cur_page, (START_RAW + already_done - 1) // 50)
                if skip_to_page > cur_page:
                    self.logger.info("[RESUME] page %d へジャンプ", skip_to_page)
                    go_to_page(drv, skip_to_page)
                    cur_page = skip_to_page

            for page in range(cur_page, total_pages + 1):
                if page != cur_page:
                    go_to_page(drv, page, prev_first_id)

                if is_system_error_page(drv.page_source):
                    drv.refresh()
                    wait_dom_complete(drv)
                    time.sleep(ACTION_SLEEP)
                    open_search_and_submit(drv)
                    try:
                        go_to_page(drv, page)
                    except Exception:
                        pass

                base_params = harvest_hidden_params(drv)
                detail_ids = extract_detail_ids_on_page(drv)
                prev_first_id = detail_ids[0] if detail_ids else ""
                self.logger.info("[page %d] ids=%d", page, len(detail_ids))

                if len(detail_ids) == 0:
                    empty_pages += 1
                    if empty_pages >= EMPTY_PAGE_THRES:
                        self.logger.warning("[block?] empty %d pages → cooldown %ds", empty_pages, COOLDOWN_SEC)
                        time.sleep(COOLDOWN_SEC)
                        open_search_and_submit(drv)
                        update_cookie_snapshot_from_driver(drv)
                        try:
                            go_to_page(drv, page)
                        except Exception:
                            pass
                        empty_pages = 0
                    continue
                else:
                    empty_pages = 0

                update_cookie_snapshot_from_driver(drv)
                current_epoch = COOKIE_EPOCH

                work_queue: List[Tuple[str, Dict[str, str]]] = []
                for did in detail_ids:
                    global_index += 1
                    if global_index < START_RAW + already_done:
                        continue
                    if global_index > END_RAW:
                        self.logger.info("[range] reached END_RAW")
                        detail_ids = []
                        break
                    work_queue.append((did, dict(base_params)))

                page_rows: List[dict] = []
                if work_queue:
                    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                        fut2id = {
                            ex.submit(fetch_detail_html_fast, p, did, current_epoch): (did, p)
                            for (did, p) in work_queue
                        }
                        for fut in as_completed(fut2id):
                            did, p = fut2id[fut]
                            html = None
                            try:
                                html = fut.result()
                            except Exception:
                                pass
                            if html and looks_like_detail(html):
                                try:
                                    row = parse_overview(html)
                                    if not any(row.get(k) for k in [Schema.NAME, "許可番号", Schema.ADDR]):
                                        failed_for_fallback.append((did, p))
                                    else:
                                        row[Schema.TEL] = tel_to_digits_text(row.pop("_tel_raw", ""))
                                        row[Schema.URL] = f"{DETAIL_URL}?{urlencode({'sv_licenseNo': did})}"
                                        page_rows.append(row)
                                        yield row
                                except Exception:
                                    failed_for_fallback.append((did, p))
                            else:
                                failed_for_fallback.append((did, p))

                append_to_checkpoint(page_rows)
                self.logger.info(
                    "[PROGRESS] page=%d/%d 取得=%d fallback待ち=%d",
                    page, total_pages, len(page_rows), len(failed_for_fallback),
                )

            # Selenium フォールバック
            self.logger.info("[fallback] %d 件を Selenium で再取得", len(failed_for_fallback))
            fallback_rows: List[dict] = []
            for did, _p in failed_for_fallback:
                try:
                    drv.execute_script(f"if (typeof js_ShowDetail==='function') js_ShowDetail('{did}');")
                    for attempt in range(3):
                        WebDriverWait(drv, WAIT_SEC).until(
                            lambda d: "do" in d.current_url or "Gaiyo" in d.current_url
                        )
                        time.sleep(DETAIL_SLEEP)
                        if not is_system_error_page(drv.page_source):
                            break
                        drv.execute_script(f"if (typeof js_ShowDetail==='function') js_ShowDetail('{did}');")
                        time.sleep(0.3 + 0.3 * attempt)
                    html = drv.page_source
                    if is_system_error_page(html):
                        raise RuntimeError("system error on detail")
                    row = parse_overview(html)
                    row[Schema.TEL] = tel_to_digits_text(row.pop("_tel_raw", ""))
                    row[Schema.URL] = drv.current_url
                    fallback_rows.append(row)
                    yield row
                except Exception as e:
                    self.logger.warning("[fallback] failed id=%s: %s", did, e)
                finally:
                    try:
                        drv.execute_script("if (typeof kensetuKensaku_submit==='function') kensetuKensaku_submit();")
                        WebDriverWait(drv, 8).until(EC.presence_of_element_located((By.ID, "pageCount")))
                    except Exception:
                        try:
                            a = drv.find_element(By.XPATH, "//a[contains(., '前画面に戻る')]")
                            a.click()
                            WebDriverWait(drv, 8).until(EC.presence_of_element_located((By.ID, "pageCount")))
                        except Exception:
                            try:
                                drv.execute_script("history.go(-1);")
                                WebDriverWait(drv, 8).until(EC.presence_of_element_located((By.ID, "pageCount")))
                            except Exception:
                                pass

            append_to_checkpoint(fallback_rows)
            CHECKPOINT_CSV.unlink(missing_ok=True)
            self.logger.info("[DONE] チェックポイント削除・完了")

        finally:
            try:
                drv.quit()
            except Exception:
                pass


# ====================== ローカル実行用エントリーポイント ======================
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    scraper = MlitKensetsuScraper()
    scraper.execute(BASE_URL)
    print(f"\n取得件数: {scraper.item_count}")
    print(f"出力先:   {scraper.output_filepath}")
