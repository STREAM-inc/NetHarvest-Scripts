import datetime
import json
import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 詳細ページ (/job-NNNNNN) の href パターン。/job-lists は数字が続かないため除外される。
_DETAIL_RE = re.compile(r"/job-\d+")
# 求人 ID (/job-1280605 → 1280605)
_ID_RE = re.compile(r"/job-(\d+)")
# 企業ページ (/company/10346)
_COMPANY_RE = re.compile(r"/company/(\d+)")
# 総件数表示 "20,759 件の求人が見つかりました"
_TOTAL_RE = re.compile(r"([\d,]+)\s*件の求人")
# 電話番号 (例: 05018834123, 03-1234-5678)
_TEL_RE = re.compile(r"0\d[\d\-]{7,}")
# 郵便番号 (〒272-0004 / 〒2720004)
_ZIP_RE = re.compile(r"〒\s*(\d{3})-?(\d{4})")
# JSON-LD datePosted ("2026-09-30T10:22")
_DATE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class Ever2Scraper(StaticCrawler):
    """ドラEVER-2 ドライバー求人スクレイパー（doraever.jp / 求人単位）

    - 既存 `ever`（doraever.py）が「会社概要のみ・会社名で dedup」なのに対し、
      本スクレイパーは一覧 `/job-lists` の **求人 1 件ごと** を出力する（重複除去しない）。
    - 一覧（2ページ目以降は `/job-lists/{page}`）から詳細 `/job-NNNNNN` を収集し、
      詳細を 1 件取得するごとに即 yield する（Pattern B）。
    - 詳細ページは `div.mid_box`（`.mid_box_ttl` がセクション名）内の `h2.c_ttl`(ラベル)
      + 直後 `<p>`(値) 構造。「募集中の求人」ボックスは他求人の一覧なので除外する。
    - 掲載日・事業所名は HTML の表示要素としては存在せず、詳細ページの JSON-LD
      （JobPosting.datePosted / BreadcrumbList の末尾要素）にのみ入っている。
      掲載日は `2026年9月30日` 形式の日本語表記に整形して出力する。
    - 企業ページ `/company/{企業ID}` は「募集中の求人」カードの `div.occupation`
      に事業所名が並ぶ。企業 ID ごとに 1 回だけ取得して `_company_cache` に保持し、
      同一企業の求人では再利用する。
    - doraever.jp は短い間隔でアクセスすると WAF 403（`<title>403 Forbidden</title>`
      の極小ページ／HTTP 403）を返すため、全リクエストを `_REQ_INTERVAL` 秒以上
      空けて送り、403 検知時は `_RETRY_WAITS` の秒数だけ待って再取得する。
    - 求人 detail の「応募について」ボックスに `連絡先`（電話）があるため TEL を取得できる。
    - 利用規約（/terms）にスクレイピング／クローリングの明示的禁止は無い
      （無断転載・営利目的提供は禁止）。
    """

    DELAY = 2.5
    TIMEOUT = 30

    EXTRA_COLUMNS = [
        "求人ID",
        "職種",
        "雇用形態",
        "必要免許",
        "車形状",
        "輸送品目",
        "車両保有台数",
        "主要取引先",
        "担当者",
        "会社所在地",
        "掲載日",
        "事業所名",
        "勤務地住所",
        "企業ページURL",
        "企業ページ事業所一覧",
        "取得日",
    ]

    # 連続リクエストの最小間隔（秒）。403 対策で 2〜3 秒以上空ける。
    _REQ_INTERVAL = 2.5
    # 403 検知時の待機秒数（リトライ回数 = len）。長めに待ってから再取得する。
    _RETRY_WAITS = [10.0, 20.0, 40.0]
    # 連続空ページがこの数に達したら一覧巡回を打ち切る
    _MAX_EMPTY = 5
    # 一覧ページ数の安全上限
    _MAX_PAGE = 2000

    # 企業ページ URL → 事業所名（「|」連結済み）のキャッシュ
    _company_cache: dict[str, str] = {}
    # 直近の実リクエスト時刻（キャッシュヒットは含めない）
    _last_request_at = 0.0
    # 実行日 (YYYY-MM-DD)
    _today = ""

    def prepare(self):
        """インスタンス単位の状態を初期化する（__init__ のオーバーライドは禁止）。"""
        self._company_cache = {}
        self._last_request_at = 0.0
        self._today = datetime.date.today().strftime("%Y-%m-%d")

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url を唯一のルート（起点）として使う。ページネーションはここから派生させる。
        root = url
        seen_details: set[str] = set()
        consecutive_empty = 0
        page = 1

        while consecutive_empty < self._MAX_EMPTY and page <= self._MAX_PAGE:
            list_url = self._list_page_url(root, page)
            soup = self._get_soup_ok(list_url)
            if soup is None:
                consecutive_empty += 1
                page += 1
                continue

            if page == 1 and not self.total_items:
                m = _TOTAL_RE.search(_clean(soup.get_text()))
                if m:
                    self.total_items = int(m.group(1).replace(",", ""))

            detail_urls = self._extract_detail_urls(root, soup, seen_details)
            if not detail_urls:
                consecutive_empty += 1
                page += 1
                continue
            consecutive_empty = 0

            for detail_url in detail_urls:
                try:
                    item = self._scrape_detail(detail_url, root)
                except Exception as e:  # 個別求人のエラーはログして継続
                    self.logger.warning("詳細取得エラー: %s — %s", detail_url, e)
                    continue
                if not item or not item.get(Schema.NAME):
                    continue
                yield item

            page += 1

    def _list_page_url(self, root: str, page: int) -> str:
        """一覧ページの URL をルート url から派生させる。"""
        if page <= 1:
            return root
        return root.rstrip("/") + f"/{page}"

    def _extract_detail_urls(self, root: str, soup, seen: set[str]) -> list[str]:
        urls: list[str] = []
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if not _DETAIL_RE.search(href):
                continue
            full = urljoin(root, href).split("#")[0].split("?")[0]
            if not _DETAIL_RE.search(full):
                continue
            if full in seen:
                continue
            seen.add(full)
            urls.append(full)
        return urls

    # ------------------------------------------------------------------
    # 取得（レート制御 + 403 リトライ）
    # ------------------------------------------------------------------
    def _wait_turn(self):
        """直前の実リクエストから `_REQ_INTERVAL` 秒以上空くまで待つ。"""
        if not self._last_request_at:
            return
        remain = self._REQ_INTERVAL - (time.monotonic() - self._last_request_at)
        if remain > 0:
            time.sleep(remain)

    def _get_soup_ok(self, url: str):
        """403 Forbidden ページ／通信失敗を検知し、正常ページが得られるまで再取得する。"""
        for attempt in range(len(self._RETRY_WAITS) + 1):
            self._wait_turn()
            soup = self.get_soup(url)
            if not self._last_fetch_from_cache:
                self._last_request_at = time.monotonic()

            if soup is not None and not self._is_forbidden(soup):
                return soup

            if attempt < len(self._RETRY_WAITS):
                wait = self._RETRY_WAITS[attempt]
                self.logger.warning("403/取得失敗 — %.0f秒待って再取得: %s", wait, url)
                time.sleep(wait)
        return None

    @staticmethod
    def _is_forbidden(soup) -> bool:
        title = _clean(soup.title.get_text()) if soup.title else ""
        if "403 Forbidden" in title:
            return True
        h1 = soup.select_one("h1")
        return bool(h1 and "403 Forbidden" in _clean(h1.get_text()))

    # ------------------------------------------------------------------
    # 詳細ページ
    # ------------------------------------------------------------------
    def _scrape_detail(self, url: str, root: str) -> dict | None:
        soup = self._get_soup_ok(url)
        if soup is None:
            return None

        today = self._today or datetime.date.today().strftime("%Y-%m-%d")
        data = {Schema.URL: url, "取得日": today}

        mid = _ID_RE.search(url)
        if mid:
            data["求人ID"] = mid.group(1)

        # 全 mid_box のラベル→値を収集（「募集中の求人」ボックスは他求人一覧なので除外）
        info: dict[str, str] = {}
        for box in soup.select("div.mid_box"):
            ttl = box.select_one(".mid_box_ttl")
            box_name = _clean(ttl.get_text()) if ttl else ""
            if "募集中の求人" in box_name:
                continue
            for h2 in box.select("h2.c_ttl"):
                label = _clean(h2.get_text())
                if not label:
                    continue
                val_el = h2.find_next_sibling()
                val = _clean(val_el.get_text(" ")) if val_el else ""
                # 同一ラベルは最初の非空値を優先（勤務地が2つある等）
                if not info.get(label):
                    info[label] = val

        work_addr = ""
        company_addr = ""
        for label, val in info.items():
            if not val:
                continue
            if "会社名" in label or "企業名" in label:
                data.setdefault(Schema.NAME, val)
            elif "会社所在地" in label:
                company_addr = company_addr or val
                data.setdefault("会社所在地", val)
            elif "勤務地" in label:
                work_addr = work_addr or val
            elif "連絡先" in label:
                m = _TEL_RE.search(val)
                if m:
                    data.setdefault(Schema.TEL, m.group(0))
            elif "代表者" in label:
                data.setdefault(Schema.REP_NM, val)
            elif "事業内容" in label:
                data.setdefault(Schema.LOB, val)
            elif "従業員" in label:
                data.setdefault(Schema.EMP_NUM, val)
            elif "資本金" in label:
                # "資本金／売上 = 9,700万円/6400百万" のような複合値を分割
                parts = re.split(r"[／/]", val, maxsplit=1)
                data.setdefault(Schema.CAP, _clean(parts[0]))
                if "売上" in label and len(parts) > 1:
                    data.setdefault(Schema.SALES, _clean(parts[1]))
            elif "売上" in label:
                data.setdefault(Schema.SALES, val)
            elif "創立" in label or "創業" in label or "設立" in label:
                data.setdefault(Schema.OPEN_DATE, val)
            elif "Web" in label or "ＵＲＬ" in label or "URL" in label:
                data.setdefault(Schema.WEBSITE, val)
            elif label == "職種":
                data.setdefault("職種", val)
            elif "雇用形態" in label:
                data.setdefault("雇用形態", val)
            elif "必要免許" in label:
                data.setdefault("必要免許", val)
            elif "車形状" in label:
                data.setdefault("車形状", val)
            elif "輸送品目" in label:
                data.setdefault("輸送品目", val)
            elif "車両" in label:
                data.setdefault("車両保有台数", val)
            elif "主要取引先" in label or "取引先" in label:
                data.setdefault("主要取引先", val)
            elif label == "担当":
                data.setdefault("担当者", val)

        # 住所は勤務地を優先し、無ければ会社所在地を採用
        addr = work_addr or company_addr
        if addr:
            data[Schema.ADDR] = addr

        # --- 郵便番号 / 勤務地住所 (〒 を分離) ---
        zipcode = self._pick_zip(work_addr) or self._pick_zip(company_addr)
        if work_addr:
            data["勤務地住所"] = _clean(_ZIP_RE.sub("", work_addr))

        # --- JSON-LD (掲載日 / 事業所名 / 郵便番号フォールバック) ---
        job_ld, crumb_ld = self._extract_ld(soup)
        if job_ld:
            posted = self._format_date(job_ld.get("datePosted"))
            if posted:
                data["掲載日"] = posted
            if not zipcode:
                loc = job_ld.get("jobLocation") or {}
                if isinstance(loc, list):
                    loc = loc[0] if loc else {}
                pc = _clean((loc.get("address") or {}).get("postalCode"))
                zipcode = self._format_zip(pc)
        if zipcode:
            data[Schema.POST_CODE] = zipcode

        office = self._extract_office(crumb_ld)
        if office:
            data["事業所名"] = office

        # --- 企業ページ URL / 企業ページ事業所一覧 ---
        company_url = self._extract_company_url(soup, root)
        if company_url:
            data["企業ページURL"] = company_url
            offices = self._get_company_offices(company_url)
            if offices:
                data["企業ページ事業所一覧"] = offices

        if not data.get(Schema.NAME):
            return None
        return data

    # ------------------------------------------------------------------
    # 部品
    # ------------------------------------------------------------------
    @staticmethod
    def _pick_zip(text: str) -> str:
        m = _ZIP_RE.search(text or "")
        return f"{m.group(1)}-{m.group(2)}" if m else ""

    @staticmethod
    def _format_zip(raw: str) -> str:
        digits = re.sub(r"\D", "", raw or "")
        return f"{digits[:3]}-{digits[3:]}" if len(digits) == 7 else ""

    @staticmethod
    def _format_date(raw) -> str:
        """JSON-LD の datePosted (2026-05-13T10:22) を「2026年5月13日」に整形する。"""
        m = _DATE_RE.match(_clean(raw))
        if not m:
            return ""
        return f"{int(m.group(1))}年{int(m.group(2))}月{int(m.group(3))}日"

    @staticmethod
    def _extract_ld(soup) -> tuple[dict | None, dict | None]:
        """詳細ページの JSON-LD から JobPosting と BreadcrumbList を取り出す。"""
        job_ld = None
        crumb_ld = None
        for sc in soup.select('script[type="application/ld+json"]'):
            raw = sc.string or sc.get_text()
            try:
                obj = json.loads(raw.strip())
            except Exception:
                continue
            for d in obj if isinstance(obj, list) else [obj]:
                if not isinstance(d, dict):
                    continue
                if d.get("@type") == "JobPosting" and job_ld is None:
                    job_ld = d
                elif d.get("@type") == "BreadcrumbList" and crumb_ld is None:
                    crumb_ld = d
        return job_ld, crumb_ld

    @staticmethod
    def _extract_office(crumb_ld) -> str:
        """パンくず末尾（例: 本社 / 川口市（車庫））が事業所名。"""
        if not crumb_ld:
            return ""
        items = crumb_ld.get("itemListElement") or []
        if len(items) < 2:
            return ""
        last = items[-1]
        return _clean(last.get("name")) if isinstance(last, dict) else ""

    @staticmethod
    def _extract_company_url(soup, root: str) -> str:
        for a in soup.select("a[href]"):
            m = _COMPANY_RE.search(a.get("href", ""))
            if m:
                return urljoin(root, f"/company/{m.group(1)}")
        return ""

    def _get_company_offices(self, company_url: str) -> str:
        """企業ページの募集中求人から事業所名を重複除去して「|」連結。企業単位でキャッシュ。"""
        if company_url in self._company_cache:
            return self._company_cache[company_url]

        self._company_cache[company_url] = ""  # 失敗時も再取得しない
        soup = self._get_soup_ok(company_url)
        if soup is None:
            return ""

        names: list[str] = []
        box = soup.select_one("div.division_jobs_container") or soup
        for el in box.select("div.occupation"):
            name = _clean(el.get_text(" "))
            if name and name not in names:
                names.append(name)

        joined = "|".join(names)
        self._company_cache[company_url] = joined
        return joined


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    Ever2Scraper().execute("https://doraever.jp/job-lists")
