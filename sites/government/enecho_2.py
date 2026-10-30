"""
登録ガス小売事業者一覧 (資源エネルギー庁 / METI) — enecho_2

対象サイト: https://www.enecho.meti.go.jp/category/electricity_and_gas/gas/liberalization/retailers_list/

取得対象:
    - 資源エネルギー庁が公表する「登録ガス小売事業者一覧」の全事業者 (約1,290件)。
      登録番号・名称・法人番号・住所・代表者・担当部署・電話番号・HP・供給予定地域・地域詳細・
      一般家庭への販売・登録年月日・事業開始の予定年月日 を取得する。

取得フロー:
    - 登録小売電気事業者一覧 (government/enecho.py) と同じく、単一の静的 HTML ページに全件が掲載されている。
      経済産業省本省 / 各経済産業局 / 沖縄総合事務局ごとに table.small が 11 個並び、
      ページネーション・詳細ページ・Excel 版は無い (「（別紙）一般家庭への販売地点」att.html は
      供給地点の補足のみで事業者属性は持たないため対象外)。
    - 各データ行は th 2 個 (登録番号/名称) + td 10 個 = 12 セル。ヘッダー 2 行は th のみ。
    - 廃止・休止の区分はガス一覧には掲載されていない (電気版の「休止予定期間」に相当する列は無い)。
      名称セルの「※令和X年…より社号変更/事業承継」注記は「沿革注記」として保持する。

注意:
    - ページは約1MBと大きく応答が遅いため TIMEOUT を延長している。
    - UA が "Mozilla/5.0" のみだと 403。さらに CloudFront + AWS WAF の JS チャレンジ
      (HTTP 202 空ボディ / x-amzn-waf-action: challenge) が連続アクセス時などに発動する。
      202 は raise_for_status を素通りするため get_soup を override し、検知したら
      stealth 引数付き Playwright でチャレンジを解かせて aws-waf-token を requests に移植する
      (jobs/baitoru_6.py と同じ方式。UA はブラウザと requests で完全一致させる)。

実行方法:
    python scripts/sites/government/enecho_2.py
    docker compose exec worker python /app/bin/run_flow.py --site-id enecho_2
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator, Optional

import bs4
import requests

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

TABLE_CSS = "table.small"

# データ行のセル位置 (th 2 + td 10)
COL_REG_NO = 0      # 登録番号 (th)
COL_NAME = 1        # 氏名又は名称 + （法人番号 N） + ※沿革注記 (th)
COL_ADDR = 2        # 住所
COL_REP = 3         # 代表者氏名 (役職 + 氏名)
COL_DEPT = 4        # 担当部署
COL_TEL = 5         # 電話番号
COL_HP = 6          # 問合せ先 (HP リンク)
COL_AREA = 7        # 供給予定地域
COL_AREA_DETAIL = 8  # 供給予定地域 地域詳細
COL_HOUSEHOLD = 9   # 一般家庭への販売
COL_REG_DATE = 10   # 登録年月日
COL_START_DATE = 11  # 事業開始の予定年月日

EXPECTED_CELLS = 12

# AWS WAF のチャレンジページの目印
_WAF_MARKERS = ("awsWafCookieDomainList", "gokuProps", "Human Verification")
# AutomationControlled 無効化と navigator.webdriver 除去が無いと CAPTCHA 画面で止まる
_BROWSER_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-dev-shm-usage",
]
_STEALTH_JS = "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"

_PREF_RE = re.compile(r"^(東京都|北海道|(?:京都|大阪)府|.{2,3}県)")
_CO_NUM_RE = re.compile(r"法人番号\s*([0-9０-９]{13})")
_TEL_RE = re.compile(r"0\d{1,4}-\d{1,4}-\d{3,5}|0(?:120|570|800)-\d{6}")
# 全角数字・各種ハイフン (U+2010 等) → 半角
_TEL_TRANS = str.maketrans("０１２３４５６７８９‐‑–—−－ー", "0123456789-------")
# 空欄表記
_EMPTY = {"-", "－", "―", "ー", "‐"}


class _WafChallenge(requests.exceptions.RequestException):
    """AWS WAF のチャレンジに阻まれ続けた (通常の通信エラーと区別する)。"""


def _clean(text: str) -> str:
    text = re.sub(r"[ \t\r\n]+", " ", text).strip()
    return "" if text in _EMPTY else text


def _row_cells(tr) -> list:
    """行のセル一覧。

    E0008 (津島瓦斯) の行は地域詳細の <ul> が閉じられておらず、後続の td が
    ul の中に入れ子になっている。そのため直下の子だけでなく子孫の th/td を
    文書順に拾う (正常行では両者は一致する)。
    """
    return tr.find_all(["th", "td"])


def _cell_text(cell, sep: str = " ") -> str:
    """セル自身のテキスト (入れ子になった別セルのテキストは除く)。"""
    parts = [
        str(t).strip() for t in cell.find_all(string=True)
        if t.find_parent(["th", "td"]) is cell and str(t).strip()
    ]
    return sep.join(parts)


def _split_rep(text: str) -> tuple[str, str]:
    """「代表取締役 社長執行役員 神谷　泰範」→ (役職, 氏名)。氏名は全角スペースで姓名区切り。"""
    tokens = text.split(" ")
    if len(tokens) < 2:
        return "", text
    if "　" in tokens[-1]:
        return " ".join(tokens[:-1]), tokens[-1]
    if len(tokens) >= 3:
        # 「取締役社長 岡 達也」のように姓名が半角スペース区切りのケース
        return " ".join(tokens[:-2]), " ".join(tokens[-2:])
    return tokens[0], tokens[1]


class Enecho2Scraper(StaticCrawler):
    """資源エネルギー庁 登録ガス小売事業者一覧 スクレイパー"""

    DELAY = 3.0
    # 約1MBの単一ページで応答が遅いため延長
    TIMEOUT = 120
    # 202 (WAF) を踏んだときの再試行回数と、ブラウザでチャレンジが解けるのを待つ上限(秒)
    WAF_RETRIES = 3
    WAF_SOLVE_TIMEOUT = 40.0
    # ブラウザと requests で UA を完全一致させないとトークンが弾かれる
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
    EXTRA_COLUMNS = [
        "登録番号",
        "所管",
        "沿革注記",
        "担当部署",
        "電話番号_原文",
        "供給予定地域",
        "供給予定地域_詳細",
        "一般家庭への販売",
        "登録年月日",
        "事業開始予定年月日",
    ]

    def prepare(self):
        self.session.headers.update({
            "User-Agent": self.USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        })

    # ------------------------------------------------------------------ #
    # 通信 (AWS WAF トークン)
    # ------------------------------------------------------------------ #
    def _solve_challenge(self, url: str) -> bool:
        """Playwright で JS チャレンジを解かせ、クッキーを requests に移植する。"""
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            self.logger.error("playwright が利用できないため AWS WAF を突破できません")
            return False

        self.logger.info("AWS WAF のチャレンジをブラウザで突破します: %s", url)
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True, args=_BROWSER_ARGS)
                try:
                    context = browser.new_context(
                        user_agent=self.USER_AGENT,
                        locale="ja-JP",
                        extra_http_headers={"Accept-Language": "ja,en-US;q=0.9,en;q=0.8"},
                    )
                    context.add_init_script(_STEALTH_JS)
                    page = context.new_page()
                    page.goto(url, wait_until="commit", timeout=45000)
                    deadline = time.monotonic() + self.WAF_SOLVE_TIMEOUT
                    while time.monotonic() < deadline:
                        if any(c["name"] == "aws-waf-token" for c in context.cookies()):
                            break
                        page.wait_for_timeout(1000)
                    else:
                        self.logger.warning("WAF チャレンジが時間内に解けませんでした")
                        return False
                    cookies = context.cookies()
                finally:
                    browser.close()
        except Exception as e:  # noqa: BLE001 — ブラウザ起動失敗等はログして requests 側で再試行
            self.logger.warning("WAF 突破に失敗しました: %s", e)
            return False

        for ck in cookies:
            self.session.cookies.set(
                ck["name"], ck["value"], domain=ck.get("domain", ""), path=ck.get("path", "/"))
        self.logger.info("WAF トークンを取得しました")
        return True

    def get_soup(self, url: str) -> Optional[bs4.BeautifulSoup]:
        """HTML を取得する。202 (WAF) を検知したらトークンを取得して再試行する。"""
        def _fetch() -> str:
            for attempt in range(self.WAF_RETRIES):
                self.logger.info("取得中: %s", url)
                resp = self.session.get(url, timeout=self.TIMEOUT)
                body = resp.text
                if resp.status_code == 202 or not body.strip() \
                        or any(m in body[:4000] for m in _WAF_MARKERS):
                    self.logger.warning("WAF 応答 (HTTP %s) — トークン取得 %d/%d",
                                        resp.status_code, attempt + 1, self.WAF_RETRIES)
                    if not self._solve_challenge(url):
                        time.sleep(min(2 ** attempt * 2, 10))
                    continue
                resp.raise_for_status()
                if "charset=" not in resp.headers.get("Content-Type", "").lower():
                    resp.encoding = resp.apparent_encoding
                return resp.text
            raise _WafChallenge(f"WAF により取得不能: {url}")

        try:
            html = self._fetch_html_cached(url, variant="", fetcher=_fetch)
            return bs4.BeautifulSoup(html, "html.parser")
        except requests.exceptions.RequestException as e:
            self.error_count += 1
            self.logger.warning("取得失敗: %s — %s", url, e)
            return None

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)
        if soup is None:
            return

        tables = soup.select(TABLE_CSS)
        if not tables:
            self.logger.warning("一覧テーブル (%s) が見つかりませんでした: %s", TABLE_CSS, url)
            return

        self.total_items = sum(
            1 for t in tables for tr in t.find_all("tr")
            if len(_row_cells(tr)) >= EXPECTED_CELLS
        )
        self.logger.info("テーブル %d 個 / データ行 %d 件を検出", len(tables), self.total_items)

        for table in tables:
            # 直前の見出し「関東経済産業局　342事業者」→ 所管
            heading = table.find_previous(["h2", "h3", "h4"])
            office = ""
            if heading:
                office = re.sub(r"\s*\d+\s*事業者\s*$", "", heading.get_text(" ", strip=True)).strip()

            for tr in table.find_all("tr"):
                cells = _row_cells(tr)
                if len(cells) < EXPECTED_CELLS:
                    continue
                try:
                    item = self._parse_row(cells, url, office)
                except Exception as e:  # noqa: BLE001 — 個別行の失敗はログして継続
                    self.logger.warning("行の解析に失敗しスキップ: %s", e)
                    continue
                if item:
                    yield item

    def _parse_row(self, cells, url: str, office: str) -> dict | None:
        texts = [_clean(_cell_text(c)) for c in cells]

        # 名称セル: 1行目=名称、（法人番号 N）、※注記 が <br> 区切り
        name_parts = [_clean(s) for s in _cell_text(cells[COL_NAME], "\n").split("\n")]
        name_parts = [p for p in name_parts if p]
        if not name_parts:
            return None
        name_cell = " ".join(name_parts)
        m = _CO_NUM_RE.search(name_cell)
        co_num = m.group(1) if m else ""
        # 名称は法人番号表記の手前まで
        name = re.split(r"[（(]\s*法人番号", name_parts[0])[0].strip()
        notes = [p for p in name_parts[1:] if "法人番号" not in p]
        history = " ".join(notes).lstrip("※").strip()
        if not name:
            return None

        addr_full = texts[COL_ADDR]
        pm = _PREF_RE.match(addr_full)
        pref = pm.group(1) if pm else ""
        addr = addr_full[len(pref):].strip() if pref else addr_full

        pos, rep = _split_rep(texts[COL_REP])

        tel_raw = texts[COL_TEL]
        tm = _TEL_RE.search(tel_raw.translate(_TEL_TRANS))
        tel = tm.group(0) if tm else ""

        hp_a = cells[COL_HP].find("a", href=True)
        hp = hp_a["href"].strip() if hp_a and hp_a["href"].strip().startswith("http") else ""


        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.CO_NUM: co_num,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.POS_NM: pos,
            Schema.REP_NM: rep,
            Schema.TEL: tel,
            Schema.HP: hp,
            "登録番号": texts[COL_REG_NO],
            "所管": office,
            "沿革注記": history,
            "担当部署": texts[COL_DEPT],
            "電話番号_原文": tel_raw,
            "供給予定地域": texts[COL_AREA],
            "供給予定地域_詳細": texts[COL_AREA_DETAIL],
            "一般家庭への販売": texts[COL_HOUSEHOLD],
            "登録年月日": texts[COL_REG_DATE],
            "事業開始予定年月日": texts[COL_START_DATE],
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = Enecho2Scraper()
    scraper.execute(
        "https://www.enecho.meti.go.jp/category/electricity_and_gas/gas/liberalization/retailers_list/"
    )

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
