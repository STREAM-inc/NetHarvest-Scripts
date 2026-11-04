"""
バイトル 中国5県 企業情報 (baitoru.com)

対象サイト: https://www.baitoru.com/chushikoku/jlist/hiroshima/

取得対象:
    中国5県 (鳥取・島根・岡山・広島・山口) の求人一覧を巡回し、求人詳細から
    その求人を出している企業の企業情報ページ (/cjlistNNNNNN/) へ辿って、
    企業単位 (1企業ページ = 1行) で出力する。同じ企業ページは重複出力しない。

取得フロー:
    1. 引数 url (= /chushikoku/jlist/hiroshima/) を唯一のルートとし、配信元(origin)・
       リージョン(chushikoku)・起点県(hiroshima) を派生させる。備考の指示に従い、
       起点県に続けて残り4県も同じクローラーで巡回する (url の県セグメントを差し替え)。
    2. バイトルの一覧は 1 URL あたり約 1 万件 (page385) で頭打ちになる。9,000 件を
       超える県は、一覧上の地理リンク (広島市中区 / 呉市 / 安芸郡 …) へ分割して巡回する。
    3. 一覧の本体求人は `pname=link_job_detail…_p_{順番}_{件数}_` が付くリンクのみ
       (上部広告枠 `…_alist_…` / 下部レコメンド `recom_…` は除外)。ページ送りは `page{N}/`。
    4. 未処理の求人ごとに /jobdetail/{id} を開き、企業ページ /cjlistNNNNNN/ を特定する。
       未出力の企業なら企業ページを取得して即 yield する (Pattern B)。
       企業ページに並ぶ求人IDは処理済みに登録し、同じ企業の求人詳細を開き直さない。

★ UA:
    baitoru.com は AWS WAF を導入しており、Chrome 系のフル UA だと HTTP 202 (空ボディの
    JS チャレンジ) が返る。2026-11 時点では素の "Mozilla/5.0" UA には通常の HTML
    (スマホ版 SSR) が返るため、そちらを使う。202 が続く場合は失敗として扱う。

取得カラム (備考で指定されたもの):
    企業名 / 都道府県 / 本社所在地 / TEL / HP / 従業員数 / 事業内容 / 企業ページURL /
    代表求人URL / 求人の掲載開始日・更新日 / 求人件数
    ※ 企業ページに本社所在地・TEL・従業員数が無い場合は空欄 (求人の勤務地で代用しない)。
       TEL は企業ページ「所在地」欄の代表TEL → 求人詳細「事業主情報」の代表TEL の順。
    ※ 事業内容は備考で明示的に取得指示があるため出力する。
    ※ 都道府県は本社所在地の先頭から導出する (所在地が無ければ空欄)。

利用規約 (https://www.baitoru.com/about/kiyaku_base.html):
    スクレイピング/クローリングを名指しで禁止する条項は無い。第9条(3)に「本サービスを
    利用した営業活動または営利を目的とした情報収集」の禁止があるため運用側で用途に留意。
    備考に従い全リクエストを 1.5 秒以上空けて逐次 (並列なし) で巡回する。

実行方法:
    python scripts/sites/jobs/baitoru_7.py
    docker compose exec worker python /app/bin/run_flow.py --site-id baitoru_7
"""

import logging
import re
import sys
import time
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import urljoin, urlparse

import bs4
import requests

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 備考の巡回対象 (中国5県)。起点県 (引数 url) を先頭に置き、残りをこの順で続ける。
PREF_ORDER = ["tottori", "shimane", "okayama", "hiroshima", "yamaguchi"]
PREF_JA = {
    "tottori": "鳥取県", "shimane": "島根県", "okayama": "岡山県",
    "hiroshima": "広島県", "yamaguchi": "山口県",
}

# 1 URL の一覧で辿れる件数の上限 (約1万件)。余裕を見て 9,000 超で分割する。
AREA_LIMIT = 9000
# 1エリアあたりの巡回上限ページ数 (暴走防止)。
MAX_PAGES = 400
# エリア分割の再帰上限。
MAX_DEPTH = 3

# 一覧の本体求人リンク (広告枠 _alist_ / レコメンド recom_ は除外)。
_BODY_JOB_RE = re.compile(
    r"/jobdetail/(\d+)\?[^\"']*pname=link_job_detail[a-z_]*_p_\d+_\d+_"
)
# 求人IDだけ拾う (企業ページの求人一覧用)
_JOB_ID_RE = re.compile(r"/jobdetail/(\d+)")
# 企業ページ
_COMPANY_RE = re.compile(r"/cjlist(\d+)/")
# 総件数テキスト ("求人件数は、現在40,614件あります。")
_COUNT_TEXT_RE = re.compile(r"現在\s*([\d,]+)\s*件")
# 企業ページの件数表示 ("1~30件 / 310件")
_PAGE_INFO_RE = re.compile(r"/\s*([\d,]+)\s*件")
# 絞り込みコード (srt2 / mrt13 / trm2-trm3 / hb …)。エリアではない。
_FILTER_SLUG_RE = re.compile(r"^(?:[a-z]{2,4}\d+(?:-[a-z]{2,4}\d+)*|hb)$")
# 地理リンクの文言 (「広島市すべて」「広島市中区」「呉市」「安芸郡」など)
_GEO_TEXT_RE = re.compile(r"[都道府県市区町村郡](?:すべて)?$")
# 「代表TEL: 082-xxx-xxxx」
_TEL_RE = re.compile(r"代表TEL[:：]?\s*([0-9０-９][\d０-９\-‐ー－()（）\s]*\d)")
_TEL_FAX_LINE_RE = re.compile(r"^(?:代表)?(?:TEL|FAX|ＴＥＬ|ＦＡＸ)")
_PREF_RE = re.compile(r"^(東京都|北海道|(?:京都|大阪)府|[^\s都道府県]{2,3}県)")
# "2026年10月13日" → 2026-10-13
_JP_DATE_RE = re.compile(r"(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日")

# CSS Modules のハッシュ付きクラス名は接頭辞で照合する。
_CLS_ITEM_TITLE = re.compile(r"^_itemTitle_")
_CLS_ITEM_VALUE = re.compile(r"^_itemValue_")
_CLS_SECTION = re.compile(r"^_section_")
_CLS_TITLE = re.compile(r"^_title_")
_CLS_PAGE_INFO = re.compile(r"^_pageInfo_")
_CLS_JOB_INFO_ITEM = re.compile(r"^_jobInfoItem_")
_CLS_COMPANY_NAME = re.compile(r"^_companyName_")


class _WafChallenge(requests.exceptions.RequestException):
    """AWS WAF のチャレンジ (HTTP 202 空ボディ) に阻まれ続けた。"""


def _clean(value) -> str:
    """空白を 1 個に潰して前後を削ったテキストを返す。"""
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def _lines(tag) -> list[str]:
    """タグ内テキストを改行で分割し、空行を除いて返す。"""
    if tag is None:
        return []
    raw = tag.get_text("\n").replace("\r", "\n")
    return [line for line in (_clean(x) for x in raw.split("\n")) if line]


class Baitoru7Scraper(StaticCrawler):
    """バイトル 中国5県 企業情報スクレイパー"""

    # 備考「1リクエストごとに1〜2秒」。全リクエスト (一覧・詳細・企業) を
    # REQ_INTERVAL 秒以上空けるスロットルで流量を管理するため、アイテム間ウェイトは重ねない。
    DELAY = 1.5
    ITEM_DELAY = 0.0
    REQ_INTERVAL = 1.5
    TIMEOUT = 20
    # 202 (WAF) を踏んだときの再試行回数とクールダウン秒
    WAF_RETRIES = 3
    WAF_COOLDOWN = (5.0, 15.0, 30.0)

    # Chrome 系のフル UA は AWS WAF のチャレンジ (202) になる。素の UA を使う。
    USER_AGENT = "Mozilla/5.0"

    EXTRA_COLUMNS = [
        "企業ページURL",
        "代表求人URL",
        "掲載開始日",     # 代表求人の掲載開始日
        "更新日",         # 代表求人の更新日
        "求人件数",       # 企業ページの「N件」(全国の掲載求人数)
        "設立年",
        "拠点数",
        "求人掲載県",     # 代表求人を見つけた一覧の県
    ]

    _next_request_at = 0.0   # 次にリクエストしてよい時刻 (monotonic)

    def prepare(self):
        self.session.headers.update({
            "User-Agent": self.USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        })

    # ------------------------------------------------------------------ #
    # 通信 (スロットル + 202 検知)
    # ------------------------------------------------------------------ #
    def _throttle(self, extra_wait: float = 0.0):
        start_at = max(time.monotonic(), self._next_request_at) + extra_wait
        delay = start_at - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        self._next_request_at = time.monotonic() + self.REQ_INTERVAL

    def get_soup(self, url: str) -> Optional[bs4.BeautifulSoup]:
        """HTML を取得する。全リクエストを REQ_INTERVAL 秒以上空ける。

        基底の get_soup は 202 (WAF チャレンジ) を成功扱いにしてしまうため、
        202 / 空ボディを検知して間隔を空けて再試行する。404 等は None を返す。
        """
        def _fetch() -> str:
            cooldown = 0.0
            for attempt in range(self.WAF_RETRIES):
                self._throttle(cooldown)
                self.logger.info("取得中: %s", url)
                resp = self.session.get(url, timeout=self.TIMEOUT)
                if resp.status_code == 202 or not resp.text.strip():
                    cooldown = self.WAF_COOLDOWN[min(attempt, len(self.WAF_COOLDOWN) - 1)]
                    self.logger.warning("WAF 応答 (HTTP %s) — 再試行 %d/%d: %s",
                                        resp.status_code, attempt + 1, self.WAF_RETRIES, url)
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
            self.logger.warning("取得失敗 (スキップして継続): %s — %s", url, e)
            return None

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str) -> Generator[dict, None, None]:
        # 🔒 URL一貫性: 引数 url を唯一のルートとし、origin / リージョン / 県を派生させる。
        root = url if url.endswith("/") else url + "/"
        parsed = urlparse(root)
        self.origin = f"{parsed.scheme}://{parsed.netloc}"
        parts = [p for p in parsed.path.split("/") if p]
        # 形: [{region}, 'jlist', {pref}]
        self.region = parts[0]
        start_pref = parts[2] if len(parts) >= 3 else ""

        prefs = [start_pref] if start_pref else []
        prefs += [p for p in PREF_ORDER if p not in prefs]

        self.seen_jobs: set[str] = set()
        self.seen_companies: set[str] = set()
        seen_areas: set[str] = set()
        for pref in prefs:
            base = f"{self.origin}/{self.region}/jlist/{pref}/"
            self.logger.info("県巡回開始: %s", base)
            yield from self._crawl_area(base, pref, seen_areas, depth=0)

        self.logger.info("処理求人数: %d件 / 企業数: %d社",
                         len(self.seen_jobs), len(self.seen_companies))

    # ------------------------------------------------------------------ #
    # エリア巡回 (件数に応じてページ送り or 市区町村へ分割)
    # ------------------------------------------------------------------ #
    def _crawl_area(self, base: str, pref: str, seen_areas: set,
                    depth: int) -> Generator[dict, None, None]:
        if base in seen_areas:
            return
        seen_areas.add(base)

        soup = self.get_soup(base)
        if soup is None:
            return
        total = self._total_count(soup)
        self.logger.info("一覧: %s 総件数=%s depth=%d",
                         base, total if total is not None else "?", depth)

        # 1ページ目の求人は常に先に処理する (最初の1件を速やかに yield する)。
        yield from self._emit_jobs(soup, pref)

        if total is not None and total > AREA_LIMIT and depth < MAX_DEPTH:
            children = self._child_areas(soup, base)
            if children:
                self.logger.info("上限超過(%d件)につき %d エリアへ分割: %s",
                                 total, len(children), base)
                for child in children:
                    yield from self._crawl_area(child, pref, seen_areas, depth + 1)
                return
            self.logger.warning("子エリアが見つからないため上限までページ送りします: %s", base)

        # 2ページ目以降。範囲外ページは 404 → None、または本体求人 0 件で終了。
        for page_no in range(2, MAX_PAGES + 1):
            soup = self.get_soup(f"{base}page{page_no}/")
            if soup is None or not self._page_job_ids(soup):
                break
            yield from self._emit_jobs(soup, pref)

    def _emit_jobs(self, soup, pref: str) -> Generator[dict, None, None]:
        for job_id in self._page_job_ids(soup):
            if job_id in self.seen_jobs:
                continue
            self.seen_jobs.add(job_id)
            job_url = f"{self.origin}/jobdetail/{job_id}"
            detail = self.get_soup(job_url)
            if detail is None:
                continue
            match = _COMPANY_RE.search(str(detail))
            if not match:
                self.logger.info("企業ページへのリンクが無い求人: %s", job_url)
                continue
            company_id = match.group(1)
            if company_id in self.seen_companies:
                continue
            self.seen_companies.add(company_id)
            item = self._scrape_company(company_id, job_url, detail, pref)
            if item and item.get(Schema.NAME):
                yield item

    @staticmethod
    def _total_count(soup) -> Optional[int]:
        match = _COUNT_TEXT_RE.search(soup.get_text(" ", strip=True))
        if match:
            return int(match.group(1).replace(",", ""))
        counts = [int(d) for d in (re.sub(r"[^\d]", "", s.get_text())
                                   for s in soup.find_all("span", class_=re.compile("countValue")))
                  if d]
        return max(counts) if counts else None

    @staticmethod
    def _page_job_ids(soup) -> list[str]:
        ids: list[str] = []
        for a in soup.select("a[href*='/jobdetail/']"):
            match = _BODY_JOB_RE.search(a.get("href") or "")
            if match and match.group(1) not in ids:
                ids.append(match.group(1))
        return ids

    def _child_areas(self, soup, base: str) -> list[str]:
        """現エリアより深い地理リンク (区・市町村・郡) の末端を列挙する。

        県一覧には /hiroshima/hiroshimashi/ (広島市すべて) と
        /hiroshima/hiroshimashi/hiroshimashinakaku/ (広島市中区) が両方並ぶため、
        他のリンクの祖先になっているもの (「〇〇市すべて」) は落として末端だけ巡回する。
        絞り込みコード (srt2/mrt13…) と職種 (hallstaff 等) は文言で除外する。
        """
        base_segs = [p for p in urlparse(base).path.split("/") if p][2:]
        found: list[tuple] = []
        for a in soup.select("a[href*='/jlist/']"):
            href = (a.get("href") or "").split("?")[0].split("#")[0]
            path = [p for p in urlparse(urljoin(base, href)).path.split("/") if p]
            if len(path) < 4 or path[0] != self.region or path[1] != "jlist":
                continue
            segs = path[2:]
            if len(segs) <= len(base_segs) or segs[:len(base_segs)] != base_segs:
                continue
            if any(_FILTER_SLUG_RE.match(s) or s.startswith("page") for s in segs):
                continue
            if not _GEO_TEXT_RE.search(_clean(a.get_text())):
                continue
            if tuple(segs) not in found:
                found.append(tuple(segs))
        leaves = [s for s in found
                  if not any(o != s and o[:len(s)] == s for o in found)]
        return [f"{self.origin}/{self.region}/jlist/{'/'.join(s)}/" for s in leaves]

    # ------------------------------------------------------------------ #
    # 企業ページ
    # ------------------------------------------------------------------ #
    def _scrape_company(self, company_id: str, job_url: str, detail,
                        pref: str) -> Optional[dict]:
        company_url = f"{self.origin}/cjlist{company_id}/"
        soup = self.get_soup(company_url)
        if soup is None:
            return None

        # 企業ページに並ぶ求人は処理済みにする (同じ企業の詳細を開き直さない)。
        for a in soup.select("a[href*='/jobdetail/']"):
            match = _JOB_ID_RE.search(a.get("href") or "")
            if match:
                self.seen_jobs.add(match.group(1))

        info = self._company_info(soup)
        title = soup.find("h2", class_=_CLS_TITLE)
        name = _clean(title.get_text()) if title else ""
        if not name:
            tag = detail.find("p", class_=_CLS_COMPANY_NAME)
            name = _clean(tag.get_text()) if tag else ""

        # 所在地: 住所行と 代表TEL/FAX 行を分ける。
        addr_lines = info.get("所在地") or []
        tel = ""
        for line in addr_lines:
            m = _TEL_RE.search(line)
            if m:
                tel = _clean(m.group(1))
                break
        address = " ".join(line for line in addr_lines
                           if not _TEL_FAX_LINE_RE.match(line) and not _TEL_RE.search(line))
        if not tel:
            tel = self._detail_tel(detail)
        pref_match = _PREF_RE.match(address)
        hq_pref = pref_match.group(1) if pref_match else ""

        page_info = soup.find("p", class_=_CLS_PAGE_INFO)
        count_match = _PAGE_INFO_RE.search(_clean(page_info.get_text(""))) if page_info else None

        dates = self._job_dates(detail)
        return {
            Schema.URL: company_url,
            Schema.NAME: name,
            Schema.PREF: hq_pref,
            Schema.ADDR: address[len(hq_pref):].strip() if hq_pref else address,
            Schema.TEL: tel,
            Schema.HP: self._value(info.get("URL")),
            Schema.EMP_NUM: self._value(info.get("従業員数")),
            Schema.LOB: "\n".join(info.get("事業内容") or []),
            Schema.REP_NM: self._value(info.get("代表者名")),
            Schema.CAP: self._value(info.get("資本金")),
            "企業ページURL": company_url,
            "代表求人URL": job_url,
            "掲載開始日": dates.get("掲載開始日", ""),
            "更新日": dates.get("更新日", ""),
            "求人件数": count_match.group(1).replace(",", "") if count_match else "",
            "設立年": self._value(info.get("設立年")),
            "拠点数": self._bases(info.get("拠点")),
            "求人掲載県": PREF_JA.get(pref, pref),
        }

    @staticmethod
    def _company_info(soup) -> dict:
        """会社情報パネルの h4._itemTitle_ ラベル → 値の行リスト (h4 の後ろの兄弟要素)。"""
        panel = soup.find(id="company-job-list-company-panel") or soup
        info: dict = {}
        for h4 in panel.find_all("h4", class_=_CLS_ITEM_TITLE):
            label = _clean(h4.get_text())
            if not label or label in info:
                continue
            lines: list[str] = []
            for sib in h4.find_next_siblings():
                # 値は <p> 単位。"240<!-- -->人" のような分割ノードは連結し、
                # 値内の改行 (住所の建物名など) だけを行として分ける。
                for p in ([sib] if sib.name == "p" else sib.find_all("p")) or [sib]:
                    for br in p.find_all("br"):
                        br.replace_with("\n")
                    lines += [x for x in (_clean(t) for t in
                              p.get_text("").replace("\r", "\n").split("\n")) if x]
            info[label] = lines
        return info

    @staticmethod
    def _value(lines) -> str:
        """値の最初の行 ("240人" / "2011年")。"""
        return lines[0] if lines else ""

    @staticmethod
    def _bases(lines) -> str:
        """「拠点」の件数部分 ("154ヶ所")。後ろの補足文は落とす。"""
        text = lines[0] if lines else ""
        m = re.match(r"[\d,]+\s*(?:ヶ所|か所|箇所|カ所)?", text)
        return _clean(m.group(0)) if m else text

    @staticmethod
    def _detail_tel(detail) -> str:
        """求人詳細「事業主情報 > 所在地」の代表TEL (企業の代表番号)。"""
        for h4 in detail.find_all("h4", class_=_CLS_ITEM_TITLE):
            if _clean(h4.get_text()) != "所在地":
                continue
            block = h4.find_parent("div", class_=_CLS_SECTION) or h4.parent
            m = _TEL_RE.search(_clean(block.get_text(" ")))
            if m:
                return _clean(m.group(1))
        return ""

    @staticmethod
    def _job_dates(soup) -> dict:
        """「更新日：/掲載開始日：」→ YYYY-MM-DD。"""
        dates: dict = {}
        for div in soup.find_all("div", class_=_CLS_JOB_INFO_ITEM):
            text = _clean(div.get_text(" "))
            for label in ("更新日", "掲載開始日"):
                if text.startswith(label) and label not in dates:
                    m = _JP_DATE_RE.search(text)
                    if m:
                        dates[label] = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        if not dates:
            # クラス名が変わった場合のフォールバック (本文から直接拾う)
            text = soup.get_text(" ")
            for label in ("更新日", "掲載開始日"):
                m = re.search(label + r"[：:]\s*(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日", text)
                if m:
                    dates[label] = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        return dates


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    scraper = Baitoru7Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.baitoru.com/chushikoku/jlist/hiroshima/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
