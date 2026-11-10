"""
バイトル 14都府県・新着30日・店舗単位 (baitoru.com)

対象サイト: https://www.baitoru.com/kanto/jlist/tokyo/

取得対象:
    東京都・神奈川県・埼玉県・千葉県・三重県・滋賀県・京都府・大阪府・兵庫県・奈良県・
    和歌山県・岐阜県・愛知県・福岡県 の全職種の求人のうち、掲載開始日が実行日から
    30日以内のもの。同じ店舗 (名称＋勤務地住所) は新着順で最初の1件だけ出力する。

取得フロー:
    1. 引数 url (= /kanto/jlist/tokyo/) を唯一のルートとし、配信元(origin) と起点県を派生させる。
       起点県に続けて残り13都府県を巡回する (url のリージョン/県セグメントを差し替え)。
    2. 県一覧ページ上の地理リンク (区・市町村・郡) の末端を列挙し、市区町村単位に分割する
       (1検索 1 万件上限の回避)。「大阪市すべて」のように他のリンクの祖先になるものは落とす。
    3. 市区町村ごとに新着順 (`srt2/`) の一覧を `page{N}/` で巡回する。一覧の本体求人は
       `pname=link_job_detail…_p_{順番}_{件数}_` が付くリンクのみ (広告枠/レコメンドは除外)。
    4. 一覧カードの店舗名＋勤務地で店舗キーを作り、未出現の店舗だけ /jobdetail/{id} を開く
       (同じ店舗の2件目以降は詳細を開かない)。詳細で名称＋勤務地住所が既出なら出力しない。
    5. 詳細の掲載開始日が「実行日 - 30日」より前なら古い求人として出力しない。古い求人が
       OLD_STREAK 件続いたらその市区町村を打ち切る (新着順は更新日も加味されるため、
       1件だけ古い求人が混ざっても即打ち切りにはしない)。

★ UA:
    baitoru.com は AWS WAF を導入しており、Chrome 系のフル UA だと HTTP 202 (空ボディの
    JS チャレンジ) が返る。2026-11 時点では素の "Mozilla/5.0" UA には通常の HTML
    (スマホ版 SSR) が返るため、そちらを使う。202 が続く場合は失敗として扱う。

取得カラム (備考で指定されたもの):
    名称 / 勤務地住所 / 所在地 / 代表TEL / 代表者名 / 事業内容 / 職種 / 雇用形態 / 求人タイトル /
    掲載開始日 / 更新日 / 掲載終了日 / 求人票URL / 取得日 (= Schema.GET_TIME、基盤が付与)
    ※ TEL は事業主情報「所在地」欄の代表TEL のみ。「電話番号を表示」の応募用専用番号は取らない。
    ※ 事業内容は事業主情報の短い業種表記 (例: 飲食)。備考で明示的に取得指示あり。
    ※ 派遣会社・除外業種の絞り込みはしない (後工程で行う)。

利用規約 (https://www.baitoru.com/about/kiyaku_base.html):
    スクレイピング/クローリングを名指しで禁止する条項は無い。第9条(3)に「本サービスを
    利用した営業活動または営利を目的とした情報収集」の禁止があるため運用側で用途に留意。
    全リクエストを REQ_INTERVAL 秒以上空けて逐次 (並列なし) で巡回する。

実行方法:
    python scripts/sites/jobs/baitoru_8.py
    docker compose exec worker python /app/bin/run_flow.py --site-id baitoru_8
"""

import logging
import re
import sys
import time
from datetime import date, timedelta
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

# 備考の巡回対象 (14都府県) → バイトルのリージョン。起点県 (引数 url) を先頭に置き、残りをこの順で続ける。
PREF_REGION = {
    "tokyo": "kanto", "kanagawa": "kanto", "saitama": "kanto", "chiba": "kanto",
    "mie": "tokai", "gifu": "tokai", "aichi": "tokai",
    "shiga": "kansai", "kyoto": "kansai", "osaka": "kansai", "hyogo": "kansai",
    "nara": "kansai", "wakayama": "kansai",
    "fukuoka": "kyushu",
}

# 新着順の並び替えコード
SORT_NEW = "srt2"
# 掲載開始日がこの日数より前の求人は対象外
RECENT_DAYS = 30
# 古い求人がこの件数連続したら市区町村を打ち切る
OLD_STREAK = 3
# 1市区町村あたりの巡回上限ページ数 (1万件 / 26件 ≒ 385)
MAX_PAGES = 400

# 一覧の本体求人リンク (広告枠 _alist_ / レコメンド recom_ は除外)。
_BODY_JOB_RE = re.compile(
    r"/jobdetail/(\d+)\?[^\"']*pname=link_job_detail[a-z_]*_p_\d+_\d+_"
)
# 絞り込みコード (srt2 / mrt13 / trm2-trm3 / hb …)。エリアではない。
_FILTER_SLUG_RE = re.compile(r"^(?:[a-z]{2,4}\d+(?:-[a-z]{2,4}\d+)*|hb)$")
# 地理リンクの文言 (「大阪市すべて」「大阪市北区」「新宿区」「西多摩郡」など)
_GEO_TEXT_RE = re.compile(r"[都道府県市区町村郡](?:すべて)?$")
# 「代表TEL:03-xxxx-xxxx」
_TEL_RE = re.compile(r"代表TEL[:：]?\s*([0-9０-９][\d０-９\-‐ー－()（）\s]*\d)")
_TEL_FAX_LINE_RE = re.compile(r"^(?:代表)?(?:TEL|FAX|ＴＥＬ|ＦＡＸ)")
_PREF_RE = re.compile(r"^(東京都|北海道|(?:京都|大阪)府|[^\s都道府県]{2,3}県)")
# h1 「【店舗名】のアルバイト・パートの求人情報」
_H1_RE = re.compile(r"^【(.+)】の(.+?)の求人情報$")

# CSS Modules のハッシュ付きクラス名は接頭辞で照合する。
_CLS_CARD = re.compile(r"^_card_")
_CLS_STORE_NAME = re.compile(r"^_storeName_")
_CLS_LOCATION_VALUE = re.compile(r"^_locationValue_")
_CLS_ITEM_TITLE = re.compile(r"^_itemTitle_")
_CLS_ITEM_VALUE = re.compile(r"^_itemValue_")
_CLS_HEADER_TITLE = re.compile(r"^_headerTitle_")
_CLS_SUBSECTION = re.compile(r"^_subsection_")
_CLS_INFO_LABEL = re.compile(r"^_infoLabel_")
_CLS_INFO_BLOCK = re.compile(r"^_infoBlock_")
_CLS_CONTENT_TEXT = re.compile(r"^_contentText_")
_CLS_JOB_TITLE = re.compile(r"^_jobTitle_")
_CLS_JOB_INFO_ITEM = re.compile(r"^_jobInfoItem_")


class _WafChallenge(requests.exceptions.RequestException):
    """AWS WAF のチャレンジ (HTTP 202 空ボディ) に阻まれ続けた。"""


def _clean(value) -> str:
    """空白を 1 個に潰して前後を削ったテキストを返す。"""
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def _text(tag) -> str:
    """改行を空白にしたタグ内テキスト ("東京都<!-- -->新宿区" のような分割ノードは連結)。"""
    return _clean(tag.get_text("")) if tag is not None else ""


class Baitoru8Scraper(StaticCrawler):
    """バイトル 14都府県・新着30日・店舗単位 スクレイパー"""

    # 全リクエスト (一覧・詳細) を REQ_INTERVAL 秒以上空けるスロットルで流量を管理するため、
    # アイテム間ウェイトは重ねない。
    DELAY = 3.0
    ITEM_DELAY = 0.0
    REQ_INTERVAL = 3.0
    TIMEOUT = 20
    # 202 (WAF) を踏んだときの再試行回数とクールダウン秒
    WAF_RETRIES = 3
    WAF_COOLDOWN = (5.0, 15.0, 30.0)

    # Chrome 系のフル UA は AWS WAF のチャレンジ (202) になる。素の UA を使う。
    USER_AGENT = "Mozilla/5.0"

    EXTRA_COLUMNS = [
        "勤務地住所",
        "所在地",
        "職種",
        "雇用形態",
        "求人タイトル",
        "掲載開始日",
        "更新日",
        "掲載終了日",
        "求人票URL",
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
        # 🔒 URL一貫性: 引数 url を唯一のルートとし、origin / 起点県を派生させる。
        root = url if url.endswith("/") else url + "/"
        parsed = urlparse(root)
        self.origin = f"{parsed.scheme}://{parsed.netloc}"
        parts = [p for p in parsed.path.split("/") if p]
        # 形: [{region}, 'jlist', {pref}]
        start_pref = parts[2] if len(parts) >= 3 and parts[2] in PREF_REGION else ""

        prefs = [start_pref] if start_pref else []
        prefs += [p for p in PREF_REGION if p not in prefs]

        self.cutoff = date.today() - timedelta(days=RECENT_DAYS)
        self.seen_jobs: set[str] = set()
        self.seen_list_stores: set[tuple] = set()
        self.seen_stores: set[tuple] = set()
        seen_areas: set[str] = set()
        for pref in prefs:
            region = PREF_REGION[pref]
            pref_url = f"{self.origin}/{region}/jlist/{pref}/"
            self.logger.info("県巡回開始: %s (掲載開始日 %s 以降)", pref_url, self.cutoff)
            soup = self.get_soup(pref_url)
            if soup is None:
                continue
            areas = self._city_areas(soup, pref_url, region, pref) or [pref_url]
            self.logger.info("市区町村 %d エリア: %s", len(areas), pref)
            for area in areas:
                if area in seen_areas:
                    continue
                seen_areas.add(area)
                yield from self._crawl_city(area)

        self.logger.info("処理求人数: %d件 / 店舗数: %d",
                         len(self.seen_jobs), len(self.seen_stores))

    # ------------------------------------------------------------------ #
    # 市区町村の新着順一覧
    # ------------------------------------------------------------------ #
    def _crawl_city(self, area: str) -> Generator[dict, None, None]:
        base = f"{area}{SORT_NEW}/"
        old_streak = 0
        for page_no in range(1, MAX_PAGES + 1):
            soup = self.get_soup(base if page_no == 1 else f"{base}page{page_no}/")
            if soup is None:
                return
            cards = self._page_cards(soup)
            if not cards:
                return
            for job_id, list_key in cards:
                if job_id in self.seen_jobs:
                    continue
                self.seen_jobs.add(job_id)
                # 店舗単位: 一覧カードの店舗名＋勤務地で既出なら詳細を開かない。
                if list_key in self.seen_list_stores:
                    continue
                job_url = f"{self.origin}/jobdetail/{job_id}"
                detail = self.get_soup(job_url)
                if detail is None:
                    continue
                item = self._scrape_detail(detail, job_url)
                start = item.get("掲載開始日", "")
                if start and start < self.cutoff.isoformat():
                    old_streak += 1
                    if old_streak >= OLD_STREAK:
                        self.logger.info("掲載開始日 %s が期限外の求人が %d 件連続 → 打ち切り: %s",
                                         start, old_streak, area)
                        return
                    continue
                old_streak = 0
                self.seen_list_stores.add(list_key)
                store_key = (item[Schema.NAME], item["勤務地住所"])
                if not item[Schema.NAME] or store_key in self.seen_stores:
                    continue
                self.seen_stores.add(store_key)
                yield item

    @staticmethod
    def _page_cards(soup) -> list[tuple]:
        """一覧の本体求人 (求人ID, (店舗名, 勤務地)) を表示順に返す。"""
        cards: list[tuple] = []
        seen: set[str] = set()
        for a in soup.select("a[href*='/jobdetail/']"):
            match = _BODY_JOB_RE.search(a.get("href") or "")
            if not match or match.group(1) in seen:
                continue
            seen.add(match.group(1))
            card = a.find_parent("div", class_=_CLS_CARD)
            name = _text(card.find(class_=_CLS_STORE_NAME)) if card else ""
            loc = _text(card.find(class_=_CLS_LOCATION_VALUE)) if card else ""
            # カードから店舗名が取れない場合は求人IDをキーにして必ず詳細を開く。
            key = (name, loc) if name else ("#" + match.group(1), "")
            cards.append((match.group(1), key))
        return cards

    def _city_areas(self, soup, pref_url: str, region: str, pref: str) -> list[str]:
        """県一覧ページの地理リンク (区・市町村・郡) の末端を列挙する。

        「大阪市すべて」(/osaka/osakashi/) と「大阪市北区」(/osaka/osakashi/osakashikitaku/) が
        両方並ぶため、他のリンクの祖先になっているものは落として末端だけ巡回する。
        絞り込みコード (srt2/mrt13…) と職種 (hallstaff 等) は文言で除外する。
        """
        found: list[tuple] = []
        for a in soup.select(f"a[href*='/jlist/{pref}/']"):
            href = (a.get("href") or "").split("?")[0].split("#")[0]
            path = [p for p in urlparse(urljoin(pref_url, href)).path.split("/") if p]
            if len(path) < 4 or path[0] != region or path[1] != "jlist" or path[2] != pref:
                continue
            segs = path[3:]
            if any(_FILTER_SLUG_RE.match(s) or s.startswith("page") for s in segs):
                continue
            if not _GEO_TEXT_RE.search(_clean(a.get_text())):
                continue
            if tuple(segs) not in found:
                found.append(tuple(segs))
        leaves = [s for s in found
                  if not any(o != s and o[:len(s)] == s for o in found)]
        return [f"{self.origin}/{region}/jlist/{pref}/{'/'.join(s)}/" for s in leaves]

    # ------------------------------------------------------------------ #
    # 求人詳細
    # ------------------------------------------------------------------ #
    def _scrape_detail(self, soup, job_url: str) -> dict:
        h1 = _text(soup.find("h1"))
        m = _H1_RE.match(h1)
        name, emp_type = (m.group(1), m.group(2)) if m else (h1, "")

        work_addr = self._work_address(soup)
        pref_match = _PREF_RE.match(work_addr)
        pref = pref_match.group(1) if pref_match else ""
        city_addr = work_addr[len(pref):].strip() if pref else work_addr

        company = self._company_info(soup)
        addr_lines = company.get("所在地") or []
        tel = ""
        for line in addr_lines:
            tm = _TEL_RE.search(line)
            if tm:
                tel = _clean(tm.group(1))
                break
        hq_addr = " ".join(line for line in addr_lines
                           if not _TEL_FAX_LINE_RE.match(line) and not _TEL_RE.search(line))

        dates = self._job_dates(soup)
        return {
            Schema.URL: job_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: city_addr,
            Schema.TEL: tel,
            Schema.REP_NM: " ".join(company.get("代表者名") or []),
            Schema.LOB: " ".join(company.get("事業内容") or []),
            "勤務地住所": work_addr,
            "所在地": hq_addr,
            "職種": self._job_category(soup),
            "雇用形態": emp_type,
            "求人タイトル": _text(soup.find("h2", class_=_CLS_JOB_TITLE)),
            "掲載開始日": dates.get("掲載開始日", ""),
            "更新日": dates.get("更新日", ""),
            "掲載終了日": dates.get("掲載終了日", ""),
            "求人票URL": job_url,
        }

    @staticmethod
    def _subsection(soup, label: str):
        for h4 in soup.find_all("h4", class_=_CLS_HEADER_TITLE):
            if _clean(h4.get_text()) == label:
                return h4.find_parent("div", class_=_CLS_SUBSECTION)
        return None

    def _work_address(self, soup) -> str:
        """「勤務地・面接地」(または「勤務地」) の最初の住所 (例: 東京都新宿区西新宿7-12-24 …)。

        派遣・警備などは市区町村までしか掲載されない (例: 東京都新宿区)。「面接地」は取らない。
        """
        sec = self._subsection(soup, "勤務地・面接地") or self._subsection(soup, "勤務地")
        if sec is None:
            return ""
        for label in sec.find_all("p", class_=_CLS_INFO_LABEL):
            if _clean(label.get_text()) != "住所":
                continue
            block = label.find_parent("div", class_=_CLS_INFO_BLOCK) or label.parent
            value = block.find("p", class_=_CLS_CONTENT_TEXT)
            if value is not None:
                return _text(value)
        return ""

    def _job_category(self, soup) -> str:
        """「仕事内容」直下の職種行 (例: ホールスタッフ(配膳)、レジ打ち)。本文プロースは取らない。"""
        sec = self._subsection(soup, "仕事内容")
        if sec is None:
            return ""
        value = sec.find("p", class_=_CLS_CONTENT_TEXT, recursive=False)
        return _text(value)

    @staticmethod
    def _company_info(soup) -> dict:
        """事業主情報の h4._itemTitle_ ラベル → 値 (p._itemValue_) の行リスト。"""
        info: dict = {}
        for h4 in soup.find_all("h4", class_=_CLS_ITEM_TITLE):
            label = _clean(h4.get_text())
            if not label or label in info:
                continue
            # 値は h4 の直後の p か、折りたたみ用 div の中の p (事業内容など)。
            info[label] = [v for v in (_text(p) for p in
                           h4.parent.find_all("p", class_=_CLS_ITEM_VALUE)) if v]
        return info

    @staticmethod
    def _job_dates(soup) -> dict:
        """「更新日：/掲載開始日：/掲載終了日：」→ YYYY-MM-DD。"""
        labels = ("更新日", "掲載開始日", "掲載終了日")
        texts = [_clean(d.get_text(" ")) for d in soup.find_all("div", class_=_CLS_JOB_INFO_ITEM)]
        if not texts:
            # クラス名が変わった場合のフォールバック (本文から直接拾う)
            texts = [_clean(soup.get_text(" "))]
        dates: dict = {}
        for text in texts:
            for label in labels:
                if label in dates:
                    continue
                m = re.search(label + r"[：:]\s*(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日", text)
                if m:
                    dates[label] = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        return dates


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    scraper = Baitoru8Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.baitoru.com/kanto/jlist/tokyo/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
