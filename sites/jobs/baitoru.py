"""
---------------------------------------------------------------------------
ver 1.0.0 不明       新規作成
ver 1.1.0 20260526 kanda TELカラム追加し、住所から都道府県を抽出するように修正。
ver 1.2.0 20260529 全企業網羅対応。旧 /search/list/(404) を廃止し、地域別
                   求人一覧(/{region}/jlist/{pref}/)をサイトマップから自動探索。
                   全ページをページネーション巡回し、掲載企業を取りこぼさず収集。
ver 1.3.0 20260629 全国全求人(約300万件)網羅対応。
                   - parse() 引数 url を唯一のルートとし、配信元(origin)・
                     サイトマップ・各URLを url から派生(URL一貫性ルール準拠)。
                   - sitemap_ba_area.xml の「葉(leaf)」エリア一覧(市区町村粒度)
                     のみを巡回起点に採用。各エリアのページ送りはその総件数を
                     完全に網羅する(例: 新宿区=14,842件=400ページ)ため、
                     全エリアの和集合で全国の全求人を取りこぼさず収集できる。
                   - 重複排除キーを「企業」から「求人(求人詳細URL)」へ変更。
                     1求人=1行で出力し、約300万件の想定規模に一致させる。
ver 1.4.0 20260702 各地区の2ページ目以降が取得できない不具合を修正。
                   - 原因: ページ送りの継続判定に「新規求人の有無(found_new)」を
                     使っていたが、seen_jobs はエリア横断で共有されるため、後続
                     エリアの1ページ目が既取得求人と重複すると found_new=False と
                     なり 2ページ目へ進む前に巡回が打ち切られていた。
                   - 対処: ページ送りの継続はページ自体の有効性で判定する。
                     範囲外ページは HTTP 404 を返す(実測)ので、goto の応答
                     ステータスと求人リンクの有無で最終ページ超えを検出し、
                     それ以外は最大ページまで無条件に page{N}/ を辿る。
                     seen_jobs は出力の重複排除にのみ使用する。
ver 1.5.0 20260706 someya 1エリア1万件上限(=400ページ)の取りこぼしを解消。
                   - バイトルの一覧は1エリアあたり最大10,000件(=400ページ)しか
                     返さない。市区町村(区)単位でも1万件を超えるエリア(例: 新宿区
                     =12,428件)があり、旧実装はページ送りだけでは超過分を取得
                     できず取りこぼしていた。
                   - 対処: 各エリアの総件数(<b>N件</b>)を読み取り、10,000件を
                     超える場合はページ送りせず、そのページ上の1段深い絞り込み
                     リンク(職種/駅・エリア等の子リンク)へ再帰的に降りて分割
                     取得する(例: 新宿区 → 新宿東口 等)。子リンクの和集合で
                     全求人を被覆し、seen_jobs で重複出力を排除する。総件数が
                     10,000件以下のエリアは従来どおり全ページを巡回する。
                   - 電話番号は求人詳細の「電話番号を表示する」ボタン
                     (a.tel-entry[data-obo_tel])から取得する方式を最優先に変更。
                     住所内 TEL 抽出は data-obo_tel が無い場合のフォールバック。
                     想定件数は全国約250万件。
---------------------------------------------------------------------------
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

# 正規ルートURL（sites.yml 登録値）。コンテナ実行・テスト実行ともに parse() へ
# この URL が渡される。__main__ の execute() にも必ずこの文字列を渡すこと。
ROOT_URL = "https://baitoru.com/search/list/"

# サイトマップ取得に失敗した場合のフォールバック（地域トップを丸ごと巡回）。
REGIONS = [
    "kanto", "tohoku", "tokai", "kansai",
    "koshinetsu", "chushikoku", "kyushu",
]

# バイトルが1エリアの一覧で返す上限件数。これを超えるエリアはページ送り
# だけでは取りこぼすため、1段深い絞り込みリンクへ再帰的に降りて分割取得する。
AREA_LIMIT = 10000

# 1エリアあたりの巡回上限ページ数（暴走防止のセーフティ）。
# 上限件数(10,000)まで巡回できるよう、実測の最大ページ数(=400)に合わせる。
MAX_PAGES = 400

# 絞り込みの再帰上限（region/jlist/pref/city/ward/subarea… の想定最大深度）。
MAX_DEPTH = 6

# 都道府県スラッグ → 日本語名（住所から取れない場合のデフォルト用）。
# バイトルのURLスラッグは標準ローマ字と一部異なるため別名も登録する。
PREF_JA = {
    "hokkaido": "北海道", "aomori": "青森県", "iwate": "岩手県", "miyagi": "宮城県",
    "akita": "秋田県", "yamagata": "山形県", "fukushima": "福島県", "ibaraki": "茨城県",
    "tochigi": "栃木県", "gunma": "群馬県", "gumma": "群馬県", "saitama": "埼玉県",
    "chiba": "千葉県", "tokyo": "東京都", "kanagawa": "神奈川県", "niigata": "新潟県",
    "nigata": "新潟県", "toyama": "富山県", "ishikawa": "石川県", "fukui": "福井県",
    "yamanashi": "山梨県", "nagano": "長野県", "gifu": "岐阜県", "shizuoka": "静岡県",
    "aichi": "愛知県", "mie": "三重県", "shiga": "滋賀県", "kyoto": "京都府",
    "osaka": "大阪府", "hyogo": "兵庫県", "nara": "奈良県", "wakayama": "和歌山県",
    "tottori": "鳥取県", "shimane": "島根県", "okayama": "岡山県", "hiroshima": "広島県",
    "yamaguchi": "山口県", "tokushima": "徳島県", "kagawa": "香川県", "ehime": "愛媛県",
    "kochi": "高知県", "fukuoka": "福岡県", "saga": "佐賀県", "nagasaki": "長崎県",
    "kumamoto": "熊本県", "oita": "大分県", "miyazaki": "宮崎県", "kagoshima": "鹿児島県",
    "okinawa": "沖縄県",
}

# 求人詳細ページのURL（…/job123456/）にマッチ。応募フォーム(/entry/)は除外する。
_JOB_DETAIL_RE = re.compile(r"/job\d+/?$")
# 地域別求人一覧エリアURL（…/{region}/jlist/{pref}/…/）を抽出する。
# 末尾が job\d は求人詳細なので、後段で別途除外する。
_AREA_LIST_RE = re.compile(r"https?://[^/]+/[a-z]+/jlist/[a-z0-9/]+/")
# 「N件」表記から件数を取り出す（総件数は <b>N件</b> に入る）。
_COUNT_RE = re.compile(r"^\s*([\d,]+)\s*件\s*$")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def _norm_area(u: str) -> str:
    """エリア一覧URLを正規化（フラグメント/クエリ除去・末尾スラッシュ付与）。"""
    u = (u or "").split("#")[0].split("?")[0].strip()
    if u and not u.endswith("/"):
        u += "/"
    return u


class BaitoruScraper(DynamicCrawler):
    """バイトル 全国求人スクレイパー（baitoru.com）

    sitemap_ba_area.xml から市区町村粒度の「葉」エリア一覧を自動探索し、各エリアを
    巡回する。1エリアの一覧はバイトル側の上限(10,000件=400ページ)までしか返さ
    ないため、総件数がこの上限を超えるエリアは、そのページ上の1段深い絞り込み
    リンク(職種/駅・エリア等)へ再帰的に降りて分割取得する。求人詳細URLを重複排除
    キーにして全国の全求人（約250万件想定）を取りこぼさず収集する。
    """

    DELAY = 1.0
    EXTRA_COLUMNS = ["業種", "代表者", "採用人数"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # URL一貫性ルール: 引数 url を唯一のルートとし、配信元を派生させる。
        self.root = url
        parsed = urlparse(url)
        self.origin = f"{parsed.scheme}://{parsed.netloc}"

        seen_jobs: set[str] = set()   # 訪問済み求人詳細URL（重複排除キー）
        seen_areas: set[str] = set()  # 巡回済みエリアURL（再帰の重複防止）

        area_lists = self._discover_area_lists()
        self.logger.info("巡回対象の葉エリア一覧: %d件", len(area_lists))

        for base in area_lists:
            yield from self._crawl_area(base, seen_jobs, seen_areas, depth=0)

        self.logger.info("収集求人数: %d件", len(seen_jobs))

    # ------------------------------------------------------------------ #
    # 巡回対象URLの探索
    # ------------------------------------------------------------------ #
    def _discover_area_lists(self) -> list[str]:
        """サイトマップから市区町村粒度の「葉」エリア一覧URLを取得する。

        sitemap_ba_area.xml には地域>都道府県>市区町村 の各階層URLが含まれる。
        他URLの接頭辞になっていない（=より深い子を持たない）URL＝葉のみを採用
        することで、親子の重複巡回を避けつつ全エリアを網羅する。
        各エリアのページ送りはその総件数を完全にカバーするため、葉エリアの
        和集合で全国の全求人を取りこぼさない。

        取得できなければ地域トップ(/{region}/jlist/)へフォールバックする。
        """
        try:
            sitemap = urljoin(self.origin + "/", "sitemap_ba_area.xml")
            self.page.goto(sitemap, wait_until="domcontentloaded")
            content = self.page.content()
        except Exception as e:  # noqa: BLE001
            self.logger.warning("サイトマップ取得失敗(%s)。地域トップで巡回します。", e)
            content = ""

        locs: set[str] = set()
        for m in _AREA_LIST_RE.findall(content):
            u = _norm_area(m)
            # 求人詳細URL(…/jobNNN/)が紛れ込んだ場合は除外する。
            if _JOB_DETAIL_RE.search(u):
                continue
            locs.add(u)

        if not locs:
            return [urljoin(self.origin + "/", f"{r}/jlist/") for r in REGIONS]

        # 葉抽出: 他のどのURLの真の接頭辞にもなっていないURLだけを残す。
        loc_list = sorted(locs)
        leaves = [
            u for u in loc_list
            if not any(o != u and o.startswith(u) for o in loc_list)
        ]
        return leaves

    @staticmethod
    def _pref_slug(base: str) -> str:
        parts = [p for p in urlparse(base).path.split("/") if p]
        # ['{region}', 'jlist', '{pref}', ...]
        return parts[2] if len(parts) >= 3 else ""

    # ------------------------------------------------------------------ #
    # エリア巡回（件数に応じてページ送り or 再帰分割）
    # ------------------------------------------------------------------ #
    def _crawl_area(self, base: str, seen_jobs: set, seen_areas: set,
                    depth: int) -> Generator[dict, None, None]:
        """1エリアを巡回する。

        まず1ページ目を取得して総件数(<b>N件</b>)を読む。総件数が上限
        (10,000件)以下なら全ページをページ送りで巡回する。上限を超える場合は
        ページ送りでは超過分を取りこぼすため、そのページ上の1段深い絞り込み
        リンク（職種/駅・エリア等の子リンク）へ再帰的に降りて分割取得する。
        いずれの場合も1ページ目の求人は先に出力するため、最初の1件は速やかに
        yield される。seen_jobs は求人の重複出力を、seen_areas はエリア再帰の
        重複を防ぐために用いる。
        """
        base = _norm_area(base)
        if base in seen_areas:
            return
        seen_areas.add(base)

        try:
            resp = self.page.goto(base, wait_until="domcontentloaded")
        except Exception:
            return
        if resp is not None and resp.status >= 400:
            return
        try:
            self.page.wait_for_selector("a[href*='/job']", timeout=8000)
        except Exception:
            return

        page_url = self.page.url
        soup = BeautifulSoup(self.page.content(), "html.parser")
        pref_ja = PREF_JA.get(self._pref_slug(base), "")

        total = self._total_count(soup)
        self.logger.info("一覧巡回: %s (%s) 総件数=%s depth=%d",
                         base, pref_ja or self._pref_slug(base),
                         total if total is not None else "?", depth)

        # 1ページ目の求人は常に先に出力する（最初の1件を速やかに yield）。
        yield from self._emit_jobs(soup, page_url, pref_ja, seen_jobs)

        # 上限超過エリアはページ送りせず、1段深い絞り込みへ降りて分割取得する。
        if total is not None and total > AREA_LIMIT and depth < MAX_DEPTH:
            children = self._child_areas(soup, page_url, base)
            if children:
                self.logger.info("上限超過(%d件)につき %d 個の子エリアへ分割: %s",
                                 total, len(children), base)
                for child in children:
                    yield from self._crawl_area(
                        child, seen_jobs, seen_areas, depth + 1)
                return
            # 子リンクが無い最深エリアは、取れる範囲(=上限まで)だけ巡回する。

        # 上限以下 or これ以上分割できないエリア → 2ページ目以降を巡回。
        page_no = 2
        while page_no <= MAX_PAGES:
            list_url = f"{base}page{page_no}/"
            try:
                resp = self.page.goto(list_url, wait_until="domcontentloaded")
            except Exception:
                break
            # 範囲外ページ(最終ページ超え)は 404。8秒待たず即終了する。
            if resp is not None and resp.status >= 400:
                break
            try:
                self.page.wait_for_selector("a[href*='/job']", timeout=8000)
            except Exception:
                break

            page_url = self.page.url
            soup = BeautifulSoup(self.page.content(), "html.parser")
            if not self._page_job_urls(soup, page_url):
                break  # このページに求人が無い → 巡回終了
            yield from self._emit_jobs(soup, page_url, pref_ja, seen_jobs)
            page_no += 1

    def _emit_jobs(self, soup, page_url: str, pref_ja: str,
                   seen_jobs: set) -> Generator[dict, None, None]:
        """一覧ページ上の各求人詳細を取得して出力する（重複排除つき）。"""
        for job_url in self._page_job_urls(soup, page_url):
            if job_url in seen_jobs:
                continue  # 別エリアで取得済みの求人は出力しない（重複排除）
            seen_jobs.add(job_url)
            item = self._scrape_detail(job_url, pref_ja)
            if not item or not item.get(Schema.NAME):
                continue
            yield item

    @staticmethod
    def _total_count(soup) -> int | None:
        """一覧ページの総件数を返す。総件数は <b>N件</b> に入る（実測）。

        表示件数の切替(20/30/40件)は em/a 要素なので <b> のみを対象にすることで
        混同を避ける。複数あれば最大値（＝そのエリアの総件数）を採用する。
        """
        counts: list[int] = []
        for b in soup.find_all("b"):
            m = _COUNT_RE.match(b.get_text())
            if m:
                counts.append(int(m.group(1).replace(",", "")))
        return max(counts) if counts else None

    def _child_areas(self, soup, page_url: str, base: str) -> list[str]:
        """現在エリアの「1段深い」絞り込みリンク（子エリア）を抽出する。

        子リンクは現在エリアURLをパス接頭辞に持ち、パスセグメントが1つだけ深い
        /…/jlist/… のURL（職種・駅・エリア等の絞り込み）。求人詳細(/jobNNN/)は
        除外する。職種／地理いずれの子リンクでも和集合は全求人を被覆するため、
        分割起点として利用できる（重複は seen_jobs で排除）。
        """
        base_parts = [p for p in urlparse(base).path.split("/") if p]
        children: set[str] = set()
        for a in soup.select("a[href*='/jlist/']"):
            href = (a.get("href", "") or "")
            if "/job" in href:
                continue
            full = _norm_area(urljoin(page_url, href))
            if _JOB_DETAIL_RE.search(full):
                continue
            parts = [p for p in urlparse(full).path.split("/") if p]
            # 現在エリアの真下（セグメント +1）の子だけを採用する。
            if len(parts) == len(base_parts) + 1 and parts[:len(base_parts)] == base_parts:
                children.add(full)
        return sorted(children)

    @staticmethod
    def _page_job_urls(soup, page_url: str) -> list[str]:
        """一覧ページから求人詳細URL(…/jobNNN/)を抽出（ページ内重複排除）。"""
        urls: list[str] = []
        seen: set[str] = set()
        for a in soup.select("a[href*='/job']"):
            href = (a.get("href", "") or "").split("?")[0].split("#")[0]
            if "/entry/" in href:
                continue
            if not _JOB_DETAIL_RE.search(href):
                continue
            full = urljoin(page_url, href).rstrip("/") + "/"
            if full not in seen:
                seen.add(full)
                urls.append(full)
        return urls

    # ------------------------------------------------------------------ #
    # 求人詳細ページから企業情報を抽出
    # ------------------------------------------------------------------ #
    def _scrape_detail(self, url: str, pref_ja: str) -> dict | None:
        try:
            self.page.goto(url, wait_until="domcontentloaded")
            self.page.wait_for_selector("div.detail-companyInfo", timeout=10000)
        except Exception:
            return None
        soup = BeautifulSoup(self.page.content(), "html.parser")

        # 重複排除キー兼出力URLは求人詳細URL（1求人=1行）。
        data = {Schema.URL: url, Schema.PREF: pref_ja}

        # 電話番号は「電話番号を表示する」ボタン(a.tel-entry[data-obo_tel])から
        # 取得する方式を最優先とする。data-obo_tel には実番号(03-…)または
        # フリーダイヤル(0120-…)が入り、末尾に空白を含むことがあるので除去する。
        tel_btn = soup.select_one("a.tel-entry[data-obo_tel], a[data-obo_tel]")
        if tel_btn:
            obo_tel = (tel_btn.get("data-obo_tel") or "").strip()
            if obo_tel:
                data[Schema.TEL] = obo_tel

        company_info = soup.find("div", class_="detail-companyInfo")
        if company_info:
            pt02 = company_info.find("div", class_="pt02")
            if pt02:
                p = pt02.find("p")
                if p:
                    a = p.find("a")
                    data[Schema.NAME] = _clean(a.get_text() if a else p.get_text())

            pt03 = company_info.find("div", class_="pt03")
            if pt03:
                for dl in pt03.find_all("dl"):
                    dt = dl.find("dt")
                    dd = dl.find("dd")
                    if not dt or not dd:
                        continue
                    key = dt.get_text(strip=True)
                    val = _clean(dd.get_text(" "))
                    if "所在地" in key:
                        tel_match = re.search(
                            r"(TEL|ＴＥＬ|電話)[番号]*[：:\s　]*([\d\-（）()０-９ー‐]+)",
                            val,
                            flags=re.IGNORECASE,
                        )
                        if tel_match and not data.get(Schema.TEL):
                            data[Schema.TEL] = tel_match.group(2).strip()
                        addr = re.sub(
                            r"[\s　]*(TEL|FAX|ＴＥＬ|ＦＡＸ|電話|Fax)[番号]*[：:\s　]*[\d\-（）()０-９ー‐]+",
                            "",
                            val,
                            flags=re.IGNORECASE,
                        ).strip()
                        data[Schema.ADDR] = addr
                        pref_match = re.match(
                            r"(北海道|東京都|京都府|大阪府|.{2,3}[都道府県])", addr
                        )
                        if pref_match:
                            data[Schema.PREF] = pref_match.group(1)
                    elif "代表電話番号" in key or "電話番号" in key:
                        if not data.get(Schema.TEL):  # obo_tel を最優先
                            data[Schema.TEL] = val
                    elif "代表者" in key:
                        data[Schema.REP_NM] = val
                    elif "事業内容" in key or "業種" in key:
                        data["業種"] = val
                    elif "ホームページ" in key or "URL" in key:
                        a = dd.find("a", href=True)
                        data[Schema.HP] = a["href"] if a else val
                    elif "採用予定人数" in key:
                        data["採用人数"] = val

        if not data.get(Schema.NAME):
            h1 = soup.select_one("h1")
            if h1:
                data[Schema.NAME] = _clean(h1.get_text())

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    # ローカル実行とコンテナ実行を一致させるため、必ず正規ルートURLを渡す。
    BaitoruScraper().execute(ROOT_URL)
