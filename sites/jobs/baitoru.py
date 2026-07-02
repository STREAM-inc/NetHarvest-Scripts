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

# 1エリアあたりの巡回上限ページ数（暴走防止のセーフティ）。
# 市区町村粒度では最大でも数百ページ（新宿区=400ページ）に収まる。
MAX_PAGES = 1000

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

    sitemap_ba_area.xml から市区町村粒度の「葉」エリア一覧を自動探索し、
    各エリアを全ページ巡回。各求人詳細から企業情報を抽出し、求人詳細URLを
    重複排除キーにして全国の全求人（約300万件想定）を取りこぼさず収集する。
    """

    DELAY = 1.0
    EXTRA_COLUMNS = ["業種", "代表者", "採用人数"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # URL一貫性ルール: 引数 url を唯一のルートとし、配信元を派生させる。
        self.root = url
        parsed = urlparse(url)
        self.origin = f"{parsed.scheme}://{parsed.netloc}"

        seen_jobs: set[str] = set()  # 訪問済み求人詳細URL（重複排除キー）

        area_lists = self._discover_area_lists()
        self.logger.info("巡回対象の葉エリア一覧: %d件", len(area_lists))

        for base in area_lists:
            slug = self._pref_slug(base)
            pref_ja = PREF_JA.get(slug, "")
            self.logger.info("一覧巡回: %s (%s)", base, pref_ja or slug)
            yield from self._scrape_list(base, pref_ja, seen_jobs)

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
    # 一覧ページのページネーション巡回
    # ------------------------------------------------------------------ #
    def _scrape_list(self, base: str, pref_ja: str,
                     seen_jobs: set) -> Generator[dict, None, None]:
        # ページ送りの継続はページ自体の有効性で判定する（新規求人の有無では
        # 判定しない）。範囲外ページ(最終ページ超え)はバイトルが HTTP 404 を
        # 返すため、goto の応答ステータスと求人リンクの有無で終端を検出し、
        # それ以外は最大ページまで無条件に page{N}/ を辿る。seen_jobs は
        # エリア横断で共有され、重複求人の「出力」抑止にのみ用いる。
        page_no = 1
        while page_no <= MAX_PAGES:
            list_url = base if page_no == 1 else f"{base}page{page_no}/"
            try:
                resp = self.page.goto(list_url, wait_until="domcontentloaded")
            except Exception:
                break  # 取得失敗 → このエリアの巡回終了

            # 範囲外ページ(最終ページ超え)は 404。8秒待たず即終了する。
            if resp is not None and resp.status >= 400:
                break

            try:
                self.page.wait_for_selector("a[href*='/job']", timeout=8000)
            except Exception:
                break  # 求人リンクが無い＝実質的に最終ページ超え

            page_url = self.page.url
            soup = BeautifulSoup(self.page.content(), "html.parser")

            job_urls = self._page_job_urls(soup, page_url)
            if not job_urls:
                break  # このページに求人が無い → 巡回終了

            for job_url in job_urls:
                if job_url in seen_jobs:
                    continue  # 別エリアで取得済みの求人は出力しない（重複排除）
                seen_jobs.add(job_url)

                item = self._scrape_detail(job_url, pref_ja)
                if not item or not item.get(Schema.NAME):
                    continue
                yield item

            # ページ内の求人が全て既取得(seen_jobs)でも、次ページは無条件に辿る。
            # ここで打ち切ると重複エリアで2ページ目以降が取得できなくなる。
            page_no += 1

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
