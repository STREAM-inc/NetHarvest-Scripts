"""
---------------------------------------------------------------------------
ver 1.0.0 20260701 新規作成（バイトル関西）。
                   - baitoru.com の地域別求人一覧を「関西エリアに限定」して網羅収集。
                   - parse() 引数 url（=https://www.baitoru.com/kansai/jlist/）を
                     唯一のルートとし、配信元(origin)・地域スラッグ・サイトマップ・
                     各ページURLをすべて url から派生させる（URL一貫性ルール準拠）。
                   - sitemap_ba_area.xml の「葉(leaf)」エリア一覧のうち、url の
                     地域(kansai)配下の市区町村粒度のみを巡回起点に採用。各エリアの
                     ページ送りはその総件数を完全にカバーするため、関西の全求人を
                     取りこぼさず収集できる。
                   - 重複排除キーは求人詳細URL（1求人=1行）。
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

# 1エリアあたりの巡回上限ページ数（暴走防止のセーフティ）。
MAX_PAGES = 1000

# 都道府県スラッグ → 日本語名（住所から取れない場合のデフォルト用）。
# バイトルのURLスラッグは標準ローマ字と一部異なるため別名も登録する。
PREF_JA = {
    "osaka": "大阪府", "hyogo": "兵庫県", "kyoto": "京都府", "shiga": "滋賀県",
    "nara": "奈良県", "wakayama": "和歌山県", "mie": "三重県",
}

# 求人詳細ページのURL（…/job123456/）にマッチ。応募フォーム(/entry/)は除外する。
_JOB_DETAIL_RE = re.compile(r"/job\d+/?$")
# 地域別求人一覧エリアURL（…/{region}/jlist/{pref}/…/）を抽出する。
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


class Baitoru5Scraper(DynamicCrawler):
    """バイトル関西 求人スクレイパー（baitoru.com /kansai/jlist/）

    引数 url の地域(kansai)配下の市区町村粒度エリアを sitemap_ba_area.xml から
    自動探索し、各エリアを全ページ巡回。各求人詳細から企業情報を抽出し、
    求人詳細URLを重複排除キーにして関西の全求人を取りこぼさず収集する。
    """

    DELAY = 1.0
    # 構造化された短いラベル/コードのみを EXTRA に採用（自由記述プロースは除外）。
    EXTRA_COLUMNS = ["求人タイトル", "派遣許可番号", "有料職業紹介事業許可番号"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # URL一貫性ルール: 引数 url を唯一のルートとし、配信元・地域を派生させる。
        self.root = _norm_area(url)
        parsed = urlparse(self.root)
        self.origin = f"{parsed.scheme}://{parsed.netloc}"

        seen_jobs: set[str] = set()  # 訪問済み求人詳細URL（重複排除キー）

        area_lists = self._discover_area_lists()
        self.logger.info("巡回対象の関西エリア一覧: %d件", len(area_lists))

        for base in area_lists:
            slug = self._pref_slug(base)
            pref_ja = PREF_JA.get(slug, "")
            self.logger.info("一覧巡回: %s (%s)", base, pref_ja or slug)
            yield from self._scrape_list(base, pref_ja, seen_jobs)

        self.logger.info("収集求人数: %d件", len(seen_jobs))

    # ------------------------------------------------------------------ #
    # 巡回対象URLの探索（引数 url の地域配下に限定）
    # ------------------------------------------------------------------ #
    def _discover_area_lists(self) -> list[str]:
        """サイトマップから、引数 url 配下の「葉」エリア一覧URLを取得する。

        sitemap_ba_area.xml には全国の地域>都道府県>市区町村 の各階層URLが
        含まれる。ここでは引数 url（=関西トップ）を接頭辞に持つURLだけに絞り、
        さらに他URLの接頭辞になっていない（=より深い子を持たない）葉のみを採用
        することで、関西エリアを親子重複なく網羅する。

        取得できなければ引数 url そのものを巡回起点にフォールバックする。
        """
        try:
            sitemap = urljoin(self.origin + "/", "sitemap_ba_area.xml")
            self.page.goto(sitemap, wait_until="domcontentloaded")
            content = self.page.content()
        except Exception as e:  # noqa: BLE001
            self.logger.warning("サイトマップ取得失敗(%s)。url を直接巡回します。", e)
            content = ""

        locs: set[str] = set()
        for m in _AREA_LIST_RE.findall(content):
            u = _norm_area(m)
            # 求人詳細URL(…/jobNNN/)が紛れ込んだ場合は除外する。
            if _JOB_DETAIL_RE.search(u):
                continue
            # 引数 url（関西トップ）配下のエリアだけに限定する。
            if not u.startswith(self.root):
                continue
            locs.add(u)

        if not locs:
            return [self.root]

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
        page_no = 1
        while page_no <= MAX_PAGES:
            list_url = base if page_no == 1 else f"{base}page{page_no}/"
            try:
                self.page.goto(list_url, wait_until="domcontentloaded")
                self.page.wait_for_selector("a[href*='/job']", timeout=8000)
            except Exception:
                break  # ページ無し or 取得失敗 → このエリアの巡回終了

            page_url = self.page.url
            soup = BeautifulSoup(self.page.content(), "html.parser")

            found_new = False
            for job_url in self._page_job_urls(soup, page_url):
                if job_url in seen_jobs:
                    continue
                seen_jobs.add(job_url)
                found_new = True

                item = self._scrape_detail(job_url, pref_ja)
                if not item or not item.get(Schema.NAME):
                    continue
                yield item  # 1求人取得ごとに即 yield（全件バッファ禁止）

            if not found_new:
                break  # 新規求人が無い（最終ページを越えた等）→ 終了
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

        # 求人タイトル（h1）。短い見出しなので EXTRA に採用。
        h1 = soup.select_one("h1")
        if h1:
            data["求人タイトル"] = _clean(h1.get_text())

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
                    elif "事業内容" in key:
                        # 短い業種の箇条書き（自由記述プロースではない）→ Schema.LOB
                        data[Schema.LOB] = val
                    elif "ホームページ" in key or "URL" in key:
                        a = dd.find("a", href=True)
                        data[Schema.HP] = a["href"] if a else val
                    elif "派遣許可番号" in key:
                        data["派遣許可番号"] = val
                    elif "有料職業紹介事業許可番号" in key:
                        data["有料職業紹介事業許可番号"] = val
                    # 「拠点」「応募プロセス」等は自由記述プロースのため取得しない。

        if not data.get(Schema.NAME) and h1:
            data[Schema.NAME] = _clean(h1.get_text())

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    # ローカル実行とコンテナ実行を一致させるため、必ず正規ルートURL（sites.yml の
    # url と同一）を渡す。
    Baitoru5Scraper().execute("https://www.baitoru.com/kansai/jlist/")
