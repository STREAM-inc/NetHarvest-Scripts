"""
---------------------------------------------------------------------------
ver 1.0.0 不明       新規作成
ver 1.1.0 20260526 kanda TELカラム追加し、住所から都道府県を抽出するように修正。
ver 1.2.0 20260529 全企業網羅対応。旧 /search/list/(404) を廃止し、地域別
                   求人一覧(/{region}/jlist/{pref}/)をサイトマップから自動探索。
                   全ページをページネーション巡回し、掲載企業を取りこぼさず収集。
---------------------------------------------------------------------------
"""

import re
import sys
from pathlib import Path
from typing import Generator

from bs4 import BeautifulSoup

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

BASE_URL = "https://www.baitoru.com"

# 地域別求人一覧の探索元サイトマップ
AREA_SITEMAP = f"{BASE_URL}/sitemap_ba_area.xml"

# サイトマップ取得に失敗した場合のフォールバック（地域トップを丸ごと巡回）
REGIONS = [
    "kanto", "tohoku", "tokai", "kansai",
    "koshinetsu", "chushikoku", "kyushu",
]

# 1都道府県あたりの巡回上限ページ数（暴走防止のセーフティ）
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
# 地域別求人一覧の都道府県トップ（…/{region}/jlist/{pref}/）を抽出する
_PREF_LIST_RE = re.compile(r"https://www\.baitoru\.com/([a-z]+)/jlist/([a-z0-9]+)/")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def _abs_url(href: str) -> str:
    href = (href or "").strip()
    if not href:
        return ""
    return href if href.startswith("http") else BASE_URL + href


class BaitoruScraper(DynamicCrawler):
    """バイトル 求人企業情報スクレイパー（baitoru.com）

    地域別求人一覧(/{region}/jlist/{pref}/)をサイトマップから自動探索し、
    全ページをページネーション巡回。各求人詳細から企業情報を抽出し、
    企業ページ(/cjlist:id/)を重複排除キーにして全掲載企業を収集する。
    """

    DELAY = 1.0
    EXTRA_COLUMNS = ["業種", "代表者", "採用人数"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_companies: set[str] = set()   # 収集済み企業URL（/cjlist:id/）
        seen_jobs: set[str] = set()        # 訪問済み求人詳細URL

        pref_lists = self._discover_pref_lists()
        self.logger.info("巡回対象の都道府県一覧: %d件", len(pref_lists))

        for base in pref_lists:
            slug = self._pref_slug(base)
            pref_ja = PREF_JA.get(slug, "")
            self.logger.info("一覧巡回: %s (%s)", base, pref_ja or slug)
            yield from self._scrape_list(base, pref_ja, seen_companies, seen_jobs)

        self.logger.info("収集企業数: %d社", len(seen_companies))

    # ------------------------------------------------------------------ #
    # 巡回対象URLの探索
    # ------------------------------------------------------------------ #
    def _discover_pref_lists(self) -> list[str]:
        """サイトマップから都道府県別一覧URL(/{region}/jlist/{pref}/)を取得。

        取得できなければ地域トップ(/{region}/jlist/)へフォールバックする。
        いずれも全件をページ送りで巡回するので網羅性は保たれる。
        """
        try:
            self.page.goto(AREA_SITEMAP, wait_until="domcontentloaded")
            content = self.page.content()
        except Exception as e:  # noqa: BLE001
            self.logger.warning("サイトマップ取得失敗(%s)。地域トップで巡回します。", e)
            content = ""

        bases: list[str] = []
        seen: set[str] = set()
        for region, pref in _PREF_LIST_RE.findall(content):
            base = f"{BASE_URL}/{region}/jlist/{pref}/"
            if base not in seen:
                seen.add(base)
                bases.append(base)

        if not bases:
            bases = [f"{BASE_URL}/{r}/jlist/" for r in REGIONS]
        return bases

    @staticmethod
    def _pref_slug(base: str) -> str:
        parts = [p for p in base.replace(BASE_URL, "").split("/") if p]
        # ['{region}', 'jlist', '{pref}']
        return parts[2] if len(parts) >= 3 else ""

    # ------------------------------------------------------------------ #
    # 一覧ページのページネーション巡回
    # ------------------------------------------------------------------ #
    def _scrape_list(self, base: str, pref_ja: str, seen_companies: set,
                     seen_jobs: set) -> Generator[dict, None, None]:
        page_no = 1
        while page_no <= MAX_PAGES:
            url = base if page_no == 1 else f"{base}page{page_no}/"
            try:
                self.page.goto(url, wait_until="domcontentloaded")
                self.page.wait_for_selector("a[href*='job']", timeout=8000)
            except Exception:
                break  # ページ無し or 取得失敗 → このエリアの巡回終了

            soup = BeautifulSoup(self.page.content(), "html.parser")
            cards = soup.select("article") or [soup]

            found_new = False
            for card in cards:
                job_url = self._card_job_url(card)
                if not job_url or job_url in seen_jobs:
                    continue
                seen_jobs.add(job_url)
                found_new = True

                # カードに企業ページ(/cjlist:id/)リンクがあり収集済みなら詳細を省略
                cj = card.select_one("a[href*='cjlist']")
                if cj:
                    cj_url = _abs_url(cj.get("href", "")).split("#")[0].rstrip("/")
                    if cj_url in seen_companies:
                        continue

                item = self._scrape_detail(job_url, pref_ja)
                if not item or not item.get(Schema.NAME):
                    continue

                company_url = item.get(Schema.URL, job_url)
                if company_url in seen_companies:
                    continue
                seen_companies.add(company_url)
                yield item

            if not found_new:
                break  # 新規求人が無い（最終ページを越えた等）→ 終了
            page_no += 1

    @staticmethod
    def _card_job_url(card) -> str:
        """求人カードから求人詳細URL(…/job123456/)を取り出す。"""
        for a in card.select("a[href*='job']"):
            href = (a.get("href", "") or "").split("?")[0]
            if "/entry/" in href:
                continue
            if _JOB_DETAIL_RE.search(href):
                return _abs_url(href.rstrip("/") + "/")
        return ""

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

        data = {Schema.URL: url, Schema.PREF: pref_ja}

        company_info = soup.find("div", class_="detail-companyInfo")
        if company_info:
            # 企業ページURL（/cjlist:id/）を取得して重複排除に使う
            link01 = company_info.find("a", class_="link01")
            if link01:
                cj_href = link01.get("href", "").split("#")[0].rstrip("/")
                if cj_href:
                    data[Schema.URL] = _abs_url(cj_href)

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
    BaitoruScraper().execute(BASE_URL)
