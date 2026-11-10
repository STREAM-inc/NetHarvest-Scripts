"""
マイナビバイト（14都府県・新着30日） スクレイパー

対象サイト: https://baito.mynavi.jp/tokyo/

取得対象:
    東京都・神奈川県・埼玉県・千葉県・三重県・滋賀県・京都府・大阪府・兵庫県・奈良県・
    和歌山県・岐阜県・愛知県・福岡県 の全職種の求人のうち、掲載開始日 (JSON-LD datePosted)
    が実行日から 30 日以内のもの。1 求人 = 1 行。

取得フロー:
    1. 引数 url から配信元 (origin) を派生させ、robots.txt に記載された求人サイトマップ
       索引 /sitemap_job_index_new.xml を取得する。
       ※ 市区町村一覧 (/tokyo/city-35/ 等) のページ送りは ?pageNo=N だが、robots.txt が
         `Disallow: /*?*pageNo=` で禁止しているため一覧のページ送りは使わない。
         サイトマップは全国の求人詳細 URL を求人番号の降順 (= 新しい順) に列挙しており
         (2026-11 時点 job_1〜8 で約 32 万件)、datePosted もほぼ単調に減少する。
    2. 求人詳細 /cl-{企業番号}/job-{求人番号}/ を順に開き、JSON-LD JobPosting から
       都道府県 (jobLocation.address.addressRegion) と datePosted を判定する。
       対象 14 都府県以外・掲載開始日が 30 日より前の求人は出力しない。
    3. 掲載開始日が期限より前の求人が STOP_AFTER_OLD 件連続した時点で巡回を打ち切る
       (サイトマップが新しい順のため、以降はすべて対象外)。
    4. サイトマップには掲載終了済み (404) の URL も混在するため CONTINUE_ON_ERROR で読み飛ばす。
    5. 出力しない求人 (404 / 対象外県 / 期限前) の取得にも待機が必要なため、ITEM_DELAY=0 とし
       DELAY は _fetch() で 1 リクエストごとに入れる (キャッシュ再生時は待たない)。

取得カラム:
    名称 = JSON-LD hiringOrganization.name (末尾の「※勤務地 …」注記は除去)
    勤務地住所 = 市区郡町村 + Nuxt 状態の locationAddress1 (番地)
                 市区郡町村は Nuxt の locationSikugun.label (「愛知郡東郷町」等)。
                 ラベルが変数参照で読めない場合は JSON-LD addressLocality (郡までしか無い)
    都道府県 = addressRegion / 職種 = JSON-LD title / サイト定義業種 = JSON-LD industry
    雇用形態 = 画面の基本情報「雇用形態」 / 求人タイトル = 画面 h2 キャッチコピー
    掲載開始日・掲載終了日 = 画面「掲載期間」(無ければ datePosted / validThrough)
    求人票URL / HP = Nuxt 状態の homepageAddress (無ければ hiringOrganization.sameAs) / 取得日
    ※ 電話番号は応募受付用 050 番号のため取得しない (備考指示)。
    ※ お仕事内容・待遇等の自由記述は著作権リスクのため取得しない。

利用規約 (https://baito.mynavi.jp/user/html/kiyaku.html):
    スクレイピング/クローリングを名指しで禁止する条項は無い。第4条(7)「入手した情報を
    私的利用の範囲を超えて複製・販売…」の禁止があるため運用側で用途に留意すること。
"""
import json
import logging
import re
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import urljoin

import bs4

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 対象 14 都府県 (JSON-LD addressRegion と照合)
TARGET_PREFS = {
    "東京都", "神奈川県", "埼玉県", "千葉県", "三重県", "滋賀県", "京都府",
    "大阪府", "兵庫県", "奈良県", "和歌山県", "岐阜県", "愛知県", "福岡県",
}
NEW_DAYS = 30
# 掲載開始日が期限前の求人がこの件数連続したら打ち切る
STOP_AFTER_OLD = 500

_JOB_URL_RE = re.compile(r"^https?://[^/]+/cl-\d+/job-\d+/?$")
_TERM_RE = re.compile(r"(\d{4})/(\d{1,2})/(\d{1,2})\s*[〜～~]\s*(\d{4})/(\d{1,2})/(\d{1,2})")
_ADDR1_RE = re.compile(r'locationAddress1:"((?:[^"\\]|\\.)*)"')
_SIKUGUN_RE = re.compile(r'locationSikugun:\{id:\d+,label:"((?:[^"\\]|\\.)*)"\}')
_HP_RE = re.compile(r'homepageAddress:"((?:[^"\\]|\\.)*)"')
_NAME_NOTE_RE = re.compile(r"[\s　]*※勤務地.*$")


def _clean(s) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip()


def _js_str(s: str) -> str:
    """Nuxt 状態中の JS 文字列リテラルを復元する (\\u002F 等)。"""
    try:
        return json.loads(f'"{s}"')
    except ValueError:
        return s


class Baito2Scraper(StaticCrawler):
    """マイナビバイト 14都府県・新着30日 求人スクレイパー"""
    DELAY = 3.0
    ITEM_DELAY = 0  # 待機は _fetch() でリクエストごとに入れる
    CONTINUE_ON_ERROR = True
    EXTRA_COLUMNS = [
        "勤務地住所", "職種", "雇用形態", "求人タイトル",
        "掲載開始日", "掲載終了日", "求人票URL", "取得日",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        cutoff = date.today() - timedelta(days=NEW_DAYS)
        index = self._fetch(urljoin(url, "/sitemap_job_index_new.xml"))
        if index is None:
            raise RuntimeError("求人サイトマップ索引を取得できませんでした")
        sitemaps = [_clean(loc.get_text()) for loc in index.find_all("loc")]
        self.logger.info("求人サイトマップ %d 本 / 掲載開始日 %s 以降", len(sitemaps), cutoff)

        seen: set[str] = set()
        old_streak = 0
        for sm_url in sitemaps:
            sm = self._fetch(sm_url)
            if sm is None:
                continue
            for loc in sm.find_all("loc"):
                job_url = _clean(loc.get_text())
                if not _JOB_URL_RE.match(job_url) or job_url in seen:
                    continue
                seen.add(job_url)

                soup = self._fetch(job_url)
                if soup is None:
                    continue  # 掲載終了 (404) 等
                job = self._job_posting(soup)
                if not job:
                    continue

                posted = self._parse_date(job.get("datePosted"))
                if posted and posted < cutoff:
                    old_streak += 1
                    if old_streak >= STOP_AFTER_OLD:
                        self.logger.info("掲載開始日が %s より前の求人が %d 件連続 → 巡回終了",
                                         cutoff, old_streak)
                        return
                    continue
                old_streak = 0

                address = (job.get("jobLocation") or {})
                if isinstance(address, list):
                    address = address[0] if address else {}
                address = address.get("address") or {}
                pref = _clean(address.get("addressRegion"))
                if pref not in TARGET_PREFS:
                    continue

                yield self._build_item(soup, job, job_url, pref, address)

    # ------------------------------------------------------------------ #
    def _build_item(self, soup: bs4.BeautifulSoup, job: dict, job_url: str,
                    pref: str, address: dict) -> dict:
        html = str(soup)
        org = job.get("hiringOrganization") or {}

        m = _ADDR1_RE.search(html)
        street = _clean(_js_str(m.group(1))) if m else ""
        m = _SIKUGUN_RE.search(html)
        city = _clean(_js_str(m.group(1))) if m else ""
        if not city:
            city = _clean(address.get("addressLocality"))
        addr = city + street

        m = _HP_RE.search(html)
        hp = _clean(_js_str(m.group(1))) if m else _clean(org.get("sameAs"))

        start = self._parse_date(job.get("datePosted"))
        end = self._parse_date(job.get("validThrough"))
        term = soup.select_one(".postingTerm span")
        if term:
            m = _TERM_RE.search(term.get_text())
            if m:
                g = list(map(int, m.groups()))
                start, end = date(*g[:3]), date(*g[3:])

        title_el = soup.select_one("h2.jobContentShopPrText")

        return {
            Schema.NAME: _NAME_NOTE_RE.sub("", _clean(org.get("name"))),
            Schema.PREF: pref,
            Schema.POST_CODE: _clean(address.get("postalCode")),
            Schema.ADDR: addr,
            Schema.CAT_SITE: _clean(job.get("industry")),
            Schema.HP: hp,
            Schema.URL: job_url,
            "勤務地住所": pref + addr,
            "職種": _clean(job.get("title")),
            "雇用形態": self._base_info(soup, "雇用形態"),
            "求人タイトル": _clean(title_el.get_text()) if title_el else "",
            "掲載開始日": start.isoformat() if start else "",
            "掲載終了日": end.isoformat() if end else "",
            "求人票URL": job_url,
            "取得日": date.today().isoformat(),
        }

    def _fetch(self, url: str) -> Optional[bs4.BeautifulSoup]:
        soup = self.get_soup(url)
        if not self._last_fetch_from_cache:
            time.sleep(self.DELAY)
        return soup

    @staticmethod
    def _job_posting(soup: bs4.BeautifulSoup) -> Optional[dict]:
        for sc in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(sc.string or "")
            except ValueError:
                continue
            for d in data if isinstance(data, list) else [data]:
                if isinstance(d, dict) and d.get("@type") == "JobPosting":
                    return d
        return None

    @staticmethod
    def _base_info(soup: bs4.BeautifulSoup, label: str) -> str:
        """画面上部の基本情報 (給与/雇用形態/勤務地…) からラベルの値を取る。"""
        for item in soup.select("p.jobContentBaseInformationItem"):
            if _clean(item.get_text()) != label:
                continue
            box = item.find_parent(class_=re.compile("^jobContentBaseInformationSet"))
            content = box.select_one(".jobContentBaseInformationContent") if box else None
            if content:
                return _clean(content.get_text(" "))
        return ""

    @staticmethod
    def _parse_date(s) -> Optional[date]:
        m = re.match(r"(\d{4})-(\d{1,2})-(\d{1,2})", str(s or ""))
        return date(*map(int, m.groups())) if m else None


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    scraper = Baito2Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://baito.mynavi.jp/tokyo/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
