"""
シェアフル (sharefull.com) — スポットワーク (単発・短期バイト) 求人クローラー

取得対象:
    - 埼玉県・千葉県・東京都・神奈川県の求人 (備考の指示による絞り込み)
    - 掲載企業名 / 勤務先名称 / 勤務地 (都道府県・市区町村) / 職種 / 時給 / 掲載URL

取得フロー:
    1. sites.yml の url (/jobs) から都県別一覧 (/jobs/{slug}/) を導出
    2. 都県別一覧を /jobs/{slug}/pages/{N}/ で巡回し、求人カードから詳細 URL を収集
       - 範囲外ページは最終ページの内容を返すため「新規求人ゼロ」で打ち切る
    3. 求人詳細 (/jobs/{slug}/{市区町村}/{職種}/{ID}) を 1 件取得するごとに即 yield
       - JSON-LD (JobPosting) から 企業名 / 住所 / 時給 / 掲載日 を取得
       - JSON-LD (BreadcrumbList) から 市区町村 / 職種 / 勤務先名称 を取得
    4. 勤務地住所が対象 4 都県以外の求人は yield しない

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/sharefull.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id sharefull
"""

import json
import logging
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import quote, unquote, urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

# 取得対象の都県 (URL スラッグ, 都道府県名)  ※備考: 埼玉・千葉・東京・神奈川
_TARGET_PREFS = (
    ("saitama", "埼玉県"),
    ("chiba", "千葉県"),
    ("tokyo", "東京都"),
    ("kanagawa", "神奈川県"),
)
# 求人詳細 (例: /jobs/tokyo/渋谷区/delivery/ODM2OTczOW9yZGVy)
_JOB_PATTERN = re.compile(r"^/jobs/[a-z]+/[^/]+/[a-z_]+/([A-Za-z0-9=]+)$")
# パンくずの勤務先表記 "企業名(店舗コード/店舗名)" / "企業名(-)" から括弧内を取り出す
_FACILITY_PATTERN = re.compile(r"\((.*)\)\s*$")
_FACILITY_CODE_PATTERN = re.compile(r"^[\dA-Za-z\-_]+\s*[/／]\s*")
# description 内の【日時】ブロック
_DATETIME_PATTERN = re.compile(r"【日時】\s*<br\s*/?>\s*(.*?)\s*<br", re.S)

_SALARY_UNIT = {"HOUR": "時給", "DAY": "日給", "MONTH": "月給", "YEAR": "年収"}
_EMPLOYMENT_TYPE = {
    "FULL_TIME": "正社員",
    "PART_TIME": "アルバイト・パート",
    "CONTRACTOR": "業務委託",
    "TEMPORARY": "派遣",
    "OTHER": "その他",
}


class SharefullCrawler(StaticCrawler):
    """シェアフル スクレイパー"""

    DELAY = 3.0
    TIMEOUT = 30
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
    # 1 都県あたりの一覧ページ上限 (無限ループ防止のセーフティ)
    MAX_PAGES = 500

    EXTRA_COLUMNS = [
        "勤務先名称",
        "市区町村",
        "求人タイトル",
        "給与",
        "勤務日時",
        "雇用形態",
        "掲載日",
        "掲載終了日時",
        "求人ID",
    ]

    def prepare(self):
        self._seen_jobs: set[str] = set()

    def parse(self, url: str) -> Generator[dict, None, None]:
        base = url.rstrip("/") + "/"
        for slug, pref_name in _TARGET_PREFS:
            pref_url = urljoin(base, f"{slug}/")
            logger.info("%s の一覧を巡回します: %s", pref_name, pref_url)
            yield from self._crawl_pref(pref_url, pref_name)

    def _crawl_pref(self, pref_url: str, pref_name: str) -> Generator[dict, None, None]:
        for page in range(1, self.MAX_PAGES + 1):
            list_url = pref_url if page == 1 else urljoin(pref_url, f"pages/{page}/")
            soup = self.get_soup(list_url)
            if soup is None:
                break

            new_links = []
            for a in soup.select("a[href]"):
                href = a["href"]
                m = _JOB_PATTERN.match(href)
                if not m or m.group(1) in self._seen_jobs:
                    continue
                self._seen_jobs.add(m.group(1))
                new_links.append(href)

            # 範囲外ページは最終ページの内容が返るため、新規求人ゼロで終了
            if not new_links:
                logger.info("%s: %d ページ目で新規求人なし → 終了", pref_name, page)
                break

            for href in new_links:
                detail_url = urljoin(pref_url, quote(href, safe="/=%"))
                item = self._parse_detail(detail_url, pref_name)
                if item:
                    yield item

    def _parse_detail(self, detail_url: str, pref_name: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        job, crumbs = {}, []
        for script in soup.select('script[type="application/ld+json"]'):
            try:
                data = json.loads(script.string or "")
            except json.JSONDecodeError:
                continue
            if data.get("@type") == "JobPosting":
                job = data
            elif data.get("@type") == "BreadcrumbList":
                crumbs = [i.get("name", "") for i in data.get("itemListElement", [])]

        if not job:
            logger.warning("JobPosting が見つかりません: %s", detail_url)
            return None

        address = (job.get("jobLocation") or {}).get("address") or {}
        pref = address.get("addressRegion", "")
        street = address.get("streetAddress", "")
        # 備考: 対象 4 都県以外は除外 (一覧の都県と勤務地住所の両方で判定)
        if pref != pref_name and not street.startswith(pref_name):
            return None
        if not pref:
            pref = pref_name
        addr = street[len(pref):] if street.startswith(pref) else street

        post_code = address.get("postalCode", "")
        if re.fullmatch(r"\d{7}", post_code):
            post_code = f"{post_code[:3]}-{post_code[3:]}"

        # パンくず: TOP > 都道府県 > 市区町村 > 職種 > 企業名(店舗) > 求人タイトル
        city = crumbs[2] if len(crumbs) > 2 else ""
        category = crumbs[3] if len(crumbs) > 3 else ""
        facility = ""
        if len(crumbs) > 4:
            m = _FACILITY_PATTERN.search(crumbs[4])
            if m:
                facility = _FACILITY_CODE_PATTERN.sub("", m.group(1)).strip()
                if facility == "-":
                    facility = ""

        salary = ""
        value = ((job.get("baseSalary") or {}).get("value")) or {}
        if value.get("value") not in (None, ""):
            unit = _SALARY_UNIT.get(value.get("unitText", ""), "")
            salary = f"{unit}{int(value['value']):,}円" if unit else f"{value['value']}円"

        description = job.get("description", "")
        m = _DATETIME_PATTERN.search(description)
        work_datetime = bs4.BeautifulSoup(m.group(1), "html.parser").get_text(" ", strip=True) if m else ""

        org = job.get("hiringOrganization") or {}
        identifier = job.get("identifier") or {}

        return {
            Schema.URL: unquote(detail_url),
            Schema.NAME: org.get("name", "").strip(),
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr.strip(),
            Schema.CAT_SITE: category,
            "勤務先名称": facility,
            "市区町村": city,
            "求人タイトル": job.get("title", "").strip(),
            "給与": salary,
            "勤務日時": work_datetime,
            "雇用形態": _EMPLOYMENT_TYPE.get(job.get("employmentType", ""), job.get("employmentType", "")),
            "掲載日": job.get("datePosted", "")[:10],
            "掲載終了日時": job.get("validThrough", ""),
            "求人ID": str(identifier.get("value", "")),
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = SharefullCrawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://sharefull.com/jobs")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
