"""
シェアフル (sharefull.com) — スポットワーク求人クローラー (14都府県・勤務先単位)

対象サイト: https://sharefull.com/jobs/tokyo/

取得対象 (備考の指示):
    - 東京都・神奈川県・埼玉県・千葉県・三重県・滋賀県・京都府・大阪府・
      兵庫県・奈良県・和歌山県・岐阜県・愛知県・福岡県 の求人
    - 掲載日が実行日から 30 日以内の求人のみ
    - 店舗 (勤務先) 単位で 1 行 (同一勤務先の求人は最新の 1 件のみ)

取得フロー:
    1. sites.yml の url (/jobs/tokyo/) の親 (/jobs/) から都府県別一覧 /jobs/{slug}/ を導出
    2. 都府県別一覧を /jobs/{slug}/pages/{N}/ で巡回
       - 各カードは 勤務先リンク (/jobs/order/{勤務先ID}) + 求人詳細リンク の組
       - 先頭 2 件は全ページ共通の PickUp 枠、以降は新着順
       - 範囲外ページは PickUp 枠のみ返るため「新規求人ゼロ」で打ち切る
    3. 未出力の勤務先の求人詳細を 1 件取得するごとに即 yield
       - JSON-LD (JobPosting) から 企業名 / 住所 / 給与 / 掲載日 / 掲載終了日時
       - JSON-LD (BreadcrumbList) から 市区町村 / 職種 / 勤務先名称
    4. 新着順のため、掲載日が 30 日より古い求人が連続したら当該都府県を打ち切る

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/sharefull_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id sharefull_2
"""

import json
import logging
import re
import sys
from datetime import date, timedelta
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

# 取得対象の都府県 (URL スラッグ, 都道府県名)  ※備考: 14 都府県
_TARGET_PREFS = (
    ("tokyo", "東京都"),
    ("kanagawa", "神奈川県"),
    ("saitama", "埼玉県"),
    ("chiba", "千葉県"),
    ("mie", "三重県"),
    ("shiga", "滋賀県"),
    ("kyoto", "京都府"),
    ("osaka", "大阪府"),
    ("hyogo", "兵庫県"),
    ("nara", "奈良県"),
    ("wakayama", "和歌山県"),
    ("gifu", "岐阜県"),
    ("aichi", "愛知県"),
    ("fukuoka", "福岡県"),
)
# 勤務先 (例: /jobs/order/MTA4MDQxb3JkZXI=)
_ORDER_PATTERN = re.compile(r"^/jobs/order/([A-Za-z0-9=]+)$")
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


class Sharefull2Crawler(StaticCrawler):
    """シェアフル (14都府県・勤務先単位) スクレイパー"""

    DELAY = 3.0
    TIMEOUT = 30
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
    # 1 都府県あたりの一覧ページ上限 (無限ループ防止のセーフティ)
    MAX_PAGES = 500
    # 掲載日がこの日数以内の求人のみ取得 (備考: 作業日から 30 日以内)
    RECENT_DAYS = 30
    # 新着順のため、古い求人がこの件数連続したら当該都府県を打ち切る
    OLD_STREAK_LIMIT = 30

    EXTRA_COLUMNS = [
        "勤務先名称",
        "市区町村",
        "求人タイトル",
        "雇用形態",
        "給与",
        "勤務日時",
        "掲載日",
        "掲載終了日時",
        "求人票URL",
    ]

    def prepare(self):
        self._seen_jobs: set[str] = set()
        self._seen_orders: set[str] = set()
        self._cutoff = (date.today() - timedelta(days=self.RECENT_DAYS)).isoformat()

    def parse(self, url: str) -> Generator[dict, None, None]:
        # /jobs/tokyo/ → /jobs/ を起点に各都府県の一覧を導出
        jobs_root = urljoin(url.rstrip("/") + "/", "../")
        for slug, pref_name in _TARGET_PREFS:
            pref_url = urljoin(jobs_root, f"{slug}/")
            logger.info("%s の一覧を巡回します: %s", pref_name, pref_url)
            yield from self._crawl_pref(pref_url, pref_name)

    def _crawl_pref(self, pref_url: str, pref_name: str) -> Generator[dict, None, None]:
        old_streak = 0
        for page in range(1, self.MAX_PAGES + 1):
            list_url = pref_url if page == 1 else urljoin(pref_url, f"pages/{page}/")
            soup = self.get_soup(list_url)
            if soup is None:
                break

            cards = []
            for a in soup.select("a[href^='/jobs/order/']"):
                m_order = _ORDER_PATTERN.match(a["href"])
                job_a = a.parent.find("a", href=_JOB_PATTERN) if m_order else None
                if not job_a:
                    continue
                job_id = _JOB_PATTERN.match(job_a["href"]).group(1)
                if job_id in self._seen_jobs:
                    continue
                self._seen_jobs.add(job_id)
                cards.append((m_order.group(1), job_a["href"]))

            # 範囲外ページは PickUp 枠 (既出) のみ返るため、新規求人ゼロで終了
            if not cards:
                logger.info("%s: %d ページ目で新規求人なし → 終了", pref_name, page)
                break

            for order_id, href in cards:
                # 勤務先単位で 1 行: 新着順のため最初に出た (最新の) 求人を採用
                if order_id in self._seen_orders:
                    continue
                detail_url = urljoin(pref_url, quote(href, safe="/=%"))
                item = self._parse_detail(detail_url, pref_name)
                if item is None:
                    continue
                if item["掲載日"] and item["掲載日"] < self._cutoff:
                    old_streak += 1
                    if old_streak >= self.OLD_STREAK_LIMIT:
                        logger.info("%s: 掲載日 %s 以前の求人が連続 → 終了", pref_name, self._cutoff)
                        return
                    continue
                old_streak = 0
                self._seen_orders.add(order_id)
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
        # 備考: 一覧の都府県以外 (近隣県の求人混入) は除外 (一覧の都県と勤務地住所の両方で判定)
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
            "求人票URL": unquote(detail_url),
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Sharefull2Crawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://sharefull.com/jobs/tokyo/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
