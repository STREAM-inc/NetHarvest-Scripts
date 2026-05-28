"""
キタイシホン — 上場企業 IR 情報集約サイト

取得対象:
    - https://kitaishihon.com/company/list に掲載されている全上場企業 (約4,039件 / 162ページ)
    - 一覧テーブルから 12 列のうちプロース項目「ストーリー」を除く 11 列
    - 各企業の /company/{code}/top から基本情報(企業名・市場区分・業種・上場年月日・決算期・IR URL)と
      人的資本(社員数・平均年齢・平均勤続年数・平均年収) と OpenWork URL を抽出

取得フロー:
    1. /company/list?page=N で全ページ (1..162) を巡回
    2. 各行から証券コード・社名・市場区分・業界・統合報告書有無・エンゲージメントレーティング・
       PBR・実績PER・ROE・売上・お気に入り数・詳細URLを抽出
    3. 各詳細ページ (/company/{code}/top) を取得し、基本情報テーブル + 人的資本 + OpenWork URL を抽出

実行方法:
    python scripts/sites/corporate/kitaishihon.py
    python bin/run_flow.py --site-id kitaishihon
"""

import logging
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

BASE_URL = "https://kitaishihon.com"
LIST_PATH = "/company/list"

_RE_TANTAI = re.compile(r"（単体）|\(単体\)")
_RE_RENKETSU = re.compile(r"（連結）|\(連結\)")
_RE_SPACES = re.compile(r"\s+")
_RE_CODE_FROM_URL = re.compile(r"/company/([0-9A-Z]+)/top")


def _clean(text) -> str:
    if text is None:
        return ""
    return _RE_SPACES.sub(" ", str(text)).strip()


def _strip_suffix(text: str, suffix_re: re.Pattern) -> str:
    return suffix_re.sub("", text).strip()


class Kitaishihon(StaticCrawler):
    """キタイシホン (上場企業 IR 集約) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "証券コード",
        "市場区分",
        "決算期",
        "統合報告書",
        "お気に入り数",
        "エンゲージメント・レーティング",
        "PBR",
        "実績PER",
        "ROE",
        "売上_億円",
        "社員数_連結",
        "平均年齢",
        "平均勤続年数",
        "平均年収",
        "OpenWork_URL",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        page = 1
        total_rows: list[dict] = []
        while True:
            list_url = f"{BASE_URL}{LIST_PATH}?page={page}"
            soup = self.get_soup(list_url)
            if soup is None:
                logger.warning("一覧ページを取得できませんでした: %s", list_url)
                break

            table = soup.select_one("table.js-search-result-table")
            if table is None:
                logger.warning("一覧テーブルが見つかりませんでした: %s", list_url)
                break

            rows = [tr for tr in table.find_all("tr") if tr.find("td")]
            if not rows:
                logger.info("ページ %d に行が無いため終了", page)
                break

            logger.info("ページ %d: %d 件", page, len(rows))

            for tr in rows:
                row = self._extract_list_row(tr)
                if row:
                    total_rows.append(row)

            if page == 1:
                last_pager = soup.select_one(".pagenation li:not(.active):not(.pager) a")
                last_pages = [
                    a for a in soup.select(".pagenation li a")
                    if a.get_text(strip=True).isdigit()
                ]
                if last_pages:
                    max_page = max(int(a.get_text(strip=True)) for a in last_pages)
                    self.total_items = max_page * 25
                    logger.info("推定総件数: %d (max page=%d)", self.total_items, max_page)

            # 次ページの判定: pagination の next が無ければ終了
            next_link = soup.select_one('.pagenation a[rel="next"]')
            if not next_link:
                break
            page += 1

        self.total_items = len(total_rows)
        logger.info("一覧から %d 件を検出。詳細を取得します", self.total_items)

        for row in total_rows:
            try:
                item = self._scrape_detail(row)
                if item:
                    yield item
            except Exception as e:
                logger.warning(
                    "詳細ページの解析に失敗: %s — %s",
                    row.get(Schema.URL, "(no url)"),
                    e,
                )
                continue

    def _extract_list_row(self, tr) -> dict | None:
        tds = tr.find_all("td", recursive=False)
        if len(tds) < 12:
            return None

        code = _clean(tds[0].get_text())
        favorite = _clean(tds[1].get_text())
        name_a = tds[2].find("a")
        name = _clean(name_a.get_text()) if name_a else _clean(tds[2].get_text())
        detail_url = (
            urljoin(BASE_URL, name_a.get("href"))
            if name_a and name_a.get("href")
            else f"{BASE_URL}/company/{code}/top"
        )
        market = _clean(tds[3].get_text())

        annual_report_a = tds[5].find("a")
        annual_report = _clean(annual_report_a.get_text()) if annual_report_a else _clean(tds[5].get_text())

        # tds[6] = ストーリー (プロース) → 取得しない
        engagement = _clean(tds[7].get_text())
        pbr = _clean(tds[8].get_text())
        per = _clean(tds[9].get_text())
        roe = _clean(tds[10].get_text())
        sales = _clean(tds[11].get_text())

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            "証券コード": code,
            "市場区分": market,
            "統合報告書": annual_report,
            "お気に入り数": favorite,
            "エンゲージメント・レーティング": engagement,
            "PBR": pbr,
            "実績PER": per,
            "ROE": roe,
            "売上_億円": sales,
        }

    def _scrape_detail(self, list_row: dict) -> dict | None:
        url = list_row[Schema.URL]
        soup = self.get_soup(url)
        if soup is None:
            return None

        # 基本情報テーブル
        info: dict[str, str] = {}
        for table in soup.select(".basic-information table tr"):
            th = table.find("th")
            td = table.find("td")
            if not th or not td:
                continue
            key = _clean(th.get_text())
            if key == "IR情報":
                a = td.find("a")
                info[key] = a.get("href") if a and a.get("href") else _clean(td.get_text())
            else:
                info[key] = _clean(td.get_text())

        official_name = info.get("企業名") or list_row.get(Schema.NAME, "")

        # 人的資本
        emp_tantai = ""
        emp_renketsu = ""
        avg_age = ""
        avg_tenure = ""
        avg_salary = ""
        for li in soup.select(".circunstance .details li"):
            item = li.select_one(".detail-item")
            num = li.select_one(".detail-number")
            if not item or not num:
                continue
            key = _clean(item.get_text())
            if key == "社員数":
                spans = num.find_all("span")
                if spans:
                    emp_tantai = _strip_suffix(_clean(spans[0].get_text()), _RE_TANTAI)
                    if len(spans) > 1:
                        emp_renketsu = _strip_suffix(_clean(spans[1].get_text()), _RE_RENKETSU)
                else:
                    emp_tantai = _strip_suffix(_clean(num.get_text()), _RE_TANTAI)
            elif key == "平均年齢":
                avg_age = _strip_suffix(_clean(num.get_text()), _RE_TANTAI)
            elif key == "平均勤続年数":
                avg_tenure = _strip_suffix(_clean(num.get_text()), _RE_TANTAI)
            elif key == "平均年収":
                avg_salary = _strip_suffix(_clean(num.get_text()), _RE_TANTAI)

        # OpenWork URL
        openwork = ""
        openwork_a = soup.select_one("a.openwork")
        if openwork_a and openwork_a.get("href"):
            openwork = openwork_a.get("href")

        # エンゲージメント・レーティング (詳細優先、無ければ一覧から)
        engagement_detail = ""
        rate_el = soup.select_one(".engagement-rating .score-rate")
        if rate_el:
            engagement_detail = _clean(rate_el.get_text())
        engagement = engagement_detail or list_row.get("エンゲージメント・レーティング", "")

        item = {
            Schema.URL: url,
            Schema.NAME: official_name,
            Schema.HP: info.get("IR情報", ""),
            Schema.OPEN_DATE: info.get("上場年月日", ""),
            Schema.EMP_NUM: emp_tantai,
            "証券コード": list_row.get("証券コード", "") or info.get("証券番号", ""),
            "市場区分": info.get("市場区分", "") or list_row.get("市場区分", ""),
            "決算期": info.get("決算期", ""),
            "統合報告書": list_row.get("統合報告書", ""),
            "お気に入り数": list_row.get("お気に入り数", ""),
            "エンゲージメント・レーティング": engagement,
            "PBR": list_row.get("PBR", ""),
            "実績PER": list_row.get("実績PER", ""),
            "ROE": list_row.get("ROE", ""),
            "売上_億円": list_row.get("売上_億円", ""),
            "社員数_連結": emp_renketsu,
            "平均年齢": avg_age,
            "平均勤続年数": avg_tenure,
            "平均年収": avg_salary,
            "OpenWork_URL": openwork,
        }
        return item


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Kitaishihon()
    scraper.execute(urljoin(BASE_URL, LIST_PATH))

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
