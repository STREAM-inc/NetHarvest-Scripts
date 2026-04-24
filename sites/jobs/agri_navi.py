"""
あぐりナビ — 農業・酪農求人サイトのクローラー

取得対象:
    - agri-navi.com/search の一覧に掲載された求人(約 2,577 件 / 258 ページ)
    - 各求人の会社情報 (会社名、住所、事業内容、代表者など) を詳細ページから取得

取得フロー:
    1. 一覧ページ (?page=N) をページネーションで巡回
    2. 各ページから jobdetail/{id} へのリンクを収集
    3. 詳細ページで会社情報テーブル/概要テーブルをパース

実行方法:
    python scripts/sites/jobs/agri_navi.py
    python bin/run_flow.py --site-id agri_navi
"""

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

BASE_URL = "https://www.agri-navi.com"
LISTING_URL = "https://www.agri-navi.com/search"


def _clean(text) -> str:
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def _strip_suffix(text: str, suffixes: tuple[str, ...]) -> str:
    for s in suffixes:
        idx = text.find(s)
        if idx >= 0:
            text = text[:idx]
    return text.strip()


class AgriNaviScraper(StaticCrawler):
    """あぐりナビ (agri-navi.com) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "求人ID",
        "地域",
        "職種",
        "雇用形態",
        "寮・社宅",
        "求人種類",
        "掲載終了日",
        "キャッチコピー",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_entries = self._collect_listing_entries()
        self.total_items = len(detail_entries)
        self.logger.info("詳細URL収集完了: %d 件", len(detail_entries))

        for entry in detail_entries:
            try:
                item = self._scrape_detail(entry)
                if item and item.get(Schema.NAME):
                    yield item
            except Exception as e:
                self.logger.warning("詳細取得失敗: %s (%s)", entry.get("url"), e)
                continue

    def _collect_listing_entries(self) -> list[dict]:
        entries: list[dict] = []
        seen_ids: set[str] = set()
        page = 1

        while True:
            page_url = f"{LISTING_URL}?page={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                break

            posts = soup.select("section.job_posting .post")
            if not posts:
                break

            new_on_page = 0
            for post in posts:
                link = post.select_one('a[href*="/jobdetail/"]')
                if not link:
                    continue
                href = link.get("href", "").strip()
                m = re.search(r"/jobdetail/(\d+)", href)
                if not m:
                    continue
                job_id = m.group(1)
                if job_id in seen_ids:
                    continue
                seen_ids.add(job_id)

                detail_url = urljoin(BASE_URL, f"/jobdetail/{job_id}")
                catch = post.select_one("p.txt_point")
                end_date = post.select_one("p.date")

                entries.append({
                    "url": detail_url,
                    "job_id": job_id,
                    "catch": _clean(catch.get_text(" ")) if catch else "",
                    "end_date": _clean(end_date.get_text(" ")) if end_date else "",
                })
                new_on_page += 1

            self.logger.info("page %d: %d 件収集 (累計 %d)", page, new_on_page, len(entries))

            # Stop when this page has no next link
            next_link = soup.select_one('.pages a[rel="next"]')
            if not next_link or new_on_page == 0:
                break
            page += 1

        return entries

    def _scrape_detail(self, entry: dict) -> dict | None:
        url = entry["url"]
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {
            Schema.URL: url,
            "求人ID": entry.get("job_id", ""),
            "キャッチコピー": entry.get("catch", ""),
            "掲載終了日": self._parse_end_date(entry.get("end_date", "")),
        }

        # Summary table: 地域 / 都道府県 / 業種 / 職種 / 雇用形態 / 寮・社宅
        summary_table = soup.select_one(".detail > table")
        if summary_table:
            for tr in summary_table.select("tr"):
                ths = tr.find_all("th")
                tds = tr.find_all("td")
                for th, td in zip(ths, tds):
                    key = _clean(th.get_text())
                    val = _clean(td.get_text(" / "))
                    if key == "都道府県":
                        data[Schema.PREF] = val
                    elif key == "業種":
                        data[Schema.CAT_SITE] = val
                    elif key == "地域":
                        data["地域"] = val
                    elif key == "職種":
                        data["職種"] = val
                    elif key == "雇用形態":
                        data["雇用形態"] = val
                    elif key == "寮・社宅":
                        data["寮・社宅"] = val

        # Company info: last table under .detail (contains 会社名/設立/資本金など)
        tables = soup.select(".detail table")
        company_table = tables[-1] if tables else None
        if company_table:
            for tr in company_table.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                key = _clean(th.get_text())
                val = _clean(td.get_text(" "))
                if key == "会社名":
                    data[Schema.NAME] = val
                elif key == "求人種類":
                    data["求人種類"] = val
                elif key == "事業内容":
                    data[Schema.LOB] = val
                elif key == "設立":
                    data[Schema.OPEN_DATE] = val
                elif key == "資本金":
                    data[Schema.CAP] = val
                elif key == "郵便番号":
                    data[Schema.POST_CODE] = val.replace("〒", "").strip()
                elif key == "本社所在地":
                    data[Schema.ADDR] = _strip_suffix(val, ("地図はこちら", "※面接"))
                elif key == "代表者":
                    data[Schema.REP_NM] = val
                elif key == "従業員数":
                    data[Schema.EMP_NUM] = val
                elif key == "関連URL":
                    a = td.find("a", href=True)
                    if a:
                        data[Schema.HP] = a["href"]

        # Fallback: company name from first h2 in main content
        if not data.get(Schema.NAME):
            h2 = soup.select_one("section.job_posting h2, article h2, h2")
            if h2:
                data[Schema.NAME] = _clean(h2.get_text())

        if not data.get(Schema.NAME):
            return None
        return data

    @staticmethod
    def _parse_end_date(text: str) -> str:
        if not text:
            return ""
        m = re.search(r"掲載終了日[：:]\s*(\S+?)(?:\s|※|$)", text)
        if m:
            return m.group(1)
        return text


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = AgriNaviScraper()
    scraper.execute(LISTING_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
