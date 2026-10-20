"""
対象サイト: マイナビDOCTOR https://doctor.mynavi.jp/search/regular/result/pref_cd/13/

取得対象:
    医師求人 (求人単位)。東京都(13)・大阪府(27) × 常勤/非常勤/スポット の6一覧。

取得フロー:
    1. 引数 url (例: /search/regular/result/pref_cd/13/) から検索ルートを導出し、
       引数の一覧を先頭に 6 一覧を順に巡回
    2. 一覧 article.job-card (20件/ページ) を解析。ページ送りは /page=N/
       1ページ全件が「募集停止」(span.status-ended) なら、その一覧は打ち切り
    3. 医療機関名 (h3.job-description) が出ている求人のみ詳細ページを取得し、
       dl.job-offer-detail__list から 医療機関名/施設種別/標榜科目/病床数/医療機関ID(cno) を補完
       施設名非公開の求人は一覧情報のみで出力
"""

import re
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 巡回対象 (備考指定): 都道府県コード × 勤務形態
_PREF_CODES = ["13", "27"]
_JOB_TYPES = {"regular": "常勤", "parttime": "非常勤", "spot": "スポット"}

_ROOT_RE = re.compile(r"^(https?://[^/]+/search/)(regular|parttime|spot)/result/pref_cd/(\d+)/")
_PREF_RE = re.compile(r"^(東京都|北海道|(?:京都|大阪)府|.{2,3}?県)")
_PLACEHOLDER = "※詳細はお問い合わせください"


class DoctorScraper(StaticCrawler):
    """マイナビDOCTOR 医師求人スクレイパー"""
    DELAY = 3.0       # 通信ごとの待機秒数 (_fetch 内で適用)
    ITEM_DELAY = 0.0  # 待機は通信単位で行うため、アイテム単位の待機は不要

    EXTRA_COLUMNS = [
        "求人No",
        "医療機関ID",
        "医療機関名(詳細)",
        "勤務形態",
        "募集科目",
        "業務内容",
        "標榜科目",
        "病床数",
        "勤務地住所",
        "求人更新日",
        "募集ステータス",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        m = _ROOT_RE.match(url)
        if not m:
            raise ValueError(f"想定外の一覧URLです: {url}")
        search_root, first_type, first_pref = m.groups()

        # 引数 url の一覧を先頭に、残りの一覧を続ける
        targets = [(first_type, first_pref)]
        for pref in _PREF_CODES:
            for job_type in _JOB_TYPES:
                if (job_type, pref) not in targets:
                    targets.append((job_type, pref))

        seen: set[str] = set()
        for job_type, pref in targets:
            list_url = f"{search_root}{job_type}/result/pref_cd/{pref}/"
            yield from self._crawl_list(list_url, job_type, seen)

    # ------------------------------------------------------------------
    def _fetch(self, url: str) -> bs4.BeautifulSoup | None:
        """get_soup + 通信間隔の確保 (キャッシュリプレイ時は待たない)"""
        soup = self.get_soup(url)
        if not self._last_fetch_from_cache:
            time.sleep(self.DELAY)
        return soup

    def _crawl_list(self, list_url: str, job_type: str, seen: set) -> Generator[dict, None, None]:
        page = 1
        last_page = None
        while True:
            page_url = list_url if page == 1 else f"{list_url}page={page}/"
            soup = self._fetch(page_url)
            if soup is None:
                break
            cards = soup.select("article.job-card")
            if not cards:
                break
            if last_page is None:
                nums = [int(n) for a in soup.select('a[href*="page="]')
                        for n in re.findall(r"page=(\d+)", a["href"])]
                last_page = max(nums) if nums else 1

            # 1ページ全件が募集停止 → 以降も募集停止のみなので打ち切り
            if all(c.select_one(".job-status .status-ended") for c in cards):
                self.logger.info("全件募集停止のため打ち切り: %s", page_url)
                break

            for card in cards:
                item = self._parse_card(card, page_url, job_type)
                if item is None or item["求人No"] in seen:
                    continue
                seen.add(item["求人No"])
                yield item

            if page >= last_page:
                break
            page += 1

    def _parse_card(self, card: bs4.Tag, page_url: str, job_type: str) -> dict | None:
        link = card.select_one("h2.job-title a[href]") or card.select_one('a[href*="/detail/"]')
        if link is None:
            return None
        detail_url = urllib.parse.urljoin(page_url, link["href"])

        job_no = ""
        no_m = re.search(r"/detail/(\d+)/", detail_url)
        if no_m:
            job_no = no_m.group(1)

        fields = {}
        for dt in card.select(".job-card-detail-main dt"):
            dd = dt.find_next_sibling("dd")
            if dd is not None:
                fields[dt.get_text(strip=True)] = dd.get_text(" ", strip=True)

        place = fields.get("勤務地", "")
        pref_m = _PREF_RE.match(place)
        pref = pref_m.group(1) if pref_m else ""
        city = place[len(pref):] if pref else place

        updated = ""
        for p in card.select(".job-number p"):
            um = re.search(r"求人更新日\s*[:：]\s*(\S+)", p.get_text(" ", strip=True))
            if um:
                updated = um.group(1)

        name_el = card.select_one("h3.job-description")
        name = name_el.get_text(" ", strip=True) if name_el else ""
        if "非公開" in name:
            name = ""

        item = {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: city,
            Schema.CAT_SITE: "",
            "求人No": job_no,
            "医療機関ID": "",
            "医療機関名(詳細)": "",
            "勤務形態": _JOB_TYPES[job_type],
            "募集科目": fields.get("募集科目", ""),
            "業務内容": fields.get("業務内容", ""),
            "標榜科目": "",
            "病床数": "",
            "勤務地住所": "",
            "求人更新日": updated,
            "募集ステータス": "募集停止" if card.select_one(".job-status .status-ended") else "募集中",
        }

        # 医療機関名が出ている求人のみ詳細を取得
        if name:
            detail = self._fetch(detail_url)
            if detail is not None:
                self._merge_detail(detail, item)
        return item

    def _merge_detail(self, soup: bs4.BeautifulSoup, item: dict) -> None:
        info = {}
        for dl in soup.select("dl.job-offer-detail__list, dl.job-offer-summary__item"):
            for dt in dl.find_all("dt", recursive=False):
                dd = dt.find_next_sibling("dd")
                if dd is None:
                    continue
                label = dt.get_text(strip=True)
                if label == "医療機関名":
                    cno_a = dd.select_one('a[href*="/cno/"]')
                    if cno_a:
                        cm = re.search(r"/cno/(\d+)", cno_a["href"])
                        if cm:
                            item["医療機関ID"] = cm.group(1)
                        cno_a.extract()
                value = dd.get_text(" ", strip=True)
                if value == _PLACEHOLDER:
                    value = ""
                # 詳細下部 (job-offer-detail__list) の値を優先
                if value and (label not in info or "job-offer-detail__list" in dl.get("class", [])):
                    info[label] = value

        item["医療機関名(詳細)"] = info.get("医療機関名", "")
        item[Schema.CAT_SITE] = info.get("施設種別", "")
        item["標榜科目"] = info.get("標榜科目", "")
        item["病床数"] = info.get("病床数", "")
        item["勤務地住所"] = info.get("勤務地", "")


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = DoctorScraper()
    scraper.execute("https://doctor.mynavi.jp/search/regular/result/pref_cd/13/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
