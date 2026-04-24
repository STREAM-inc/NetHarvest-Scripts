"""
おすすめディスカバイト — 全国アルバイト求人情報スクレイパー (moppy-baito.com)

取得対象:
    - 詳細ページ /list1/{id}/ および /list2/{id}/
        応募先企業名, 求人募集店舗, 勤務地(住所), 都道府県, 募集職種,
        雇用形態, 給与, 勤務時間, 応募資格, 特徴・待遇, 交通アクセス

取得フロー:
    47都道府県をフリーワード検索 (/freeword/?freeword={pref_ja}&kw=false)
    → ?pageNo=N で全ページ巡回
    → 一覧の a[href^="/list1/"] および a[href^="/list2/"] を収集 (URLで重複除外)
    → 各詳細ページをフェッチしテーブルから情報を抽出

実行方法:
    python scripts/sites/jobs/moppy_baito.py
    python bin/run_flow.py --site-id moppy_baito
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


BASE_URL = "https://moppy-baito.com"

PREFECTURES_JA: list[str] = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県",
    "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]

_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県"
    r"|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県"
    r"|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県"
    r"|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県"
    r"|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_DETAIL_HREF_RE = re.compile(r"^/list[12]/\d+/?$")


def _clean(text) -> str:
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def _th_next_td(soup, label_keywords: tuple[str, ...]):
    """detail__table 内で <th> ラベルにマッチする次兄弟 <td> を返す。"""
    for th in soup.select("table.detail__table th"):
        text = _clean(th.get_text())
        if any(k in text for k in label_keywords):
            td = th.find_next_sibling("td")
            if td is not None:
                return td
    return None


class MoppyBaitoScraper(StaticCrawler):
    """おすすめディスカバイト 求人情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "求人募集店舗",
        "給与",
        "雇用形態",
        "勤務時間",
        "応募資格",
        "特徴・待遇",
        "交通アクセス",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_urls: set[str] = set()
        detail_urls: list[str] = []

        for pref_ja in PREFECTURES_JA:
            self.logger.info("一覧収集: %s", pref_ja)
            for detail_url in self._collect_pref_detail_urls(pref_ja):
                if detail_url in seen_urls:
                    continue
                seen_urls.add(detail_url)
                detail_urls.append(detail_url)

        self.total_items = len(detail_urls)
        self.logger.info("詳細URL収集完了: %d 件", len(detail_urls))

        for detail_url in detail_urls:
            item = self._scrape_detail(detail_url)
            if item:
                yield item

    def _collect_pref_detail_urls(self, pref_ja: str) -> Generator[str, None, None]:
        page = 1
        while page <= 500:
            list_url = (
                f"{BASE_URL}/freeword/?freeword={pref_ja}&kw=false&pageNo={page}"
            )
            soup = self.get_soup(list_url)
            if soup is None:
                return

            found_on_page = 0
            for a in soup.select("a[href]"):
                href = a.get("href", "") or ""
                if _DETAIL_HREF_RE.match(href):
                    yield urljoin(BASE_URL, href)
                    found_on_page += 1

            if found_on_page == 0:
                return
            page += 1

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}

        company_td = _th_next_td(soup, ("応募先企業名",))
        store_td = _th_next_td(soup, ("求人募集店舗",))

        company_name = _clean(company_td.get_text()) if company_td else ""
        store_name = _clean(store_td.get_text()) if store_td else ""

        name = company_name or store_name
        if not name:
            return None

        data[Schema.NAME] = name
        if store_name:
            data["求人募集店舗"] = store_name

        addr_td = _th_next_td(soup, ("勤務地",))
        if addr_td is not None:
            map_link = addr_td.find("a")
            if map_link is not None:
                map_link.decompose()
            img = addr_td.find("img")
            if img is not None:
                img.decompose()
            raw_addr = _clean(addr_td.get_text(" ", strip=True))
            if store_name and raw_addr.startswith(store_name):
                raw_addr = raw_addr[len(store_name):].strip(" 　,，、")
            m = _PREF_RE.match(raw_addr)
            if m:
                data[Schema.PREF] = m.group(1)
                data[Schema.ADDR] = raw_addr[m.end():].strip(" 　,，、")
            else:
                data[Schema.ADDR] = raw_addr

        access_td = _th_next_td(soup, ("交通アクセス",))
        if access_td is not None:
            data["交通アクセス"] = _clean(access_td.get_text(" ", strip=True))

        cat_td = _th_next_td(soup, ("募集職種",))
        if cat_td is not None:
            data[Schema.CAT_SITE] = _clean(cat_td.get_text(" ", strip=True))

        emp_td = _th_next_td(soup, ("雇用形態",))
        if emp_td is not None:
            data["雇用形態"] = _clean(emp_td.get_text(" ", strip=True))

        wage_td = _th_next_td(soup, ("給与",))
        if wage_td is not None:
            data["給与"] = _clean(wage_td.get_text(" ", strip=True))

        time_td = _th_next_td(soup, ("勤務時間",))
        if time_td is not None:
            data["勤務時間"] = _clean(time_td.get_text(" ", strip=True))

        qualify_td = _th_next_td(soup, ("応募資格",))
        if qualify_td is not None:
            data["応募資格"] = _clean(qualify_td.get_text(" ", strip=True))

        merit_td = _th_next_td(soup, ("特徴・待遇",))
        if merit_td is not None:
            data["特徴・待遇"] = _clean(merit_td.get_text(" ", strip=True))

        job_desc_heading = soup.find(
            lambda tag: tag.name in ("h2", "h3", "h4")
            and "仕事内容" in tag.get_text()
        )
        if job_desc_heading is not None:
            nxt = job_desc_heading.find_next(["p", "div"])
            if nxt is not None:
                text = _clean(nxt.get_text(" ", strip=True))
                if text:
                    data[Schema.LOB] = text

        return data


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = MoppyBaitoScraper()
    scraper.execute("https://moppy-baito.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
