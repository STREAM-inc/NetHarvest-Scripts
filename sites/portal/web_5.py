"""
カラフルカンパニー情報Web（富山・金沢・新潟）「くらす」カテゴリ

対象サイト: https://colorfulcompany.jp/toyama/venues?topCategoryId=6

取得フロー:
    3エリア (/toyama /kanazawa /niigata) の一覧 /{area}/venues?page=N&topCategoryId=6
    (SSR, 1頁12件, rel=next が無くなるまで) → 詳細 /{area}/venues/{id} で 1 件ずつ yield

備考:
    - 都道府県はサイト上に無いためエリアから付与 (toyama=富山県 / kanazawa=石川県 / niigata=新潟県)
    - 住所は都道府県抜き (市区町村以降) で掲載されている
    - 営業時間は概要 dl の「営業時間」を優先し、無ければ table の「お問い合わせ時間」

実行方法:
    python scripts/sites/portal/web_5.py
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_AREAS = [
    ("toyama", "富山県"),
    ("kanazawa", "石川県"),
    ("niigata", "新潟県"),
]
_DETAIL_RE = re.compile(r"^/[a-z]+/venues/(\d+)$")
_EMPTY = {"", "-", "－", "ー"}


def _clean(text: str) -> str:
    text = (text or "").replace("​", "").strip()
    return "" if text in _EMPTY else text


class Web5Scraper(StaticCrawler):
    """カラフルカンパニー情報Web くらす スクレイパー"""

    DELAY = 1.0  # 備考指定: 低レート (1req/秒程度)
    EXTRA_COLUMNS = ["エリア", "掲載ページURL"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        top_category = (query.get("topCategoryId") or ["6"])[0]
        start_area = parsed.path.strip("/").split("/")[0]
        # 引数 url のエリアを先頭にし、残りのエリアを続けて巡回する
        areas = sorted(_AREAS, key=lambda a: a[0] != start_area)

        seen: set[str] = set()
        for area, pref in areas:
            list_url = urlunparse(parsed._replace(
                path=f"/{area}/venues",
                query=urlencode({"topCategoryId": top_category}),
            ))
            yield from self._crawl_area(list_url, pref, seen)

    def _crawl_area(self, list_url: str, pref: str, seen: set) -> Generator[dict, None, None]:
        page_url = list_url
        while page_url:
            soup = self.get_soup(page_url)
            if soup is None:
                return
            # 「すべてのお店」セクションのカードのみ (ピックアップ等の重複枠を避ける)
            container = soup.select_one(".c-foldable-navigation__main .c-collection") or soup
            for a in container.select("a.p-venue__target[href]"):
                href = a["href"].split("?")[0]
                if not _DETAIL_RE.match(href):
                    continue
                detail_url = urljoin(page_url, href)
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                item = self._scrape_detail(detail_url, pref)
                if item:
                    yield item
            nxt = soup.select_one('nav.p-pagination_navi a[rel="next"]:not(.p-pagination__item--disabled)')
            page_url = urljoin(page_url, nxt["href"]) if nxt and nxt.get("href") else None

    def _scrape_detail(self, url: str, pref: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        table: dict[str, str] = {}
        for tr in soup.select("table tr"):
            th, td = tr.find("th"), tr.find("td")
            if th and td:
                table.setdefault(_clean(th.get_text(strip=True)), _clean(td.get_text(" ", strip=True)))

        summary: dict[str, str] = {}
        for dt in soup.select("dl dt"):
            dd = dt.find_next_sibling("dd")
            if dd:
                summary.setdefault(_clean(dt.get_text(strip=True)), _clean(dd.get_text(" ", strip=True)))

        name = table.get("店舗名", "")
        if not name:
            h2 = soup.select_one("h2.c-section-title--with-background")
            name = _clean(h2.get_text(strip=True)) if h2 else ""
        if not name:
            return None

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.NAME_KANA: table.get("店名ヨミガナ", ""),
            Schema.PREF: pref,
            Schema.ADDR: table.get("住所", ""),
            Schema.TEL: table.get("電話番号", ""),
            Schema.CAT_SITE: table.get("ジャンル", "") or summary.get("ジャンル", ""),
            Schema.TIME: summary.get("営業時間", "") or table.get("お問い合わせ時間", ""),
            Schema.HOLIDAY: table.get("定休日", "") or summary.get("定休日", ""),
            "エリア": summary.get("エリア", ""),
            "掲載ページURL": url,
        }


if __name__ == "__main__":
    scraper = Web5Scraper()
    scraper.execute("https://colorfulcompany.jp/toyama/venues?topCategoryId=6")
