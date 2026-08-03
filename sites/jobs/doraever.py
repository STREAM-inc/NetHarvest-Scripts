import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

# 詳細ページ (/job-NNNNNN) の href パターン。/job-lists は数字が続かないため除外される。
_DETAIL_RE = re.compile(r"/job-\d+")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class DoraeverScraper(DynamicCrawler):
    """ドラEVER ドライバー求人スクレイパー（doraever.jp）

    - 一覧 `/job-lists`（2ページ目以降は `/job-lists/{page}`）から詳細 `/job-NNNNNN` を収集。
    - doraever.jp は一過性の WAF 403（`<title>403 Forbidden</title>` の極小ページ）を
      返すことがある。get_soup は HTTP ステータスを見ず 403 ページも正常返却するため、
      `_get_soup_ok` で 403 を検知して再取得する（毎回 goto するので数回で解消する）。
    - 会社概要は `div.mid_box`（`.mid_box_ttl`=="会社概要"）内の `h2.c_ttl`(ラベル)+
      直後 `<p>`(値)。業種・TEL フィールドは無い。
    - 同一会社が複数求人で重複するため会社名で dedup。
    - 全件収集してから yield せず、ページごとに詳細を巡回して逐次 yield する。
    """

    DELAY = 0.5
    EXTRA_COLUMNS = ["創立", "車両保有台数"]

    # 403 検知時の再取得回数
    _FETCH_RETRIES = 4
    # 連続空ページがこの数に達したら一覧巡回を打ち切る
    _MAX_EMPTY = 5

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url を唯一のルート（起点）として使う。ページネーションはここから派生させる。
        root = url
        seen_companies: set[str] = set()
        seen_details: set[str] = set()
        consecutive_empty = 0
        page = 1

        while consecutive_empty < self._MAX_EMPTY and page < 10000:
            list_url = self._list_page_url(root, page)
            soup = self._get_soup_ok(list_url)
            if soup is None:
                consecutive_empty += 1
                page += 1
                continue

            detail_urls = self._extract_detail_urls(root, soup, seen_details)
            if not detail_urls:
                consecutive_empty += 1
                page += 1
                continue
            consecutive_empty = 0

            for detail_url in detail_urls:
                item = self._scrape_detail(detail_url)
                if not item or not item.get(Schema.NAME):
                    continue
                key = item[Schema.NAME]
                if key in seen_companies:
                    continue
                seen_companies.add(key)
                yield item

            page += 1

    def _list_page_url(self, root: str, page: int) -> str:
        """一覧ページの URL をルート url から派生させる。"""
        if page <= 1:
            return root
        return root.rstrip("/") + f"/{page}"

    def _extract_detail_urls(self, root: str, soup, seen: set[str]) -> list[str]:
        urls: list[str] = []
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if not _DETAIL_RE.search(href):
                continue
            full = urljoin(root, href).split("#")[0].split("?")[0]
            if not _DETAIL_RE.search(full):
                continue
            if full in seen:
                continue
            seen.add(full)
            urls.append(full)
        return urls

    def _get_soup_ok(self, url: str, wait_until: str = "domcontentloaded"):
        """403 Forbidden ページを検知し、正常ページが得られるまで再取得する。"""
        for _ in range(self._FETCH_RETRIES):
            soup = self.get_soup(url, wait_until=wait_until)
            if soup is None:
                continue
            title = _clean(soup.title.get_text()) if soup.title else ""
            if "403 Forbidden" in title:
                continue
            h1 = soup.select_one("h1")
            if h1 and "403 Forbidden" in _clean(h1.get_text()):
                continue
            return soup
        return None

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self._get_soup_ok(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        # 会社概要ボックスを特定する
        section = None
        for box in soup.select("div.mid_box"):
            ttl = box.select_one(".mid_box_ttl")
            if ttl and "会社概要" in _clean(ttl.get_text()):
                section = box
                break
        if section is None:
            section = soup

        info: dict[str, str] = {}
        for h2 in section.select("h2.c_ttl"):
            label = _clean(h2.get_text())
            if not label:
                continue
            val_el = h2.find_next_sibling()
            val = _clean(val_el.get_text(" ")) if val_el else ""
            info[label] = val

        # テーブル形式のフォールバック
        for tr in section.select("table tr"):
            th = tr.find("th")
            td = tr.find("td")
            if th and td:
                info.setdefault(_clean(th.get_text()), _clean(td.get_text(" ")))

        for label, val in info.items():
            if not val:
                continue
            if "会社名" in label or "企業名" in label:
                data.setdefault(Schema.NAME, val)
            elif "所在地" in label or "住所" in label:
                data.setdefault(Schema.ADDR, val)
            elif "代表" in label:
                data.setdefault(Schema.REP_NM, val)
            elif "事業内容" in label:
                data.setdefault(Schema.LOB, val)
            elif "従業員" in label:
                data.setdefault(Schema.EMP_NUM, val)
            elif "資本金" in label:
                data.setdefault(Schema.CAP, val)
            elif "売上" in label:
                data.setdefault(Schema.SALES, val)
            elif "創立" in label or "創業" in label or "設立" in label:
                data.setdefault("創立", val)
            elif "車両" in label:
                data.setdefault("車両保有台数", val)
            elif "Web" in label or "サイト" in label or "ＵＲＬ" in label or "URL" in label:
                data.setdefault(Schema.WEBSITE, val)

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    DoraeverScraper().execute("https://doraever.jp/job-lists")
