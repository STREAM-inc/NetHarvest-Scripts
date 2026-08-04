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

# 詳細ページ (/job-NNNNNN) の href パターン。/job-lists は数字が続かないため除外される。
_DETAIL_RE = re.compile(r"/job-\d+")
# 求人 ID (/job-1280605 → 1280605)
_ID_RE = re.compile(r"/job-(\d+)")
# 総件数表示 "20,759 件の求人が見つかりました"
_TOTAL_RE = re.compile(r"([\d,]+)\s*件の求人")
# 電話番号 (例: 05018834123, 03-1234-5678)
_TEL_RE = re.compile(r"0\d[\d\-]{7,}")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class Ever2Scraper(StaticCrawler):
    """ドラEVER-2 ドライバー求人スクレイパー（doraever.jp / 求人単位）

    - 既存 `ever`（doraever.py）が「会社概要のみ・会社名で dedup」なのに対し、
      本スクレイパーは一覧 `/job-lists` の **求人 1 件ごと** を出力する（重複除去しない）。
    - 一覧（2ページ目以降は `/job-lists/{page}`）から詳細 `/job-NNNNNN` を収集し、
      詳細を 1 件取得するごとに即 yield する（Pattern B）。
    - 詳細ページは `div.mid_box`（`.mid_box_ttl` がセクション名）内の `h2.c_ttl`(ラベル)
      + 直後 `<p>`(値) 構造。「募集中の求人」ボックスは他求人の一覧なので除外する。
    - doraever.jp は一過性の WAF 403（`<title>403 Forbidden</title>` の極小ページ／
      HTTP 403）を返すことがあるため、`_get_soup_ok` で検知して再取得する。
    - 求人 detail の「応募について」ボックスに `連絡先`（電話）があるため TEL を取得できる。
    - 利用規約（/terms）にスクレイピング／クローリングの明示的禁止は無い
      （無断転載・営利目的提供は禁止）。
    """

    DELAY = 0.5
    TIMEOUT = 30

    EXTRA_COLUMNS = [
        "求人ID",
        "職種",
        "雇用形態",
        "必要免許",
        "車形状",
        "輸送品目",
        "車両保有台数",
        "主要取引先",
        "担当者",
        "会社所在地",
    ]

    # 403 検知時の再取得回数
    _FETCH_RETRIES = 4
    # 連続空ページがこの数に達したら一覧巡回を打ち切る
    _MAX_EMPTY = 5
    # 一覧ページ数の安全上限
    _MAX_PAGE = 2000

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url を唯一のルート（起点）として使う。ページネーションはここから派生させる。
        root = url
        seen_details: set[str] = set()
        consecutive_empty = 0
        page = 1

        while consecutive_empty < self._MAX_EMPTY and page <= self._MAX_PAGE:
            list_url = self._list_page_url(root, page)
            soup = self._get_soup_ok(list_url)
            if soup is None:
                consecutive_empty += 1
                page += 1
                continue

            if page == 1 and not self.total_items:
                m = _TOTAL_RE.search(_clean(soup.get_text()))
                if m:
                    self.total_items = int(m.group(1).replace(",", ""))

            detail_urls = self._extract_detail_urls(root, soup, seen_details)
            if not detail_urls:
                consecutive_empty += 1
                page += 1
                continue
            consecutive_empty = 0

            for detail_url in detail_urls:
                try:
                    item = self._scrape_detail(detail_url)
                except Exception as e:  # 個別求人のエラーはログして継続
                    self.logger.warning("詳細取得エラー: %s — %s", detail_url, e)
                    continue
                if not item or not item.get(Schema.NAME):
                    continue
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

    def _get_soup_ok(self, url: str):
        """403 Forbidden ページ／通信失敗を検知し、正常ページが得られるまで再取得する。"""
        for _ in range(self._FETCH_RETRIES):
            soup = self.get_soup(url)
            if soup is None:  # HTTP 403 等 → get_soup が None
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

        mid = _ID_RE.search(url)
        if mid:
            data["求人ID"] = mid.group(1)

        # 全 mid_box のラベル→値を収集（「募集中の求人」ボックスは他求人一覧なので除外）
        info: dict[str, str] = {}
        for box in soup.select("div.mid_box"):
            ttl = box.select_one(".mid_box_ttl")
            box_name = _clean(ttl.get_text()) if ttl else ""
            if "募集中の求人" in box_name:
                continue
            for h2 in box.select("h2.c_ttl"):
                label = _clean(h2.get_text())
                if not label:
                    continue
                val_el = h2.find_next_sibling()
                val = _clean(val_el.get_text(" ")) if val_el else ""
                # 同一ラベルは最初の非空値を優先（勤務地が2つある等）
                if not info.get(label):
                    info[label] = val

        work_addr = ""
        company_addr = ""
        for label, val in info.items():
            if not val:
                continue
            if "会社名" in label or "企業名" in label:
                data.setdefault(Schema.NAME, val)
            elif "会社所在地" in label:
                company_addr = company_addr or val
                data.setdefault("会社所在地", val)
            elif "勤務地" in label:
                work_addr = work_addr or val
            elif "連絡先" in label:
                m = _TEL_RE.search(val)
                if m:
                    data.setdefault(Schema.TEL, m.group(0))
            elif "代表者" in label:
                data.setdefault(Schema.REP_NM, val)
            elif "事業内容" in label:
                data.setdefault(Schema.LOB, val)
            elif "従業員" in label:
                data.setdefault(Schema.EMP_NUM, val)
            elif "資本金" in label:
                # "資本金／売上 = 9,700万円/6400百万" のような複合値を分割
                parts = re.split(r"[／/]", val, maxsplit=1)
                data.setdefault(Schema.CAP, _clean(parts[0]))
                if "売上" in label and len(parts) > 1:
                    data.setdefault(Schema.SALES, _clean(parts[1]))
            elif "売上" in label:
                data.setdefault(Schema.SALES, val)
            elif "創立" in label or "創業" in label or "設立" in label:
                data.setdefault(Schema.OPEN_DATE, val)
            elif "Web" in label or "ＵＲＬ" in label or "URL" in label:
                data.setdefault(Schema.WEBSITE, val)
            elif label == "職種":
                data.setdefault("職種", val)
            elif "雇用形態" in label:
                data.setdefault("雇用形態", val)
            elif "必要免許" in label:
                data.setdefault("必要免許", val)
            elif "車形状" in label:
                data.setdefault("車形状", val)
            elif "輸送品目" in label:
                data.setdefault("輸送品目", val)
            elif "車両" in label:
                data.setdefault("車両保有台数", val)
            elif "主要取引先" in label or "取引先" in label:
                data.setdefault("主要取引先", val)
            elif label == "担当":
                data.setdefault("担当者", val)

        # 住所は勤務地を優先し、無ければ会社所在地を採用
        addr = work_addr or company_addr
        if addr:
            data[Schema.ADDR] = addr

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    Ever2Scraper().execute("https://doraever.jp/job-lists")
