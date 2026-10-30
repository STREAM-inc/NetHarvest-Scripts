"""
新電力ネット ガス会社一覧 (pps-net.org/gascompany)

取得対象:
    - 全国のガス会社 約1,310社 (全国一覧は 20件/ページ × 66ページ)
    - 会社名 / 年間需要量(千㎥) / 需要家数(取付メーター数) / 従業員数 /
      導管延長(km) / 詳細ページURL / 地方ブロック

取得フロー:
    1. 起点 URL (全国一覧) の 1 ページ目を取得し、エリア選択 select の
       option (/gascompany/{area}) から地方ブロック一覧ページを導出する
    2. 各地方ブロック一覧を per_page=100 で /page/{n} 送りしながら読み、
       会社 ID → 一覧の指標行と所属ブロックを集約する
       (複数ブロックに供給する会社が約55社あるため、全ブロックを読み終えてから
        「北海道|東北|関東」のように結合して出力する。一覧行に必要項目が
        全て揃うため詳細ページは取得しない → 全件でも約20リクエストで完了)
    3. 地方ブロックに属さない会社が残らないよう、全国一覧の総件数と突き合わせ、
       不足があれば全国一覧を per_page=100 で巡回して補完する (ブロック空欄)

備考の指示:
    - 値「0」はデータ不明の意味なので空欄に変換する
    - 1〜2 秒間隔で取得 (ページ取得ごとに DELAY 秒待機)

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/pps_net_3.py

    # Prefect Flow 経由 (全件)
    python bin/run_flow.py --site-id pps_net_3
"""

import logging
import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

# 詳細ページ URL (/gascompany/30951)
_DETAIL_RE = re.compile(r"/gascompany/(\d+)/?$")
_NUM_RE = re.compile(r"-?[\d,]+(?:\.\d+)?")

PER_PAGE = 100

# 一覧の数値列 (左から順)
_METRIC_COLUMNS = [
    "年間需要量(千㎥)",
    "需要家数(取付メーター数)",
    Schema.EMP_NUM,
    "導管延長(km)",
]


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("　", " ")).strip()


def _metric(text: str) -> str:
    """数値セルを正規化。「0」(=不明) や数値以外は空欄にする。"""
    m = _NUM_RE.search(_clean(text))
    if not m:
        return ""
    value = m.group(0).replace(",", "")
    try:
        if float(value) == 0:
            return ""
    except ValueError:
        return ""
    return value


class PpsNetGasScraper(StaticCrawler):
    """新電力ネット ガス会社一覧スクレイパー"""

    DELAY = 1.0
    # 1 ページから最大100件出るため、待機はページ取得側 (parse 内) で行う
    ITEM_DELAY = 0
    # 想定外の増ページに備えた 1 一覧あたりのページ上限
    MAX_PAGES = 50

    EXTRA_COLUMNS = [
        "company_id",
        "詳細ページURL",
        "年間需要量(千㎥)",
        "需要家数(取付メーター数)",
        "導管延長(km)",
        "地方ブロック",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        base = url.split("?")[0].rstrip("/")

        first = self._fetch(f"{base}?per_page={PER_PAGE}")
        if first is None:
            logger.error("起点の一覧ページを取得できません: %s", url)
            return
        total = self._total_count(first)
        if total:
            self.total_items = total
            logger.info("全国: %s 件", total)

        areas = self._area_links(first, base)
        logger.info("地方ブロック: %s", ", ".join(name for name, _ in areas))

        # 会社ID → 一覧行データ / 所属ブロック (出現順を保持)
        items: dict[str, dict] = {}
        blocks: dict[str, list[str]] = {}
        for area_name, area_url in areas:
            for row in self._iter_list(area_url):
                cid = row["company_id"]
                items.setdefault(cid, row)
                blocks.setdefault(cid, [])
                if area_name not in blocks[cid]:
                    blocks[cid].append(area_name)

        # ブロック未所属の会社が残っていれば全国一覧で補完
        if not areas or (total and len(items) < total):
            logger.info("全国一覧で補完します (ブロック集計 %s / 全国 %s)", len(items), total)
            for row in self._iter_list(base, first_soup=first):
                items.setdefault(row["company_id"], row)

        for cid, row in items.items():
            row["地方ブロック"] = "|".join(blocks.get(cid, []))
            yield row

    # ------------------------------------------------------------------ 取得
    def _fetch(self, page_url: str) -> bs4.BeautifulSoup | None:
        time.sleep(self.DELAY)
        return self.get_soup(page_url)

    def _iter_list(
        self, list_base: str, first_soup: bs4.BeautifulSoup | None = None
    ) -> Generator[dict, None, None]:
        """一覧 (全国 or 地方ブロック) を /page/{n}?per_page=100 で巡回し行を返す。"""
        for page in range(1, self.MAX_PAGES + 1):
            if page == 1 and first_soup is not None:
                soup = first_soup
            else:
                path = list_base if page == 1 else f"{list_base}/page/{page}"
                soup = self._fetch(f"{path}?per_page={PER_PAGE}")
            if soup is None:
                logger.warning("一覧ページを取得できません: %s page=%s", list_base, page)
                return
            rows = [r for r in (self._parse_row(tr, list_base) for tr in self._list_rows(soup)) if r]
            logger.info("%s page %s: %s 件", list_base, page, len(rows))
            yield from rows
            if len(rows) < PER_PAGE:
                return

    # ------------------------------------------------------------------ 解析
    @staticmethod
    def _total_count(soup: bs4.BeautifulSoup) -> int | None:
        el = soup.select_one(".count-control-box .total-count")
        m = _NUM_RE.search(el.get_text()) if el else None
        return int(m.group(0).replace(",", "")) if m else None

    @staticmethod
    def _area_links(soup: bs4.BeautifulSoup, base: str) -> list[tuple[str, str]]:
        """エリア選択 select の option から (ブロック名, 一覧URL) を得る。"""
        base_path = urlparse(base).path.rstrip("/")
        areas: list[tuple[str, str]] = []
        for opt in soup.select("form[name=ppsarea] select option[value]"):
            href = urljoin(base + "/", opt["value"]).split("?")[0].rstrip("/")
            path = urlparse(href).path.rstrip("/")
            # 「全国」(起点自身) は除外し、起点配下のブロックページのみ採用
            if path == base_path or not path.startswith(base_path + "/"):
                continue
            name = _clean(opt.get_text())
            if name and (name, href) not in areas:
                areas.append((name, href))
        return areas

    @staticmethod
    def _list_rows(soup: bs4.BeautifulSoup) -> list[bs4.Tag]:
        """件数表示ボックス直後の会社一覧テーブルのデータ行を返す。"""
        box = soup.select_one(".count-control-box")
        table = box.find_next("table") if box else None
        if table is None:
            return []
        return [
            tr for tr in table.select("tr")
            if tr.find("td") and tr.select_one("th a[href*='/gascompany/']")
        ]

    def _parse_row(self, tr: bs4.Tag, list_url: str) -> dict | None:
        link = tr.select_one("th a[href*='/gascompany/']")
        detail_url = urljoin(list_url, link["href"])
        m = _DETAIL_RE.search(urlparse(detail_url).path)
        if not m:
            return None
        values = [_metric(td.get_text()) for td in tr.find_all("td")]
        values += [""] * (len(_METRIC_COLUMNS) - len(values))

        item = {Schema.NAME: _clean(link.get_text(" ", strip=True)),
                Schema.URL: detail_url,
                Schema.CAT_SITE: "ガス会社",
                "company_id": m.group(1),
                "詳細ページURL": detail_url}
        item.update(zip(_METRIC_COLUMNS, values))
        return item


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = PpsNetGasScraper()
    scraper.execute("https://pps-net.org/gascompany")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
