"""
BusinessPlus 経営者インタビュー（東日本）クローラー

対象サイト: https://www.business-plus.net/interview/area/?area=1

取得フロー:
    1. エリアトップ (?area=1) の月別バックナンバーリンク
       (/interview/backnumber/?year=YYYY&month=MM&area=1) を収集し、2025年10月以降を巡回
    2. 各月ページ section.leftsec の記事カードから
       「企業名\xa0役職\xa0氏名」と分野タブ (a.rtab) を取得
    3. 記事詳細ページの <title> 先頭トークンから所在地 (都道府県+市区)、
       meta description から記事要約を取得
    4. 企業名が「／」で複数社併記されている場合は 1社1行 で出力

分野タブはカードの一部にしか付かないため、無い記事は分野別一覧
(/interview/{slug}/?page=N) を必要になった時点で順に読み、記事URL→分野の対応表を作る。

TEL・住所番地・HP はサイトに掲載が無いため取得しない。エリア絞り込みは行わない。

実行方法:
    python scripts/sites/corporate/streamreq_28369businessplus.py
    python bin/run_flow.py --site-id streamreq_28369businessplus
"""

import logging
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import parse_qs, urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 備考指定: 2025年10月〜現在まで
START_YM = (2025, 10)

_ARTICLE_RE = re.compile(r"/interview/(\d{4})/\w+\.html$")
_CATEGORY_RE = re.compile(r"^/interview/([a-z_]+)/$")
_PREF_RE = re.compile(r"^(北海道|東京都|京都府|大阪府|.{2,3}県)(.*)$")
_DESC_PREFIX_RE = re.compile(r"^【[^】]*】\s*")


def _clean(text) -> str:
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


class Streamreq28369BusinessplusScraper(StaticCrawler):
    """BusinessPlus 経営者インタビュー（東日本） スクレイパー"""

    DELAY = 3.0
    EXTRA_COLUMNS = ["記事URL", "掲載年月"]

    # ------------------------------------------------------------------
    # 分野 (カテゴリ) 対応表: 必要になった時点で分野別一覧を読み進める
    # ------------------------------------------------------------------
    def _init_categories(self, soup, base_url: str) -> None:
        self._cat_map: dict[str, str] = {}
        self._cat_queue: list[dict] = []
        seen = set()
        for a in soup.select("a[href]"):
            m = _CATEGORY_RE.match(a["href"])
            if not m or m.group(1) in ("archive", "area", "backnumber") or m.group(1) in seen:
                continue
            seen.add(m.group(1))
            self._cat_queue.append({
                "name": _clean(a.get_text(" ", strip=True)),
                "url": urljoin(base_url, a["href"]),
                "page": 1,
            })
        y, mo = START_YM
        # 月別一覧には前月パスの記事も混ざるため 1か月余裕を持たせる
        self._cat_cutoff = int(f"{(y - 1) % 100:02d}12") if mo == 1 else int(f"{y % 100:02d}{mo - 1:02d}")

    def _advance_category(self, cat: dict) -> None:
        """分野別一覧を1ページ読み、対応表に追加する。打ち切り時は queue から外す。"""
        soup = self.get_soup(f"{cat['url']}?page={cat['page']}")
        left = soup.select_one("section.leftsec") if soup else None
        yymms = []
        if left:
            for a in left.select("a[href]"):
                m = _ARTICLE_RE.search(a["href"])
                if m:
                    self._cat_map.setdefault(urljoin(cat["url"], a["href"]), cat["name"])
                    yymms.append(int(m.group(1)))
        cat["page"] += 1
        if not yymms or max(yymms) < self._cat_cutoff or cat["page"] > 50:
            self._cat_queue.remove(cat)

    def _lookup_category(self, article_url: str) -> str:
        while article_url not in self._cat_map and self._cat_queue:
            # 未完了の分野を round-robin で1ページずつ進める
            for cat in list(self._cat_queue):
                self._advance_category(cat)
                if article_url in self._cat_map:
                    break
        return self._cat_map.get(article_url, "")

    # ------------------------------------------------------------------
    def parse(self, url: str) -> Generator[dict, None, None]:
        top = self.get_soup(url)
        if top is None:
            return
        self._init_categories(top, url)

        months = []
        for a in top.select('a[href*="/interview/backnumber/"]'):
            href = urljoin(url, a["href"])
            q = parse_qs(urlparse(href).query)
            try:
                ym = (int(q["year"][0]), int(q["month"][0]))
            except (KeyError, ValueError):
                continue
            if ym >= START_YM and (ym, href) not in months:
                months.append((ym, href))
        months.sort()
        logger.info("巡回対象 %d か月", len(months))

        seen_articles = set()
        for (year, month), month_url in months:
            soup = self.get_soup(month_url)
            left = soup.select_one("section.leftsec") if soup else None
            if left is None:
                logger.warning("月別ページ取得失敗: %s", month_url)
                continue
            for li in left.select("ul.smodule li"):
                a = li.find("a", href=_ARTICLE_RE)
                if a is None:
                    continue
                article_url = urljoin(month_url, a["href"])
                if article_url in seen_articles:
                    continue
                seen_articles.add(article_url)

                label = ""
                for sp in li.find_all("span"):
                    if sp.get("class") or sp.find_parent("a"):
                        continue
                    if "\xa0" in sp.get_text():
                        label = sp.get_text()
                parts = [_clean(p) for p in label.split("\xa0") if _clean(p)]
                if len(parts) >= 3:
                    company, role, rep = " ".join(parts[:-2]), parts[-2], parts[-1]
                else:
                    company, role, rep = (parts[0] if parts else ""), "", ""

                tab = li.select_one("a.rtab .tt")
                category = _clean(tab.get_text()) if tab else self._lookup_category(article_url)

                detail = self.get_soup(article_url)
                pref = addr = summary = ""
                if detail is not None:
                    title = detail.title.get_text() if detail.title else ""
                    head = _clean(title).split(" ")[0] if title else ""
                    m = _PREF_RE.match(head)
                    if m:
                        pref, addr = m.group(1), m.group(2)
                    desc = detail.select_one('meta[name="description"]')
                    if desc:
                        summary = _DESC_PREFIX_RE.sub("", _clean(desc.get("content")))
                    if not company:
                        cn = detail.select_one(".interview_header .company_name")
                        company = _clean(cn.get_text()) if cn else ""

                for name in [c.strip() for c in company.split("／") if c.strip()] or [""]:
                    yield {
                        Schema.URL: article_url,
                        "記事URL": article_url,
                        "掲載年月": f"{year:04d}-{month:02d}",
                        Schema.NAME: name,
                        Schema.REP_NM: rep,
                        Schema.POS_NM: role,
                        Schema.PREF: pref,
                        Schema.ADDR: addr,
                        Schema.CAT_SITE: category,
                        Schema.LOB: summary,
                    }


if __name__ == "__main__":
    scraper = Streamreq28369BusinessplusScraper()
    scraper.execute("https://www.business-plus.net/interview/area/?area=1")
