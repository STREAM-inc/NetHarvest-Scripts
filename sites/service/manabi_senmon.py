"""
マナビジョン【専門】 (manabi.benesse.ne.jp/senmon) — 専門学校情報クローラー

取得対象:
    - 検索結果一覧 (search.html?SearchName=学校) の全ページ、全学校
      2026-04 時点で約 2515 校 / 126 ページ (20件/ページ)

取得フロー:
    1. /senmon/search.html?SearchName=学校&page=N を page=1 から巡回
    2. 各一覧ページの section.resultList-item から学校 URL + エリア + キャッチコピー を収集
    3. 各詳細ページ (/senmon/school/{id}/) で <address>、.schoolAccess-item、
       h3.sectionItemTitle、学校公式サイトパネル等を解析

実行方法:
    python scripts/sites/service/manabi_senmon.py
    python bin/run_flow.py --site-id manabi_senmon
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


BASE = "https://manabi.benesse.ne.jp"
LIST_URL = f"{BASE}/senmon/search.html"
SEARCH_NAME = "学校"
MAX_PAGES = 200  # 安全上限 (通常 126 ページ前後)


_PREF_PATTERN = re.compile(
    r"^(北海道|(?:東京|大阪|京都|神奈川|愛知|兵庫|福岡|埼玉|千葉"
    r"|静岡|広島|宮城|茨城|新潟|栃木|群馬|長野|岐阜|福島|三重"
    r"|熊本|鹿児島|岡山|山口|愛媛|長崎|滋賀|奈良|沖縄|青森|岩手"
    r"|秋田|山形|富山|石川|福井|山梨|和歌山|鳥取|島根|香川|高知"
    r"|徳島|佐賀|大分|宮崎)都?道?府?県?)"
)

_WS = re.compile(r"\s+")

_EXTRA_TITLE_MAP = {
    "教育方針": "教育方針",
    "高校の先生へ": "高校の先生へ",
    "求める人物像": "求める人物像",
    "進級・卒業条件": "進級・卒業条件",
    "学力の他に入学者に望ましい能力": "学力の他に入学者に望ましい能力",
    "卒業までに必要な費用の総額（概算）": "卒業までに必要な費用の総額",
    "奨学金制度": "奨学金制度",
}


def _clean(text: str) -> str:
    if not text:
        return ""
    return _WS.sub(" ", text.replace(" ", " ")).strip()


class ManabiSenmonScraper(StaticCrawler):
    """マナビジョン【専門】 スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "キャッチコピー",
        "紹介文",
        "アクセス",
        "教育方針",
        "高校の先生へ",
        "求める人物像",
        "進級・卒業条件",
        "学力の他に入学者に望ましい能力",
        "卒業までに必要な費用の総額",
        "奨学金制度",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        total_set = False

        for page in range(1, MAX_PAGES + 1):
            list_url = f"{LIST_URL}?SearchName={SEARCH_NAME}&page={page}"
            try:
                soup = self.get_soup(list_url)
            except Exception as e:
                self.logger.warning(f"一覧ページ取得失敗 page={page}: {e}")
                break

            items = soup.select("#search_body_result .resultList-item")
            if not items:
                break

            if not total_set:
                total_el = soup.select_one("#result_total_senmon .resultBox-number")
                if total_el:
                    try:
                        self.total_items = int(_clean(total_el.get_text()))
                    except ValueError:
                        pass
                total_set = True

            for item in items:
                link = item.select_one("h4 a.schoolNameLink")
                if not link or not link.get("href"):
                    continue
                detail_url = urljoin(BASE, link.get("href"))
                if detail_url in seen:
                    continue
                seen.add(detail_url)

                name = _clean(link.get_text())
                area_el = item.select_one(".schoolArea")
                area_text = _clean(area_el.get_text()) if area_el else ""
                catch_el = item.select_one(".schoolDesc-title")
                catch_text = _clean(catch_el.get_text()) if catch_el else ""
                desc_el = item.select_one(".schoolDesc-text")
                desc_text = _clean(desc_el.get_text()) if desc_el else ""

                try:
                    detail = self._scrape_detail(detail_url)
                except Exception as e:
                    self.logger.warning(f"詳細ページ取得失敗 {detail_url}: {e}")
                    detail = {}

                row = {
                    Schema.NAME: name,
                    Schema.URL: detail_url,
                    Schema.CAT_SITE: area_text,
                    "キャッチコピー": catch_text,
                    "紹介文": desc_text,
                }
                row.update(detail)
                yield row

    def _scrape_detail(self, url: str) -> dict:
        soup = self.get_soup(url)
        out: dict = {}

        for el in soup.select(".schoolAccess-item"):
            title_el = el.select_one("h3.sectionItemTitle")
            text_el = el.select_one("p.sectionItemText")
            if not title_el or not text_el:
                continue
            title = _clean(title_el.get_text())
            value = _clean(text_el.get_text(" "))
            if title == "アクセス":
                out["アクセス"] = value

        addr_el = soup.find("address")
        if addr_el:
            addr_text = _clean(addr_el.get_text(" "))
            tel_m = re.search(r"Tel\s*([\d\-\(\)]+)", addr_text, re.IGNORECASE)
            if tel_m:
                out[Schema.TEL] = tel_m.group(1).strip().rstrip("(")
            post_m = re.search(r"〒\s*(\d{3}-\d{4})", addr_text)
            if post_m:
                out[Schema.POST_CODE] = post_m.group(1)
            addr_after_post = re.search(r"〒\s*\d{3}-\d{4}\s*(.+)$", addr_text)
            if addr_after_post:
                full_addr = addr_after_post.group(1).strip()
                pref_m = _PREF_PATTERN.match(full_addr)
                if pref_m:
                    out[Schema.PREF] = pref_m.group(1)
                    out[Schema.ADDR] = full_addr[pref_m.end():].strip()
                else:
                    out[Schema.ADDR] = full_addr

        for panel in soup.select(".schoolPanel"):
            title_el = panel.select_one(".schoolPanel-title")
            if not title_el:
                continue
            if _clean(title_el.get_text()) == "学校公式サイト":
                a = panel.select_one("a[href]")
                if a and a.get("href"):
                    out[Schema.HP] = a.get("href")
                break

        for h3 in soup.select("h3.sectionItemTitle"):
            title = _clean(h3.get_text())
            key = _EXTRA_TITLE_MAP.get(title)
            if not key:
                continue
            sib = h3.find_next_sibling()
            if sib and sib.name in ("p", "div"):
                out[key] = _clean(sib.get_text(" "))

        return out


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = ManabiSenmonScraper()
    scraper.execute("https://manabi.benesse.ne.jp/senmon")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
