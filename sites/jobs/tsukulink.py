# scripts/sites/portal/tsukulink.py
"""
ツクリンク (tsukulink.net) — 建設業者一覧スクレイパー

取得対象:
    全国の建設業者一覧（企業名、住所、業種、代表者名）

取得フロー:
    /companies?page=N → 一覧ページからデータ取得（詳細ページへはアクセスしない）
    ※ 電話番号は問い合わせフォーム経由のみで非公開のため取得不可

実行方法:
    # ローカルテスト
    python scripts/sites/portal/tsukulink.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id tsukulink
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 都道府県抽出パターン（東京都・北海道・〇〇府・〇〇県）
_PREF_RE = re.compile(r"^(東京都|北海道|(?:.+?[都道府県]))")


class TsukulinkScraper(StaticCrawler):
    """ツクリンク 建設業者スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = []
    START_PAGE = 1  # 再開時はここを変更

    def parse(self, url: str) -> Generator[dict, None, None]:
        base_url = url.rstrip("/")
        page = self.START_PAGE
        while True:
            list_url = f"{base_url}/companies?page={page}"
            self.logger.info("一覧ページ取得: page=%d", page)

            try:
                soup = self.get_soup(list_url)
            except Exception as e:
                self.logger.warning("一覧ページ取得失敗: %s (%s)", list_url, e)
                break

            if soup is None:
                self.logger.warning("soup取得失敗（スキップ）: page=%d", page)
                page += 1
                time.sleep(self.DELAY)
                continue

            items = soup.select("li.p-companies-list-item")
            if not items:
                break

            for li in items:
                item = self._parse_item(li, base_url)
                if item:
                    yield item

            # 「次へ」リンクがあれば継続
            next_link = None
            for a in soup.select("a"):
                if "次へ" in a.get_text():
                    next_link = a
                    break

            if next_link:
                page += 1
                time.sleep(self.DELAY)
            else:
                break

    def _parse_item(self, li, base_url: str) -> dict | None:
        # 企業名 & 取得URL
        name_a = li.select_one("a.p-companies-list-item__name")
        if not name_a:
            return None

        href = name_a.get("href", "")
        item = {
            Schema.NAME: name_a.get_text(strip=True),
            Schema.URL: base_url + href if href.startswith("/") else href,
        }

        # 住所 → 都道府県と市区町村以降に分割
        addr_div = li.select_one("div.p-companies-list-item__address")
        if addr_div:
            addr_raw = addr_div.get_text(strip=True)
            m = _PREF_RE.match(addr_raw)
            if m:
                item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = addr_raw[m.end():]
            else:
                item[Schema.ADDR] = addr_raw

        # 業種（dl > dt="業種" の dd）
        for dl in li.select("dl.p-companies-list-item__job-list-item"):
            dt = dl.select_one("dt")
            dd = dl.select_one("dd")
            if dt and dd and dt.get_text(strip=True) == "業種":
                cat = re.sub(r"[\s\u3000]+", " ", dd.get_text(strip=True)).strip("、 ")
                item[Schema.CAT_SITE] = cat
                break

        # 代表者名（"代表　廣田　貢" → "廣田　貢"）
        rep_div = li.select_one("div.c-f-medium.c-t-dark.u-margin-l8p.u-text-nowrap")
        if rep_div:
            rep_text = rep_div.get_text(strip=True)
            rep_text = re.sub(r"^代表[\s\u3000]*", "", rep_text).strip()
            if rep_text:
                item[Schema.REP_NM] = rep_text

        return item


# =============================================================================
# ローカル実行用エントリーポイント
# =============================================================================
if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-page", type=int, default=1)
    args = parser.parse_args()

    scraper = TsukulinkScraper()
    scraper.START_PAGE = args.start_page
    scraper.execute("https://tsukulink.net")
