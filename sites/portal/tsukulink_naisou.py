# scripts/sites/portal/tsukulink_naisou.py
"""
ツクリンク (tsukulink.net) — 内装仕上工事業のみ 建設業者一覧スクレイパー

取得対象:
    内装仕上工事業カテゴリ（category_19）の建設業者一覧 約91,363件
    企業名、住所、業種、代表者名を取得（詳細ページへはアクセスしない）

取得URL:
    https://tsukulink.net/category_19?page=N

実行方法:
    python scripts/sites/portal/tsukulink_naisou.py
    python scripts/sites/portal/tsukulink_naisou.py --start-page 100
"""

import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_RE = re.compile(r"^(東京都|北海道|(?:.+?[都道府県]))")

# 絞り込みカテゴリ（内装仕上工事業）
CATEGORY_PATH = "/category_19"


class TsukulinkNaisouScraper(StaticCrawler):
    """ツクリンク 内装仕上工事業スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = []
    START_PAGE = 1
    STOP_AT: datetime | None = None  # この時刻を過ぎたら次ページ取得前に終了する

    def parse(self, url: str) -> Generator[dict, None, None]:
        base_url = url.rstrip("/")
        page = self.START_PAGE
        while True:
            # 指定時刻を過ぎていたら終了
            if self.STOP_AT and datetime.now() >= self.STOP_AT:
                self.logger.info("指定終了時刻 %s を超過したため終了します（page=%d 直前）", self.STOP_AT, page)
                break

            list_url = f"{base_url}{CATEGORY_PATH}?page={page}"
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
        name_a = li.select_one("a.p-companies-list-item__name")
        if not name_a:
            return None

        href = name_a.get("href", "")
        item = {
            Schema.NAME: name_a.get_text(strip=True),
            Schema.URL: base_url + href if href.startswith("/") else href,
        }

        addr_div = li.select_one("div.p-companies-list-item__address")
        if addr_div:
            addr_raw = addr_div.get_text(strip=True)
            m = _PREF_RE.match(addr_raw)
            if m:
                item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = addr_raw[m.end():]
            else:
                item[Schema.ADDR] = addr_raw

        for dl in li.select("dl.p-companies-list-item__job-list-item"):
            dt = dl.select_one("dt")
            dd = dl.select_one("dd")
            if dt and dd and dt.get_text(strip=True) == "業種":
                cat = re.sub(r"[\s\u3000]+", " ", dd.get_text(strip=True)).strip("、 ")
                item[Schema.CAT_SITE] = cat
                break

        rep_div = li.select_one("div.c-f-medium.c-t-dark.u-margin-l8p.u-text-nowrap")
        if rep_div:
            rep_text = rep_div.get_text(strip=True)
            rep_text = re.sub(r"^代表[\s\u3000]*", "", rep_text).strip()
            if rep_text:
                item[Schema.REP_NM] = rep_text

        return item


if __name__ == "__main__":
    import argparse
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--start-page", type=int, default=1)
    parser.add_argument("--stop-at", type=str, default=None,
                        help="終了時刻 'YYYY-MM-DD HH:MM'")
    parser.add_argument("--output-dir", type=str, default=None,
                        help="CSV出力先ディレクトリ（絶対パス）")
    args = parser.parse_args()

    scraper = TsukulinkNaisouScraper()
    scraper.START_PAGE = args.start_page
    if args.stop_at:
        scraper.STOP_AT = datetime.strptime(args.stop_at, "%Y-%m-%d %H:%M")
    if args.output_dir:
        import os
        from src.framework.pipeline import ItemPipeline
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        # 元のpipelineの一時ファイルを閉じて破棄し、出力先を差し替えた新pipelineに置き換える
        scraper.pipeline._tmp_file.close()
        try:
            os.unlink(scraper.pipeline._tmp_path)
        except OSError:
            pass
        scraper.local_output_dir = output_dir
        scraper.pipeline = ItemPipeline(
            output_dir=output_dir,
            site_name=scraper._site_name,
            extra_columns=scraper.EXTRA_COLUMNS,
            site_id=scraper._site_id,
        )
    scraper.execute("https://tsukulink.net")
