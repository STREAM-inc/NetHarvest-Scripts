"""
全国警備業協会（AJSSA）会員名簿(新潟県) — 一般社団法人新潟県警備業協会 会員企業一覧

取得対象:
    - 新潟県警備業協会の会員企業（約115社）
    - 業種区分・会社名・郵便番号・所在地・電話番号・FAX

取得フロー:
    /list/ の単一テーブル (1ページ・ページネーション無し) の各行を抽出。
    電話／FAX 列は <br> 区切りで 1行目=TEL / 2行目=FAX。
    都道府県は全件「新潟県」固定 (所在地は市区町村以降のため PREF を補完)。

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/ajssa_13.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_13
"""

import re
import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


_POST_RE = re.compile(r"(\d{3}[-‐−]\d{4})")


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"[\s　\xa0]+", " ", text).strip()


class Ajssa13(StaticCrawler):
    """全国警備業協会（AJSSA）会員名簿(新潟県) スクレイパー"""

    DELAY = 1.5
    # No=掲載番号, FAX は Schema に該当が無いためサイト固有列として保持
    EXTRA_COLUMNS = ["掲載番号", "FAX"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)

        table = soup.select_one("table")
        if table is None:
            return

        rows = table.select("tr")
        # ヘッダ行 (th) を除いたデータ行のみ
        data_rows = [tr for tr in rows if tr.find("td") is not None]
        self.total_items = len(data_rows)

        for tr in data_rows:
            try:
                tds = tr.find_all("td")
                if len(tds) < 6:
                    continue

                no = _clean(tds[0].get_text(strip=True))
                cat_site = _clean(tds[1].get_text(strip=True))
                name = _clean(tds[2].get_text(strip=True))

                # 郵便番号
                post_raw = _clean(tds[3].get_text(strip=True))
                m = _POST_RE.search(post_raw)
                post_code = m.group(1) if m else post_raw

                addr = _clean(tds[4].get_text(" ", strip=True))

                # 電話／FAX 列: <br> 区切りで 1行目=TEL / 2行目=FAX
                lines = [
                    _clean(s) for s in tds[5].stripped_strings if _clean(s)
                ]
                tel = lines[0] if len(lines) >= 1 else ""
                fax = lines[1] if len(lines) >= 2 else ""

                if not name:
                    continue

                yield {
                    Schema.URL: url,
                    Schema.NAME: name,
                    Schema.PREF: "新潟県",
                    Schema.POST_CODE: post_code,
                    Schema.ADDR: addr,
                    Schema.TEL: tel,
                    Schema.CAT_SITE: cat_site,
                    "掲載番号": no,
                    "FAX": fax,
                }
            except Exception as e:  # noqa: BLE001
                self.logger.warning("行の解析に失敗: %s", e)
                continue


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa13()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.niikeikyo.jp/list/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
