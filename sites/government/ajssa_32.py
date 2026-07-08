"""
一般社団法人 山口県警備業協会（AJSSA 会員名簿・山口県）— 会員企業検索

取得対象:
    - 山口県警備業協会の会員企業（約59社）
    - 会社名 / 所在地 / 電話番号 / HP / 業務種別

取得フロー:
    /membership/ の table.kaiin に会員一覧がテーブル形式で直接埋め込まれている
    (SPA ではなく静的 HTML)。ページネーションは link[rel=next] を辿る
    (/membership/ → /membership/@p2/ の 2 ページ, 各行を即 yield する Pattern B)。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_32.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_32
"""

import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# TEL セルの装飾記号 (▶ 等) を除去するための正規表現
_TEL_JUNK_RE = re.compile(r"[▶►▷\s]+")
_MAX_PAGES = 20  # 無限ループ防止のセーフガード


class Ajssa32(StaticCrawler):
    """一般社団法人 山口県警備業協会 会員企業検索 スクレイパー"""

    DELAY = 1.5
    # 業務種別 → Schema.CAT_SITE。それ以外はすべて Schema にマッピングされるため EXTRA なし。
    EXTRA_COLUMNS: list[str] = []

    def parse(self, url: str):
        # 引数 url (= sites.yml の url / 会員企業検索の一覧ページ) を唯一のルートとして辿る
        page_url = url
        seen = set()
        pages = 0

        while page_url and page_url not in seen and pages < _MAX_PAGES:
            seen.add(page_url)
            pages += 1

            soup = self.get_soup(page_url)
            if not soup:
                logger.warning("ページ取得に失敗しskip: %s", page_url)
                break

            table = soup.select_one("table.kaiin")
            rows = table.find_all("tr") if table else []
            # 先頭のヘッダ行 (th のみ) を除外
            data_rows = [r for r in rows if r.find("td")]

            for row in data_rows:
                try:
                    item = self._parse_row(row, page_url)
                    if item:
                        yield item
                except Exception as e:  # 個別行のエラーはスキップして継続
                    logger.warning("行の解析に失敗しskip: %s", e)
                    continue

            # 次ページ: link[rel=next] を url から派生させて辿る
            next_link = soup.select_one("link[rel='next']")
            next_href = next_link.get("href") if next_link else None
            page_url = urljoin(page_url, next_href) if next_href else None

    def _parse_row(self, row, source_url: str) -> dict | None:
        tds = row.find_all("td", recursive=False)
        if len(tds) < 4:
            return None

        name_td = tds[0]
        name = name_td.get_text(" ", strip=True)
        if not name:
            return None

        # HP: 会社名セル内の外部リンク
        a = name_td.find("a", href=True)
        hp = a["href"].strip() if a else ""

        addr = tds[1].get_text(" ", strip=True)
        tel = _TEL_JUNK_RE.sub("", tds[2].get_text(" ", strip=True))

        # 業務種別: span.-w-tagname を "/" 連結
        gyoushu = [
            sp.get_text(strip=True)
            for sp in tds[3].select("span.-w-tagname")
            if sp.get_text(strip=True)
        ]

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: "山口県",  # 山口県警備業協会の会員 = 全て山口県
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: hp,
            Schema.CAT_SITE: "/".join(gyoushu),
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa32()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://www.yssa.or.jp/membership/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
