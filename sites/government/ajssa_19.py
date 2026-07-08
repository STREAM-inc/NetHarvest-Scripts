"""
一般社団法人 福井県警備業協会（AJSSA 会員名簿・福井県）— 会員名簿

取得対象:
    - 福井県警備業協会の会員企業（警備会社・約55社）
    - 会社名 / 所在地(住所) / 電話番号 / ホームページ / 業務種別
    ※ページ冒頭の「会員紹介」ロゴ一覧はロゴ画像のみで住所・電話が無いため対象外。
      本文の「会員一覧」テーブル (div.memberList_table) を唯一の取得元とする。

取得フロー:
    引数 url (= sites.yml の url, https://fukuikb.jp/#member) は静的な単一ページ。
    会員一覧は `div.memberList_table table tbody tr` として並ぶ。会員 1 社 = 1 行で:
      th   : 会社名（<a href> があれば HP リンク）
      td[0]: 所在地(住所)
      td[1]: 電話番号
      td[2]: 業務種別（div.memberList_tag li の短い区分ラベル。施設警備/交通 等）
    会員を 1 件取得するごとに即 yield する (Pattern B)。ページネーション無し。

    ※所在地はすべて福井県内のため PREF は「福井県」固定。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_19.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_19
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

_PREF = "福井県"


class Ajssa19(StaticCrawler):
    """一般社団法人 福井県警備業協会 会員名簿 スクレイパー"""

    DELAY = 1.5
    # サイト固有カラムは全て Schema に収まる（業務種別 = CAT_SITE）。
    EXTRA_COLUMNS = []

    def parse(self, url: str):
        # 引数 url (= sites.yml の url) を唯一のルートとして取得する
        soup = self.get_soup(url)
        if not soup:
            logger.warning("ページ取得に失敗しました: %s", url)
            return

        # 会員一覧テーブル (会員紹介ロゴ一覧は住所/電話が無いため対象外)
        rows = soup.select("div.memberList_table table tbody tr")
        self.total_items = len(rows)

        for tr in rows:
            try:
                item = self._parse_row(tr, url)
                if item:
                    yield item
            except Exception as e:  # 個別行のエラーはスキップして継続
                logger.warning("行の解析に失敗しskip: %s", e)
                continue

    def _parse_row(self, tr, source_url: str) -> dict | None:
        th = tr.find("th")
        if th is None:
            return None
        name = self._norm(th.get_text(" ", strip=True))
        if not name:
            return None

        cells = tr.find_all("td")
        addr = self._norm(cells[0].get_text(" ", strip=True)) if len(cells) > 0 else ""
        tel = self._norm(cells[1].get_text(" ", strip=True)) if len(cells) > 1 else ""

        # 業務種別: div.memberList_tag li の短い区分ラベルを "/" 連結
        cat_site = ""
        if len(cells) > 2:
            tags = [self._norm(li.get_text(" ", strip=True)) for li in cells[2].select("li")]
            cat_site = "/".join(t for t in tags if t)

        # 会社名セル内にリンクがあれば HP として採用 (相対 href は url を起点に解決)
        a = th.find("a", href=True)
        hp = urljoin(source_url, a["href"].strip()) if a else ""

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: _PREF,  # 福井県警備業協会の会員 = 全て福井県
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: hp,
            Schema.CAT_SITE: cat_site,
        }

    @staticmethod
    def _norm(text: str) -> str:
        """全角/半角スペース・改行を単一スペースに整形する。"""
        return re.sub(r"[\s　]+", " ", text).strip()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa19()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://fukuikb.jp/#member")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
