"""
一般社団法人 茨城県警備業協会（AJSSA 会員名簿・茨城県）— 会員企業一覧

取得対象:
    - 茨城県警備業協会の会員企業（全社）
    - 会社名 / 郵便番号 / 住所 / TEL / HP / 主業務（警備種別）

取得フロー:
    トップ (index) はフレームセット。実データは会員一覧ページ kaiin.html に
    静的な HTML テーブル (7 分割) として全件掲載されている。ページネーション無し。
    各テーブルの行を 1 件ずつ即 yield する。

    テーブル構造 (各テーブル共通):
      <tr><th>会社名</th><th>電話</th><th>〒<br>所在地</th><th>主業務</th></tr>
      <tr><td>社名<br>支社</td><td>029-<br>xxx-xxxx</td>
          <td>310-xxxx<br>水戸市…<br>番地</td><td>施設警備<br>交通誘導警備</td></tr>

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_7.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_7
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

_POSTAL_RE = re.compile(r"^\d{3}-?\d{4}$")


class Ajssa7(StaticCrawler):
    """一般社団法人 茨城県警備業協会 会員名簿 スクレイパー"""

    DELAY = 1.5
    # 主業務（警備種別: 施設警備/交通誘導警備 等の短い構造化ラベル）は Schema.CAT_SITE。
    # サイト固有の追加カラムは無し。
    EXTRA_COLUMNS: list[str] = []

    def parse(self, url: str):
        # 引数 url (= sites.yml の url / トップフレームセット) を唯一のルートとして派生
        list_url = urljoin(url, "kaiin.html")

        soup = self.get_soup(list_url)
        if soup is None:
            logger.warning("会員一覧の取得に失敗: %s", list_url)
            return

        # 会社名ヘッダを持つデータ行を全テーブルから収集
        rows = []
        for table in soup.find_all("table"):
            for tr in table.find_all("tr"):
                if tr.find("td"):
                    rows.append(tr)
        self.total_items = len(rows)

        for tr in rows:
            try:
                item = self._parse_row(tr, list_url)
                if item:
                    yield item
            except Exception as e:  # 個別行のエラーはスキップして継続
                logger.warning("行の解析に失敗しskip: %s", e)
                continue

    def _parse_row(self, tr, source_url: str) -> dict | None:
        tds = tr.find_all("td", recursive=False)
        if len(tds) < 4:
            return None

        name_td, tel_td, addr_td, gyoushu_td = tds[0], tds[1], tds[2], tds[3]

        # 会社名: <br> 区切り(社名 + 支社等)を連結。<a href> があれば HP。
        name = "".join(name_td.stripped_strings)
        if not name:
            return None
        a = name_td.find("a", href=True)
        hp = a["href"].strip() if a else ""

        # 電話: "029-" + "305-6972" → "029-305-6972"
        tel = "".join(tel_td.stripped_strings)

        # 〒所在地: 先頭が郵便番号、残りが住所
        parts = list(addr_td.stripped_strings)
        if parts and _POSTAL_RE.match(parts[0]):
            post_code = parts[0]
            addr = "".join(parts[1:])
        else:
            post_code = ""
            addr = "".join(parts)

        # 主業務: <br> 区切りの警備種別を "/" 連結
        gyoushu = [s for s in gyoushu_td.stripped_strings if s]

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: "茨城県",  # 茨城県警備業協会の会員 = 全て茨城県
            Schema.POST_CODE: post_code,
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

    scraper = Ajssa7()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://ibaraki.r.ajssa.or.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
