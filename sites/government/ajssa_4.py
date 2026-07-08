"""
一般社団法人 宮城県警備業協会（AJSSA 会員名簿・宮城県）— 加盟業者一覧

取得対象:
    - 宮城県警備業協会の加盟警備業者（全社・約126社）
    - 会社名 / 所在地(都道府県・住所) / TEL / 公式サイト(HP) / 警備の種類 /
      認定番号 / 所属講師 / 各種宣言等

取得フロー:
    /kamei/ は静的な単一ページ。本文に加盟業者一覧テーブル (table.companyTable) が
    1 つあり、1 行 = 1 会員。会社名セルの <a href> が詳細ページ /kamei/{id}/。
    ページネーションは無い。
      - 一覧行から 詳細URL と「各種宣言等」列 (あり/空) を取得
      - 詳細ページ dl.cmnTable01 の dt/dd から 会社名・所在地・TEL・公式サイト・
        認定番号・所属講師 を取得
      - 詳細ページ h3「警備の種類」直後の ul.contentList から業務種別を取得
    一覧を走査しながら、詳細を 1 件取得するごとに即 yield する (Pattern B)。
    ※「ごあいさつ」欄は自由記述プロースのため著作権リスクを考慮し取得しない。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_4.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_4
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

_PREF = "宮城県"
_DETAIL_RE = re.compile(r"/kamei/\d+/?$")


def _txt(node) -> str:
    """dd/セルノードから正規化済みテキストを取り出す (None 安全)。"""
    if node is None:
        return ""
    return node.get_text(" ", strip=True).replace("　", " ").strip()


class Ajssa4(StaticCrawler):
    """一般社団法人 宮城県警備業協会 加盟業者一覧 スクレイパー"""

    DELAY = 1.5
    # Schema に収まらない固有カラム (いずれも短い構造化ラベル/コード)
    EXTRA_COLUMNS: list[str] = ["認定番号", "所属講師", "各種宣言等"]

    def parse(self, url: str):
        # 引数 url (= sites.yml の url) を唯一のルートとして取得する
        list_soup = self.get_soup(url)
        if not list_soup:
            logger.warning("一覧ページを取得できません: %s", url)
            return

        table = list_soup.select_one("table.companyTable")
        if not table:
            logger.warning("加盟業者テーブルが見つかりません: %s", url)
            return

        # 一覧行から (詳細URL, 各種宣言等) を先に抽出 (件数確定用)
        entries: list[tuple[str, str]] = []
        for tr in table.select("tr"):
            a = tr.find("a", href=_DETAIL_RE)
            if not a:
                continue  # ヘッダ行など
            detail_url = urljoin(url, a["href"].strip())
            tds = tr.find_all("td")
            decl = _txt(tds[4]) if len(tds) >= 5 else ""
            entries.append((detail_url, decl))

        self.total_items = len(entries)
        logger.info("加盟業者 %d 件を検出", len(entries))

        # 詳細を 1 件取得するごとに即 yield (全件バッファしない)
        for detail_url, decl in entries:
            try:
                item = self._scrape_detail(detail_url, decl)
                if item:
                    yield item
            except Exception as e:  # 個別詳細のエラーはスキップして継続
                logger.warning("詳細の解析に失敗しskip (%s): %s", detail_url, e)
                continue

    def _scrape_detail(self, url: str, decl: str) -> dict | None:
        soup = self.get_soup(url)
        if not soup:
            return None

        # dl.cmnTable01 の dt/dd をラベル辞書化
        fields: dict[str, "object"] = {}
        dl = soup.select_one("dl.cmnTable01")
        if dl:
            dts = dl.find_all("dt")
            dds = dl.find_all("dd")
            for dt, dd in zip(dts, dds):
                fields[dt.get_text(strip=True)] = dd

        name = _txt(fields.get("会社名"))
        if not name:
            return None

        # 所在地: 先頭に都道府県が付く場合は除去し PREF へ
        addr = _txt(fields.get("所在地"))
        if addr.startswith(_PREF):
            addr = addr[len(_PREF):].strip()

        tel = _txt(fields.get("TEL"))

        # 公式サイト: <a href> があれば URL、無ければテキスト
        hp = ""
        site_dd = fields.get("公式サイト")
        if site_dd is not None:
            a = site_dd.find("a", href=True)
            hp = a["href"].strip() if a else _txt(site_dd)

        nintei = _txt(fields.get("認定番号"))
        koushi = _txt(fields.get("所属講師"))

        # 警備の種類: h3「警備の種類」直後の ul.contentList
        keibi = []
        for h3 in soup.find_all("h3"):
            if h3.get_text(strip=True) == "警備の種類":
                ul = h3.find_next_sibling("ul")
                if ul:
                    keibi = [
                        li.get_text(strip=True)
                        for li in ul.find_all("li")
                        if li.get_text(strip=True)
                    ]
                break

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: _PREF,  # 宮城県警備業協会の加盟業者 = 全て宮城県
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: hp,
            Schema.CAT_SITE: "/".join(keibi),
            "認定番号": nintei,
            "所属講師": koushi,
            "各種宣言等": decl,
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa4()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://www.mssa.jp/kamei/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
