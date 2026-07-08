"""
一般社団法人 静岡県警備業協会（AJSSA 会員名簿・静岡県）— 会員名簿

取得対象:
    - 静岡県警備業協会の全会員企業（3 エリア・約172社）
      東部地区 (/pages/34/)=57 / 中部地区 (/pages/35/)=72 / 西部地区 (/pages/36/)=43

取得フロー:
    引数 url (= sites.yml の url, .../pages/38/) は「会員情報」の索引ページ。
    本体の会員名簿は 3 エリアページ (東部/中部/西部地区) に分かれており、索引ページ内の
    リンク (テキスト末尾が「地区」の <a>) から urljoin で各エリア URL を導出して巡回する
    (別 URL はハードコードしない)。
    各エリアページでは 会員 1 社 = 1 個の <table class="type007Table"> で、1 行 4 列:
      [0] 会社名（<a> があれば HP リンク） / [1] 所在地 / [2] 電話番号 / [3] 業種
    先頭セルが「会社名」の行は見出し行なのでスキップする。ページネーションは無い。
    会員を 1 件取得するごとに即 yield する (Pattern B)。

    ※ 所在地はすべて静岡県内のため PREF は「静岡県」固定。索引ページ上の「○号業務」の
      説明文（長文プロース）は会員データには取り込まない。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_16.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_16
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


class Ajssa16(StaticCrawler):
    """一般社団法人 静岡県警備業協会 会員名簿 スクレイパー"""

    DELAY = 1.5
    # 業務区分(○号業務) / 地区 はサイト固有の短い構造化ラベル → EXTRA。
    EXTRA_COLUMNS = ["業種", "地区"]

    def parse(self, url: str):
        # 引数 url (索引ページ) を唯一の基点とし、ページ内リンクから 3 エリア URL を導出する。
        index_soup = self.get_soup(url)
        if not index_soup:
            logger.warning("索引ページの取得に失敗: %s", url)
            return

        areas = []  # [(area_name, area_url)] (href で重複排除)
        seen = set()
        for a in index_soup.find_all("a", href=True):
            text = a.get_text(strip=True)
            if not text.endswith("地区"):
                continue
            area_url = urljoin(url, a["href"])
            if area_url in seen or area_url.rstrip("/") == url.rstrip("/"):
                continue
            seen.add(area_url)
            areas.append((text.replace("地区", ""), area_url))

        if not areas:
            logger.warning("エリアリンクが見つからない: %s", url)
            return

        total = 0
        for area_name, area_url in areas:
            soup = self.get_soup(area_url)
            if not soup:
                logger.warning("エリアページ取得に失敗しskip: %s", area_url)
                continue

            for table in soup.select("table.type007Table"):
                try:
                    item = self._parse_member(table, area_url, area_name)
                    if item:
                        total += 1
                        self.total_items = total  # 進捗表示用（累積）
                        yield item
                except Exception as e:  # 個別会員のエラーはスキップして継続
                    logger.warning("会員の解析に失敗しskip: %s", e)
                    continue

    def _parse_member(self, table, source_url: str, area_name: str) -> dict | None:
        row = table.find("tr")
        if not row:
            return None
        cells = row.find_all(["td", "th"])
        if len(cells) < 4:
            return None

        name = cells[0].get_text(" ", strip=True).replace("　", " ").strip()
        # 見出し行（会社名/所在地/電話番号/業種）と空行はスキップ
        if not name or name == "会社名":
            return None

        addr = cells[1].get_text(" ", strip=True).replace("　", " ").strip()
        tel = cells[2].get_text(" ", strip=True).strip()
        gyoushu = cells[3].get_text(" ", strip=True).replace("　", " ").strip()

        # 会社名セル内にリンクがあれば HP として採用
        a = cells[0].find("a", href=True)
        hp = a["href"].strip() if a else ""

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: "静岡県",  # 静岡県警備業協会の会員 = 全て静岡県
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: hp,
            "業種": gyoushu,
            "地区": area_name,
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa16()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://www.shizu-keikyo.jp/pages/38/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
