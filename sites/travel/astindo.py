"""
インドネシア ASTINDO 会員名簿 — astindo

取得対象:
    ASTINDO (Asosiasi Travel Agent Indonesia / インドネシア旅行代理店協会) の
    会員一覧ページ `about-astindo/anggota-astindo/` に掲載された全会員。
    2026-09 時点で 435 件。

取得フロー:
    1. 引数 url (= sites.yml の url) を唯一の起点として 1 回だけ取得する。
    2. ページ内の TablePress テーブル (table.tablepress / #tablepress-2) の
       tbody 行を 1 行パースするたびに即 yield する。
    3. ページネーションは存在しない (DataTables による JS 表示切替のみで、
       435 行すべてが静的 HTML の 1 テーブルに含まれる) ため巡回は 1 ページで完結する。

テーブル構造 (thead のラベルは末尾にアンダースコアの桁揃えが付く):
    column-1  NO                … 連番 (取得対象外)
    column-2  NAMA_BRAND        … 会社名 (ブランド名)        → Schema.NAME
    column-3  NO_ANGGOTA        … 会員番号 (例 "005-001423/KALTARA/DPP/VII/2023")。
                                   未記載の行が多く空文字になる
    column-4  PROVINSI          … 州 (例 "JAWA BARAT" / "KALIMANTAN UTARA")
    column-5  KOTA_DOMISILI     … 都市 (例 "BANDUNG"、空の行あり)
    column-6  PRODUK_UNGGULAN   … 主力商品。"TIKET DOMESTIK, TOUR OUTBOUND (...)" の
                                   ようにカンマ区切りの定型カテゴリ列挙 (自由記述の
                                   文章ではない)
    ※ 各セルにリンクは無く、会員ごとの詳細ページはサイト上に存在しない。
      住所 / 電話 / メール / URL / SNS の掲載も一切無い。

備考 (依頼指示の反映):
    - 取得カラム: 会社名 (NAMA_BRAND) / 会員番号 (NO_ANGGOTA) / 州 (PROVINSI) /
      都市 (KOTA_DOMISILI) / 主力商品(作業用) (PRODUK_UNGGULAN) / 国 / 取得元 URL。
    - 「国」は「インドネシア」で固定する。
    - 「主力商品(作業用)」は後工程 (アウトバウンド社の優先判定) 用の作業カラムとして
      保持する。納品前に削除される想定。
    - インドネシアの事業者のため Schema.PREF (日本の都道府県) は使わず、
      EXTRA カラム "州" / "都市" に入れる。
    - 掲載は上記の構造化項目のみで、長文の自由記述フィールドはサイト上に存在しない。
    - robots.txt は `Disallow:` (全許可)。サイト上に利用規約 /
      スクレイピング禁止条項のページは存在しない。過負荷を避けるため DELAY = 1.0。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/astindo.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id astindo
"""

import logging
import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)


class Astindo(StaticCrawler):
    """ASTINDO (インドネシア旅行代理店協会) 会員名簿スクレイパー"""

    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
    DELAY = 1.0
    TIMEOUT = 60

    EXTRA_COLUMNS = [
        "会員番号",
        "州",
        "都市",
        "主力商品(作業用)",
        "国",
    ]

    # 「国」は ASTINDO = インドネシアの協会なので固定
    COUNTRY = "インドネシア"

    # thead のラベル (末尾のアンダースコア桁揃えを除去した値) → 内部キー
    HEADER_TO_KEY = {
        "NAMA_BRAND": "name",
        "NO_ANGGOTA": "member_no",
        "PROVINSI": "province",
        "KOTA_DOMISILI": "city",
        "PRODUK_UNGGULAN": "product",
    }

    # thead が取れなかった場合の位置フォールバック (0 始まり)
    FALLBACK_INDEX = {
        "name": 1,
        "member_no": 2,
        "province": 3,
        "city": 4,
        "product": 5,
    }

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str):
        # 引数 url が唯一のルート。ページネーションは無く 1 ページで全件。
        soup = self.get_soup(url)
        if soup is None:
            logger.warning("一覧ページを取得できませんでした: %s", url)
            return

        table = soup.select_one("table.tablepress") or soup.find("table")
        if table is None:
            logger.warning("会員テーブルが見つかりませんでした: %s", url)
            return

        index_map = self._build_index_map(table)
        rows = table.select("tbody tr")
        logger.info("会員テーブル: %d 行", len(rows))

        seen: set[tuple[str, str]] = set()
        for row in rows:
            try:
                item = self._parse_row(row, index_map, url)
            except Exception as e:  # 1 行の失敗で全体を止めない
                self.error_count += 1
                logger.warning("行の解析に失敗 (スキップ): %s — %s", url, e)
                continue
            if not item:
                continue
            key = (item[Schema.NAME], item["会員番号"])
            if key in seen:
                continue
            seen.add(key)
            yield item

    # ------------------------------------------------------------------ #
    # ヘッダー → 列インデックス
    # ------------------------------------------------------------------ #
    def _build_index_map(self, table: bs4.Tag) -> dict[str, int]:
        """thead のラベルから列位置を決める。取れなければ既定の位置を使う。"""
        index_map: dict[str, int] = {}
        header_cells = table.select("thead tr th")
        for pos, cell in enumerate(header_cells):
            label = self._clean(cell.get_text(" ", strip=True))
            # "NAMA_BRAND__________________" のような桁揃えのアンダースコアを除去
            label = re.sub(r"_+$", "", label).strip().upper()
            key = self.HEADER_TO_KEY.get(label)
            if key and key not in index_map:
                index_map[key] = pos

        for key, pos in self.FALLBACK_INDEX.items():
            index_map.setdefault(key, pos)
        return index_map

    # ------------------------------------------------------------------ #
    # 1 行
    # ------------------------------------------------------------------ #
    def _parse_row(self, row: bs4.Tag, index_map: dict[str, int], url: str) -> dict | None:
        cells = row.find_all(["td", "th"], recursive=False)
        if not cells:
            return None

        def value(key: str) -> str:
            pos = index_map.get(key)
            if pos is None or pos >= len(cells):
                return ""
            return self._clean(cells[pos].get_text(" ", strip=True))

        name = value("name")
        if not name:
            # 会社名が無い行 (区切り行など) は対象外
            return None

        return {
            Schema.URL: url,
            Schema.NAME: name,
            "会員番号": value("member_no"),
            "州": value("province"),
            "都市": value("city"),
            "主力商品(作業用)": value("product"),
            "国": self.COUNTRY,
        }

    # ------------------------------------------------------------------ #
    # 正規化ユーティリティ
    # ------------------------------------------------------------------ #
    @staticmethod
    def _clean(text: str) -> str:
        """改行・連続空白・ノーブレークスペースを 1 つの半角スペースに畳む。"""
        return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    scraper = Astindo()
    scraper.execute("https://astindo.org/about-astindo/anggota-astindo/")
