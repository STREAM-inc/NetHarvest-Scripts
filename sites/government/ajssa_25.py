"""
一般社団法人 大阪府警備業協会（大警協 / AJSSA 会員名簿・大阪府）— 加盟会社名簿

取得対象:
    - 大阪府警備業協会の加盟会社（6支部・約555社）
    - 会社名 / 支部 / 都道府県 / 住所 / TEL / HP / 営業種目①(警備種別) / 営業種目②

取得フロー:
    引数 url (= sites.yml の url = /member/) は加盟会社トップ（支部別件数の索引）。
    実データは検索結果ページ /member/search/（フィルタ無しで全556行）の単一 <table> にある。
    url から /member/search/ を派生させ、テーブルの各データ行 (会社名 th を持つ
    ヘッダ行を除く td 行) を 1 件ずつ即 yield する (Pattern B)。詳細ページ・
    ページネーションは無く、全件が 1 ページに収まる。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_25.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_25
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

# 住所先頭の都道府県。大阪市/守口市/堺市 等の県名省略は 大阪府 を既定とする。
_PREF_RE = re.compile(r"^(北海道|東京都|(?:京都|大阪)府|..県)")
# TEL 末尾の代表電話マーカー ㈹ 等を除去 (Pipeline の全角→半角正規化とは別)
_TEL_CLEAN_RE = re.compile(r"[㈹\(（].*$")


class Ajssa25(StaticCrawler):
    """一般社団法人 大阪府警備業協会 加盟会社名簿 スクレイパー"""

    DELAY = 1.5
    # 営業種目① は警備種別(施設/交通誘導 等の短い構造化ラベル)→ Schema.CAT_SITE。
    # 支部・営業種目② はサイト固有の構造化ラベルとして EXTRA。
    EXTRA_COLUMNS = ["支部", "営業種目②"]

    def parse(self, url: str):
        # 引数 url (= /member/) を唯一のルートとして検索結果ページを派生させる
        list_url = urljoin(url, "search/")
        soup = self.get_soup(list_url)
        if soup is None:
            logger.warning("加盟会社一覧の取得に失敗: %s", list_url)
            return

        table = soup.find("table")
        if table is None:
            logger.warning("会員テーブルが見つかりません: %s", list_url)
            return

        # ヘッダ行 (th のみ) を除いた td を持つデータ行を 1 件ずつ処理
        rows = [tr for tr in table.find_all("tr") if tr.find("td")]
        self.total_items = len(rows)

        for tr in rows:
            try:
                item = self._parse_row(tr, list_url)
                if item:
                    yield item
            except Exception as e:  # 個別会員のエラーはスキップして継続
                logger.warning("会員の解析に失敗しskip: %s", e)
                continue

    def _parse_row(self, tr, source_url: str) -> dict | None:
        tds = tr.find_all("td")
        if len(tds) < 7:
            return None

        # BOM(﻿) や余分な空白を除去して会社名を取得
        name = tds[0].get_text(" ", strip=True).replace("﻿", "").strip()
        if not name:
            return None

        branch = tds[1].get_text(" ", strip=True)

        addr_full = tds[2].get_text(" ", strip=True)
        prefm = _PREF_RE.match(addr_full)
        if prefm:
            pref = prefm.group(1)
            addr = addr_full[prefm.end():].strip()
        else:
            # 県名省略 (大阪市/守口市/堺市 等) は大阪府本社
            pref = "大阪府"
            addr = addr_full

        tel_raw = tds[3].get_text(" ", strip=True)
        tel = _TEL_CLEAN_RE.sub("", tel_raw).strip()

        # HP: 会社外部サイトへのリンク href (テキストは "HP")
        a = tds[4].find("a", href=True)
        hp = a["href"].strip() if a else ""

        gyoumu1 = tds[5].get_text(" ", strip=True)
        gyoumu2 = tds[6].get_text(" ", strip=True)

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: hp,
            Schema.CAT_SITE: gyoumu1,
            "支部": branch,
            "営業種目②": gyoumu2,
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa25()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://daikeikyo.or.jp/member/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
