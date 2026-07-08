"""
一般社団法人 山梨県警備業協会（AJSSA 会員名簿・山梨県）— 会員名簿

取得対象:
    - 山梨県警備業協会の全会員企業（1 ページに全社を掲載・約45社）

取得フロー:
    /membership-list/ は WordPress の静的ページ。会員は単一の <table> に列挙され、
    ページネーションは無い。各行 (tr) は 4 セル構成:
      - cell0: 会社名 (<a> に HP リンク) + <br> + 代表者名
      - cell1: 〒郵便番号 + <br> + 住所
      - cell2: TEL + <br> + FAX
      - cell3: 業務区分 (○号) + <br> + 種別 (ビルメン / 機械 など・任意)
    ヘッダ行を除いた各データ行を 1 件ずつ即 yield する。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_14.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_14
"""

import logging
import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 業務区分（警備業法の号）を示す行のパターン。これ以外(ビルメン/機械等)は「種別」扱い。
_GO_RE = re.compile(r"^\d+号")


class Ajssa14(StaticCrawler):
    """一般社団法人 山梨県警備業協会 会員名簿 スクレイパー"""

    DELAY = 1.5
    # 業務区分(号) / 種別 / FAX はサイト固有の構造化情報 → EXTRA。
    EXTRA_COLUMNS = ["業務区分", "種別", "FAX"]

    def parse(self, url: str):
        # 引数 url (= sites.yml の url) を唯一のルートとして使う
        soup = self.get_soup(url)
        if not soup:
            logger.warning("ページ取得に失敗: %s", url)
            return

        table = soup.find("table")
        rows = table.find_all("tr") if table else []
        # 先頭行はヘッダ (名称/代表者・所在地・電話/FAX・業種)
        data_rows = [r for r in rows if r.find("td")]
        self.total_items = len(data_rows)

        for row in data_rows:
            try:
                item = self._parse_row(row, url)
                if item:
                    yield item
            except Exception as e:  # 個別行のエラーはスキップして継続
                logger.warning("行の解析に失敗しskip: %s", e)
                continue

    @staticmethod
    def _lines(cell) -> list[str]:
        """セル内を <br> 区切りで分割し、空行を除いた文字列リストを返す。"""
        return [x.strip() for x in cell.get_text("\n", strip=True).split("\n") if x.strip()]

    def _parse_row(self, row, source_url: str) -> dict | None:
        tds = row.find_all("td")
        if len(tds) < 4:
            return None

        # cell0: 会社名 (+ HP リンク) / 代表者名
        name_cell = tds[0]
        a = name_cell.find("a", href=True)
        hp = a["href"].strip() if a else ""
        name_lines = self._lines(name_cell)
        if not name_lines:
            return None
        name = name_lines[0]
        rep = name_lines[1] if len(name_lines) > 1 else ""

        # cell1: 〒郵便番号 / 住所
        addr_lines = self._lines(tds[1])
        post_code = ""
        addr = ""
        for ln in addr_lines:
            if ln.startswith("〒") or re.match(r"^\d{3}-?\d{4}$", ln):
                post_code = ln.lstrip("〒").strip()
            else:
                addr = (addr + " " + ln).strip()

        # cell2: TEL / FAX
        tel_lines = self._lines(tds[2])
        tel = tel_lines[0] if tel_lines else ""
        fax = tel_lines[1] if len(tel_lines) > 1 else ""

        # cell3: 業務区分(○号) / 種別
        cat_lines = self._lines(tds[3])
        go_parts = [ln for ln in cat_lines if _GO_RE.match(ln)]
        type_parts = [ln for ln in cat_lines if not _GO_RE.match(ln)]
        gyoumu = " ".join(go_parts)
        shubetsu = " ".join(type_parts)

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: "山梨県",  # 山梨県警備業協会の会員 = 全て山梨県
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.REP_NM: rep,
            Schema.HP: hp,
            "業務区分": gyoumu,
            "種別": shubetsu,
            "FAX": fax,
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa14()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("http://nashikeikyo.jp/membership-list/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
