"""
一般社団法人 秋田県警備業協会（AJSSA 会員名簿・秋田県）— 会員一覧

取得対象:
    - 秋田県警備業協会の会員企業（警備会社・約52社）
    - 会社名 / 業務種別 / 住所 / TEL / HP
    ※賛助会員（大塚製薬等・警備業者以外の支援企業）は対象外とする。

取得フロー:
    /free/kaiinnitiran は goope.jp(グーペ) 製の静的な単一ページ。本文に 2 つの
    テーブルがある:
      - 会員一覧テーブル: № / 会員名 / 業種 / 所在地 / 電話 (5 列) … 取得対象
      - 賛助会員テーブル: № / 賛助会員名 / 所在地 / 電話 (4 列) … 対象外
    ヘッダ行に「業種」を含むテーブルを会員一覧として特定し、データ行を 1 件ずつ
    即 yield する。ページネーション無し。
      - 会員名セル: テキスト＝会社名、<a href> があれば会社 HP
      - 業種セル: "１号" "２号" 等（複数併記あり）。ページ上部の【業種凡例】に
                  基づき号数を業務種別名に展開して CAT_SITE に格納する。
      - 所在地セル: "秋田市..." 形式（都道府県プレフィクス無し・全社 秋田県）。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_5.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_5
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

# 業種凡例（ページ上部）: N号 → 業務種別名
_GYOUSHU_LEGEND = {
    "1": "施設、保安、空港保安、機械警備業務",
    "2": "雑踏、交通誘導警備業務",
    "3": "貴重品運搬警備業務",
    "4": "身辺警備業務",
}
# 全角/半角数字 + 号 を抽出
_GO_RE = re.compile(r"([0-9０-９])\s*号")
_ZEN2HAN = str.maketrans("０１２３４５６７８９", "0123456789")
_PREF = "秋田県"


class Ajssa5(StaticCrawler):
    """一般社団法人 秋田県警備業協会 会員一覧 スクレイパー"""

    DELAY = 1.5
    # 業務種別 → Schema.CAT_SITE。Schema に収まらない固有カラムは無し。
    EXTRA_COLUMNS: list[str] = []

    def parse(self, url: str):
        # 引数 url (= sites.yml の url) を唯一のルートとして取得する
        soup = self.get_soup(url)
        if not soup:
            logger.warning("ページ取得に失敗しました: %s", url)
            return

        table = self._find_member_table(soup)
        if not table:
            logger.warning("会員一覧テーブルが見つかりません: %s", url)
            return

        data_rows = []
        for tr in table.find_all("tr"):
            tds = tr.find_all("td", recursive=False)
            if len(tds) < 5:
                continue
            head = tds[0].get_text(strip=True).translate(_ZEN2HAN)
            # ヘッダ行 (№/会員名/業種/所在地/電話) と空行を除外
            if not head.isdigit():
                continue
            data_rows.append(tds)
        self.total_items = len(data_rows)

        for tds in data_rows:
            try:
                item = self._parse_row(tds, url)
                if item:
                    yield item
            except Exception as e:  # 個別行のエラーはスキップして継続
                logger.warning("行の解析に失敗しskip: %s", e)
                continue

    @staticmethod
    def _find_member_table(soup):
        """ヘッダ行に「業種」を含むテーブル（会員一覧）を返す。"""
        for table in soup.find_all("table"):
            first_tr = table.find("tr")
            if not first_tr:
                continue
            header = re.sub(r"\s|　", "", first_tr.get_text("", strip=True))
            if "業種" in header and "賛助" not in header:
                return table
        return None

    def _parse_row(self, tds, source_url: str) -> dict | None:
        name_td, gyoushu_td, addr_td, tel_td = tds[1], tds[2], tds[3], tds[4]

        name = self._clean_name(name_td.get_text(" ", strip=True))
        if not name:
            return None

        a = name_td.find("a", href=True)
        hp = a["href"].strip() if a else ""

        # 業種: "１号２号" 等を凡例で業務種別名に展開
        gyoushu = []
        for m in _GO_RE.finditer(gyoushu_td.get_text(" ", strip=True)):
            key = m.group(1).translate(_ZEN2HAN)
            name_ = _GYOUSHU_LEGEND.get(key)
            if name_ and name_ not in gyoushu:
                gyoushu.append(name_)

        addr = re.sub(r"\s+", " ", addr_td.get_text(" ", strip=True)).strip()
        if addr.startswith(_PREF):
            addr = addr[len(_PREF):].strip()

        tel = tel_td.get_text(" ", strip=True)

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: _PREF,  # 秋田県警備業協会の会員 = 全て秋田県
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: hp,
            Schema.CAT_SITE: "/".join(gyoushu),
        }

    @staticmethod
    def _clean_name(text: str) -> str:
        """会社名の余分な空白（"( 株 )" 等）を整形する。"""
        text = re.sub(r"\s+", " ", text).strip()
        # 括弧内外の不要な空白を除去: "( 株 ) 友愛" → "(株)友愛"
        text = re.sub(r"([（(])\s+", r"\1", text)
        text = re.sub(r"\s+([）)])", r"\1", text)
        text = re.sub(r"([）)])\s+", r"\1", text)
        return text.strip()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa5()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://akita-keibi-k.com/free/kaiinnitiran")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
