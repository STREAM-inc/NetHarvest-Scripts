"""
一般社団法人 岐阜県警備業協会（AJSSA 会員名簿・岐阜県）— 加盟会社一覧表

取得対象:
    - 岐阜県警備業協会の加盟会社（警備会社・約150社。末尾に賛助会員の
      ユニフォーム等業者も含む。いずれも会社名/住所/電話/業種を持つ）
    - 会社名 / 都道府県 / 住所 / 電話番号 / ホームページ / 業種

取得フロー:
    引数 url (= sites.yml の url, https://www.gssa.or.jp/pages/21/) は静的な単一ページ。
    加盟会社は五十音のグループごとに複数の `table.type004Table` に分かれて並ぶ。
    各テーブルの先頭行は見出し（会社名/住所/電話番号/業種、いずれも td）なのでスキップし、
    データ行 1 行 = 1 社として:
      td[0]: 会社名（<a href> があれば HP リンク）
      td[1]: 住所
      td[2]: 電話番号
      td[3]: 業種（交通誘導/施設 等の短い区分ラベル）
    会社を 1 件取得するごとに即 yield する (Pattern B)。ページネーション無し。

    ※協会は岐阜県のため大半は岐阜県内。ただし名古屋市・大阪市・滋賀県 等の
      県外会員も混在するため、PREF は住所から判定する（既定は岐阜県）。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_20.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_20
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

# 協会の所在県。住所から都道府県を判定できないときの既定値。
_DEFAULT_PREF = "岐阜県"

# 住所の先頭に現れうる都道府県名（47 都道府県）
_PREFECTURES = (
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
)

# 都道府県名の prefix が無い県外住所を市名で補完する（岐阜県協会に混在する会員）
_CITY_PREF = {
    "名古屋市": "愛知県",
    "一宮市": "愛知県",
    "大阪市": "大阪府",
}


class Ajssa20(StaticCrawler):
    """一般社団法人 岐阜県警備業協会 会員名簿 スクレイパー"""

    DELAY = 1.5
    # サイト固有カラムは全て Schema に収まる（業種 = CAT_SITE）。
    EXTRA_COLUMNS = []

    def parse(self, url: str):
        # 引数 url (= sites.yml の url) を唯一のルートとして取得する
        soup = self.get_soup(url)
        if not soup:
            logger.warning("ページ取得に失敗しました: %s", url)
            return

        # 五十音グループごとの加盟会社テーブル（全て同一クラス type004Table）
        rows = []
        for table in soup.select("table.type004Table"):
            rows.extend(table.find_all("tr"))
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
        cells = tr.find_all(["td", "th"])
        if len(cells) < 3:
            return None

        name = self._norm(cells[0].get_text(" ", strip=True))
        # 見出し行（会社名/住所/電話番号/業種）・空行はスキップ
        if not name or name == "会社名":
            return None

        addr_raw = self._norm(cells[1].get_text(" ", strip=True))
        tel = self._norm(cells[2].get_text(" ", strip=True))
        cat_site = self._norm(cells[3].get_text(" ", strip=True)) if len(cells) > 3 else ""

        pref, addr = self._split_pref(addr_raw)

        # 会社名セル内のリンクを HP として採用（相対 href は url を起点に解決）
        hp = ""
        a = cells[0].find("a", href=True)
        if a:
            href = a["href"].strip()
            if href and not href.startswith(("javascript:", "#", "mailto:", "tel:")):
                hp = urljoin(source_url, href)

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: hp,
            Schema.CAT_SITE: cat_site,
        }

    @staticmethod
    def _split_pref(addr_raw: str) -> tuple[str, str]:
        """住所から都道府県を判定し (都道府県, 都道府県を除いた住所) を返す。"""
        # 1) 明示的に都道府県 prefix がある場合はそれを採用し住所から除く
        for pref in _PREFECTURES:
            if addr_raw.startswith(pref):
                return pref, addr_raw[len(pref):].strip()
        # 2) 県外の主要市名で補完（prefix 無し県外会員）
        for city, pref in _CITY_PREF.items():
            if addr_raw.startswith(city):
                return pref, addr_raw
        # 3) 既定は協会所在県（岐阜県）
        return _DEFAULT_PREF, addr_raw

    @staticmethod
    def _norm(text: str) -> str:
        """全角/半角スペース・改行を単一スペースに整形する。"""
        return re.sub(r"[\s　]+", " ", text).strip()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa20()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://www.gssa.or.jp/pages/21/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
