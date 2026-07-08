"""
一般社団法人 京都府警備業協会（AJSSA 会員名簿・京都府）— 会員企業一覧

取得対象:
    - 京都府警備業協会の正会員（警備会社・約182社）と賛助会員（用品・
      システム供給業者等・約11社）
    - 会社名 / 住所 / 都道府県 / TEL / 業務種別(取扱業務) / HP / 会員区分

取得フロー:
    引数 url (= sites.yml の url, https://kyokeikyo.or.jp/company.html) は
    静的な単一ページ (JustSystems Homepage Builder 生成 / UTF-8)。会員一覧は
    複数の <table> に全件掲載されている。ページネーション無し。

    テーブル構造:
      - 正会員テーブル: 見出し = №/会社名/住所/ＴＥＬ/※業務種別
        業務種別は "施・交" のような警備種別コード連結
        (施=施設警備 交=交通・雑踏 貴=貴重品運搬 身=身辺警備
         機=機械警備 保=保安警備 ホ=ホームセキュリティ)
        HP を持つ会社は会社名セルに <a href> リンクを持つ (下線表示)。
      - 賛助会員テーブル: 見出し = 会社名/所在地/TEL/取扱業務
        (制服・無線機・システム等の供給業者。府外企業を含む)
      - 正会員テーブルは器 (wrapper) <table> の中に入れ子で置かれるため、
        入れ子 <table> を内包する wrapper は処理対象から除外する。

    見出し行 (会社名を含む tr) を持つテーブルを会員一覧として扱い、見出し
    キーワードから列インデックスを決定して各データ行を 1 件ずつ即 yield する
    (Pattern B)。住所が都道府県名または政令市名で始まる場合はそこから PREF を
    切り出し、それ以外 (正会員は市区町村始まり) は PREF="京都府" を補完する。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_24.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_24
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

_DEFAULT_PREF = "京都府"

# 住所先頭の都道府県抽出用
_PREFECTURES = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]

# 政令指定都市・東京特別区 → 都道府県 (住所が「大阪市…」等で始まる賛助会員向け)
_CITY_PREF = {
    "札幌市": "北海道", "仙台市": "宮城県", "さいたま市": "埼玉県",
    "千葉市": "千葉県", "横浜市": "神奈川県", "川崎市": "神奈川県",
    "相模原市": "神奈川県", "新潟市": "新潟県", "静岡市": "静岡県",
    "浜松市": "静岡県", "名古屋市": "愛知県", "京都市": "京都府",
    "大阪市": "大阪府", "堺市": "大阪府", "神戸市": "兵庫県",
    "岡山市": "岡山県", "広島市": "広島県", "北九州市": "福岡県",
    "福岡市": "福岡県", "熊本市": "熊本県",
}

# データ行ではない注記行を弾く (「未掲載…」「下線のある会社を…」等)
_NOTE_RE = re.compile(r"未掲載|下線|クリック|ホームページへ移動")


class Ajssa24(StaticCrawler):
    """一般社団法人 京都府警備業協会 会員名簿 スクレイパー"""

    DELAY = 1.5
    # 業務種別 (警備種別の短い構造化コード) は Schema.CAT_SITE に格納。
    # 会員区分 (正会員 / 賛助会員) のみ EXTRA。
    EXTRA_COLUMNS = ["会員区分"]

    def parse(self, url: str):
        # 引数 url (= sites.yml の url) を唯一のルートとして取得する
        soup = self.get_soup(url)
        if soup is None:
            logger.warning("ページ取得に失敗しました: %s", url)
            return

        # 会社名見出しを持つテーブルが会員一覧。ただし入れ子 <table> を内包する
        # wrapper テーブルは実データを持たない器なので除外する。
        member_tables = []
        for table in soup.find_all("table"):
            if table.find("table") is not None:
                continue  # 入れ子テーブルを持つ wrapper
            if self._find_header(table) is not None:
                member_tables.append(table)

        # 進捗表示のため総件数を事前集計 (単一ページ内・ネットワーク不要)
        total = 0
        for table in member_tables:
            col_index, header_row = self._find_header(table)
            for tr in table.find_all("tr"):
                if tr is header_row:
                    continue
                if self._is_data_row(tr, col_index):
                    total += 1
        self.total_items = total

        for table in member_tables:
            col_index, header_row = self._find_header(table)
            # 会員区分: 業務種別列(=正会員テーブル) or 取扱業務列(=賛助会員)
            member_type = "正会員" if "gyoushu" in col_index else "賛助会員"
            for tr in table.find_all("tr"):
                if tr is header_row:
                    continue
                if not self._is_data_row(tr, col_index):
                    continue
                try:
                    item = self._parse_row(tr, col_index, member_type, url)
                    if item:
                        yield item
                except Exception as e:  # 個別行のエラーはスキップして継続
                    logger.warning("行の解析に失敗しskip: %s", e)
                    continue

    def _find_header(self, table):
        """会社名を含む見出し行と、列種別→インデックスの対応を返す。

        見つからなければ ``None`` (会員一覧テーブルではない)。
        戻り値: (col_index: dict, header_row: Tag) または None
        """
        for tr in table.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            texts = [c.get_text(" ", strip=True) for c in cells]
            if not any("会社名" in t for t in texts):
                continue
            idx = {}
            for i, t in enumerate(texts):
                if "会社名" in t and "name" not in idx:
                    idx["name"] = i
                elif ("住所" in t or "所在地" in t) and "addr" not in idx:
                    idx["addr"] = i
                elif ("ＴＥＬ" in t or "TEL" in t or "電話" in t) and "tel" not in idx:
                    idx["tel"] = i
                elif "業務種別" in t and "gyoushu" not in idx:
                    idx["gyoushu"] = i
                elif "取扱業務" in t and "toriatsukai" not in idx:
                    idx["toriatsukai"] = i
            if "name" in idx:
                return idx, tr
        return None

    def _is_data_row(self, tr, col_index: dict) -> bool:
        tds = tr.find_all("td")
        name_i = col_index.get("name")
        if name_i is None or len(tds) <= name_i:
            return False
        name = tds[name_i].get_text(" ", strip=True)
        if not name or _NOTE_RE.search(name):
            return False
        return True

    def _parse_row(self, tr, col_index: dict, member_type: str, source_url: str) -> dict | None:
        tds = tr.find_all("td")

        def cell(key: str):
            i = col_index.get(key)
            if i is None or i >= len(tds):
                return None
            return tds[i]

        name_cell = cell("name")
        if name_cell is None:
            return None
        name = self._norm(name_cell.get_text(" ", strip=True))
        if not name:
            return None

        # HP: 会社名セルの <a href> (下線表示の会社のみ)
        a = name_cell.find("a", href=True)
        hp = urljoin(source_url, a["href"].strip()) if a else ""

        tel_cell = cell("tel")
        tel = self._norm(tel_cell.get_text(" ", strip=True)) if tel_cell else ""

        # 業務種別 (正会員) / 取扱業務 (賛助会員) → CAT_SITE
        cat_cell = cell("gyoushu") or cell("toriatsukai")
        cat = self._norm(cat_cell.get_text(" ", strip=True)) if cat_cell else ""

        # 住所: 都道府県名 or 政令市名で始まればそこから PREF を切り出す
        pref, addr = _DEFAULT_PREF, ""
        addr_cell = cell("addr")
        if addr_cell is not None:
            full_addr = self._norm(addr_cell.get_text(" ", strip=True)).replace(" ", "")
            matched = False
            for p in _PREFECTURES:
                if full_addr.startswith(p):
                    pref = p
                    full_addr = full_addr[len(p):]
                    matched = True
                    break
            if not matched:
                for city, p in _CITY_PREF.items():
                    if full_addr.startswith(city):
                        pref = p
                        break
            addr = full_addr

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: hp,
            Schema.CAT_SITE: cat,
            "会員区分": member_type,
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

    scraper = Ajssa24()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://kyokeikyo.or.jp/company.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
