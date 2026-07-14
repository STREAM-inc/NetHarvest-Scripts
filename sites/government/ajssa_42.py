"""
一般社団法人 鹿児島県警備業協会（AJSSA 会員名簿・鹿児島県）— 会員企業一覧

取得対象:
    - 鹿児島県警備業協会の会員企業（会員企業 約85社 + 賛助会員 約4社、
      1 ページ・ページネーション無し）

取得フロー:
    引数 url (= sites.yml の url, https://www.kakeikyo.or.jp/member/) が会員名簿ページ。
    ページ内には <table class="table th-pink"> が 2 個あり、直前の <h3> で区分が分かる:
      1) 「会員企業一覧」: 5 列 [企業名 / 代表者 / 所在地 / 電話番号 FAX番号 / 業務種別]
      2) 「賛助会員」    : 4 列 [企業名 / 代表者 / 所在地 / 電話番号 FAX番号]（業務種別欄なし）
    列の対応は先頭の見出し行（th）から動的に決めるため、列数の差異に強い。
    企業名セルに <a> があれば HP。電話番号セルは TEL を <br> で FAX と区切る（1 行目=TEL）。
    会員を 1 件取得するごとに即 yield（Pattern B）。

    業務種別（凡例の 9 種別: 交=交通誘導警備 施=施設警備 機=機械警備 貴=貴重品運搬警備
    身=身辺警備 保=保安警備 ホ=ホームセキュリティ ビ=ビルメンテナンス 空=空港保安警備）は
    記号→短ラベルの構造化情報のため EXTRA「業務種別」に "/" 連結で格納（自由記述プロース無し）。

    ※ 県内会員は住所に都道府県名が付かない（例: 鹿児島市石谷町…）ため PREF は既定「鹿児島県」。
      県外会員（福岡市博多区… / 東京都港区…）は住所先頭から都道府県を判定する。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_42.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_42
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

# 業務種別マークの記号→ラベル（凡例の 9 種別警備業務）
_SERVICE_MARKS = {
    "交": "交通誘導警備",
    "施": "施設警備",
    "機": "機械警備",
    "貴": "貴重品運搬警備",
    "身": "身辺警備",
    "保": "保安警備",
    "ホ": "ホームセキュリティ",
    "ビ": "ビルメンテナンス",
    "空": "空港保安警備",
}

# 郵便番号（〒付き）
_POST_RE = re.compile(r"〒?\s*(\d{3})-?\s*(\d{4})")

# 都道府県（住所先頭一致用）
_PREFECTURES = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]
# 政令指定都市など、都道府県名を省いて書かれがちな市の補完
_CITY_TO_PREF = {
    "札幌市": "北海道", "仙台市": "宮城県", "さいたま市": "埼玉県",
    "千葉市": "千葉県", "横浜市": "神奈川県", "川崎市": "神奈川県",
    "相模原市": "神奈川県", "新潟市": "新潟県", "静岡市": "静岡県",
    "浜松市": "静岡県", "名古屋市": "愛知県", "京都市": "京都府",
    "大阪市": "大阪府", "堺市": "大阪府", "神戸市": "兵庫県",
    "岡山市": "岡山県", "広島市": "広島県", "北九州市": "福岡県",
    "福岡市": "福岡県",
}
_DEFAULT_PREF = "鹿児島県"  # 鹿児島県警備業協会 → 県内会員は住所に県名が付かない

# 見出しテキスト → 内部キー（列位置に依存せず見出しで対応付ける）
_HEADER_MAP = {
    "企業名": "name",
    "代表者": "rep",
    "所在地": "addr",
    "電話番号": "tel",   # 「電話番号 FAX番号」を含む見出し
    "業務種別": "service",
}


class Ajssa42(StaticCrawler):
    """一般社団法人 鹿児島県警備業協会 会員企業一覧 スクレイパー"""

    DELAY = 1.5
    # いずれも短い構造化情報（記号ラベル/FAX番号/会員区分）。長文の自由記述プロースは無い。
    EXTRA_COLUMNS = ["業務種別", "FAX番号", "会員区分"]

    def parse(self, url: str):
        # 引数 url を唯一の基点とする（別 URL はハードコードしない）。
        soup = self.get_soup(url)
        if not soup:
            logger.warning("会員名簿ページの取得に失敗: %s", url)
            return

        tables = soup.select("table")
        if not tables:
            logger.warning("会員名簿テーブルが見つからない: %s", url)
            return

        total = 0
        for table in tables:
            # 直前の <h3> を会員区分として使う（会員企業一覧 / 賛助会員）
            heading = table.find_previous(["h2", "h3", "h4"])
            membership = self._clean(heading.get_text(strip=True)) if heading else ""
            membership = membership.replace("一覧", "")

            rows = table.find_all("tr")
            if not rows:
                continue
            col_index = self._build_col_index(rows[0])
            if "name" not in col_index:
                continue  # 会員テーブルでない（見出しに企業名が無い）

            for row in rows[1:]:
                cells = row.find_all(["td", "th"])
                if not cells:
                    continue
                try:
                    item = self._parse_member(cells, col_index, membership, url)
                except Exception as e:  # 個別会員のエラーはスキップして継続
                    logger.warning("会員の解析に失敗しskip: %s", e)
                    continue
                if item:
                    total += 1
                    self.total_items = total  # 進捗表示用（累積）
                    yield item

    @staticmethod
    def _build_col_index(header_row) -> dict:
        """見出し行の th テキストから {内部キー: 列インデックス} を作る。"""
        col_index = {}
        for i, cell in enumerate(header_row.find_all(["th", "td"])):
            text = cell.get_text(" ", strip=True)
            for label, key in _HEADER_MAP.items():
                if label in text and key not in col_index:
                    col_index[key] = i
        return col_index

    def _parse_member(self, cells, col_index: dict, membership: str, source_url: str) -> dict | None:
        name_cell = self._cell(cells, col_index.get("name"))
        if name_cell is None:
            return None
        name = self._clean(name_cell.get_text(" ", strip=True))
        if not name or name == "企業名":
            return None

        # HP: 企業名セル内の <a>
        hp = ""
        a = name_cell.find("a", href=True)
        if a:
            hp = a["href"].strip()

        rep_cell = self._cell(cells, col_index.get("rep"))
        rep = self._clean(rep_cell.get_text(" ", strip=True)) if rep_cell else ""

        addr_cell = self._cell(cells, col_index.get("addr"))
        raw_addr = self._clean(addr_cell.get_text(" ", strip=True)) if addr_cell else ""
        post_code, pref, addr = self._split_address(raw_addr)

        tel, fax = "", ""
        tel_cell = self._cell(cells, col_index.get("tel"))
        if tel_cell is not None:
            # TEL / FAX は <br> 区切り。1 行目=TEL, 2 行目=FAX。
            lines = [self._clean(t) for t in tel_cell.stripped_strings]
            lines = [t for t in lines if t]
            if lines:
                tel = lines[0]
            if len(lines) > 1:
                fax = lines[1]

        services = ""
        svc_cell = self._cell(cells, col_index.get("service"))
        if svc_cell is not None:
            services = self._parse_services(svc_cell.get_text("", strip=True))

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.REP_NM: rep,
            Schema.POST_CODE: post_code,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: hp,
            "業務種別": services,
            "FAX番号": fax,
            "会員区分": membership,
        }

    @staticmethod
    def _cell(cells, idx):
        if idx is None or idx >= len(cells):
            return None
        return cells[idx]

    @staticmethod
    def _clean(text: str) -> str:
        return text.replace("\xa0", " ").replace("　", " ").strip()

    @staticmethod
    def _parse_services(text: str) -> str:
        """業務種別セルの記号を出現順にラベル化し "/" 連結（重複除去）。"""
        labels = []
        for ch in text:
            label = _SERVICE_MARKS.get(ch)
            if label and label not in labels:
                labels.append(label)
        return "/".join(labels)

    @classmethod
    def _split_address(cls, raw: str):
        """住所文字列から (郵便番号, 都道府県, 残り住所) を返す。
        県名が省略された県内会員は既定の鹿児島県とし、住所本体はそのまま残す。"""
        if not raw:
            return "", _DEFAULT_PREF, ""
        post_code = ""
        m = _POST_RE.search(raw)
        if m:
            post_code = f"{m.group(1)}-{m.group(2)}"
            raw = raw[m.end():].strip()
        raw = raw.strip()
        for pref in _PREFECTURES:
            if raw.startswith(pref):
                return post_code, pref, raw[len(pref):].strip()
        for city, pref in _CITY_TO_PREF.items():
            if raw.startswith(city):
                return post_code, pref, raw
        return post_code, _DEFAULT_PREF, raw


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa42()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://www.kakeikyo.or.jp/member/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
