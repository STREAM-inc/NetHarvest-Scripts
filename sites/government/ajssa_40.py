"""
一般社団法人 熊本県警備業協会（AJSSA 会員名簿・熊本県）— 会員名簿

取得対象:
    - 熊本県警備業協会の全会員企業（約81社、1ページ・ページネーション無し）

取得フロー:
    引数 url (= sites.yml の url, https://www.kssa.or.jp/pages/10/#page-content) が
    会員名簿ページ。ページ内には会員名簿の <table> が 1 個だけある。
    先頭行は見出し行（会社名 / 業務種別 / 住所 / 電話番号）。
    データ行は 2 パターン:
      4 セル: [0] 会社名 / [1] 業務種別 / [2] 住所 / [3] 電話番号
      3 セル: [0] 会社名 / [1] 住所 / [2] 電話番号   （県外支社等で業務種別欄が無い）
    会社名セルに <a> があれば HP。会員を 1 件取得するごとに即 yield (Pattern B)。

    業務種別（凡例: ○機械警備 △施設警備 ◎交通誘導 □運搬警備 ☆身辺警備 ×保安）は
    記号→ラベルの短い構造化情報のため EXTRA「業務種別」に "/" 連結で格納（自由記述プロース無し）。

    ※ 県内会員は住所に都道府県名が付かない（例: 菊池市木柑子）ため PREF は既定「熊本県」。
      県外会員（東京都港区… / 福岡市博多区…）は住所先頭から都道府県を判定する。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_40.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_40
"""

import logging
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 業務種別の記号→ラベル（凡例: ○機械警備 △施設警備 ◎交通誘導 □運搬警備 ☆身辺警備 ×保安）
# 全角・半角ゆらぎを吸収するため複数記号を同一ラベルにマップする。
_SERVICE_MARKS = {
    "○": "機械警備", "〇": "機械警備", "◯": "機械警備",
    "△": "施設警備", "▲": "施設警備",
    "◎": "交通誘導",
    "□": "運搬警備", "■": "運搬警備",
    "☆": "身辺警備", "★": "身辺警備",
    "×": "保安", "✕": "保安", "✖": "保安", "☓": "保安",
}

# 都道府県（住所先頭一致用・長い名称を優先させるため並び順は問わない）
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
_DEFAULT_PREF = "熊本県"  # 熊本県警備業協会の会員 → 県内会員は住所に県名が付かない


class Ajssa40(StaticCrawler):
    """一般社団法人 熊本県警備業協会 会員名簿 スクレイパー"""

    DELAY = 1.5
    # 業務種別は記号→短ラベルの構造化情報 → EXTRA。長文の自由記述プロースは無い。
    EXTRA_COLUMNS = ["業務種別"]

    def parse(self, url: str):
        # 引数 url を唯一の基点とする（別 URL はハードコードしない）。
        soup = self.get_soup(url)
        if not soup:
            logger.warning("会員名簿ページの取得に失敗: %s", url)
            return

        table = soup.find("table")
        if not table:
            logger.warning("会員名簿テーブルが見つからない: %s", url)
            return

        total = 0
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 3:
                continue
            try:
                item = self._parse_member(cells, url)
            except Exception as e:  # 個別会員のエラーはスキップして継続
                logger.warning("会員の解析に失敗しskip: %s", e)
                continue
            if item:
                total += 1
                self.total_items = total  # 進捗表示用（累積）
                yield item

    def _parse_member(self, cells, source_url: str) -> dict | None:
        name = self._clean(cells[0].get_text(" ", strip=True))
        # 見出し行・空行はスキップ
        if not name or name == "会社名":
            return None

        # 4 セル: 会社名 / 業務種別 / 住所 / 電話番号
        # 3 セル: 会社名 / 住所 / 電話番号（業務種別欄なし）
        if len(cells) >= 4:
            services = self._parse_services(cells[1].get_text(" ", strip=True))
            addr = self._clean(cells[2].get_text(" ", strip=True))
            tel = self._clean(cells[3].get_text(" ", strip=True))
        else:
            services = ""
            addr = self._clean(cells[1].get_text(" ", strip=True))
            tel = self._clean(cells[2].get_text(" ", strip=True))

        pref, addr_rest = self._split_pref(addr)

        # HP: 会社名セル内の <a>
        hp = ""
        a0 = cells[0].find("a", href=True)
        if a0:
            hp = a0["href"].strip()

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr_rest,
            Schema.TEL: tel,
            Schema.HP: hp,
            "業務種別": services,
        }

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

    @staticmethod
    def _split_pref(addr: str):
        """住所から都道府県を判定して (pref, 残り住所) を返す。
        県名が省略された県内会員は既定の熊本県とし住所はそのまま残す。"""
        if not addr:
            return _DEFAULT_PREF, addr
        for pref in _PREFECTURES:
            if addr.startswith(pref):
                return pref, addr[len(pref):].strip()
        for city, pref in _CITY_TO_PREF.items():
            if addr.startswith(city):
                return pref, addr
        return _DEFAULT_PREF, addr


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa40()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://www.kssa.or.jp/pages/10/#page-content")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
