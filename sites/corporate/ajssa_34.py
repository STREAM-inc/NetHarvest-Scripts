"""
全国警備業協会（AJSSA）会員名簿(香川県) — 一般社団法人香川県警備業協会 加盟会員一覧

取得対象:
    - 加盟会員(約54社)の一覧
    - 会社名 / 住所(郵便番号・都道府県分離) / 電話番号 / FAX / 業務区分 / 備考(協会役職)

取得フロー:
    - 一覧ページ (https://www.yonkeikyo.or.jp/keibi/index.html) は単一の静的ページ
      (Shift_JIS/CP932。framework の get_soup が apparent_encoding=CP932 で自動デコード)。
    - table.meibo-table 内の <tr> を1社=1レコードとして走査。
      先頭3行は多段ヘッダ (会社名 / 業務区分[9列] / 備考) のため、
      第1セルに '〒' を含む行のみをデータ行として抽出する。
    - 各データ行の列構成 (計12列):
        col0=会社名+〒郵便番号+住所+TEL+FAX (1セルに連結)
        col1=ヘッダ無しの空列 (ラベル不明のためスキップ)
        col2..col10=業務区分9種 (施設/空港保安/保安/機械警備/交通誘導/雑踏/
                    貴重品運搬/身辺警備/その他)。○ 印で該当を表す
        col11=備考 (協会役職: 会長/副会長/理事/顧問 等)

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/ajssa_34.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_34
"""

import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 都道府県プレフィックス
_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_POST_PATTERN = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_TEL_PATTERN = re.compile(r"TEL[:：\s]*([0-9０-９\-－()（）\s]+?)(?=\s*FAX|$)")
_FAX_PATTERN = re.compile(r"FAX[:：\s]*([0-9０-９\-－()（）\s]+)")

# table.meibo-table のデータ列(col2..col10)に対応する業務区分ラベル
_CATEGORY_LABELS = [
    "施設",
    "空港保安",
    "保安",
    "機械警備",
    "交通誘導",
    "雑踏",
    "貴重品運搬",
    "身辺警備",
    "その他",
]


class AjssaKagawa(StaticCrawler):
    """全国警備業協会（AJSSA）会員名簿(香川県) スクレイパー"""

    DELAY = 1.5
    # 香川県協会のホーム都道府県。都道府県表記を省いた住所の既定値に使う。
    _DEFAULT_PREF = "香川県"
    EXTRA_COLUMNS = ["FAX", "備考"]

    def parse(self, url: str):
        soup = self.get_soup(url)

        table = soup.select_one("table.meibo-table")
        if table is None:
            self.logger.warning("会員名簿テーブルが見つかりません: %s", url)
            return

        rows = table.find_all("tr")
        # データ行 = 第1セルに '〒' を含む行 (先頭の多段ヘッダ行を除外)
        data_rows = [
            tr
            for tr in rows
            if "〒" in (tr.find(["td", "th"]).get_text() if tr.find(["td", "th"]) else "")
        ]
        self.total_items = len(data_rows)

        for tr in data_rows:
            try:
                item = self._parse_row(tr, url)
                if item:
                    yield item
            except Exception as e:  # 個別行の失敗はスキップして継続
                self.logger.warning("行のパースに失敗: %s", e)
                continue

    def _parse_row(self, tr, url: str) -> dict | None:
        cells = tr.find_all(["td", "th"])
        if not cells:
            return None

        # --- col0: 会社名 + 〒郵便番号 + 住所 + TEL + FAX が連結された1セル ---
        raw = cells[0].get_text()
        raw = raw.replace("　", " ")
        raw = re.sub(r"\s+", " ", raw).strip()
        if not raw:
            return None

        post_code = ""
        name = raw
        rest = ""
        mp = _POST_PATTERN.search(raw)
        if mp:
            name = raw[: mp.start()].strip()
            post_code = mp.group(1)
            rest = raw[mp.end():]
        else:
            # 〒 が無い場合は TEL/FAX の手前までを名称とみなす
            mlabel = re.search(r"(TEL|FAX)", raw)
            if mlabel:
                name = raw[: mlabel.start()].strip()
                rest = raw[mlabel.start():]

        if not name:
            return None

        tel = ""
        mt = _TEL_PATTERN.search(rest)
        if mt:
            tel = mt.group(1).strip()

        fax = ""
        mf = _FAX_PATTERN.search(rest)
        if mf:
            fax = mf.group(1).strip()

        # 住所 = 〒直後 〜 最初の TEL/FAX ラベルの手前
        addr_raw = rest
        mlabel = re.search(r"(TEL|FAX)", rest)
        if mlabel:
            addr_raw = rest[: mlabel.start()]
        addr_raw = re.sub(r"\s+", " ", addr_raw).strip()
        pref, addr = self._split_pref(addr_raw)

        # --- 業務区分 (col2..col10 の ○ 印) ---
        gyomu = ""
        if len(cells) >= 11:
            marked = [
                label
                for i, label in enumerate(_CATEGORY_LABELS)
                if cells[2 + i].get_text(strip=True)
            ]
            gyomu = "/".join(marked)

        # --- 備考 (col11: 協会役職など短い区分ラベル) ---
        bikou = ""
        if len(cells) >= 12:
            bikou = cells[11].get_text(strip=True).replace("　", " ")
            bikou = re.sub(r"\s+", " ", bikou).strip()

        return {
            Schema.NAME: name,
            Schema.POST_CODE: post_code,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.CAT_SITE: gyomu,
            Schema.URL: url,
            "FAX": fax,
            "備考": bikou,
        }

    def _split_pref(self, address: str):
        """住所文字列から都道府県を分離する。"""
        if not address:
            return self._DEFAULT_PREF, ""
        m = _PREF_PATTERN.match(address)
        if m:
            return m.group(1), address[m.end():].strip()
        # 香川県協会の会員名簿は都道府県表記を省いた市名始まりが基本
        return self._DEFAULT_PREF, address


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = AjssaKagawa()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.yonkeikyo.or.jp/keibi/index.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
