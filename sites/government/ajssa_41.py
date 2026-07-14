"""
一般社団法人 宮崎県警備業協会（AJSSA 会員名簿・宮崎県）— 会員名簿

取得対象:
    - 宮崎県警備業協会の全会員企業（約46社）
    - 業種 / 名称 / 所在地 / 代表者 / TEL / FAX / HP / 備考(役職)

取得フロー:
    引数 url (= sites.yml の url, https://miyazaki-keibi.or.jp/pages/51/) が
    会員名簿ページ。会員名簿は複数の <table class="type007Table"> で構成される
    （県内会員と県外(福岡・東京)の営業所で表が分かれ、県外表には「業種」列が無い）。
    各表の先頭行が見出し行なので、見出しラベル→列インデックスの対応を表ごとに
    構築し、列位置ではなくラベルでセルを取り出す。会員を 1 件取得するごとに
    即 yield する (Pattern B)。ページネーションは無い。

    - 所在地セルは 〒郵便番号 と 住所(市区町村以降・都道府県表記なし) の 2 行。
      郵便番号を POST_CODE に分離し、都道府県は郵便番号先頭3桁から導出する
      （宮崎県外の営業所も含まれるため PREF 固定にしない）。
    - 電話番号・FAX セルは TEL と FAX の 2 行。1 行目を TEL、2 行目を EXTRA「FAX」。
    - 業種(施・機・空・交・雑・貴・身 等) は短い構造化ラベルのため EXTRA「業種」。
    - 備考(理事・監事・理事（会長）等) は短い役職ラベルのため EXTRA「備考」。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_41.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_41
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

_POST_RE = re.compile(r"〒?\s*(\d{3})-?(\d{4})\b")

# 郵便番号 先頭3桁 → 都道府県（全国。境界は日本郵便の3桁区分に準拠）。
# 会員名簿には宮崎県内(88x)のほか福岡(812)・東京(160)営業所も含まれるため、
# PREF は固定せず郵便番号から導出する。
_POSTAL_PREF_RANGES = [
    (1, 9, "北海道"), (10, 19, "秋田県"), (20, 29, "岩手県"), (30, 39, "青森県"),
    (40, 99, "北海道"), (100, 208, "東京都"), (210, 259, "神奈川県"),
    (260, 299, "千葉県"), (300, 319, "茨城県"), (320, 329, "栃木県"),
    (330, 369, "埼玉県"), (370, 379, "群馬県"), (380, 399, "長野県"),
    (400, 409, "山梨県"), (410, 439, "静岡県"), (440, 498, "愛知県"),
    (500, 509, "岐阜県"), (510, 519, "三重県"), (520, 529, "滋賀県"),
    (530, 599, "大阪府"), (600, 629, "京都府"), (630, 639, "奈良県"),
    (640, 649, "和歌山県"), (650, 679, "兵庫県"), (680, 689, "鳥取県"),
    (690, 699, "島根県"), (700, 719, "岡山県"), (720, 739, "広島県"),
    (740, 759, "山口県"), (760, 769, "香川県"), (770, 779, "徳島県"),
    (780, 789, "高知県"), (790, 799, "愛媛県"), (800, 839, "福岡県"),
    (840, 849, "佐賀県"), (850, 859, "長崎県"), (860, 869, "熊本県"),
    (870, 879, "大分県"), (880, 889, "宮崎県"), (890, 899, "鹿児島県"),
    (900, 909, "沖縄県"), (910, 919, "福井県"), (920, 929, "石川県"),
    (930, 939, "富山県"), (940, 959, "新潟県"), (960, 979, "福島県"),
    (980, 989, "宮城県"), (990, 999, "山形県"),
]


def _pref_from_postal(post3: str) -> str:
    """郵便番号先頭3桁 → 都道府県名（該当なしは空文字）。"""
    try:
        n = int(post3)
    except (TypeError, ValueError):
        return ""
    for lo, hi, pref in _POSTAL_PREF_RANGES:
        if lo <= n <= hi:
            return pref
    return ""


class Ajssa41(StaticCrawler):
    """一般社団法人 宮崎県警備業協会 会員名簿 スクレイパー"""

    DELAY = 1.5
    # いずれも短い構造化ラベル（自由記述プロースなし）→ EXTRA。
    EXTRA_COLUMNS = ["業種", "FAX", "備考"]

    def parse(self, url: str):
        # 引数 url を唯一の基点とする（別 URL はハードコードしない）。
        soup = self.get_soup(url)
        if not soup:
            logger.warning("会員名簿ページの取得に失敗: %s", url)
            return

        total = 0
        for table in soup.select("table.type007Table"):
            trs = table.find_all("tr")
            if not trs:
                continue
            header = [c.get_text(strip=True) for c in trs[0].find_all(["th", "td"])]
            # 名称列を持つ表だけが会員名簿（講習日程等の別表を除外）
            if "名称" not in header:
                continue
            idx = {h: i for i, h in enumerate(header)}

            for tr in trs[1:]:
                cells = tr.find_all(["td", "th"])
                if len(cells) < len(header):
                    continue
                try:
                    item = self._parse_member(cells, idx, url)
                except Exception as e:  # 個別会員のエラーはスキップして継続
                    logger.warning("会員の解析に失敗しskip: %s", e)
                    continue
                if item:
                    total += 1
                    self.total_items = total  # 進捗表示用（累積）
                    yield item

    def _cell(self, cells, idx: dict, label: str):
        """見出しラベルに対応するセルを返す（無ければ None）。"""
        i = idx.get(label)
        return cells[i] if i is not None and i < len(cells) else None

    def _parse_member(self, cells, idx: dict, source_url: str) -> dict | None:
        name_cell = self._cell(cells, idx, "名称")
        if name_cell is None:
            return None
        name = name_cell.get_text(strip=True).replace("　", " ").strip()
        if not name or name == "名称":
            return None

        # HP: 名称セル内の <a>（url 基点で絶対化）
        hp = ""
        a = name_cell.find("a", href=True)
        if a:
            hp = urljoin(source_url, a["href"].strip())

        # 所在地: 〒郵便番号 + 住所（市区町村以降）。郵便番号を分離、都道府県は導出。
        post_code = pref = ""
        addr = ""
        loc_cell = self._cell(cells, idx, "所在地")
        if loc_cell is not None:
            parts = [s for s in loc_cell.stripped_strings]
            addr_parts = []
            for p in parts:
                m = _POST_RE.match(p)
                if m and not post_code:
                    post_code = f"{m.group(1)}-{m.group(2)}"
                    rest = p[m.end():].strip()
                    if rest:
                        addr_parts.append(rest)
                else:
                    addr_parts.append(p)
            addr = " ".join(addr_parts).replace("　", " ").strip()
            if post_code:
                pref = _pref_from_postal(post_code[:3])

        # 代表者
        rep = ""
        rep_cell = self._cell(cells, idx, "代表者")
        if rep_cell is not None:
            rep = rep_cell.get_text(strip=True).replace("　", " ").strip()

        # 電話番号・FAX: 1 行目 TEL / 2 行目 FAX
        tel = fax = ""
        tel_cell = self._cell(cells, idx, "電話番号・FAX")
        if tel_cell is not None:
            nums = [s for s in tel_cell.stripped_strings if re.search(r"\d", s)]
            if nums:
                tel = nums[0]
            if len(nums) > 1:
                fax = nums[1]

        # 業種（県外表には列が無い）
        gyoushu = ""
        g_cell = self._cell(cells, idx, "業種")
        if g_cell is not None:
            gyoushu = g_cell.get_text(strip=True)

        # 備考（役職: 理事・監事 等）
        remarks = ""
        r_cell = self._cell(cells, idx, "備考")
        if r_cell is not None:
            remarks = r_cell.get_text(strip=True)

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.REP_NM: rep,
            Schema.TEL: tel,
            Schema.HP: hp,
            "業種": gyoushu,
            "FAX": fax,
            "備考": remarks,
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa41()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://miyazaki-keibi.or.jp/pages/51/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
