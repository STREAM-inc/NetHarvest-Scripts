"""
Rejob (株式会社リジョブ) — コーポレートサイトの会社概要を 1 レコード抽出するクローラー

取得対象:
    - https://rejob.co.jp/company の会社概要・役員・所在地
    - トップページのSNSリンク (Instagram, Facebook)

取得フロー:
    1. /company を取得 → 概要・役員・所在地テーブルをパース
    2. / を取得 → SNSリンクを抽出
    3. 1 レコード yield

実行方法:
    python scripts/sites/corporate/rejob.py
    python bin/run_flow.py --site-id rejob
"""

import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://rejob.co.jp"
COMPANY_URL = f"{BASE_URL}/company"

_POST_CODE_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


def _normalize_post_code(raw: str) -> str:
    m = _POST_CODE_RE.search(raw)
    if not m:
        return ""
    p = m.group(1)
    return p if "-" in p else f"{p[:3]}-{p[3:]}"


def _split_pref(addr: str) -> tuple[str, str]:
    m = _PREF_PATTERN.match(addr)
    if not m:
        return "", addr
    return m.group(1), addr[m.end():].strip()


def _normalize_date(s: str) -> str:
    """'2009.11.02' / '2009年11月2日' → '2009-11-02'"""
    m = re.match(r"^(\d{4})[.\-/年]\s*(\d{1,2})[.\-/月]\s*(\d{1,2})", s)
    if not m:
        return _clean(s)
    y, mo, d = m.groups()
    return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"


class RejobScraper(StaticCrawler):
    """株式会社リジョブ コーポレートサイト スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "役員",
        "主要取引銀行",
        "大阪支社郵便番号",
        "大阪支社住所",
    ]

    def parse(self, url: str):
        self.total_items = 1

        company_soup = self.get_soup(COMPANY_URL)
        if company_soup is None:
            self.logger.warning("会社概要ページの取得に失敗: %s", COMPANY_URL)
            return

        item: dict = {
            Schema.URL: COMPANY_URL,
            Schema.HP: f"{BASE_URL}/",
            Schema.NAME: "",
            Schema.OPEN_DATE: "",
            Schema.LOB: "",
            Schema.REP_NM: "",
            Schema.POS_NM: "",
            Schema.POST_CODE: "",
            Schema.PREF: "",
            Schema.ADDR: "",
            Schema.INSTA: "",
            Schema.FB: "",
            "役員": "",
            "主要取引銀行": "",
            "大阪支社郵便番号": "",
            "大阪支社住所": "",
        }

        # --- 概要テーブル / 役員テーブル / 所在地テーブル を順に処理 ---
        tables = company_soup.find_all("table")
        # heading 順は h4 と一致 (概要 / 役員 / 所在地)
        headings = [_clean(h.get_text()) for h in company_soup.find_all("h4")]

        # 概要テーブル
        overview_rows = self._table_rows(tables[0]) if len(tables) >= 1 else []
        # 役員テーブル
        officer_rows = self._table_rows(tables[1]) if len(tables) >= 2 else []
        # 所在地テーブル
        loc_rows = self._table_rows(tables[2]) if len(tables) >= 3 else []

        # 概要パース: 直前の th を引き継いで多行値をマージ
        current_key = ""
        overview: dict[str, list[str]] = {}
        for row in overview_rows:
            if len(row) == 2:
                current_key = row[0]
                overview.setdefault(current_key, []).append(row[1])
            elif len(row) == 1 and current_key:
                overview.setdefault(current_key, []).append(row[0])

        item[Schema.NAME] = _clean(" ".join(overview.get("会社名", [])))
        item[Schema.OPEN_DATE] = _normalize_date(
            " ".join(overview.get("設立", []))
        )
        item[Schema.LOB] = " / ".join(
            _clean(v) for v in overview.get("事業内容", []) if _clean(v)
        )
        item["主要取引銀行"] = " / ".join(
            _clean(v) for v in overview.get("主要取引銀行", []) if _clean(v)
        )

        # 役員パース: 「代表取締役社長 大貫祐輝」を REP_NM/POS_NM に。
        # 残りの取締役・監査役は EXTRA_COLUMNS の "役員" にまとめる
        officer_pairs: list[tuple[str, str]] = []
        for row in officer_rows:
            if len(row) == 2:
                officer_pairs.append((_clean(row[0]), _clean(row[1])))

        for pos, name in officer_pairs:
            if "代表" in pos and not item[Schema.REP_NM]:
                item[Schema.POS_NM] = pos
                item[Schema.REP_NM] = name
                break

        item["役員"] = " / ".join(
            f"{pos} {name}" for pos, name in officer_pairs if pos and name
        )

        # 所在地パース: 「東京本社 〒170-6047」 / 「(住所のみ)」 / 「大阪支社 〒...」 / 「(住所)」
        # th が拠点名、td が郵便番号 → 次の単独セル行が住所
        locations: list[dict[str, str]] = []
        current_loc: dict[str, str] | None = None
        for row in loc_rows:
            if len(row) == 2:
                if current_loc:
                    locations.append(current_loc)
                current_loc = {
                    "label": _clean(row[0]),
                    "post": _normalize_post_code(row[1]),
                    "addr_raw": "",
                }
            elif len(row) == 1 and current_loc:
                current_loc["addr_raw"] = _clean(row[0])
        if current_loc:
            locations.append(current_loc)

        if locations:
            tokyo = locations[0]
            pref, rest = _split_pref(tokyo["addr_raw"])
            item[Schema.POST_CODE] = tokyo["post"]
            item[Schema.PREF] = pref
            item[Schema.ADDR] = rest
        if len(locations) >= 2:
            osaka = locations[1]
            item["大阪支社郵便番号"] = osaka["post"]
            item["大阪支社住所"] = osaka["addr_raw"]

        # --- SNS (トップページから) ---
        try:
            top_soup = self.get_soup(f"{BASE_URL}/")
            if top_soup is not None:
                for a in top_soup.find_all("a", href=True):
                    href = a["href"]
                    if not item[Schema.INSTA] and "instagram.com" in href:
                        item[Schema.INSTA] = href
                    elif not item[Schema.FB] and "facebook.com" in href:
                        item[Schema.FB] = href
        except Exception as e:
            self.logger.warning("トップページからのSNS取得に失敗: %s", e)

        yield item

    @staticmethod
    def _table_rows(table) -> list[list[str]]:
        rows: list[list[str]] = []
        for tr in table.find_all("tr"):
            cells = [_clean(c.get_text(" ")) for c in tr.find_all(["th", "td"])]
            if cells:
                rows.append(cells)
        return rows


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = RejobScraper()
    scraper.execute(f"{BASE_URL}/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
