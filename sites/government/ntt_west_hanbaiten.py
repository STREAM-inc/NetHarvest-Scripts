"""
NTT西日本 特約店・販売委託店一覧 スクレイパー

対象: https://www2.hanbaiten.cpe.isp.ntt-west.co.jp/lists

【取得方法】
  タブクリックは新規ウィンドウへの POST フォーム送信のため、
  requests で各エンドポイントに直接 POST し、レスポンス HTML を解析する。

  ※ レスポンス HTML は <tr> 閉じタグ欠落の不正形式のため、
     <tr> ではなく td のクラス属性 (text_a / text_b) でデータセルを特定する。

取得セクション:
    - 特約店一覧        (/lists/tokuyakuten, 50音順タブ)
    - 販売委託店一覧    (/lists/hanbaiten,   50音順タブ)
    - エリア別一覧      (/lists/area,        地域タブ)
    - 解約一覧          (/lists/kaiyaku,     50音順タブ)

実行方法:
    python scripts/sites/government/ntt_west_hanbaiten.py
    python bin/run_flow.py --site-id ntt_west_hanbaiten
"""

import sys
import time
from pathlib import Path
from typing import Generator

import bs4

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

BASE_URL = "https://www2.hanbaiten.cpe.isp.ntt-west.co.jp"
LIST_URL  = f"{BASE_URL}/lists"

COL_STATUS       = "ステータス"
COL_UPDATE       = "更新日時"
COL_SECTION      = "セクション"
COL_TAB          = "タブ"
COL_PARENT       = "販売委託店が契約している特約店名"
COL_AFTER_MAINT  = "解約後の保守会社"
COL_MAINT_TEL    = "保守会社連絡先"
COL_CANCEL_DATE  = "解約申請受理日"

# レスポンス HTML のカラム名 → Schema 定数マッピング
_COLUMN_MAP: dict[str, str] = {
    # 氏名・社名
    "会社名":               Schema.NAME,
    "企業名":               Schema.NAME,
    "名称":                 Schema.NAME,
    "特約店名":             Schema.NAME,
    "販売委託店名":         Schema.NAME,
    "特約店・販売委託店名": Schema.NAME,
    # よみがな
    "よみがな":         Schema.NAME_KANA,
    "フリガナ":         Schema.NAME_KANA,
    "カナ名":           Schema.NAME_KANA,
    # 住所
    "都道府県":         Schema.PREF,
    "所在地":           Schema.ADDR,
    "住所":             Schema.ADDR,
    "郵便番号":         Schema.POST_CODE,
    # 電話
    "電話番号":         Schema.TEL,
    "TEL":              Schema.TEL,
    # ウェブ
    "URL":              Schema.HP,
    "ホームページ":     Schema.HP,
    "HP":               Schema.HP,
    # NTT 固有カラム
    "契約形態":         Schema.CAT_SITE,
    "支店名・営業所名": Schema.FAC_NAME,
    "エリア":           Schema.CAT_LV1,
    # 解約・エリア別セクション固有（EXTRA_COLUMNS として保持）
    "販売委託店が契約している特約店名": COL_PARENT,
    "解約後の保守会社":               COL_AFTER_MAINT,
    "保守会社連絡先":                 COL_MAINT_TEL,
    "解約申請受理日":                 COL_CANCEL_DATE,
}

_META_KEYS  = frozenset({COL_STATUS, COL_UPDATE, COL_SECTION, COL_TAB, Schema.URL})
# pipeline が許可するカラム名のセット (Schema + EXTRA_COLUMNS)
_SCHEMA_SET = frozenset(Schema.COLUMNS)
_EXTRA_SET  = frozenset([
    COL_STATUS, COL_UPDATE, COL_SECTION, COL_TAB,
    COL_PARENT, COL_AFTER_MAINT, COL_MAINT_TEL, COL_CANCEL_DATE,
])
_ALLOWED    = _SCHEMA_SET | _EXTRA_SET

# セクション定義（ページ調査結果から確定）
SECTIONS = [
    {
        "name":     "特約店一覧",
        "update":   "2025年10月20日",
        "status":   "NTT西日本と情報機器特約店契約を締結している企業（特約店）の一覧です。",
        "endpoint": "/lists/tokuyakuten",
        "param":    "TokuyakutenParam",
        "tabs": [
            ("ア行", "a"), ("カ行", "k"), ("サ行", "s"), ("タ行", "t"),
            ("ナ行", "n"), ("ハ行", "h"), ("マ行", "m"), ("ヤ行", "y"),
            ("ラ行", "r"), ("ワ行", "w"),
        ],
    },
    {
        "name":     "販売委託店一覧",
        "update":   "2026年4月1日",
        "status":   "各NTT西日本情報機器特約店と個別に販売委託店契約を締結している企業（販売委託店）の一覧です。",
        "endpoint": "/lists/hanbaiten",
        "param":    "HanbaitenParam",
        "tabs": [
            ("ア行", "a"), ("カ行", "k"), ("サ行", "s"), ("タ行", "t"),
            ("ナ行", "n"), ("ハ行", "h"), ("マ行", "m"), ("ヤ行", "y"),
            ("ラ行", "r"), ("ワ行", "w"),
        ],
    },
    {
        "name":     "エリア別一覧",
        "update":   "2026年4月1日",
        "status":   "NTT西日本と情報機器特約店契約を締結している企業（特約店）と各NTT西日本情報機器特約店と個別に販売委託店契約を締結している企業（販売委託店）のエリア別一覧です。",
        "endpoint": "/lists/area",
        "param":    "AreaParam",
        "tabs": [
            ("北陸エリア", "北陸"), ("東海エリア", "東海"), ("関西エリア", "関西"),
            ("中国エリア", "中国"), ("四国エリア", "四国"), ("九州エリア", "九州"),
        ],
    },
    {
        "name":     "解約一覧",
        "update":   "2025年10月10日",
        "status":   "NTT西日本と契約を解消した特約店及び特約店と契約を解消した販売委託店一覧（現在ご利用中の機器の故障等の際の連絡先）",
        "endpoint": "/lists/kaiyaku",
        "param":    "KaiyakuParam",
        "tabs": [
            ("ア行", "a"), ("カ行", "k"), ("サ行", "s"), ("タ行", "t"),
            ("ナ行", "n"), ("ハ行", "h"), ("マ行", "m"), ("ヤ行", "y"),
            ("ラ行", "r"), ("ワ行", "w"),
        ],
    },
]


class NttWestHanbaitenScraper(StaticCrawler):
    """NTT西日本 特約店・販売委託店一覧 スクレイパー"""

    DELAY = 0.5
    CONTINUE_ON_ERROR = True
    EXTRA_COLUMNS = [
        COL_STATUS, COL_UPDATE, COL_SECTION, COL_TAB,
        COL_PARENT, COL_AFTER_MAINT, COL_MAINT_TEL, COL_CANCEL_DATE,
    ]

    def prepare(self):
        # 初期アクセスでセッション Cookie を確立してから POST する
        try:
            self.session.get(LIST_URL, timeout=30)
        except Exception as e:
            self.logger.warning("初期アクセス失敗（続行）: %s", e)
        self.session.headers.update({
            "Referer": LIST_URL,
            "Origin":  BASE_URL,
        })

    def parse(self, url: str) -> Generator[dict, None, None]:
        for section in SECTIONS:
            self.logger.info("セクション: %s", section["name"])
            for tab_name, tab_param in section["tabs"]:
                self.logger.info("  タブ: %s", tab_name)
                yield from self._scrape_tab(section, tab_name, tab_param)
                time.sleep(self.DELAY)

    # ------------------------------------------------------------------

    def _scrape_tab(self, section: dict, tab_name: str, tab_param: str) -> Generator[dict, None, None]:
        endpoint = BASE_URL + section["endpoint"]
        try:
            resp = self.session.post(
                endpoint,
                data={section["param"]: tab_param},
                timeout=30,
            )
            resp.raise_for_status()
        except Exception as e:
            self.logger.warning("POST失敗 (%s / %s): %s", section["name"], tab_name, e)
            return

        soup = bs4.BeautifulSoup(resp.content, "html.parser")
        meta = {
            COL_SECTION: section["name"],
            COL_STATUS:  section["status"],
            COL_UPDATE:  section["update"],
            COL_TAB:     tab_name,
            Schema.URL:  LIST_URL,
        }
        yield from self._parse_response(soup, meta, section["name"], tab_name)

    def _parse_response(
        self,
        soup: bs4.BeautifulSoup,
        meta: dict,
        section_name: str,
        tab_name: str,
    ) -> Generator[dict, None, None]:
        """
        レスポンス HTML からデータ行を抽出する。

        HTML は <tr> 閉じタグ欠落の不正形式のため、<tr> による行単位処理が機能しない。
        代わりに以下の class 属性でセルを区別する:
            head_1 : ヘッダー行のセル (カラム名)
            head_2 : 50音セクション区切り (スキップ)
            text_a / text_b : データセル (交互スタイル)
        """
        # ヘッダー行のカラム名を取得
        # エリア別・解約セクションではページ内にヘッダー行が繰り返し出現するため、
        # 最初のヘッダーセットのみを使用する（2回目以降の重複を打ち切る）
        headers: list[str] = []
        seen_headers: set[str] = set()
        for td in soup.find_all("td", class_="head_1"):
            text = td.get_text(strip=True)
            if text in seen_headers:
                break
            seen_headers.add(text)
            headers.append(text)
        n_cols = len(headers)

        if n_cols == 0:
            self.logger.debug("ヘッダーなし (%s / %s)", section_name, tab_name)
            return

        # データセルをすべて取得 (text_a / text_b のみ)
        data_cells = soup.find_all(
            "td",
            class_=lambda c: c in ("text_a", "text_b"),
        )

        # n_cols 個ずつグループ化して1行として処理
        for i in range(0, len(data_cells) - n_cols + 1, n_cols):
            chunk = data_cells[i : i + n_cols]
            if len(chunk) < n_cols:
                break

            cells = [td.get_text(strip=True) for td in chunk]
            row: dict = dict(meta)
            for j, cell in enumerate(cells):
                if not cell:
                    continue
                raw_col  = headers[j] if j < len(headers) else ""
                col_key  = _COLUMN_MAP.get(raw_col, raw_col)
                # 許可されていないカラムはスキップ（パイプラインのスキーマ検証を通過させる）
                if col_key in _ALLOWED:
                    row[col_key] = cell

            # メタ情報以外のデータが1件でもあれば yield
            if any(v for k, v in row.items() if k not in _META_KEYS):
                yield row


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    scraper = NttWestHanbaitenScraper()
    scraper.execute(LIST_URL)
    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
