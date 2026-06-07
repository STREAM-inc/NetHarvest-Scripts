"""
金融商品取引業者等一覧 (金融事業者一括検索 / 金融庁) — 免許・許可・登録等を受けた金融事業者の公表情報

運営: 金融庁 (search.fsa.go.jp / 「金融事業者一括検索」)
一覧URL: https://search.fsa.go.jp/

取得対象:
    - 金融庁から免許・許可・登録・届出等を受けている全国の金融事業者 (約 15,690 件)
    - 商号・名称 / 法人番号 / 本店等所在地 / 代表等電話番号 / ホームページ /
      業種 (金融庁の業種分類) / 登録等番号 / 登録等年月日 / 法人個人の別 /
      行政処分等の状況 等の構造化情報

サイト構造:
    - フロントは React SPA (<div id="root">)。データは下記 JSON API を POST で叩いて取得する。
      Playwright は不要 (StaticCrawler + self.session.post で完結する)。
    - API ベース: https://search.fsa.go.jp/api
        1. POST /industries
             body {"industry_1":"","industry_2":"","industry_3":""}
             → 業種マスタ (industry_bit -> industry_name, 全98業種)。
        2. POST /financial-businesses   ← 一覧 (ページング)
             body {検索条件…, "page":"N", "limit":"50", "context_name":"results"}
             → {"results":[{finance_name, corporate_number, head_office_address}], "total": 15690}
             空の検索条件で全件をページ送りできる。1ページ50件、page を増やすと最後は results=[]。
        3. POST /financial-business-details   ← 詳細 (1件ずつ)
             body {"finance_name":..., "head_office_address":..., "context_name":"results"}
             → {"results":[{…213カラム…}]}
             業種ごとにスロット化された住所/電話/登録番号等 (xxx_1..xxx_61) と、
             industry_summary (150桁のビット列。立っている桁 = industry_bit) を持つ。

取得フロー (一覧→詳細 / Pattern B = 1件取得ごとに即 yield):
    1. prepare() で業種マスタを取得し industry_bit -> 業種名 の辞書を構築。
    2. /financial-businesses を page=1,2,... と 50件ずつ巡回。
    3. 各レコードについて finance_name + head_office_address をキーに /financial-business-details
       を引き、業種・電話・HP・登録番号等を統合して即 yield する。

設計メモ:
    - 詳細の住所/電話/登録番号は業種スロット (_1.._61) に分散して格納されるため、スロット番号と
      industry_bit は一致しない。そのため「スロットの対応付け」はせず、非NULL値を全スロット横断で
      収集・連結して 1事業者 = 1行 に統合する。
    - 業種は industry_summary (150桁ビット列) の立っている桁位置 (1始まり) を industry_bit として
      業種マスタ名に変換し "/" 連結する。
    - "―" は「該当なし」を表すプレースホルダなので空文字に正規化する。
    - 詳細ページには直リンク可能な URL が無い (sessionStorage 経由遷移) ため、Schema.URL には
      検索サイトのトップ URL を入れる。
    - 著作権リスク回避のため、自由記述カラム (remarks_* 備考 / annotation_* 注意事項 /
      industry_notes_* 業種注記 / cryptocurrency_handled 取扱暗号資産の長大リスト 等) は取得しない。
      取得するのは番号・日付・短い構造化ラベルのみ。

実行方法:
    # ローカルテスト
    python scripts/sites/government/search.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id search
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

API_BASE = "https://search.fsa.go.jp/api"
SITE_URL = "https://search.fsa.go.jp/"
LIST_URL = f"{API_BASE}/financial-businesses"
DETAIL_URL = f"{API_BASE}/financial-business-details"
INDUSTRIES_URL = f"{API_BASE}/industries"

LIMIT = 50  # 1ページあたり件数 (read timeout 回避のため小さめに固定)

# 空の検索条件 = 全件対象
_BASE_QUERY = {
    "finance_name": "",
    "finance_name_mode": "partial",
    "industry_1": "",
    "industry_2": "",
    "industry_3": "",
    "registration_number": "",
    "head_office_address": "",
    "head_office_address_mode": "partial",
    "main_phone_number": "",
    "event_number": "1",
    "context_name": "results",
}

# 都道府県抽出
_PREF_RE = re.compile(
    r"^\s*(北海道|東京都|京都府|大阪府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|新潟|富山|"
    r"石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|和歌山|鳥取|島根|岡山|"
    r"広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県)"
)

# 詳細の番号系カラム (登録番号・許可番号・届出番号 等)。スロット番号順に連結する。
_NUM_PREFIXES = ("registration_number_", "license_number_")
# 詳細の日付系カラム
_DATE_PREFIXES = ("registration_date_", "date_permission_")
_SLOT_RE = re.compile(r"_(\d+)$")


def _clean(s) -> str:
    """改行・全角空白を含む値を 1行の文字列に正規化する。"――" 等のプレースホルダは空に。"""
    if s is None:
        return ""
    t = re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()
    if t and set(t) <= set("―ー-"):  # "―" 等のみ = 該当なし
        return ""
    return t


class SearchScraper(StaticCrawler):
    """金融商品取引業者等一覧 (金融庁・金融事業者一括検索) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "登録等番号",        # 例: 関東財務局長(金商)第142号 / 関東財務局長（代信）第1号 (複数を " / " 連結)
        "登録等年月日",      # 例: 2007-09-30 / 2005-03-10 (複数を " / " 連結)
        "法人個人の別",      # 例: 法人 / 個人
        "行政処分等の状況",  # 例: (通常は空。短いラベルのみ)
        "金融商品取引業者",  # 例: 第一種金融商品取引業者 等の区分ラベル
    ]

    def prepare(self):
        """業種マスタ (industry_bit -> 業種名) を取得する。"""
        self._bit2name: dict[int, str] = {}
        try:
            resp = self.session.post(
                INDUSTRIES_URL,
                json={"industry_1": "", "industry_2": "", "industry_3": ""},
                timeout=self.TIMEOUT,
            )
            resp.raise_for_status()
            for row in resp.json().get("results", []):
                bit = row.get("industry_bit")
                name = row.get("industry_name")
                if bit and name:
                    self._bit2name[int(bit)] = name
            self.logger.info("業種マスタ: %d 件", len(self._bit2name))
        except Exception as e:
            self.logger.warning("業種マスタ取得失敗 (業種カラムは空になります): %s", e)

    def parse(self, url: str) -> Generator[dict, None, None]:
        page = 1
        while True:
            data = self._fetch_list(page)
            if data is None:
                break
            if page == 1:
                self.total_items = int(data.get("total") or 0) or None
            results = data.get("results") or []
            if not results:
                break

            for rec in results:
                name = _clean(rec.get("finance_name"))
                if not name:
                    continue
                # 詳細キーには一覧の生の住所 (空白パディング込み) をそのまま渡す
                raw_addr = rec.get("head_office_address") or ""
                try:
                    item = self._build_item(rec, name, raw_addr)
                except Exception as e:
                    self.logger.warning("レコード解析失敗 name=%s: %s", name, e)
                    continue
                if item:
                    yield item

            page += 1
            time.sleep(self.DELAY)

    # ------------------------------------------------------------------
    # 一覧 / 詳細の取得
    # ------------------------------------------------------------------

    def _fetch_list(self, page: int):
        body = dict(_BASE_QUERY, page=str(page), limit=str(LIMIT))
        try:
            resp = self.session.post(LIST_URL, json=body, timeout=self.TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            self.logger.warning("一覧取得失敗 page=%s: %s", page, e)
            return None

    def _fetch_detail(self, finance_name: str, raw_addr: str) -> list[dict]:
        body = {
            "finance_name": finance_name,
            "head_office_address": raw_addr,
            "context_name": "results",
        }
        try:
            resp = self.session.post(DETAIL_URL, json=body, timeout=self.TIMEOUT)
            resp.raise_for_status()
            return resp.json().get("results") or []
        except Exception as e:
            self.logger.warning("詳細取得失敗 name=%s: %s", finance_name, e)
            return []

    # ------------------------------------------------------------------
    # 1事業者 = 1行 への統合
    # ------------------------------------------------------------------

    def _build_item(self, rec: dict, name: str, raw_addr: str) -> dict:
        details = self._fetch_detail(name, raw_addr)

        # 住所は一覧の値を正規化して使用 (常に存在する)。詳細スロットも同値。
        address = _clean(raw_addr)
        co_num = _clean(rec.get("corporate_number"))

        industries: list[str] = []
        tel = ""
        hp = ""
        corp_indiv = ""
        admin = ""
        fibo = ""
        numbers: list[str] = []
        dates: list[str] = []

        for d in details:
            # 業種: industry_summary (150桁) の立っている桁位置 = industry_bit
            summary = d.get("industry_summary") or ""
            for i, ch in enumerate(summary):
                if ch == "1":
                    nm = self._bit2name.get(i + 1)
                    if nm and nm not in industries:
                        industries.append(nm)

            # 住所/電話以外の単一カラム
            corp_indiv = corp_indiv or _clean(d.get("corporation_or_individual"))
            hp = hp or _clean(d.get("home_page_address"))
            admin = admin or _clean(d.get("administrative_disposition_status"))
            fibo = fibo or _clean(d.get("financial_instruments_business_operator"))

            # スロット化されたカラムを横断収集
            for key, val in d.items():
                cv = _clean(val)
                if not cv:
                    continue
                if not tel and key.startswith("main_phone_number_"):
                    tel = cv
                elif key.startswith(_NUM_PREFIXES) or key == "notification_number":
                    if cv not in numbers:
                        numbers.append(cv)
                elif key.startswith(_DATE_PREFIXES) or key in (
                    "approval_date",
                    "notification_date",
                    "specified_date",
                ):
                    dv = cv.split(" ")[0]  # "2007-09-30 00:00:00" -> "2007-09-30"
                    if dv and dv not in dates:
                        dates.append(dv)

        item = {
            Schema.NAME: name,
            Schema.URL: SITE_URL,
            Schema.CO_NUM: co_num,
            Schema.ADDR: address,
            Schema.TEL: tel,
            Schema.HP: hp,
            Schema.CAT_SITE: " / ".join(industries),
            "登録等番号": " / ".join(numbers),
            "登録等年月日": " / ".join(dates),
            "法人個人の別": corp_indiv,
            "行政処分等の状況": admin,
            "金融商品取引業者": fibo,
        }

        # 都道府県を住所先頭から分離
        m = _PREF_RE.match(address)
        if m:
            item[Schema.PREF] = m.group(1)
            item[Schema.ADDR] = address[m.end():].strip()

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = SearchScraper()
    scraper.execute(SITE_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
