"""
ダイヤモンド就活ナビ (shukatsu.jp) — 掲載企業の会社概要クローラー

取得対象:
    - ダイヤモンド就活ナビ (対象年度) に掲載されている全企業の会社概要

取得フロー:
    1. 一覧ページ (引数 url = https://www.shukatsu.jp/{year}/company/list) の
       year から contract_term_id (= year - 2020) を導出する。
    2. 公開 JSON API `GET https://admin.shukatsu.jp/api/v1/companies` を
       per_page=50 でページングして企業レコードを取得する
       (この API が一覧ページの企業リストを AJAX 描画している実体)。
    3. 1 社取得するごとに即 yield する (Pattern B / 早期 yield)。
       各社の Schema.URL は引数 url から派生した詳細ページ
       (`{base}/detail?id={company_id}`) を設定する。

備考対応:
    フィルター指示は無いため全件取得。API から取得できる構造化フィールドを
    網羅的に取得する。会社紹介文 (business_description) は長文の自由記述の
    ため著作権リスクを避けて除外。company_links (イベント/PR リンクの見出し)
    も同様に除外。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/shukatsu.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id shukatsu
"""

import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 一覧ページを AJAX 描画している公開 API のバックエンド (サイト固有の定数)。
_API_BASE = "https://admin.shukatsu.jp/api/v1"
_PER_PAGE = 50
_MAX_PAGE_ATTEMPTS = 3


def _s(v) -> str:
    """API 値を安全な文字列へ。None / "null" / 空白のみは "" に正規化。"""
    if v is None:
        return ""
    text = str(v).strip()
    if text in ("", "None", "null", "NULL"):
        return ""
    return text


def _year_from_url(url: str) -> int:
    """一覧ページ URL のパスから年度 (例: 2028) を取り出す。"""
    m = re.search(r"/(\d{4})(?:/|$)", url)
    if not m:
        raise ValueError(f"URL から年度を特定できません: {url}")
    return int(m.group(1))


def _fmt_amount(trillion, billion, base) -> str:
    """資本金・売上高を CL3200.js と同じロジックで整形する。

    trillion兆 + billion億 + base(万円) を連結。base が 0/未設定なら末尾を "円" に。
    """
    s = ""
    if trillion:
        s = f"{trillion}兆"
    if billion:
        s = f"{s}{billion}億"
    try:
        base_int = int(base)
    except (TypeError, ValueError):
        base_int = 0
    if base_int > 0:
        s = f"{s}{base_int:,}万円"
    elif s != "":
        s = f"{s}円"
    return s


def _postcode(raw) -> str:
    """7 桁郵便番号を NNN-NNNN に整形。それ以外はそのまま返す。"""
    digits = re.sub(r"\D", "", _s(raw))
    if len(digits) == 7:
        return f"{digits[:3]}-{digits[3:]}"
    return _s(raw)


def _address(rec, idx: int) -> str:
    """addressN_* から都道府県を除いた住所 (市区町村+丁目+番地) を組み立てる。"""
    parts = [
        _s(rec.get(f"address{idx}_city_id")),
        _s(rec.get(f"address{idx}_line1")),
        _s(rec.get(f"address{idx}_line2")),
    ]
    return "".join(p for p in parts if p)


class ShukatsuScraper(StaticCrawler):
    """ダイヤモンド就活ナビ (shukatsu.jp) 掲載企業 会社概要スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "企業ID",
        "売上高",
        "売上高年度",
        "従業員数調査年",
        "第2本社所在地",
        "ロゴURL",
        "インターンシップ有無",
        "採用ガイド有無",
        "ライブイベント有無",
    ]

    def prepare(self):
        # SPA と同じ AJAX ヘッダを付与しておく
        self.session.headers.update(
            {
                "Accept": "application/json",
                "X-Requested-With": "XMLHttpRequest",
            }
        )

    def _fetch_page(self, contract_term_id: int, page: int) -> dict | None:
        """companies API を 1 ページ取得。最大 _MAX_PAGE_ATTEMPTS 回リトライ。

        self.session.get をそのまま使うため、テストランナーのソフトタイムアウトで
        中断可能。全試行失敗時は None を返す (呼び出し側でページ送りを打ち切る)。
        """
        params = {
            "contract_term_id": contract_term_id,
            "partner_id": 0,
            "per_page": _PER_PAGE,
            "page": page,
            "from_top": "true",
        }
        api_url = f"{_API_BASE}/companies"
        for attempt in range(_MAX_PAGE_ATTEMPTS):
            try:
                resp = self.session.get(api_url, params=params, timeout=self.TIMEOUT)
                resp.raise_for_status()
                payload = resp.json()
                if isinstance(payload, dict):
                    return payload
                self.logger.warning("API 応答が dict でない (page=%s)", page)
            except Exception as e:  # noqa: BLE001 — リトライで復帰を試みる
                self.logger.warning(
                    "companies API 取得失敗 page=%s attempt=%s/%s — %s",
                    page,
                    attempt + 1,
                    _MAX_PAGE_ATTEMPTS,
                    e,
                )
                time.sleep(min(2 ** attempt, 8))
        return None

    def _build_item(self, rec: dict, base_detail: str) -> dict:
        company_id = _s(rec.get("company_id"))
        detail_url = f"{base_detail}/detail?id={company_id}" if company_id else base_detail

        establishment = _s(rec.get("establishment_year"))
        capital = _fmt_amount(
            rec.get("capital_stock_trillion"),
            rec.get("capital_stock_billion"),
            rec.get("capital_stock"),
        )
        sales = _fmt_amount(
            rec.get("sales_trillion"),
            rec.get("sales_billion"),
            rec.get("sales"),
        )

        addr2 = _address(rec, 2)
        pref2 = _s(rec.get("address2_prefecture_name"))
        address2_full = f"{pref2}{addr2}".strip()

        def _yn(v) -> str:
            return "有" if v in (True, 1, "1", "true", "True") else "無"

        return {
            Schema.NAME: _s(rec.get("company_name")),
            Schema.NAME_KANA: _s(rec.get("company_name_kana")),
            Schema.PREF: _s(rec.get("address1_prefecture_name")),
            Schema.POST_CODE: _postcode(rec.get("address1_postcode")),
            Schema.ADDR: _address(rec, 1),
            Schema.REP_NM: _s(rec.get("representative_name")),
            Schema.OPEN_DATE: establishment,
            Schema.CAP: capital,
            Schema.EMP_NUM: _s(rec.get("employees_num")),
            Schema.CAT_LV1: _s(rec.get("industry_type_main")),
            Schema.CAT_LV2: _s(rec.get("industry_type_sub")),
            Schema.URL: detail_url,
            # EXTRA
            "企業ID": company_id,
            "売上高": sales,
            "売上高年度": _s(rec.get("sales_year")),
            "従業員数調査年": _s(rec.get("employees_num_year")),
            "第2本社所在地": address2_full,
            "ロゴURL": _s(rec.get("company_logo_url")),
            "インターンシップ有無": _yn(rec.get("have_internship")),
            "採用ガイド有無": _yn(rec.get("have_recruit_guide")),
            "ライブイベント有無": _yn(rec.get("have_live_event")),
        }

    def parse(self, url: str):
        contract_term_id = _year_from_url(url) - 2020
        # 詳細ページ URL の基点 (.../{year}/company)。引数 url から派生させる。
        base_detail = url.rsplit("/", 1)[0]

        page = 1
        total_pages = None
        while True:
            payload = self._fetch_page(contract_term_id, page)
            if payload is None:
                if page == 1:
                    raise RuntimeError(
                        "companies API の初回ページ取得に失敗しました "
                        f"(contract_term_id={contract_term_id})"
                    )
                self.logger.warning("page=%s 取得失敗のためページ送りを打ち切ります", page)
                break

            records = payload.get("data") or []
            if not records:
                break

            if page == 1:
                pagination = payload.get("pagination") or {}
                total = pagination.get("total")
                total_pages = pagination.get("totalPages")
                if isinstance(total, int):
                    self.total_items = total

            for rec in records:
                if not isinstance(rec, dict):
                    continue
                try:
                    item = self._build_item(rec, base_detail)
                except Exception as e:  # noqa: BLE001 — 個別レコードのエラーはスキップ
                    self.logger.warning("レコード整形失敗 (スキップ): %s", e)
                    continue
                if item[Schema.NAME]:
                    yield item

            if total_pages is not None and page >= total_pages:
                break
            page += 1


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = ShukatsuScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.shukatsu.jp/2028/company/list")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
