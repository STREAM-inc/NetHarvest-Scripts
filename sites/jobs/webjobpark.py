"""
ジョブこねっと — 京都お仕事マッチング診断 (webjobpark.kyoto.jp)

取得対象:
    - 京都ジョブパーク運営「ジョブこねっと」に掲載されている企業情報 (約18,740件)

取得フロー:
    1. POST /rest/kyoto-v1/com/get_list で企業IDを列挙 (50件/ページ)
    2. 各企業IDについて POST /rest/kyoto-v1/com/get で詳細情報を取得
    3. JSON レスポンスから Schema / EXTRA カラムへマッピング

実行方法:
    python scripts/sites/jobs/webjobpark.py
    python bin/run_flow.py --site-id webjobpark
"""

import json
import re
import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://webjobpark.kyoto.jp"
LIST_API = f"{BASE_URL}/rest/kyoto-v1/com/get_list"
DETAIL_API = f"{BASE_URL}/rest/kyoto-v1/com/get"
DETAIL_PAGE = f"{BASE_URL}/com/detail/?id={{id}}"
PAGE_SIZE = 50

_PREF_PATTERN = re.compile(
    r"^(北海道|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


def _clean(v) -> str:
    if v is None:
        return ""
    return re.sub(r"\s+", " ", str(v).replace("　", " ")).strip()


def _split_pref(addr: str) -> tuple[str, str]:
    addr = _clean(addr)
    if not addr:
        return "", ""
    m = _PREF_PATTERN.match(addr)
    if m:
        return m.group(1), addr[m.end():].strip()
    return "", addr


def _fmt_branches(branches) -> str:
    if not isinstance(branches, list):
        return ""
    parts = []
    for b in branches:
        if not isinstance(b, dict):
            continue
        name = _clean(b.get("office_name"))
        place = _clean(b.get("office_place"))
        tel = _clean(b.get("tel"))
        line = " ".join(x for x in [name, place, tel] if x)
        if line:
            parts.append(line)
    return " / ".join(parts)


class WebJobParkScraper(StaticCrawler):
    """ジョブこねっと (webjobpark.kyoto.jp) 企業情報スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "企業ID",
        "従業員数_男",
        "従業員数_女",
        "従業員規模",
        "資本金区分",
        "FAX",
        "福利厚生",
        "企業特徴",
        "育児休業取得実績",
        "介護休業取得実績",
        "看護休暇取得実績",
        "営業所数",
        "支店数",
        "工場数",
        "地方事務所数",
        "事業所一覧",
        "京都ジョブナビURL",
        "更新日時",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_ids: set[int] = set()
        page = 1

        while True:
            list_data = self._post_json(
                LIST_API,
                {"page_per_count": PAGE_SIZE, "current_page": page},
            )
            if not list_data:
                break
            results = list_data.get("result") or []
            if not results:
                break

            if page == 1:
                try:
                    self.total_items = int(list_data.get("total_count") or 0)
                    self.logger.info(
                        "総件数: %d 件 (総ページ数: %s)",
                        self.total_items,
                        list_data.get("total_page"),
                    )
                except (TypeError, ValueError):
                    pass

            for entry in results:
                com_id = entry.get("id")
                if not isinstance(com_id, int) or com_id in seen_ids:
                    continue
                seen_ids.add(com_id)

                try:
                    item = self._fetch_detail(com_id)
                except Exception as e:
                    self.logger.warning("詳細取得失敗 id=%s: %s", com_id, e)
                    continue
                if item:
                    yield item

            total_page = list_data.get("total_page") or 0
            try:
                if page >= int(total_page):
                    break
            except (TypeError, ValueError):
                break
            page += 1

    def _post_json(self, url: str, payload: dict) -> dict | None:
        try:
            resp = self.session.post(
                url,
                json=payload,
                timeout=self.TIMEOUT,
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
        except Exception as e:
            self.logger.warning("POST 失敗 %s payload=%s: %s", url, payload, e)
            return None
        try:
            return resp.json()
        except json.JSONDecodeError as e:
            self.logger.warning("JSON パース失敗 %s: %s", url, e)
            return None

    def _fetch_detail(self, com_id: int) -> dict | None:
        data = self._post_json(DETAIL_API, {"id": com_id})
        if not data:
            return None
        result = data.get("result") or []
        if not result:
            return None
        return self._map(result[0])

    def _map(self, d: dict) -> dict:
        addr_raw = _clean(d.get("head_office_place"))
        pref, addr = _split_pref(addr_raw)

        capital = _clean(d.get("capital_label")) or _clean(d.get("capital_value"))

        industry = d.get("industry_type") or []
        cat_site = " / ".join(_clean(x) for x in industry if _clean(x))

        com_id = d.get("id") or ""

        return {
            Schema.NAME: _clean(d.get("company_name")),
            Schema.NAME_KANA: _clean(d.get("company_name_kana")),
            Schema.URL: DETAIL_PAGE.format(id=com_id),
            Schema.PREF: pref,
            Schema.POST_CODE: _clean(d.get("head_office_zip")),
            Schema.ADDR: addr,
            Schema.TEL: _clean(d.get("head_office_tel")),
            Schema.REP_NM: _clean(d.get("representative_name")),
            Schema.POS_NM: _clean(d.get("representative_pos")),
            Schema.EMP_NUM: _clean(d.get("employees_num_sum")),
            Schema.LOB: _clean(d.get("biz_contents")),
            Schema.CAP: capital,
            Schema.CAT_SITE: cat_site,
            Schema.HP: _clean(d.get("url")),
            Schema.OPEN_DATE: _clean(d.get("establish_date")),
            Schema.SALES: _clean(d.get("sales")),
            "企業ID": com_id,
            "従業員数_男": _clean(d.get("employees_num_male")),
            "従業員数_女": _clean(d.get("employees_num_female")),
            "従業員規模": _clean(d.get("employees_num_label")),
            "資本金区分": _clean(d.get("capital_label")),
            "FAX": _clean(d.get("head_office_fax")),
            "福利厚生": _clean(d.get("welfare")),
            "企業特徴": _clean(d.get("feature")),
            "育児休業取得実績": _clean(d.get("childcare_leave")),
            "介護休業取得実績": _clean(d.get("nurse_leave")),
            "看護休暇取得実績": _clean(d.get("nursingcare_leave")),
            "営業所数": _clean(d.get("sales_office_num")),
            "支店数": _clean(d.get("branch_office_num")),
            "工場数": _clean(d.get("factory_num")),
            "地方事務所数": _clean(d.get("regional_office_num")),
            "事業所一覧": _fmt_branches(d.get("branch_office")),
            "京都ジョブナビURL": _clean(d.get("jobnavi_url")),
            "更新日時": _clean(d.get("update_date")),
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = WebJobParkScraper()
    scraper.execute(BASE_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
