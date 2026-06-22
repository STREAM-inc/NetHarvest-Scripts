"""
アクセス就活2027 — 新卒就職活動応援サイト掲載企業スクレイパー

取得対象:
    - 企業一覧 (1,065 件) の基本情報
    - 業種 / 設立 / 資本金 / 売上高 / 従業員数 / 事業所 / 支社事業部工場 / グループ名

取得フロー:
    1. GET /f{year}/company/listfirst.json  → 最初の 10 件 + セッション Cookie 確立
    2. GET /f{year}/company/listnext.json   → 次の 10 件（Cookie で継続）
    3. response が空になるまで繰り返す

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/ac_lab.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ac_lab
"""

import sys
from pathlib import Path
from urllib.parse import urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class AcLabScraper(StaticCrawler):
    """アクセス就活2027 スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "売上高",
        "従業員数規模",
        "勤務地",
        "事業所",
        "支社事業部工場",
        "グループ名",
        "採用予定数",
    ]

    def parse(self, url: str):
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        # URL パスの 1 段目が年度: /2027/front/top/top.html -> "2027"
        year = parsed.path.lstrip("/").split("/")[0]

        api_base = f"{base}/f{year}/company"
        detail_base = f"{base}/{year}/front/company/details.html"
        referer = url

        resp = self.session.get(
            f"{api_base}/listfirst.json",
            headers={"Referer": referer},
            timeout=self.TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()

        items = data.get("response", [])
        if items:
            self.total_items = int(items[0].get("count", 0))

        while items:
            for item in items:
                cid = item.get("company_id", "")
                capital_hidden = item.get("capital_hidden", "0")
                sales_hidden = item.get("sales_hidden", "0")

                yield {
                    Schema.NAME: item.get("company_name") or "",
                    Schema.NAME_KANA: item.get("company_kana") or "",
                    Schema.PREF: item.get("hq_pref_name") or "",
                    Schema.CAT_SITE: item.get("industry_name") or "",
                    Schema.EMP_NUM: item.get("employee") or "",
                    Schema.CAP: item.get("capital") or "" if capital_hidden != "1" else "",
                    Schema.OPEN_DATE: item.get("establishment") or "",
                    Schema.HOLIDAY: item.get("holiday") or "",
                    Schema.URL: f"{detail_base}?cid={cid}",
                    "売上高": item.get("sales") or "" if sales_hidden != "1" else "",
                    "従業員数規模": item.get("employee_size_name") or "",
                    "勤務地": item.get("search_working_place") or "",
                    "事業所": item.get("business_place") or "",
                    "支社事業部工場": item.get("branch") or "",
                    "グループ名": item.get("group_name") or "",
                    "採用予定数": item.get("recruit_num_plan_search_name") or "",
                }

            resp = self.session.get(
                f"{api_base}/listnext.json",
                headers={"Referer": referer},
                timeout=self.TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            items = data.get("response", [])


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = AcLabScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.ac-lab.jp/2027/front/top/top.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
