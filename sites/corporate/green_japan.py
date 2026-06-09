"""
グリーンジャパン (green-japan.com) — IT求人情報スクレイパー

取得対象:
    - 約 4,062 件の IT 系求人情報（204ページ）
    - 企業名・住所・都道府県・従業員数・資本金・業種・設立年月・
      勤務時間・休日・職種・年収・勤務地・雇用形態・待遇・キャッチコピー

注記:
    - 求人サイトのため同一企業が複数求人を掲載している場合は複数件取得される
    - 詳細ページの __NEXT_DATA__ JSON からデータを抽出する

取得フロー:
    /search?page=N (N=0〜) を巡回し、各求人カードから詳細 URL を収集
    /company/{id}/job/{id} の __NEXT_DATA__ から企業・求人情報を抽出する

実行方法:
    python scripts/sites/corporate/green_japan.py
    python bin/run_flow.py --site-id green_japan
"""

import json
import re
import sys
from datetime import datetime, timezone, timedelta

_JST = timezone(timedelta(hours=9))
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


BASE_URL = "https://www.green-japan.com"
LIST_URL = f"{BASE_URL}/search"

_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_JOB_HREF_RE = re.compile(r"^/company/\d+/job/\d+$")


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"[\s　\xa0\r\n]+", " ", str(text)).strip()


def _split_pref(addr: str) -> tuple[str, str]:
    """住所から (都道府県, 市区町村以降) を返す。"""
    addr = _clean(addr)
    pm = _PREF_RE.match(addr)
    if pm:
        return pm.group(1), addr[pm.end():].strip()
    return "", addr


def _ts_to_ym(ts) -> str:
    """Unix タイムスタンプ → 'YYYY-MM' 文字列（JST）。"""
    try:
        dt = datetime.fromtimestamp(int(ts), tz=_JST)
        return dt.strftime("%Y-%m")
    except Exception:
        return ""


class GreenJapanScraper(StaticCrawler):
    """グリーンジャパン IT求人スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["職種", "年収", "勤務地", "雇用形態", "待遇", "キャッチコピー"]

    def parse(self, url: str):
        self.total_items = 4060  # 204ページ × 約20件（初回推定値）
        page = 0
        while True:
            soup = self.get_soup(f"{LIST_URL}?page={page}")
            if soup is None:
                break

            hrefs = [
                a.get("href", "")
                for a in soup.select("a[href]")
                if _JOB_HREF_RE.match(a.get("href", ""))
            ]
            if not hrefs:
                break

            for href in hrefs:
                detail_url = f"{BASE_URL}{href}"
                try:
                    yield self._scrape_detail(detail_url)
                except Exception:
                    self.logger.exception("詳細取得失敗: %s", detail_url)

            page += 1

    def _scrape_detail(self, url: str) -> dict:
        soup = self.get_soup(url)
        if soup is None:
            return {Schema.URL: url}

        script = soup.find("script", {"id": "__NEXT_DATA__"})
        if script:
            try:
                data = json.loads(script.string)
                return self._parse_next_data(url, data)
            except Exception:
                self.logger.warning("__NEXT_DATA__ パース失敗: %s", url)

        return {Schema.URL: url}

    def _parse_next_data(self, url: str, data: dict) -> dict:
        pp = data.get("props", {}).get("pageProps", {})
        client = pp.get("client", {})
        job = pp.get("jobOffer", {})

        addr_raw = _clean(client.get("address", ""))
        pref, addr = _split_pref(addr_raw)

        # 業種: industryTypes の name をカンマ結合（最大3件）
        industry_names = [
            _clean(t.get("name", ""))
            for t in client.get("industryTypes", [])[:3]
            if t.get("name")
        ]
        cat_site = "、".join(industry_names)

        # 年収: minSalary〜maxSalary（単位は万円）
        min_sal = str(job.get("minSalary", "")).strip()
        max_sal = str(job.get("maxSalary", "")).strip()
        if min_sal and max_sal:
            salary = f"{min_sal}万円〜{max_sal}万円"
        elif min_sal:
            salary = f"{min_sal}万円〜"
        else:
            salary = ""

        # 勤務時間
        start = _clean(job.get("workStartingTime", ""))
        end = _clean(job.get("workEndingTime", ""))
        work_time = f"{start}〜{end}" if start and end else start or end

        return {
            Schema.URL: url,
            Schema.NAME: _clean(client.get("fullName", "")),
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.EMP_NUM: _clean(str(client.get("employees", ""))),
            Schema.CAP: _clean(client.get("capital", "")),
            Schema.CAT_SITE: cat_site,
            Schema.OPEN_DATE: _ts_to_ym(client.get("establishTimestamp", "")),
            Schema.TIME: work_time,
            Schema.HOLIDAY: _clean(job.get("holiday", "")),
            "職種": _clean(job.get("name", "")),
            "年収": salary,
            "勤務地": _clean(job.get("address", "")),
            "雇用形態": _clean(job.get("employmentStatusName", "")),
            "待遇": _clean(job.get("welfare", "")),
            "キャッチコピー": _clean(job.get("title", "")),
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = GreenJapanScraper()
    scraper.execute(LIST_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
