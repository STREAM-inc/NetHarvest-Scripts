"""
エミダス(emidas / NC Network) — 製造業工場データベース

取得対象:
    - ja.nc-net.or.jp に登録されている全製造業企業（約23,608社、日本・海外含む）

取得フロー:
    /search/search/?pno=N を全ページ巡回 → 各企業詳細ページ /company/{id}/ を取得

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/emidas_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id emidas_2
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# onclick="redirect_company_hp('https://example.com')" などを想定
_HP_RE = re.compile(r"redirect_company_hp\s*\(\s*['\"]([^'\"]+)['\"]")


class Emidas2(StaticCrawler):
    """エミダス(NC Network) 製造業企業スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["fax", "industry", "annual_sales", "main_products"]

    def parse(self, url: str):
        # url = https://ja.nc-net.or.jp/?cntry=999 を起点に検索エンドポイントを派生
        search_base = urljoin(url, "/search/search/")
        page = 1
        self.total_items = None

        while True:
            page_url = f"{search_base}?pno={page}"
            soup = self.get_soup(page_url)

            # 初回ページで総件数を取得
            if self.total_items is None:
                total_el = soup.select_one("div.subject-display em.em-02")
                if total_el:
                    m = re.search(r"[\d,]+", total_el.get_text())
                    if m:
                        self.total_items = int(m.group().replace(",", ""))

            # 企業リンクを取得
            company_links = soup.select("h2.ttl-h3-03 a[href^='/company/']")
            if not company_links:
                break

            for link in company_links:
                detail_url = urljoin(url, link["href"])
                try:
                    item = self._scrape_detail(detail_url)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.warning(f"詳細取得エラー {detail_url}: {e}")
                    continue

            page += 1

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        # 企業名 (h1 優先、なければページタイトルから)
        name_el = soup.select_one("h1")
        name = name_el.get_text(strip=True) if name_el else ""
        if not name:
            return None

        data = {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: "",
            Schema.ADDR: "",
            Schema.TEL: "",
            Schema.EMP_NUM: "",
            Schema.CAP: "",
            Schema.REP_NM: "",
            Schema.OPEN_DATE: "",
            Schema.HP: "",
            "fax": "",
            "industry": "",
            "annual_sales": "",
            "main_products": "",
        }

        # テーブルの th/td ペアからフィールドを抽出
        for tr in soup.select("table tr"):
            th = tr.select_one("th")
            td = tr.select_one("td")
            if not th or not td:
                continue
            key = th.get_text(strip=True)
            val = td.get_text(separator=" ", strip=True)

            if re.search(r"電話|TEL", key, re.I):
                if not data[Schema.TEL]:
                    data[Schema.TEL] = val
            elif "FAX" in key:
                data["fax"] = val
            elif re.search(r"住所|所在地", key):
                m = _PREF_RE.match(val)
                if m:
                    data[Schema.PREF] = m.group(1)
                    data[Schema.ADDR] = val[m.end():].strip()
                else:
                    data[Schema.ADDR] = val
            elif re.search(r"資本金", key):
                data[Schema.CAP] = val
            elif re.search(r"社員|従業員", key):
                data[Schema.EMP_NUM] = val
            elif re.search(r"代表", key):
                data[Schema.REP_NM] = val
            elif re.search(r"設立|創業", key):
                data[Schema.OPEN_DATE] = val
            elif re.search(r"産業分類|業種", key):
                data["industry"] = val
            elif re.search(r"売上", key):
                data["annual_sales"] = val
            elif re.search(r"主要.*品目|品目", key):
                data["main_products"] = val

        # HP URL (onclick="redirect_company_hp(...)" 形式)
        hp_el = soup.select_one("a[onclick*='redirect_company_hp']")
        if hp_el:
            m = _HP_RE.search(hp_el.get("onclick", ""))
            data[Schema.HP] = m.group(1) if m else hp_el.get("href", "")

        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = Emidas2()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://ja.nc-net.or.jp/?cntry=999")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
