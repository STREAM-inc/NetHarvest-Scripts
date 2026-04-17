"""
求人アルゾ — 茨城県最大級の求人情報サイト

取得対象:
    - 全求人の企業情報・求人詳細（5,068件）

取得フロー:
    一覧ページ (/search → /search?page=N) を巡回し、
    各求人の詳細ページ (/detail/{id}) から全フィールドを取得する

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/alzo.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id alzo
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

BASE_URL = "https://www.alzoweb.jp"
LIST_URL = f"{BASE_URL}/search"

_PREF_PATTERN = re.compile(
    r"^(北海道|(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|新潟|富山|"
    r"石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|和歌山|鳥取|島根|岡山|広島|山口|"
    r"徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県|東京都|(?:大阪|京都)府)"
)


class AlzoScraper(StaticCrawler):
    """求人アルゾ スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "求人タイトル", "雇用形態", "給与", "勤務地エリア", "勤務地住所",
        "勤務時間", "休日・休暇", "待遇", "仕事内容", "対象となる方",
        "メリットタグ", "応募締め切り",
    ]

    def parse(self, url: str):
        page = 1
        while True:
            page_url = LIST_URL if page == 1 else f"{LIST_URL}?page={page - 1}"
            soup = self.get_soup(page_url)
            if soup is None:
                break

            if page == 1:
                count_el = soup.select_one("#count_box strong")
                if count_el:
                    self.total_items = int(count_el.get_text(strip=True).replace(",", ""))

            items = soup.select(".kyujin_list2018")
            if not items:
                break

            for item in items:
                try:
                    detail_a = item.select_one("h2 a[href]")
                    if not detail_a:
                        continue
                    detail_url = urljoin(BASE_URL, detail_a["href"])
                    result = self._scrape_detail(detail_url)
                    if result:
                        yield result
                except Exception as e:
                    self.logger.warning("スキップ: %s", e)

            # 次ページ確認
            next_link = soup.select_one(".paging a:-soup-contains('次へ')")
            if not next_link:
                break
            page += 1

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        # table.bosyu の th→td 全ペアを辞書化
        fields: dict[str, str] = {}
        for tr in soup.select("table.bosyu tr"):
            th = tr.select_one("th")
            td = tr.select_one("td")
            if th and td:
                key = th.get_text(strip=True)
                fields[key] = td.get_text(" ", strip=True)

        # HP は <a> タグから取得
        hp = ""
        hp_td = None
        for tr in soup.select("table.bosyu tr"):
            th = tr.select_one("th")
            if th and "ホームページ" in th.get_text():
                hp_td = tr.select_one("td a[href]")
                if hp_td:
                    hp = hp_td["href"]
                break

        # 所在地から都道府県を抽出
        address_raw = fields.get("所在地", "")
        pref, addr_short = "", address_raw
        m = _PREF_PATTERN.match(address_raw)
        if m:
            pref = m.group(1)
            addr_short = address_raw[m.end():].strip()

        # 勤務地エリア
        work_area = ""
        work_addr = ""
        for tr in soup.select("table.bosyu tr"):
            th = tr.select_one("th")
            if th and "勤務地" in th.get_text():
                td = tr.select_one("td")
                if td:
                    area_a = td.select_one('a[href*="/search/area/"]')
                    if area_a:
                        work_area = area_a.get_text(strip=True)
                    # 住所はbrタグ以降のテキスト
                    for br in td.find_all("br"):
                        next_sib = br.next_sibling
                        if next_sib and isinstance(next_sib, str):
                            candidate = next_sib.strip()
                            if candidate and "地図" not in candidate:
                                work_addr = candidate
                                break
                break

        # メリットタグ
        merits = [li.get_text(strip=True) for li in soup.select(".melitBox li")]

        # 雇用形態
        emp_type = ""
        emp_el = soup.select_one(".dtl_Box_ttl span")
        if emp_el:
            emp_text = emp_el.get_text(strip=True)
            emp_type = re.sub(r"New$", "", emp_text).strip()

        # 求人タイトル
        title = ""
        ttl_ps = soup.select(".dtl_Box_ttl p")
        if len(ttl_ps) >= 3:
            title = ttl_ps[2].get_text(strip=True)

        return {
            Schema.URL: url,
            Schema.NAME: fields.get("企業名", ""),
            Schema.PREF: pref,
            Schema.ADDR: addr_short,
            Schema.HP: hp,
            Schema.REP_NM: fields.get("代表者", ""),
            Schema.CAP: fields.get("資本金", ""),
            Schema.EMP_NUM: fields.get("従業員数", ""),
            Schema.LOB: fields.get("企業概要", ""),
            "求人タイトル": title,
            "雇用形態": emp_type,
            "給与": fields.get("給与", ""),
            "勤務地エリア": work_area,
            "勤務地住所": work_addr,
            "勤務時間": fields.get("勤務時間", ""),
            "休日・休暇": fields.get("休日・休暇", ""),
            "待遇": fields.get("待遇", ""),
            "仕事内容": fields.get("仕事内容", ""),
            "対象となる方": fields.get("対象となる方", ""),
            "メリットタグ": "、".join(merits),
            "応募締め切り": fields.get("応募締め切り", ""),
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = AlzoScraper()
    scraper.execute("https://www.alzoweb.jp/search")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
