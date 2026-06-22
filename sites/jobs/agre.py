"""
Agre — 沖縄の求人情報 (webagre.com)

取得対象:
    - 沖縄県の求人票（全件）。企業名・住所・電話・代表者等の企業情報と
      求人タイトル・雇用形態・勤務エリアを取得する。

取得フロー:
    一覧ページ (job/list?page=N) → 詳細ページ (/job/view/web/{id})
    企業情報は詳細ページの #company_name / #company_address 等から取得。
    求人タイトル・雇用形態・勤務エリアは一覧カードのバッジから取得。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/agre.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id agre
"""

import re
import sys
import urllib.parse
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_CODE_RE = re.compile(r"〒([\d-]+)")


class AgreCrawler(StaticCrawler):
    """Agre 沖縄求人スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["求人タイトル", "雇用形態", "勤務エリア"]

    def parse(self, url: str):
        list_url = urllib.parse.urljoin(url, "job/list")
        page = 1
        while True:
            soup = self.get_soup(f"{list_url}?page={page}")
            articles = soup.select("article")
            if not articles:
                break
            if page == 1:
                count_el = soup.select_one(".search-count")
                if count_el:
                    try:
                        self.total_items = int(
                            count_el.get_text(strip=True).replace(",", "")
                        )
                    except ValueError:
                        pass
            for art in articles:
                try:
                    link = art.select_one("h2 a")
                    if not link or not link.get("href"):
                        continue
                    detail_url = link["href"]
                    job_title = link.get_text(strip=True)
                    emp_type = ""
                    area = ""
                    for badge in art.select(".border.rounded"):
                        icon = badge.select_one("i")
                        label = badge.select_one(".col.text-truncate-1")
                        if icon and label:
                            icon_classes = " ".join(icon.get("class", []))
                            if "fa-address-card" in icon_classes:
                                emp_type = label.get_text(strip=True)
                            elif "fa-location-dot" in icon_classes:
                                area = label.get_text(strip=True)
                    item = self._scrape_detail(detail_url)
                    if item:
                        item["求人タイトル"] = job_title
                        item["雇用形態"] = emp_type
                        item["勤務エリア"] = area
                        yield item
                except Exception as e:
                    self.logger.warning(f"page {page}: article skip — {e}")
                    continue
            page += 1

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)

        def get_section(section_id: str) -> str:
            el = soup.select_one(f"#{section_id}")
            return el.get_text(" ", strip=True) if el else ""

        addr_raw = get_section("company_address")
        # 〒郵便番号 を取り出してから都道府県を抽出
        post_code = ""
        pc_m = _POST_CODE_RE.search(addr_raw)
        if pc_m:
            post_code = pc_m.group(1)
        addr_clean = _POST_CODE_RE.sub("", addr_raw).strip()
        pref = ""
        addr = addr_clean
        m = _PREF_PATTERN.match(addr_clean)
        if m:
            pref = m.group(1)
            addr = addr_clean[m.end():].strip()

        return {
            Schema.URL: url,
            Schema.NAME: get_section("company_name"),
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: get_section("main_phone_number"),
            Schema.REP_NM: get_section("representative"),
            Schema.EMP_NUM: get_section("number_of_employees"),
            Schema.CAP: get_section("capital"),
            Schema.HP: get_section("website_url"),
            Schema.OPEN_DATE: get_section("establishment_date"),
            Schema.CAT_SITE: get_section("industry"),
            Schema.TIME: get_section("working_hours"),
            Schema.HOLIDAY: get_section("day_off"),
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = AgreCrawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://webagre.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
