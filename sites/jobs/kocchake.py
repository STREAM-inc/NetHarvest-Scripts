"""
Kocchake! (こっちゃけ) — 秋田県就活情報サイト

取得対象:
    - 秋田県内で採用活動を行う企業情報（約1,034件）

取得フロー:
    採用情報一覧 (/pages/recruit-list?p=N) を52ページ巡回し、
    各企業の詳細ページ (/pages/company/c_XXXX) から全フィールドを取得する

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/kocchake.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id kocchake
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

BASE_URL = "https://kocchake.com"
LIST_URL = f"{BASE_URL}/pages/recruit-list"

_PREF_PATTERN = re.compile(
    r"^(北海道|(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|新潟|富山|"
    r"石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|和歌山|鳥取|島根|岡山|広島|山口|"
    r"徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県|東京都|(?:大阪|京都)府)"
)
_POST_CODE_PATTERN = re.compile(r"〒?\s*(\d{3}[-\s]?\d{4})")


class KocchakeScraper(StaticCrawler):
    """Kocchake! (秋田県就活情報サイト) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "FAX番号",
        "売上高",
        "事業所県内住所",
        "沿革",
        "平均年齢",
        "その他の特徴",
        "企業担当者からのメッセージ",
        "採用情報URL",
    ]

    def parse(self, url: str):
        page = 1
        seen_company_urls: set[str] = set()

        while True:
            page_url = LIST_URL if page == 1 else f"{LIST_URL}?p={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                break

            # 1ページ目でページネーション末尾から総ページ数を取得
            if page == 1:
                last_link = soup.select_one('ul.pagination a[aria-label="最後のページ"]')
                total_pages = 1
                if last_link and last_link.get("href"):
                    m = re.search(r"p=(\d+)", last_link["href"])
                    if m:
                        total_pages = int(m.group(1))
                else:
                    # 最後のページリンクがない場合はページ番号リンクから推定
                    page_links = soup.select("ul.pagination a.page-link")
                    page_nums = [
                        int(m2.group(1))
                        for a in page_links
                        if a.get("href") and (m2 := re.search(r"p=(\d+)", a["href"]))
                    ]
                    if page_nums:
                        total_pages = max(page_nums)
                cards_on_first_page = len(soup.select("div.card.mt-4"))
                self.total_items = cards_on_first_page * total_pages

            cards = soup.select("div.card.mt-4")
            if not cards:
                break

            for card in cards:
                try:
                    info_a = card.select_one('a[href*="/pages/company/"]')
                    if not info_a or not info_a.get("href"):
                        continue
                    detail_url = urljoin(BASE_URL, info_a["href"])
                    if detail_url in seen_company_urls:
                        continue
                    seen_company_urls.add(detail_url)

                    employment_a = card.select_one('a[href*="/pages/employment/"]')
                    employment_url = (
                        urljoin(BASE_URL, employment_a["href"])
                        if employment_a and employment_a.get("href")
                        else ""
                    )

                    result = self._scrape_detail(detail_url, employment_url)
                    if result:
                        yield result
                except Exception as e:
                    self.logger.warning("カード処理スキップ: %s", e)

            # 末尾ページ到達で終了
            next_link = soup.select_one('ul.pagination a[aria-label="次のページ"]')
            if not next_link:
                break
            page += 1

    def _scrape_detail(self, url: str, employment_url: str = "") -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        name = ""
        h1 = soup.select_one("h1")
        if h1:
            name = h1.get_text(strip=True)

        fields: dict[str, str] = {}
        for tr in soup.select("table.table-stacked tr"):
            th = tr.select_one("th")
            td = tr.select_one("td")
            if th and td:
                key = th.get_text(strip=True)
                fields[key] = td.get_text(" ", strip=True)

        # URLは<a>タグからの生リンクを優先
        hp = ""
        for tr in soup.select("table.table-stacked tr"):
            th = tr.select_one("th")
            if th and th.get_text(strip=True) == "URL":
                a = tr.select_one("td a[href]")
                if a:
                    hp = a["href"].strip()
                else:
                    hp = fields.get("URL", "")
                break

        address_raw = fields.get("事業所本社住所", "")
        post_code = ""
        pref = ""
        addr_body = address_raw
        pc_m = _POST_CODE_PATTERN.search(address_raw)
        if pc_m:
            post_code = pc_m.group(1)
            addr_body = address_raw[pc_m.end():].strip()
        pref_m = _PREF_PATTERN.match(addr_body)
        if pref_m:
            pref = pref_m.group(1)
            addr_body = addr_body[pref_m.end():].strip()

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr_body,
            Schema.TEL: fields.get("電話番号", ""),
            Schema.REP_NM: fields.get("代表者名", ""),
            Schema.EMP_NUM: fields.get("従業員数", ""),
            Schema.LOB: fields.get("事業内容", ""),
            Schema.CAP: fields.get("資本金", ""),
            Schema.CAT_SITE: fields.get("業種", ""),
            Schema.HP: hp,
            Schema.OPEN_DATE: fields.get("創立年月日", ""),
            "FAX番号": fields.get("FAX番号", ""),
            "売上高": fields.get("売上高", ""),
            "事業所県内住所": fields.get("事業所県内住所", ""),
            "沿革": fields.get("沿革", ""),
            "平均年齢": fields.get("平均年齢", ""),
            "その他の特徴": fields.get("その他の特徴", ""),
            "企業担当者からのメッセージ": fields.get("企業担当者からのメッセージ", ""),
            "採用情報URL": employment_url,
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = KocchakeScraper()
    scraper.execute(LIST_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
