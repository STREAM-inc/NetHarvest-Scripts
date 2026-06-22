"""
ルーキーWeb — 沖縄中心の求人・アルバイト情報サイト

取得対象:
    - 求人情報（会社名・住所・電話番号・採用情報など）

取得フロー:
    一覧 /job?media_type=inside&showCount=40&page=N → 詳細ページ で 1 件ずつ yield

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/web_3.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id web_3
"""

import json
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
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_RE = re.compile(r"〒?\s*(\d{3})-?(\d{4})")


class Web3(StaticCrawler):
    """ルーキーWeb スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "求人タイトル",
        "雇用形態",
        "給与",
        "勤務地",
        "期間",
        "応募方法",
        "資格",
    ]

    def parse(self, url: str):
        list_url = urljoin(url, "job")
        page = 1
        while True:
            page_url = f"{list_url}?media_type=inside&showCount=40&page={page}"
            soup = self.get_soup(page_url)
            items = soup.select("div.job-list-item.row")
            if not items:
                break
            if page == 1:
                total_el = soup.select_one(".job-search-result-filter-count-total")
                if total_el:
                    m = re.search(r"[\d,]+", total_el.get_text())
                    if m:
                        self.total_items = int(m.group().replace(",", ""))
            for item in items:
                a = item.select_one(".job-list-item-heading-title h3 a")
                if not a:
                    continue
                detail_url = a.get("href", "")
                if not detail_url.startswith("http"):
                    detail_url = urljoin(url, detail_url)
                try:
                    record = self._scrape_detail(detail_url)
                    if record:
                        yield record
                except Exception as e:
                    self.logger.warning("detail error %s: %s", detail_url, e)
            page += 1

    def _get_field(self, soup, field_name: str) -> str:
        """heading テキストで検索し、accordion/plain いずれのコンテナも対応"""
        for heading in soup.select("h3.job-detail-more-item-content__title"):
            if field_name in heading.get_text():
                content = heading.find_next(class_="accordion-component__content")
                if content:
                    return content.get_text(" ", strip=True)
                body = heading.find_next(class_="job-detail-more-item-content__body")
                if body:
                    return body.get_text(" ", strip=True)
        return ""

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)

        ld_data = {}
        script = soup.find("script", {"type": "application/ld+json"})
        if script and script.string:
            try:
                ld_data = json.loads(script.string)
            except Exception:
                pass

        name = self._get_field(soup, "会社") or ld_data.get("hiringOrganization", {}).get("name", "")
        if not name:
            return None

        # 求人タイトル
        title = ld_data.get("title", "")
        if not title:
            h1 = soup.select_one("h1")
            title = h1.get_text(strip=True) if h1 else ""

        # 雇用形態
        employment_type = ld_data.get("employmentType", "")
        if not employment_type:
            emp_el = soup.select_one('[class*="contract-type"]')
            employment_type = emp_el.get_text(strip=True) if emp_el else ""

        # 会社住所
        address_raw = self._get_field(soup, "住所")
        post_code = ""
        pref = ""
        addr = ""
        if address_raw:
            m_post = _POST_RE.search(address_raw)
            if m_post:
                post_code = m_post.group(1) + m_post.group(2)
            m_pref = _PREF_RE.search(address_raw)
            if m_pref:
                pref = m_pref.group(1)
            addr = re.sub(r"〒\s*\d{3}-?\d{4}\s*", "", address_raw).strip()

        # TEL（会社直通番号。問合わせ先の0120リファラル番号は対象外）
        tel = self._get_field(soup, "TEL")

        # HP
        hp = ""
        for heading in soup.select("h3.job-detail-more-item-content__title"):
            if "URL" in heading.get_text():
                body = heading.find_next(class_="job-detail-more-item-content__body")
                if body:
                    a = body.select_one("a[href]")
                    hp = a.get("href", "") if a else body.get_text(strip=True)
                break

        # 勤務地（地図エリア優先、次いでaccordion）
        workplace = ""
        map_el = soup.select_one(".job-detail-map__item")
        if map_el:
            workplace = map_el.get_text(strip=True)
        if not workplace:
            workplace = self._get_field(soup, "勤務地")

        salary = self._get_field(soup, "給与")
        work_time = self._get_field(soup, "時間")
        holiday = self._get_field(soup, "休日")
        period = self._get_field(soup, "期間")
        apply_method = self._get_field(soup, "応募方法")
        qualification = self._get_field(soup, "資格")

        return {
            Schema.NAME: name,
            Schema.URL: detail_url,
            Schema.ADDR: addr,
            Schema.POST_CODE: post_code,
            Schema.PREF: pref,
            Schema.TEL: tel,
            Schema.HP: hp,
            Schema.TIME: work_time,
            Schema.HOLIDAY: holiday,
            "求人タイトル": title,
            "雇用形態": employment_type,
            "給与": salary,
            "勤務地": workplace,
            "期間": period,
            "応募方法": apply_method,
            "資格": qualification,
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = Web3()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://www.shigotoarimasu.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
