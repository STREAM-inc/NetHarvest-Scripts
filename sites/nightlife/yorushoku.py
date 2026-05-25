"""
夜職倶楽部 (yorushoku.jp) — 全国ナイトワーク求人情報スクレイパー

取得対象:
    - 職種カテゴリ別求人（配信者・カウンターレディ・フロアレディー等）

取得フロー:
    1. /job-list/ から 6 カテゴリURLを収集
    2. 各カテゴリページを /page/N/ でページネーション全件収集
    3. 各求人の詳細ページ（/introduce/{slug}/）から情報を抽出

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/yorushoku.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id yorushoku
"""

import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


BASE_URL = "https://yorushoku.jp"
INDEX_URL = f"{BASE_URL}/job-list/"

_CAT_URL_RE = re.compile(r"https://yorushoku\.jp/job/[^/]+/$")

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def _extract_pref_addr(raw: str) -> tuple[str, str]:
    """住所テキストから (都道府県, 住所残り) を返す。"""
    if not raw:
        return "", ""
    m = _PREF_PATTERN.match(raw)
    if m:
        return m.group(1), raw[m.end():].strip()
    return "", raw.strip()


class YorushokuScraper(StaticCrawler):
    """夜職倶楽部 (yorushoku.jp) 求人スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["職種", "給与", "応募年齢", "メールアドレス"]

    def parse(self, url: str):
        detail_urls: list[str] = []
        seen: set[str] = set()

        # Step 1: /job-list/ からカテゴリURL収集
        index_soup = self.get_soup(INDEX_URL)
        if index_soup is None:
            self.logger.error("インデックスページ取得失敗: %s", INDEX_URL)
            return

        category_urls = []
        for a in index_soup.select("main a[href]"):
            href = a.get("href", "")
            if _CAT_URL_RE.match(href) and href not in category_urls:
                category_urls.append(href)

        self.logger.info("カテゴリ数: %d", len(category_urls))

        # Step 2: 各カテゴリをページネーションで全詳細URLを収集
        for cat_url in category_urls:
            page = 1
            while True:
                paged_url = cat_url.rstrip("/") + (f"/page/{page}/" if page > 1 else "/")
                cat_soup = self.get_soup(paged_url)
                if cat_soup is None:
                    break

                articles = cat_soup.select("main article")
                if not articles:
                    break

                for article in articles:
                    a_tag = article.select_one("a[href*='/introduce/']")
                    if a_tag:
                        detail_url = a_tag.get("href", "")
                        if detail_url and detail_url not in seen:
                            seen.add(detail_url)
                            detail_urls.append(detail_url)

                # 次ページがなければ終了
                pager = cat_soup.select_one(".wp-pagenavi")
                if not pager or not pager.select_one(f'a[href*="/page/{page + 1}/"]'):
                    break
                page += 1

        self.total_items = len(detail_urls)
        self.logger.info("収集した求人数: %d", self.total_items)

        # Step 3: 詳細ページスクレイピング
        for detail_url in detail_urls:
            item = self._scrape_detail(detail_url)
            if item:
                yield item

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        try:
            # 店名（応募先名）
            name_el = soup.select_one("li.name")
            name = _clean(name_el.get_text(strip=True)).replace("応募先", "").strip() if name_el else ""

            # 業種
            cat_site = ""
            for dt in soup.select("dl.overview dt"):
                if "業種" in dt.get_text():
                    dd = dt.find_next_sibling("dd")
                    if dd:
                        cat_site = _clean(dd.get_text(strip=True))
                    break

            # 職種・給与（概要DLのリスト）
            job_type = ""
            salary = ""
            jobtype_el = soup.select_one("span.jobtype")
            if jobtype_el:
                job_type = _clean(jobtype_el.get_text(strip=True))
            salary_el = soup.select_one("span.salary")
            if salary_el:
                salary = _clean(salary_el.get_text(strip=True))

            # テーブルから情報を辞書化（見出し行・複合ヘッダーは除く）
            info: dict[str, str] = {}
            for table in soup.select("article table"):
                for tr in table.select("tr"):
                    th = tr.select_one("th")
                    td = tr.select_one("td")
                    if th and td:
                        key = _clean(th.get_text(strip=True))
                        if key and "▼" not in key and key not in info:
                            info[key] = _clean(td.get_text(strip=True))

            # 給与 fallback: テーブルから
            if not salary:
                salary = info.get("給与/報酬", "")

            # 住所・都道府県
            raw_addr = info.get("勤務地", "")
            pref, addr = _extract_pref_addr(raw_addr)

            # PREF fallback: h2.shop_name > p
            if not pref:
                pref_el = soup.select_one("h2.shop_name p")
                if pref_el:
                    pref_text = _clean(pref_el.get_text(strip=True))
                    m = _PREF_PATTERN.match(pref_text)
                    if m:
                        pref = m.group(1)

            # TEL
            tel = ""
            tel_a = soup.select_one("li.tel a[href^='tel:']")
            if tel_a:
                tel = tel_a.get("href", "").replace("tel:", "").strip()

            # メールアドレス
            mail = ""
            mail_a = soup.select_one("a[href^='mailto:']")
            if mail_a:
                mail = mail_a.get("href", "").replace("mailto:", "").split("?")[0].strip()

            return {
                Schema.URL:      url,
                Schema.NAME:     name,
                Schema.PREF:     pref,
                Schema.ADDR:     addr,
                Schema.TEL:      tel,
                Schema.REP_NM:   info.get("担当", ""),
                Schema.CAT_SITE: cat_site,
                Schema.TIME:     info.get("勤務時間", ""),
                Schema.HOLIDAY:  info.get("休日", ""),
                "職種":          job_type,
                "給与":          salary,
                "応募年齢":      info.get("応募者年齢層", ""),
                "メールアドレス": mail,
            }
        except Exception as e:
            self.logger.error("詳細取得失敗 %s: %s", url, e)
            return None


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = YorushokuScraper()
    scraper.execute(INDEX_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
