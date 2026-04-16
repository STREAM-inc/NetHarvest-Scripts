# scripts/sites/jobs/cookbiz.py
"""
クックビズ (cookbiz.co.jp) — 企業情報スクレイパー

取得対象:
    - 企業一覧ページ → 企業詳細 → 求人詳細(?tab=office) の企業・店舗情報
    - 企業名, 業種/業態, 事業内容, 代表者, 電話番号, FAX, 郵便番号, 住所,
      設立年月, 資本金, 売上高, 従業員数, 株式市場, 企業HP,
      店舗名, 席数, 店舗URL, 営業時間, 定休日, 平均客単価, SNS

取得フロー:
    企業一覧 (?page=N, 全232ページ, 50件/ページ)
      → 企業詳細ページ (最初の求人リンク取得)
        → 求人詳細 ?tab=office (全フィールド取得)

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/cookbiz.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id cookbiz
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

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|"
    r"三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

BASE_URL = "https://cookbiz.co.jp"


class CookbizScraper(StaticCrawler):
    """クックビズ 企業情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "キャッチコピー",
        "企業説明",
        "店舗名",
        "席数",
        "店舗URL",
        "平均客単価",
        "SNS",
        "株式市場",
        "設立年月",
        "売上高",
        "FAX番号",
    ]

    def parse(self, url: str):
        """企業一覧 → 企業詳細 → 求人?tab=office で全データ取得"""
        company_urls = self._collect_company_urls(url)
        self.total_items = len(company_urls)
        self.logger.info("企業ページ URL 収集完了: %d 件", len(company_urls))

        for company_url in company_urls:
            try:
                item = self._scrape_company(company_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("企業データ取得失敗: %s (%s)", company_url, e)
                continue

    # ------------------------------------------------------------------
    # Step 1: 企業一覧ページネーション → 企業詳細 URL リスト
    # ------------------------------------------------------------------
    def _collect_company_urls(self, base_url: str) -> list[str]:
        """一覧ページをページネーションしながら企業詳細の URL を収集する"""
        detail_urls = []
        page = 1

        while True:
            list_url = f"{base_url}?page={page}" if page > 1 else base_url
            self.logger.info("一覧ページ取得: page=%d", page)
            soup = self.get_soup(list_url)

            links = soup.select('a[href*="/companies/"]')
            found = set()
            for link in links:
                href = link.get("href", "")
                if re.match(r"/companies/\d+$", href):
                    full_url = urljoin(BASE_URL, href)
                    found.add(full_url)

            if not found:
                break

            for u in found:
                if u not in detail_urls:
                    detail_urls.append(u)

            next_link = soup.select_one(f'a[href*="page={page + 1}"]')
            if not next_link:
                break

            page += 1

        return detail_urls

    # ------------------------------------------------------------------
    # Step 2: 企業詳細ページ → 最初の求人 URL 取得 → 求人?tab=office スクレイプ
    # ------------------------------------------------------------------
    def _scrape_company(self, company_url: str) -> dict | None:
        """企業詳細ページから最初の求人リンクを探し、?tab=office の情報を取得"""
        soup = self.get_soup(company_url)

        # 最初の求人リンクを探す
        job_link = soup.select_one('a[href*="/jobs/"]')
        if not job_link:
            self.logger.info("求人リンクなし (企業ページのみ取得): %s", company_url)
            return self._scrape_company_page_only(soup, company_url)

        job_href = job_link.get("href", "")
        job_url = urljoin(BASE_URL, job_href)
        office_url = job_url.split("?")[0] + "?tab=office"

        return self._scrape_job_office(office_url, company_url)

    # ------------------------------------------------------------------
    # Step 3a: 求人?tab=office から全フィールド取得
    # ------------------------------------------------------------------
    def _scrape_job_office(self, office_url: str, company_url: str) -> dict | None:
        """求人詳細の企業・店舗情報タブから全データを抽出"""
        soup = self.get_soup(office_url)
        main = soup.select_one("main")
        if not main:
            return None

        item = {Schema.URL: company_url}

        # --- 店舗情報セクション ---
        self._parse_shop_info(main, item)

        # --- 企業情報セクション ---
        self._parse_company_info(main, item)

        # --- キャッチコピー・企業説明 ---
        h2s = main.select("h2")
        for h2 in h2s:
            text = h2.get_text(strip=True)
            if text == "店舗情報":
                # h2 の直後の div がキャッチコピー
                sibling = h2.find_next_sibling()
                if sibling and sibling.name == "div":
                    item["キャッチコピー"] = sibling.get_text(strip=True)
                    # その次が企業説明文
                    desc_el = sibling.find_next_sibling()
                    if desc_el:
                        item["企業説明"] = desc_el.get_text("\n", strip=True)
                break

        if Schema.NAME not in item:
            return None

        return item

    def _parse_shop_info(self, main, item: dict):
        """店舗情報のラベル-値ペアを抽出"""
        label_map = {
            "席数": "席数",
            "店舗URL": "_shop_url",
            "営業時間": Schema.TIME,
            "定休日": Schema.HOLIDAY,
            "平均客単価": "平均客単価",
            "SNS": "SNS",
        }
        # 店舗名を取得 (企業情報 h2 の前にある店舗名)
        # 構造: h2(店舗情報) → div(キャッチコピー) → div(説明) → div(店舗名) → div(店舗詳細ペア)
        # 企業情報 h2 の直前にある企業名相当の div を探す
        h2_company = None
        for h2 in main.select("h2"):
            if h2.get_text(strip=True) == "企業情報":
                h2_company = h2
                break

        # 店舗名: "企業情報" h2の2つ前あたりにある店舗名テキスト
        h2_shop = None
        for h2 in main.select("h2"):
            if h2.get_text(strip=True) == "店舗情報":
                h2_shop = h2
                break

        if h2_shop:
            # 店舗名は h2(店舗情報)セクション内で、企業情報h2の直前の div
            # パターン: 『...』形式のテキストを探す
            for div in main.select("div"):
                text = div.get_text(strip=True)
                if text.startswith("『") and text.endswith("』"):
                    item["店舗名"] = text.strip("『』")
                    break
                elif text.startswith("『") and "』" in text:
                    item["店舗名"] = text.split("』")[0].lstrip("『")
                    break

        # ラベル-値ペアを解析
        all_divs = main.find_all("div", recursive=True)
        for div in all_divs:
            text = div.get_text(strip=True)
            if text not in label_map:
                continue

            sibling = div.find_next_sibling("div")
            if not sibling:
                continue
            value = sibling.get_text("\n", strip=True)
            if not value:
                continue

            key = label_map[text]
            if key == "_shop_url":
                # 店舗URL は a タグの href から取得
                a_tag = sibling.select_one("a")
                item["店舗URL"] = a_tag.get("href", "").strip() if a_tag else value
            elif key == "SNS":
                a_tag = sibling.select_one("a")
                item["SNS"] = a_tag.get("href", "").strip() if a_tag else value
            elif key == "席数":
                item["席数"] = value
            elif key == "平均客単価":
                item["平均客単価"] = value
            else:
                item[key] = value

    def _parse_company_info(self, main, item: dict):
        """企業情報のラベル-値ペアを抽出"""
        # 企業名: "企業情報" h2 はラッパー div 内にあるため、
        # ラッパーの次の兄弟 div から企業名を取得する
        for h2 in main.select("h2"):
            if h2.get_text(strip=True) == "企業情報":
                wrapper = h2.parent
                name_div = wrapper.find_next_sibling("div")
                if name_div:
                    item[Schema.NAME] = name_div.get_text(strip=True)
                break

        label_handlers = {
            "業種／業態": self._set_cat_site,
            "事業内容": self._set_lob,
            "代表者": self._set_representative,
            "株式市場": self._set_stock_market,
            "設立年月": self._set_open_date,
            "資本金": self._set_capital,
            "売上高": self._set_sales,
            "従業員数": self._set_emp_num,
            "電話番号": self._set_tel,
            "FAX番号": self._set_fax,
            "事業所": self._set_address,
            "URL": self._set_hp,
        }

        all_divs = main.find_all("div", recursive=True)
        address_set = False
        for div in all_divs:
            text = div.get_text(strip=True)
            if text not in label_handlers:
                continue

            sibling = div.find_next_sibling("div")
            if not sibling:
                # URL は a タグが直接来るケースも
                sibling = div.find_next_sibling()
                if not sibling:
                    continue

            # 事業所は店舗情報と企業情報で2回出るため、
            # 企業情報セクション（後方）のものを優先
            if text == "事業所":
                label_handlers[text](item, sibling)
                address_set = True
            else:
                label_handlers[text](item, sibling)

    def _set_cat_site(self, item, el):
        item[Schema.CAT_SITE] = el.get_text(strip=True)

    def _set_lob(self, item, el):
        item[Schema.LOB] = el.get_text("\n", strip=True)

    def _set_representative(self, item, el):
        rep_text = el.get_text(strip=True)
        m = re.match(
            r"^(.+?(?:社長|会長|CEO|代表|オーナー|理事長|取締役|役員)(?:\s*兼\s*\S+)?)\s+(.+)$",
            rep_text,
        )
        if m:
            item[Schema.POS_NM] = m.group(1).strip()
            item[Schema.REP_NM] = m.group(2).strip()
        else:
            item[Schema.REP_NM] = rep_text

    def _set_stock_market(self, item, el):
        item["株式市場"] = el.get_text(strip=True)

    def _set_open_date(self, item, el):
        item[Schema.OPEN_DATE] = el.get_text(strip=True)
        item["設立年月"] = el.get_text(strip=True)

    def _set_capital(self, item, el):
        item[Schema.CAP] = el.get_text(strip=True)

    def _set_sales(self, item, el):
        item["売上高"] = el.get_text(strip=True)

    def _set_emp_num(self, item, el):
        item[Schema.EMP_NUM] = el.get_text(strip=True)

    def _set_tel(self, item, el):
        item[Schema.TEL] = el.get_text(strip=True).strip('"')

    def _set_fax(self, item, el):
        item["FAX番号"] = el.get_text(strip=True).strip('"')

    def _set_address(self, item, el):
        address = el.get_text("\n", strip=True)
        # 郵便番号を抽出
        zip_match = re.search(r"〒?(\d{3}-?\d{4})", address)
        if zip_match:
            item[Schema.POST_CODE] = zip_match.group(1)
            address = address.replace(zip_match.group(0), "").strip()
        # 改行で結合された住所を整理
        address = re.sub(r"\n+", "", address).strip()
        pm = _PREF_PATTERN.match(address)
        if pm:
            item[Schema.PREF] = pm.group(1)
            item[Schema.ADDR] = address[pm.end():].strip()
        else:
            item[Schema.ADDR] = address

    def _set_hp(self, item, el):
        a_tag = el.select_one("a") if hasattr(el, "select_one") else None
        if a_tag:
            item[Schema.HP] = a_tag.get("href", "").strip()
        else:
            text = el.get_text(strip=True)
            if text.startswith("http"):
                item[Schema.HP] = text

    # ------------------------------------------------------------------
    # フォールバック: 求人がない企業は企業ページのみから取得
    # ------------------------------------------------------------------
    def _scrape_company_page_only(self, soup, company_url: str) -> dict | None:
        """企業詳細ページのみから基本情報を取得 (求人リンクがない場合)"""
        item = {Schema.URL: company_url}

        h1 = soup.select_one("h1")
        if h1:
            item[Schema.NAME] = h1.get_text(strip=True)
        else:
            return None

        h2 = soup.select_one("h2")
        if h2:
            item["キャッチコピー"] = h2.get_text(strip=True)

        desc_p = soup.select_one("main p")
        if desc_p:
            item["企業説明"] = desc_p.get_text("\n", strip=True)

        main = soup.select_one("main")
        if main:
            all_divs = main.find_all("div", recursive=True)
            for div in all_divs:
                text = div.get_text(strip=True)
                sibling = div.find_next_sibling("div")
                if not sibling:
                    continue
                value = sibling.get_text("\n", strip=True)

                if text == "業種／業態":
                    item[Schema.CAT_SITE] = value
                elif text == "事業内容":
                    item[Schema.LOB] = value
                elif text == "代表者":
                    self._set_representative(item, sibling)
                elif text == "事業所":
                    self._set_address(item, sibling)

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = CookbizScraper()
    scraper.execute("https://cookbiz.co.jp/companies")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
