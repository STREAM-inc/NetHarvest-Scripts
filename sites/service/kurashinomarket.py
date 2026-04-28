# scripts/sites/service/curama.py
import re
import sys
import time
import json
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup
from src.framework.static import StaticCrawler
from src.const.schema import Schema

class CuramaScraper(StaticCrawler):
    """くらしのマーケット (curama.jp) 業者情報スクレイパー"""

    DELAY = 1.5
    
    # Schema定義に存在しない独自項目のみを宣言します
    EXTRA_COLUMNS = [
        "エリア",
        "適格請求書発行事業者登録番号"
    ]
    
    # ★ テスト設定：一覧ページは1ページのみ、詳細ページは10件でストップ
    MAX_PAGES_PER_CATEGORY = 0
    MAX_DETAILS_TO_SCRAPE = 0

    def parse(self, url: str):
        # ==========================================
        # ステップ1: カテゴリURLの収集
        # ==========================================
        self.logger.info("カテゴリ一覧を取得: %s", url)
        soup_cat = self.get_soup(url)
        if not soup_cat:
            return
            
        categories = []
        for a_tag in soup_cat.select("ul.m_acd-contented a.col_bla10"):
            href = a_tag.get("href")
            if href and href.count("/") == 2:
                cat_name = a_tag.get_text(strip=True).replace("chevron_right", "").strip()
                categories.append((urljoin(url, href), cat_name))
                
        self.logger.info("%d 個のカテゴリURLを取得しました", len(categories))

        # ==========================================
        # ステップ2: 一覧ページの巡回（業者詳細URLの収集）
        # ==========================================
        detail_links = []
        
        for cat_url, cat_name in categories: 
            page_num = 1
            while True:
                page_url = f"{cat_url}?page={page_num}&"
                self.logger.info("一覧ページ取得中: [%s] %s", cat_name, page_url)
                
                soup = self.get_soup(page_url)
                if not soup:
                    break

                items = soup.find_all("a", id=re.compile(r"^service-details\d+"))
                if not items:
                    break

                for item in items:
                    href = item.get("href")
                    if href:
                        detail_links.append((urljoin(url, href), cat_name))

                if self.MAX_PAGES_PER_CATEGORY > 0 and page_num >= self.MAX_PAGES_PER_CATEGORY:
                    break
                    
                next_btn = soup.find("a", string=re.compile(r"次の\d+件"))
                if not next_btn:
                    break
                    
                page_num += 1
                time.sleep(self.DELAY)

        # 10件でカットする
        if self.MAX_DETAILS_TO_SCRAPE > 0:
            detail_links = detail_links[:self.MAX_DETAILS_TO_SCRAPE]

        self.total_items = len(detail_links)
        self.logger.info("対象の業者詳細ページを %d 件確定。情報の取得を開始します", self.total_items)

        # --------------------------------------------------
        # 全角数字・記号を半角に変換するためのテーブル
        # --------------------------------------------------
        zenkaku = '０１２３４５６７８９－（）ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ'
        hankaku = '0123456789-()abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
        trans_table = str.maketrans(zenkaku, hankaku)

        # ==========================================
        # ステップ3: 詳細ページからのデータ抽出
        # ==========================================
        for detail_url, cat_name in detail_links:
            try:
                detail_soup = self.get_soup(detail_url)
                if not detail_soup:
                    continue
            except Exception as e:
                self.logger.warning("スキップ: %s (%s)", detail_url, e)
                continue

            scraped_item = {
                Schema.URL: detail_url,
                Schema.CAT_SITE: cat_name,
            }

            # ----------------------------------
            # 店舗名 (裏側のJSONから取得)
            # ----------------------------------
            tracking_template = detail_soup.find("template", id="trackingData")
            if tracking_template:
                try:
                    tracking_json = json.loads(tracking_template.get_text(strip=True))
                    st_nm = tracking_json.get("stNm", "")
                    
                    if "、" in st_nm:
                        st_nm = st_nm.split("、")[-1]
                        
                    scraped_item[Schema.NAME] = st_nm.translate(trans_table).strip()
                except Exception:
                    pass

            if Schema.NAME not in scraped_item or not scraped_item[Schema.NAME]:
                h1_tag = detail_soup.find("h1", itemprop="name")
                if h1_tag:
                    scraped_item[Schema.NAME] = h1_tag.get_text(strip=True).translate(trans_table)

            # ----------------------------------
            # 代表者名・役職
            # ----------------------------------
            manager_p = detail_soup.find("p", string=re.compile(r"(店長|代表|責任者)[：:]"))
            if manager_p:
                manager_text = manager_p.get_text(strip=True).translate(trans_table)
                match = re.search(r'(店長|代表|責任者)[：:](.*)', manager_text)
                if match:
                    scraped_item[Schema.POS_NM] = match.group(1).strip()
                    scraped_item[Schema.REP_NM] = match.group(2).strip()

            # ----------------------------------
            # 所在地（住所・都道府県）
            # ----------------------------------
            addr_h3 = detail_soup.find("h3", string=re.compile("所在地"))
            if addr_h3:
                addr_div = addr_h3.find_next_sibling("div")
                if addr_div:
                    raw_addr = addr_div.get_text(strip=True).translate(trans_table)
                    # 郵便番号を削除
                    clean_addr = re.sub(r'〒\s*\d{3}-?\d{4}\s*', '', raw_addr).strip()
                    scraped_item[Schema.ADDR] = clean_addr
                    
                    # 住所から都道府県を抽出
                    pref_match = re.match(r'^(東京都|北海道|大阪府|京都府|.{2,3}県)', clean_addr)
                    if pref_match:
                        scraped_item[Schema.PREF] = pref_match.group(1)

            # ----------------------------------
            # 対応エリア
            # ----------------------------------
            area_h3 = detail_soup.find("h3", string=re.compile("対応エリア"))
            if area_h3:
                area_div = area_h3.find_next_sibling("div")
                if area_div:
                    li_tags = area_div.find_all("li")
                    if li_tags:
                        areas = [li.get_text(strip=True).translate(trans_table) for li in li_tags]
                        scraped_item["エリア"] = "、".join(areas)
                    else:
                        scraped_item["エリア"] = area_div.get_text(strip=True).translate(trans_table)

            # ----------------------------------
            # インボイス番号・電話番号
            # ----------------------------------
            def get_detail_from_h3(keyword):
                target_h3 = detail_soup.find(["h3", "dt"], string=re.compile(keyword))
                if target_h3:
                    content = target_h3.find_next_sibling(["div", "dd"])
                    if content:
                        return content.get_text(strip=True).translate(trans_table)
                return ""

            tel = get_detail_from_h3("電話|TEL")
            if tel: 
                scraped_item[Schema.TEL] = tel

rr

            invoice = get_detail_from_h3("インボイス|適格請求書")
            if invoice: 
                scraped_item["適格請求書発行事業者登録番号"] = invoice

            yield scraped_item

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    CuramaScraper().execute("https://curama.jp/category/")

