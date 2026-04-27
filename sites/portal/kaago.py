# scripts/sites/portal/kaago.py
import sys
import time
import re
from pathlib import Path

# ファイルの位置から見て4階層上が「NetHarvest」フォルダになります
_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from urllib.parse import urljoin
from bs4 import BeautifulSoup
from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

class KaagoScraper(DynamicCrawler):
    """KAAGO (カーゴ) 店舗情報スクレイパー"""

    DELAY = 1.5
    
    # Schemaにない独自の取得項目を宣言します
    EXTRA_COLUMNS = ["運営法人", "適格請求書発行事業者登録番号"]
    
    # ★テスト用の制限件数（本番で全件取得する場合は 0 に設定してください）
    MAX_ITEMS = 10 

    def parse(self, url: str):
        # ==========================================
        # STEP 1: トップページから全カテゴリURLを取得
        # ==========================================
        self.logger.info("トップページからカテゴリを取得: %s", url)
        self.page.goto(url, wait_until="domcontentloaded")
        soup_top = BeautifulSoup(self.page.content(), "html.parser")
        
        categories = [urljoin(url, a.get('href')) for a in soup_top.select("ul.topSidebarList02 a") if a.get('href')]
        self.logger.info("カテゴリURLを %d 件取得しました", len(categories))
        
        seen_shops = set()
        
        # ==========================================
        # STEP 2: 各カテゴリを巡回し、ショップIDを収集
        # ==========================================
        for cat_url in categories:
            self.logger.info("カテゴリ巡回中: %s", cat_url)
            try:
                self.page.goto(cat_url, wait_until="domcontentloaded")
                time.sleep(1) # 読み込み待機
                
                last_height = self.page.evaluate("document.body.scrollHeight")
                scroll_count = 0
                
                # 無限スクロール処理
                while True:
                    self.page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)
                    new_height = self.page.evaluate("document.body.scrollHeight")
                    scroll_count += 1
                    
                    # 終了条件: 高さが変わらない、またはテスト時で3回スクロールした
                    if new_height == last_height or (self.MAX_ITEMS > 0 and scroll_count >= 3):
                        self.logger.info("スクロール完了（回数: %d）", scroll_count)
                        break
                    last_height = new_height

                # 読み込み完了後のHTMLから商品リンクを探す
                soup_cat = BeautifulSoup(self.page.content(), "html.parser")
                
                for a in soup_cat.find_all("a", href=True):
                    href = a["href"]
                    # 商品詳細のリンクには "itemcode=" が含まれる
                    if "itemcode=" in href:
                        try:
                            parts = href.split('/')
                            # URLからショップIDを抽出 (相対パス/絶対パス両対応)
                            if href.startswith('http'):
                                shop_id = parts[3]
                            elif href.startswith('/'):
                                shop_id = parts[1]
                            else:
                                continue
                                
                            # 明らかにショップIDではないシステム文字列を除外
                            ignore_list = ["powershop", "static", "customer", "cart", "mypage", "default"]
                            if shop_id and shop_id not in ignore_list:
                                seen_shops.add(shop_id)
                        except IndexError:
                            continue
                            
                # テスト時は指定件数で探索を打ち切る
                if self.MAX_ITEMS > 0 and len(seen_shops) >= self.MAX_ITEMS:
                    self.logger.info("制限件数到達のため、ID収集を終了します")
                    break
            except Exception as e:
                self.logger.warning("スキップ: %s (%s)", cat_url, e)
                continue

        # リスト化し、制限件数でスライス
        shop_id_list = list(seen_shops)[:self.MAX_ITEMS] if self.MAX_ITEMS > 0 else list(seen_shops)
        
        # 進捗表示（ETA）を有効化
        self.total_items = len(shop_id_list)
        self.logger.info("対象ショップIDを %d 件確定。詳細情報の取得を開始します", self.total_items)

        # --------------------------------------------------
        # 全角数字・記号を半角に変換するための変換テーブル
        # --------------------------------------------------
        zenkaku = '０１２３４５６７８９－（）ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ'
        hankaku = '0123456789-()abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
        trans_table = str.maketrans(zenkaku, hankaku)

        # ==========================================
        # STEP 3: 店舗情報を1件ずつ取得
        # ==========================================
        for shop_id in shop_id_list:
            info_url = f"https://kaago.com/{shop_id}/shopinfo"
            self.logger.info("店舗情報取得中: %s", info_url)
            
            try:
                self.page.goto(info_url, wait_until="domcontentloaded")
                time.sleep(self.DELAY)
                soup = BeautifulSoup(self.page.content(), "html.parser")
            except Exception as e:
                self.logger.warning("スキップ: %s (%s)", info_url, e)
                continue

            item = {Schema.URL: info_url}
            
            h1_tag = soup.find('h1', class_='headingType01')
            if h1_tag:
                # 名称を取得し、半角に変換
                item[Schema.NAME] = h1_tag.get_text(strip=True).replace('の店舗情報', '').translate(trans_table)

            # 店舗名が取得できない場合は不正なページとしてスキップ
            if Schema.NAME not in item or not item[Schema.NAME]:
                continue

            # ヘルパー関数: dtタグの名前からddタグの中身を取得し、半角変換も行う
            def get_shop_detail(dt_name):
                # 完全一致ではなく部分一致（含む）で検索
                dt_tag = soup.find('dt', string=re.compile(dt_name))
                if dt_tag:
                    dd_tag = dt_tag.find_next_sibling('dd')
                    if dd_tag:
                        val = dd_tag.get_text(strip=True).replace('\xa0', ' ')
                        return val.translate(trans_table)
                return ""

            addr = get_shop_detail('住所')
            if addr: 
                # 郵便番号（「〒103-0015」など）を正規表現で削除
                addr = re.sub(r'〒\s*\d{3}-?\d{4}\s*', '', addr)
                item[Schema.ADDR] = addr

            tel = get_shop_detail('電話番号')
            if tel: item[Schema.TEL] = tel

            manager = get_shop_detail('店舗運営責任者')
            if manager: item[Schema.REP_NM] = manager

            corp = get_shop_detail('運営法人')
            if corp: item['運営法人'] = corp

            invoice = get_shop_detail('適格請求書発行事業者登録番号')
            if invoice: item['適格請求書発行事業者登録番号'] = invoice

            # 1件分のデータをフレームワークへ渡す（自動でCSV出力される）
            yield item

# ===== 実行用ブロック =====
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    KaagoScraper().execute("https://kaago.com/")
