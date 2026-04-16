"""
HonefixScraper (【骨FIX】整体院・整骨院ナビ) - 動的コンテンツ対応のクローラー実装
URL: https://seitainavi.jp/seitai_sekkotsu

一覧ページから各おすすめ記事（詳細ページ）へのリンクを収集し、
記事内にある店舗情報テーブルからデータを抽出する。
"""

import time
import re
import sys
from pathlib import Path
from typing import Generator
from bs4 import BeautifulSoup

# 直接実行(CodeRunner等)時に src へのパスを通すため、プロジェクトルートを追加
_project_root = Path(__file__).resolve().parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.dynamic import DynamicCrawler

class HonefixScraper(DynamicCrawler):
    """
    Honefix (Seitai Navi) の店舗情報を取得するクローラー。
    """
    # 抽出したいカスタム項目 (Schemaにないもの)
    # 定休日と営業時間は基本フィールドにあるためここからは削除
    EXTRA_COLUMNS = [
        "施術ジャンル", "予約方法", "備考"
    ]
    
    # サーバー負荷軽減のための待機秒数
    DELAY = 2.0
    
    def get_soup(self, url: str) -> BeautifulSoup:
        """Playwright経由でページを取得し、BeautifulSoupオブジェクトを返す"""
        # 動的サイトの読み込み完了を待機 (WAF回避後は domcontentloaded で十分高速に取得可能)
        self.page.goto(url, wait_until="domcontentloaded")
        return BeautifulSoup(self.page.content(), "html.parser")
        
    def parse(self, list_url: str) -> Generator[dict, None, None]:
        """
        一覧ページから詳細記事ページのURLを収集し、各記事内の店舗情報を抽出・yieldする。
        """
        page = 1
        has_next = True
        
        while has_next:
            current_url = f"{list_url}/page/{page}" if page > 1 else list_url
            self.logger.info("一覧ページ取得: page=%d (%s)", page, current_url)
            
            try:
                if self.DELAY > 0:
                    time.sleep(self.DELAY)
                soup = self.get_soup(current_url)
            except Exception as e:
                self.logger.warning("一覧ページ %d の取得に失敗しました: %s。スキップします。", page, e)
                page += 1
                continue
                
            # 初回ページ取得時に最大ページ数を取得し、ETA表示用の total_items を推計する
            if page == 1 and soup:
                try:
                    # ページネーションリンクの中から最大の数字を見つける
                    # <a class="page-numbers" href=".../page/29">29</a>
                    page_numbers = []
                    for a in soup.select("a.page-numbers"):
                        text = a.get_text(strip=True)
                        if text.isdigit():
                            page_numbers.append(int(text))
                            
                    if page_numbers:
                        max_pages = max(page_numbers)
                        # 1ページあたり約10記事、1記事あたり約10店舗として推計
                        self.total_items = max_pages * 10 * 10
                        self.logger.info("全 %d ページを検知しました。ETA表示を有効化します (推計アイテム数: %d)", max_pages, self.total_items)
                except Exception as e:
                    self.logger.warning("ページ数推計に失敗しました (ETA表示オフ): %s", e)
                    
            # 詳細記事リンクを収集
            # 例: <a href="https://seitainavi.jp/seitai_sekkotsu/26021">
            detail_links = []
            for a_tag in soup.select("a[href]"):
                href = a_tag["href"]
                # paginationやカテゴリートップを除外し、記事IDを含むURLのみ抽出
                if "seitainavi.jp/seitai_sekkotsu/" in href and "/page/" not in href and href != "https://seitainavi.jp/seitai_sekkotsu":
                    if href not in detail_links:
                        detail_links.append(href)
            
            if not detail_links:
                self.logger.warning("ページ %d に記事リンクがありません。一覧の最後まで到達した可能性があります。", page)
                break
                
            self.logger.info("  -> ページ %d にて %d 件の記事リンクを発見", page, len(detail_links))
            
            # 各詳細記事ページを処理
            for detail_url in detail_links:
                yield from self._collect_entries_from_article(detail_url)
                
            # 次のページがあるか判定 (pagination に nextpage クラスの a タグがあるか)
            next_page_link = soup.select_one(".nextpage a")
            if next_page_link:
                page += 1
            else:
                self.logger.info("最終ページ (%d) に達しました。スクレイピングを完了します。", page)
                has_next = False
                
    def _collect_entries_from_article(self, article_url: str) -> Generator[dict, None, None]:
        """
        1つの記事ページから、複数含まれる店舗テーブル情報を抽出・yieldする。
        """
        self.logger.info("記事ページ取得: %s", article_url)
        try:
            if self.DELAY > 0:
                time.sleep(self.DELAY)
            soup = self.get_soup(article_url)
        except Exception as e:
            self.logger.warning("記事ページ %s の取得に失敗しました: %s。スキップします。", article_url, e)
            return
            
        # 記事内のすべてのテーブルを検索
        tables = soup.find_all("table")
        store_count = 0
        
        for table in tables:
            store_data = {}
            rows = table.find_all("tr")
            
            for row in rows:
                th = row.find("th")
                td = row.find("td")
                
                if th and td:
                    key = th.get_text(strip=True)
                    # リンクが含まれている場合（店舗URLなど）、href属性を優先して抽出
                    a_tag = td.find("a")
                    if "URL" in key and a_tag and a_tag.get("href"):
                        val = a_tag.get("href")
                    else:
                        # '<br>' 等はスペース区切りに変換して改行を潰す
                        val = td.get_text(separator=" ", strip=True) 
                        
                    store_data[key] = val
                    
            # 「店名」と「住所」が含まれているテーブルのみ、店舗情報として扱う
            if "店名" in store_data and "住所" in store_data:
                # 必須項目を基本辞書にマッピング
                row_dict = {
                    "取得URL": article_url,
                    "名称": store_data.get("店名", ""),
                    "住所": store_data.get("住所", ""),
                    "TEL": store_data.get("電話番号", ""),
                    "HP": store_data.get("店舗URL", ""),
                    "定休日": store_data.get("定休日", ""),
                    "営業時間": store_data.get("営業時間", ""),
                }
                
                # 追加項目（EXTRA_COLUMNS）をマッピング
                for col in self.EXTRA_COLUMNS:
                    row_dict[col] = store_data.get(col, "")
                    
                store_count += 1
                yield row_dict
                
        self.logger.info("  -> 記事から %d 件の店舗情報を抽出", store_count)

if __name__ == "__main__":
    from src.framework.pipeline import ItemPipeline
    from pathlib import Path
    
    # 単体テスト用実行エントリポイント
    # Pipelineに接続し、CSV出力する
    crawler = HonefixScraper()
    pipeline = ItemPipeline(site_name="骨FIX", output_dir=Path("output"), extra_columns=crawler.EXTRA_COLUMNS)
    
    # 接続して実行
    crawler.pipeline = pipeline
    crawler.execute("https://seitainavi.jp/seitai_sekkotsu")
