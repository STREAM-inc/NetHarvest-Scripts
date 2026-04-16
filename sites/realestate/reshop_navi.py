import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class ReshopNaviScraper(StaticCrawler):
    """リショップナビ スクレイパー"""

    DELAY = 2.5

    EXTRA_COLUMNS = [
        "企業理念",
        "受賞歴",
        "売上高",
        "アフターフォロー",
        "資格",
        "保険",
        "その他の保有許認可",
        "加盟団体・協会",
        "書面発行について",
        "提携ローン",
        "決済金額下限",
        "取り扱いメーカー",
        "その他",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        current_url = url
        
        while current_url:
            self.logger.info(f"一覧ページ取得: {current_url}")
            soup = self.get_soup(current_url)
            
            # ETAのために全件の数取得（初回のみ）
            if getattr(self, "total_items", None) is None:
                total_elem = soup.select_one("div.shop-list__header p strong")
                if total_elem:
                    try:
                        self.total_items = int(total_elem.get_text(strip=True).replace(",", ""))
                    except ValueError:
                        pass
            
            # 詳細ページへのリンクを取得
            shop_links = soup.select("h2.shop-card__name a.shop-card__link")
            for link in shop_links:
                detail_url = link.get("href")
                if not detail_url:
                    continue
                    
                detail_url = urljoin(current_url, detail_url)
                
                # 詳細ページ取得前の負荷軽減
                time.sleep(self.DELAY)
                yield from self._scrape_detail(detail_url)
            
            # 次のページを取得
            next_link = soup.select_one("span.next a")
            if next_link and next_link.get("href"):
                current_url = urljoin(current_url, next_link.get("href"))
                time.sleep(self.DELAY)
            else:
                current_url = None

    # 詳細ページからデータを抽出するメソッド
    def _scrape_detail(self, url: str) -> Generator[dict, None, None]:
        try:
            soup = self.get_soup(url)
            
            data = {
                Schema.URL: url,
            }
            
            # dtとddのペアから基本データを取得する
            dt_elements = soup.select("dt")
            for dt in dt_elements:
                key = dt.get_text(strip=True)
                dd = dt.find_next_sibling("dd")
                if not dd:
                    continue
                
                # 余分な改行や空白を除去
                val = " ".join(dd.get_text(strip=True).split())
                
                # Schema 定数へのマッピング
                if "会社名" in key: data[Schema.NAME] = val
                elif "電話番号" in key: data[Schema.TEL] = val
                elif "会社HP" in key: data[Schema.HP] = val
                elif "代表者名" in key: data[Schema.REP_NM] = val
                elif "事業内容" in key: data[Schema.LOB] = val
                elif "郵便番号" in key: data[Schema.POST_CODE] = val
                elif "住所" in key: data[Schema.ADDR] = val
                elif "定休日" in key: data[Schema.HOLIDAY] = val
                elif "営業時間" in key: data[Schema.TIME] = val
                elif "従業員数" in key: data[Schema.EMP_NUM] = val
                elif "資本金" in key: data[Schema.CAP] = val
                elif "創業年" in key: data[Schema.OPEN_DATE] = val
                elif "決済方法" in key: data[Schema.PAYMENTS] = val
                # 独自カラムへのマッピング
                elif key in self.EXTRA_COLUMNS:
                    data[key] = val
            
            # 口コミ評価の取得
            # <h2><span ...>口コミ・評価</span></h2> のような見出しを探す
            review_header = soup.find(lambda t: t.name == "h2" and "口コミ・評価" in t.get_text())
            if review_header:
                next_div = review_header.find_next_sibling("div")
                if next_div:
                    score_elem = next_div.find("h4")
                    if score_elem:
                        score_text = score_elem.get_text(strip=True).replace("点", "")
                        data[Schema.SCORES] = score_text

            yield data
            
        except Exception as e:
            self.logger.warning(f"詳細ページのスキップ: {url} ({e})")


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    ReshopNaviScraper().execute("https://rehome-navi.com/shops")
