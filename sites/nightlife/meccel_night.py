"""
めっけるナイト（栃木版） — ナイト系店舗情報サイト

取得対象:
    - 全店舗（約160件）の詳細情報

取得フロー:
    1. shoplist.php から全店舗リンク（shopdetail.php?shop_no=N）を収集
    2. 各詳細ページから店舗情報を取得

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/meccel_night.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id meccel_night
"""

import re
import sys
from pathlib import Path

import bs4
import requests

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_BASE = "https://meccel.net"

_POSTAL_RE = re.compile(r"〒(\d{3}-\d{4})\s*")
_PREF_RE = re.compile(
    r"^(北海道|東京都|大阪府|京都府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|"
    r"新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|"
    r"和歌山|鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|"
    r"熊本|大分|宮崎|鹿児島|沖縄)県)"
)


class MeccelNightCrawler(StaticCrawler):
    """めっけるナイト（栃木版）スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["エリア", "駐車場"]

    def get_soup(self, url: str) -> bs4.BeautifulSoup | None:
        # Content-Type に charset がなく apparent_encoding も None のため Shift-JIS を強制
        try:
            response = self.session.get(url, timeout=self.TIMEOUT)
            response.raise_for_status()
            response.encoding = "shift_jis"
            return bs4.BeautifulSoup(response.text, "html.parser")
        except requests.exceptions.RequestException as e:
            if self.CONTINUE_ON_ERROR:
                self.error_count += 1
                self.logger.warning("通信エラー (スキップして継続): %s — %s", url, e)
                return None
            self.logger.error("通信エラー: %s", e)
            raise

    def parse(self, url: str):
        list_soup = self.get_soup(f"{_BASE}/shoplist.php")

        # li.clearfix 内の dt > a から一意の店舗リンクを収集
        seen = set()
        links = []
        for a in list_soup.select("li.clearfix dt a[href*='shopdetail.php']"):
            href = a["href"]
            if href not in seen:
                seen.add(href)
                links.append(href)

        self.total_items = len(links)

        for href in links:
            detail_url = f"{_BASE}/{href}"
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.error("Failed %s: %s", detail_url, e)
                continue

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        # 店舗名
        h2 = soup.find("h2", class_="pagetitle")
        name = h2.get_text(strip=True) if h2 else ""
        if not name:
            return None

        # ジャンル・エリア（div.txt_right: "キャバクラ（宇都宮東口）"）
        genre = area = ""
        txt_right = soup.find("div", class_="txt_right")
        if txt_right:
            raw = txt_right.get_text(strip=True)
            m = re.match(r"^(.+?)（(.+?)）", raw)
            if m:
                genre = m.group(1).strip()
                area = m.group(2).strip()
            else:
                genre = raw.strip()

        # メイン情報テーブル（最初の table）
        info = {}
        main_table = soup.find("table")
        if main_table:
            for tr in main_table.find_all("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if th and td:
                    info[th.get_text(strip=True)] = td.get_text(strip=True)

        # 住所から郵便番号・都道府県を抽出
        addr_raw = info.get("所在地", "")
        post_code = pref = ""
        addr = addr_raw
        pm = _POSTAL_RE.match(addr_raw)
        if pm:
            post_code = pm.group(1)
            addr = addr_raw[pm.end():].strip()
        prm = _PREF_RE.match(addr)
        if prm:
            pref = prm.group(1)
            addr = addr[prm.end():].strip()

        # LINE URL（line.me または lin.ee）
        line_url = ""
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "line.me" in href or "lin.ee" in href:
                line_url = href
                break

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: info.get("連絡先", ""),
            Schema.TIME: info.get("営業時間", ""),
            Schema.HOLIDAY: info.get("定休日", ""),
            Schema.LINE: line_url,
            Schema.CAT_SITE: genre,
            Schema.URL: url,
            "エリア": area,
            "駐車場": info.get("駐車場", ""),
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = MeccelNightCrawler()
    scraper.execute("https://meccel.net/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
