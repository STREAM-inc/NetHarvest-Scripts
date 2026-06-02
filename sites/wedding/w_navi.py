"""
ウェディング・ナビ — 結婚式場相談サービス

取得対象:
    - 全24店舗（直営3店舗 + フランチャイズ21店舗）

取得フロー:
    1. /salon/ ページから直営3店舗（新宿・名古屋・梅田）を解析
    2. /salon/fc-{city}/ 各21ページからFC店舗情報を取得

実行方法:
    # ローカルテスト
    python scripts/sites/wedding/w_navi.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id w_navi
"""

import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_BASE = "https://w-navi.jp"

_POSTAL_RE = re.compile(r"〒(\d{3}-\d{4})\s*")
_PREF_RE = re.compile(
    r"^(北海道|東京都|大阪府|京都府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|"
    r"新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|"
    r"和歌山|鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|"
    r"熊本|大分|宮崎|鹿児島|沖縄)県)"
)

_DIRECT_IDS = ["shinjuku", "nagoya", "umeda"]


def _parse_info(container) -> dict:
    """dl > dt/dd ペアから情報辞書を構築する"""
    info = {}
    for dl in container.find_all("dl"):
        for dt in dl.find_all("dt"):
            dd = dt.find_next_sibling("dd")
            if dd:
                info[dt.get_text(strip=True)] = dd.get_text(" ", strip=True)
    return info


def _parse_address(addr_raw: str):
    """住所文字列から郵便番号・都道府県・残住所を返す"""
    post_code = pref = ""
    addr = addr_raw
    pm = _POSTAL_RE.search(addr_raw)
    if pm:
        post_code = pm.group(1)
        addr = addr_raw[pm.end():].strip()
    prm = _PREF_RE.match(addr)
    if prm:
        pref = prm.group(1)
        addr = addr[prm.end():].strip()
    return post_code, pref, addr


def _find_line(container) -> str:
    for a in container.find_all("a", href=True):
        h = a["href"]
        if "line.me" in h or "lin.ee" in h:
            return h
    return ""


class WNaviCrawler(StaticCrawler):
    """ウェディング・ナビ スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["アクセス"]

    def parse(self, url: str):
        salon_url = f"{_BASE}/salon/"
        soup = self.get_soup(salon_url)

        # 直営3店舗の解析（id="shinjuku" / "nagoya" / "umeda" のセクション）
        direct_items = []
        for shop_id in _DIRECT_IDS:
            section = soup.find(id=shop_id)
            if not section:
                continue

            h_tag = section.find(["h2", "h3", "h4"])
            name = h_tag.get_text(" ", strip=True) if h_tag else shop_id

            info = _parse_info(section)
            post_code, pref, addr = _parse_address(info.get("住所", ""))

            direct_items.append({
                Schema.NAME:      name,
                Schema.PREF:      pref,
                Schema.POST_CODE: post_code,
                Schema.ADDR:      addr,
                Schema.TEL:       info.get("電話", ""),
                Schema.TIME:      info.get("営業時間", ""),
                Schema.HOLIDAY:   info.get("定休日", ""),
                Schema.LINE:      _find_line(section),
                Schema.URL:       f"{salon_url}#{shop_id}",
                "アクセス":       info.get("アクセス", ""),
            })

        # FC店舗リンク収集
        seen = set()
        fc_links = []
        for a in soup.select('a[href*="/salon/fc-"]'):
            href = a["href"]
            if not href.startswith("http"):
                href = _BASE + href
            if href not in seen:
                seen.add(href)
                fc_links.append(href)

        self.total_items = len(direct_items) + len(fc_links)

        for item in direct_items:
            yield item

        for fc_url in fc_links:
            try:
                item = self._scrape_detail(fc_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.error("Failed %s: %s", fc_url, e)
                continue

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if not soup:
            return None

        h1 = soup.find("h1")
        name = h1.get_text(" ", strip=True) if h1 else ""
        if not name:
            return None

        info = _parse_info(soup)
        post_code, pref, addr = _parse_address(info.get("住所", ""))

        return {
            Schema.NAME:      name,
            Schema.PREF:      pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR:      addr,
            Schema.TEL:       info.get("電話", ""),
            Schema.TIME:      info.get("営業時間", ""),
            Schema.HOLIDAY:   info.get("定休日", ""),
            Schema.LINE:      _find_line(soup),
            Schema.URL:       url,
            "アクセス":       info.get("アクセス", ""),
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = WNaviCrawler()
    scraper.execute("https://w-navi.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
