"""
リクシルリフォームショップ — LIXIL 認定リフォーム加盟店検索サイト

取得対象:
    - 全国の LIXIL リフォームショップ加盟店情報 (店名、住所、電話、FAX、HP、SNS 等)

取得フロー:
    1. 47 都道府県の一覧ページ (/shop/address.php?div=...) を巡回し、詳細URL を収集
    2. 各詳細ページ (/shop/{ID}/) の shop-table から全フィールドを抽出

実行方法:
    # ローカルテスト
    python scripts/sites/realestate/lixil_reformshop.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id lixil_reformshop
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import quote, urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://www.lixil-reformshop.jp"
LIST_URL = f"{BASE_URL}/shop/address.php?search=1&search_type=prefecture&div={{pref}}"

PREFECTURES = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県",
    "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]

_POST_CODE_PATTERN = re.compile(r"〒?\s*(\d{3}-\d{4})")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


class LixilReformshopScraper(StaticCrawler):
    """リクシルリフォームショップ (lixil-reformshop.jp) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["会社名", "FAX番号", "フリーダイヤル", "対応エリア", "YouTube"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()

        all_detail_urls: list[tuple[str, str]] = []
        for pref in PREFECTURES:
            list_url = LIST_URL.format(pref=quote(pref, safe=""))
            soup = self.get_soup(list_url)
            if soup is None:
                continue
            for a in soup.select("li.js-shop-detail p.shop-name a[href]"):
                href = a.get("href", "")
                if not href:
                    continue
                full = urljoin(BASE_URL, href.rstrip("/") + "/")
                if full in seen:
                    continue
                seen.add(full)
                all_detail_urls.append((full, pref))

        self.total_items = len(all_detail_urls)
        self.logger.info("詳細ページ候補: %d 件", self.total_items)

        for detail_url, pref in all_detail_urls:
            try:
                item = self._scrape_detail(detail_url, pref)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("詳細ページ取得失敗 (スキップ): %s — %s", detail_url, e)
                continue

    def _scrape_detail(self, url: str, pref: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url, Schema.PREF: pref}

        h1 = soup.select_one("h1")
        if h1:
            data[Schema.NAME] = _clean(h1.get_text(" "))

        table = soup.select_one("table.shop-table")
        if not table:
            return data if data.get(Schema.NAME) else None

        for tr in table.select("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            key = _clean(th.get_text())

            if key == "店舗名" and not data.get(Schema.NAME):
                data[Schema.NAME] = _clean(td.get_text(" "))
            elif key == "会社名":
                data["会社名"] = _clean(td.get_text(" "))
            elif key == "連絡先":
                self._parse_contact_cell(td, data)
            elif key == "住所":
                self._parse_address_cell(td, data, pref)
            elif key == "営業時間":
                data[Schema.TIME] = _clean(td.get_text(" "))
            elif key == "休業日":
                data[Schema.HOLIDAY] = _clean(td.get_text(" "))
            elif key == "対応エリア":
                data["対応エリア"] = _clean(td.get_text(" "))
            elif key == "ホームページ":
                a = td.find("a", href=True)
                data[Schema.HP] = a["href"] if a else _clean(td.get_text(" "))
            elif key == "公式SNS":
                self._parse_sns_cell(td, data)

        if not data.get(Schema.NAME):
            return None
        return data

    def _parse_contact_cell(self, td, data: dict) -> None:
        freedial = td.select_one(".freedial a")
        if freedial:
            data["フリーダイヤル"] = _clean(freedial.get_text())

        for item in td.select(".telfax-item"):
            label = item.find("span")
            if not label:
                continue
            label_text = _clean(label.get_text())
            text = _clean(item.get_text(" ")).replace(label_text, "", 1).strip()
            if "電話" in label_text:
                a = item.find("a", href=True)
                data[Schema.TEL] = _clean(a.get_text()) if a else text
            elif "FAX" in label_text:
                data["FAX番号"] = text

        if not data.get(Schema.TEL):
            tel_link = td.select_one("a[href^='tel:']")
            if tel_link:
                data[Schema.TEL] = tel_link.get("href", "").replace("tel:", "").strip()

    def _parse_address_cell(self, td, data: dict, pref: str) -> None:
        inner = td.select_one(".shop-table-address span") or td
        for br in inner.find_all("br"):
            br.replace_with("\n")
        raw = inner.get_text("\n")
        lines = [_clean(ln) for ln in raw.split("\n") if _clean(ln)]
        if not lines:
            return
        full = "".join(lines)

        m_post = _POST_CODE_PATTERN.search(full)
        if m_post:
            data[Schema.POST_CODE] = m_post.group(1)
            full = _POST_CODE_PATTERN.sub("", full).strip()

        if pref and full.startswith(pref):
            data[Schema.ADDR] = full[len(pref):].strip()
        else:
            data[Schema.ADDR] = full

    def _parse_sns_cell(self, td, data: dict) -> None:
        for a in td.select("a[href]"):
            href = a.get("href", "")
            if not href:
                continue
            if "instagram.com" in href and not data.get(Schema.INSTA):
                data[Schema.INSTA] = href
            elif "facebook.com" in href and not data.get(Schema.FB):
                data[Schema.FB] = href
            elif ("line.me" in href or "lin.ee" in href) and not data.get(Schema.LINE):
                data[Schema.LINE] = href
            elif ("x.com/" in href or "twitter.com/" in href) and not data.get(Schema.X):
                data[Schema.X] = href
            elif "tiktok.com" in href and not data.get(Schema.TIKTOK):
                data[Schema.TIKTOK] = href
            elif "youtube.com" in href and not data.get("YouTube"):
                data["YouTube"] = href


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = LixilReformshopScraper()
    scraper.execute(f"{BASE_URL}/shop/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
