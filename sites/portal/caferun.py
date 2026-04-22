"""
カフェるん — コンカフェ・メイドカフェ求人情報サイト

取得対象:
    - 全国の掲載店舗情報 (店名、住所、電話番号、営業時間、SNS等)

取得フロー:
    1. /shoplist/?page=N を 20件単位で巡回し、各店舗の詳細URLを収集
    2. 各詳細ページ (/shop/{ID}/) から全フィールドを抽出

実行方法:
    # ローカルテスト
    python scripts/sites/portal/caferun.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id caferun
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://caferun.jp"
START_URL = f"{BASE_URL}/shoplist/"
ITEMS_PER_PAGE = 20
MAX_PAGES = 200

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_CODE_PATTERN = re.compile(r"〒?\s*(\d{3}-\d{4})")
_TEL_PATTERN = re.compile(r"0\d{1,4}-\d{1,4}-\d{4}")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


class CaferunScraper(StaticCrawler):
    """カフェるん (caferun.jp) 店舗情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["エリア", "ジャンル", "最寄駅", "時給", "採用希望"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()

        for page_idx in range(MAX_PAGES):
            page_param = page_idx * ITEMS_PER_PAGE
            list_url = START_URL if page_idx == 0 else f"{START_URL}?page={page_param}"
            soup = self.get_soup(list_url)
            if soup is None:
                break

            if page_idx == 0:
                total_el = soup.select_one("span.hit_total.pc")
                if total_el:
                    m = re.search(r"(\d[\d,]*)", total_el.get_text())
                    if m:
                        self.total_items = int(m.group(1).replace(",", ""))

            detail_urls: list[str] = []
            for li in soup.select("ul.shop_search_list > li.shop_box"):
                h2_a = li.select_one("h2 a[href]")
                if not h2_a:
                    continue
                href = h2_a.get("href", "")
                if not re.match(r"^/shop/\d+/?$", href):
                    continue
                full = urljoin(BASE_URL, href)
                if full not in seen:
                    seen.add(full)
                    detail_urls.append(full)

            if not detail_urls:
                break

            for detail_url in detail_urls:
                try:
                    item = self._scrape_detail(detail_url)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.warning("詳細ページ取得失敗 (スキップ): %s — %s", detail_url, e)
                    continue

            next_link = soup.select_one("ul.pageing li.next a[rel='next']")
            if not next_link:
                break

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}

        h1 = soup.select_one("h1")
        if not h1:
            return None
        data[Schema.NAME] = _clean(h1.get_text())

        area_genre = soup.select_one("span.shop_area_genre")
        if area_genre:
            data[Schema.CAT_SITE] = _clean(area_genre.get_text())
            area_text = ""
            genre_text = ""
            j_type = area_genre.select_one(".j_type")
            if j_type:
                genre_text = _clean(j_type.get_text())
                full = _clean(area_genre.get_text())
                area_text = _clean(full.replace(genre_text, "").rstrip("/ ").rstrip("/"))
            data["エリア"] = area_text
            data["ジャンル"] = genre_text

        self._extract_job_info(soup, data)
        self._extract_basic_info(soup, data)
        self._extract_social_links(soup, data)
        self._extract_tel(soup, data)

        return data

    def _extract_job_info(self, soup, data: dict) -> None:
        """求人情報テーブル (時給、採用希望、勤務地、面接場所) を抽出"""
        section = soup.select_one("div.shop_job_info section")
        if not section:
            return

        for tr in section.select("table tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            key = _clean(th.get_text())

            if "時" in key and "給" in key:
                data["時給"] = _clean(td.get_text(" "))
            elif "採用希望" in key:
                data["採用希望"] = _clean(td.get_text(" "))
            elif "勤 務 地" in key or "勤務地" in key:
                self._parse_address_cell(td, data)
                nearest = td.select_one("dl.nearest dd")
                if nearest:
                    data["最寄駅"] = _clean(nearest.get_text(" "))
            elif "面接場所" in key and not data.get(Schema.ADDR):
                self._parse_address_cell(td, data)

    def _parse_address_cell(self, td, data: dict) -> None:
        """td のテキストから郵便番号・住所・都道府県を抽出"""
        for br in td.find_all("br"):
            br.replace_with("\n")
        text = td.get_text("\n")
        lines = [_clean(ln) for ln in text.split("\n") if _clean(ln)]
        addr_lines: list[str] = []
        for ln in lines:
            if "地図" in ln or "交通" in ln:
                continue
            m_post = _POST_CODE_PATTERN.search(ln)
            if m_post and not data.get(Schema.POST_CODE):
                data[Schema.POST_CODE] = m_post.group(1)
                ln = _POST_CODE_PATTERN.sub("", ln).strip()
                if not ln:
                    continue
            addr_lines.append(ln)

        full_addr = "".join(addr_lines).strip()
        if not full_addr:
            return

        m_pref = _PREF_PATTERN.match(full_addr)
        if m_pref:
            data[Schema.PREF] = m_pref.group(1)
            data[Schema.ADDR] = full_addr[m_pref.end():].strip()
        else:
            data[Schema.ADDR] = full_addr

    def _extract_basic_info(self, soup, data: dict) -> None:
        """お店の情報テーブル (営業時間、定休日、公式サイト、SNS) を抽出"""
        h3 = soup.find("h3", id="shop_basic_infomation")
        if not h3:
            return
        table = h3.find_next("table")
        if not table:
            return

        for tr in table.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            key = _clean(th.get_text())
            val_text = _clean(td.get_text(" "))
            link = td.find("a", href=True)
            href = link["href"] if link else ""

            if "営業時間" in key:
                data[Schema.TIME] = val_text
            elif "定休日" in key:
                data[Schema.HOLIDAY] = val_text
            elif "公式サイト" in key or "HP" in key or "ホームページ" in key:
                data[Schema.HP] = href or val_text
            elif "Twitter" in key or "Ｘ" in key or key.startswith("X"):
                data[Schema.X] = href or val_text
            elif "Instagram" in key or "インスタ" in key:
                data[Schema.INSTA] = href or val_text
            elif "TikTok" in key:
                data[Schema.TIKTOK] = href or val_text
            elif "Facebook" in key:
                data[Schema.FB] = href or val_text
            elif "LINE" in key:
                data[Schema.LINE] = href or val_text

    def _extract_social_links(self, soup, data: dict) -> None:
        """ページ全体からSNSリンクをフォールバック抽出"""
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not data.get(Schema.LINE) and ("line.me" in href or "lin.ee" in href):
                data[Schema.LINE] = href
            elif not data.get(Schema.X) and ("x.com/" in href or "twitter.com/" in href):
                data[Schema.X] = href
            elif not data.get(Schema.INSTA) and "instagram.com/" in href:
                data[Schema.INSTA] = href
            elif not data.get(Schema.TIKTOK) and "tiktok.com/" in href:
                data[Schema.TIKTOK] = href
            elif not data.get(Schema.FB) and "facebook.com/" in href:
                data[Schema.FB] = href

    def _extract_tel(self, soup, data: dict) -> None:
        """電話番号を a[href^='tel:'] から抽出"""
        tel_link = soup.select_one("a[href^='tel:']")
        if tel_link:
            tel = tel_link.get("href", "").replace("tel:", "").strip()
            if tel:
                data[Schema.TEL] = tel
                return
        text = soup.get_text(" ")
        m = _TEL_PATTERN.search(text)
        if m:
            data[Schema.TEL] = m.group(0)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = CaferunScraper()
    scraper.execute(START_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
