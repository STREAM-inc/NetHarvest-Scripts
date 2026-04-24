"""
suumo【不動産会社ガイド】 — SUUMO 不動産会社ガイド (エリア別分譲実績)

取得対象:
    - /fudousankaisha/ をエントリに、地域ランディング → エリア別／テーマ別
      一覧 → 会社詳細 (会社概要タブ JJ081FD006) をクロールし、
      不動産会社の基本情報 (名称・住所・TEL・代表者・資本金・設立・事業内容等) を収集する。

取得フロー:
    1. /fudousankaisha/ から 7 つの地域ページ (hokkaido, tohoku, kanto, ...) を取得
    2. 各地域ページから
       - 直接掲載されている会社詳細リンク (JJ081FD001) を収集
       - /area/guide_*.html, /theme/guide_*.html のサブページ URL を収集
    3. 各サブページから追加の会社詳細リンクを収集
    4. hp パラメータで重複排除
    5. 会社概要タブ (JJ081FD006) を取得して各社の詳細フィールドを抽出

実行方法:
    python scripts/sites/realestate/suumo_2.py
    python bin/run_flow.py --site-id suumo_2
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse, parse_qs

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://suumo.jp"
ENTRY_URL = "https://suumo.jp/fudousankaisha/"

REGIONS = ["hokkaido", "tohoku", "kanto", "tokai", "chugoku", "kansai", "kyushu"]

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_CODE_PATTERN = re.compile(r"〒?\s*(\d{3}-\d{4})")
_TEL_PATTERN = re.compile(r"\[電話番号\]\s*([0-9０-９\-\(\)（）\s]+)")
_ADDR_PATTERN = re.compile(r"\[所在地\]\s*([^\[]+)")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class Suumo2Scraper(StaticCrawler):
    """SUUMO 不動産会社ガイド (地域ランディング経由) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "売上高",
        "支店",
        "関連会社",
        "ブランド名",
        "免許番号",
        "キャッチフレーズ",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        entry = url if url else ENTRY_URL
        list_page_urls = self._collect_list_pages(entry)
        self.logger.info("一覧ページ数: %d", len(list_page_urls))

        companies: dict[str, tuple[str, str]] = {}
        for list_url in list_page_urls:
            for hp, ar, catch in self._extract_companies_from_list(list_url):
                if hp not in companies:
                    companies[hp] = (ar, catch)

        self.total_items = len(companies)
        self.logger.info("ユニーク会社数: %d", self.total_items)

        for hp, (ar, catch) in companies.items():
            detail_url = f"{BASE_URL}/jj/guide/shosai/JJ081FD006/?ar={ar}&hp={hp}"
            try:
                item = self._scrape_detail(detail_url, hp=hp, catchphrase=catch)
                if item:
                    yield item
            except Exception as exc:
                self.logger.warning("詳細取得失敗 hp=%s: %s", hp, exc)
                continue

    def _collect_list_pages(self, entry_url: str) -> list[str]:
        pages: list[str] = []
        seen: set[str] = set()

        entry_soup = self.get_soup(entry_url)
        if entry_soup is None:
            return pages

        region_urls: list[str] = []
        for a in entry_soup.select("a[href]"):
            href = a.get("href", "").strip()
            if not href:
                continue
            m = re.match(r"^/fudousankaisha/([^/]+)/?$", href)
            if m and m.group(1) in REGIONS:
                full = urljoin(BASE_URL, href)
                if full not in seen:
                    seen.add(full)
                    region_urls.append(full)

        for region_url in region_urls:
            if region_url not in pages:
                pages.append(region_url)
            region_soup = self.get_soup(region_url)
            if region_soup is None:
                continue
            for a in region_soup.select("a[href]"):
                href = a.get("href", "").strip()
                if not href:
                    continue
                if re.search(r"/fudousankaisha/[^/]+/(area|theme)/guide_[^\"']+\.html", href):
                    full = urljoin(BASE_URL, href).split("#")[0]
                    if full not in seen:
                        seen.add(full)
                        pages.append(full)

        return pages

    def _extract_companies_from_list(self, list_url: str) -> list[tuple[str, str, str]]:
        """Return [(hp, ar, catchphrase)] from a list-type page."""
        results: list[tuple[str, str, str]] = []
        soup = self.get_soup(list_url)
        if soup is None:
            return results

        for block in soup.select("div.fr.pr.w620"):
            a = block.select_one("h3 a[href]")
            if not a:
                continue
            href = a.get("href", "")
            qs = parse_qs(urlparse(href).query)
            hp = (qs.get("hp") or [""])[0]
            ar = (qs.get("ar") or [""])[0]
            if not hp:
                continue
            catch_el = block.select_one("p.pV15.bld")
            catch = _clean(catch_el.get_text(" ", strip=True)) if catch_el else ""
            results.append((hp, ar, catch))

        if not results:
            for a in soup.select("a[href*='JJ081FD001']"):
                href = a.get("href", "")
                qs = parse_qs(urlparse(href).query)
                hp = (qs.get("hp") or [""])[0]
                ar = (qs.get("ar") or [""])[0]
                if hp:
                    results.append((hp, ar, ""))

        return results

    def _scrape_detail(self, url: str, hp: str, catchphrase: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}
        if catchphrase:
            data["キャッチフレーズ"] = catchphrase

        name_el = soup.select_one("h3.mT35.h3Ttl")
        if name_el:
            data[Schema.NAME] = _clean(name_el.get_text())

        table = soup.select_one("table.wFull.mT25.fs14") or soup.find(
            "table", class_=lambda c: c and "wFull" in c and "mT25" in c
        )
        if table:
            for tr in table.find_all("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                key = _clean(th.get_text(" ", strip=True))
                val = _clean(td.get_text(" ", strip=True))
                if key == "設立":
                    data[Schema.OPEN_DATE] = val
                elif key == "資本金":
                    data[Schema.CAP] = val
                elif key == "代表者名":
                    data[Schema.REP_NM] = val
                elif key == "従業員数":
                    data[Schema.EMP_NUM] = val
                elif key == "事業内容":
                    data[Schema.LOB] = val
                elif key == "売上高":
                    data["売上高"] = val
                elif key == "支店":
                    data["支店"] = val
                elif key == "関連会社":
                    data["関連会社"] = val
                elif key == "ブランド名":
                    data["ブランド名"] = val

        box = soup.select_one("div.w888.pT13.pH15.pB15.fs14.bdGuideDGray")
        if box:
            raw = _clean(box.get_text(" ", strip=True))
            m_addr = _ADDR_PATTERN.search(raw)
            if m_addr:
                addr_raw = _clean(m_addr.group(1))
                m_post = _POST_CODE_PATTERN.search(addr_raw)
                if m_post:
                    data[Schema.POST_CODE] = m_post.group(1)
                    addr_raw = _POST_CODE_PATTERN.sub("", addr_raw).strip()
                addr_raw = re.sub(r"〒\s*$", "", addr_raw).strip()
                m_pref = _PREF_PATTERN.match(addr_raw)
                if m_pref:
                    data[Schema.PREF] = m_pref.group(1)
                    data[Schema.ADDR] = addr_raw[m_pref.end():].strip()
                else:
                    data[Schema.ADDR] = addr_raw
            m_tel = _TEL_PATTERN.search(raw)
            if m_tel:
                data[Schema.TEL] = _clean(m_tel.group(1))
            license_part = _ADDR_PATTERN.split(raw)[0].strip() if _ADDR_PATTERN.search(raw) else raw
            if license_part:
                data["免許番号"] = license_part

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Suumo2Scraper()
    scraper.execute(ENTRY_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
