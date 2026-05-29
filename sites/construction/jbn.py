"""
JBN (一般社団法人JBN・全国工務店協会) — 正会員企業一覧スクレイパー

取得対象:
    - 正会員 (cat_member=member_01) 約 2,621 社
    - 会社名・ふりがな・住所(郵便番号/都道府県/番地)・TEL・代表者・HP・FAX
    - 加盟団体・所属組合・リフォームマーク有無

取得フロー:
    一覧 (/about/search/result/page/{N}/?cat_member=member_01) を全262ページ巡回
    → 各社の詳細ページ (/member/{slug}/) を取得して table[th/td] をパース

実行方法:
    python scripts/sites/construction/jbn.py
    python bin/run_flow.py --site-id jbn
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


BASE_URL = "https://www.jbn-support.jp"
LIST_URL = f"{BASE_URL}/about/search/result/?cat_member=member_01"

_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"[\s\xa0]+", " ", text).replace("　", " ").strip()


class JbnScraper(StaticCrawler):
    """JBN 正会員企業スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["FAX番号", "加盟団体・所属組合", "リフォームマーク"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls = self._collect_detail_urls()
        self.total_items = len(detail_urls)
        self.logger.info("詳細URL収集完了: %d 件", len(detail_urls))

        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception:
                self.logger.exception("詳細解析失敗: %s", detail_url)
                continue

    def _collect_detail_urls(self) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        page = 1
        while True:
            page_url = (
                LIST_URL
                if page == 1
                else f"{BASE_URL}/about/search/result/page/{page}/?cat_member=member_01"
            )
            soup = self.get_soup(page_url)
            if soup is None:
                self.logger.warning("ページ取得失敗: %s", page_url)
                break

            links = soup.select("td.companyName a[href]")
            if not links:
                self.logger.info("ページ %d: アイテム無し、終了", page)
                break

            added = 0
            for a in links:
                href = (a.get("href") or "").strip()
                if "/member/" not in href:
                    continue
                full = urljoin(BASE_URL, href)
                if full not in seen:
                    seen.add(full)
                    urls.append(full)
                    added += 1

            self.logger.info("ページ %d: %d 件追加 (累計 %d)", page, added, len(urls))
            if added == 0:
                break
            page += 1

        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        table = soup.select_one("table")
        if table is None:
            self.logger.warning("table 未検出: %s", url)
            return None

        item: dict = {Schema.URL: url}

        name = ""
        name_kana = ""
        reform_mark = ""
        post_code = ""
        pref = ""
        addr = ""
        tel = ""
        fax = ""
        rep_nm = ""
        hp = ""
        org = ""

        for tr in table.find_all("tr", recursive=False):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            key = _clean(th.get_text(" ", strip=True))

            if key == "会社名":
                kana_el = td.select_one(".nameRead")
                if kana_el:
                    name_kana = _clean(kana_el.get_text(" ", strip=True))
                    kana_el.extract()
                logo_el = td.select_one(".logo")
                td_classes = td.get("class") or []
                has_reform_logo = bool(td.select_one('img[src*="logo_reform_group"]'))
                if "gourpReformLogo" in td_classes or has_reform_logo:
                    reform_mark = "有"
                if logo_el:
                    logo_el.extract()
                name = _clean(td.get_text(" ", strip=True))

            elif key == "住所":
                raw = td.get_text("\n", strip=True)
                pm = _POST_RE.search(raw)
                if pm:
                    post_code = pm.group(1)
                    if "-" not in post_code:
                        post_code = f"{post_code[:3]}-{post_code[3:]}"
                without_post = _POST_RE.sub("", raw)
                addr_line = _clean(without_post.replace("\n", " "))
                pref_m = _PREF_RE.match(addr_line)
                if pref_m:
                    pref = pref_m.group(1)
                    addr = addr_line[pref_m.end():].strip()
                else:
                    addr = addr_line

            elif key == "URL":
                a = td.find("a", href=True)
                if a:
                    hp = a["href"].strip()
                else:
                    hp = _clean(td.get_text(" ", strip=True))

            elif key == "電話番号":
                tel = _clean(td.get_text(" ", strip=True))

            elif key == "FAX番号":
                fax = _clean(td.get_text(" ", strip=True))

            elif key == "代表者":
                rep_nm = _clean(td.get_text(" ", strip=True))

            elif key == "加盟団体・所属組合":
                org = _clean(td.get_text(" ", strip=True))

        if not name:
            h3 = soup.select_one("h3.companyName")
            if h3:
                name = _clean(h3.get_text(" ", strip=True))

        if not name:
            return None

        item[Schema.NAME] = name
        item[Schema.NAME_KANA] = name_kana
        item[Schema.PREF] = pref
        item[Schema.POST_CODE] = post_code
        item[Schema.ADDR] = addr
        item[Schema.TEL] = tel
        item[Schema.REP_NM] = rep_nm
        item[Schema.HP] = hp
        item["FAX番号"] = fax
        item["加盟団体・所属組合"] = org
        item["リフォームマーク"] = reform_mark

        return item


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = JbnScraper()
    scraper.execute(LIST_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
