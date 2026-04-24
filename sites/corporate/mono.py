"""
イプロスものづくり (mono.ipros.com) — 製造業向け企業情報スクレイパー

取得対象:
    - mono.ipros.com/search/company/ に掲載されている全企業 (約 47,509 件 / 223 ページ)
    - 1ページ 45件、?p=N でページネーション

取得フロー:
    /search/company/?p=N を 1..223 まで巡回 → 各 .search-result-company-item から
    /company/detail/{id}/ を抽出 → 詳細ページを取得 → CSV出力

実行方法:
    python scripts/sites/corporate/mono.py
    python bin/run_flow.py --site-id mono
"""

import re
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

BASE_URL = "https://mono.ipros.com"
LIST_PATH = "/search/company/"
MAX_PAGES = 300  # 実測 223 ページ、将来増加分のバッファ込み

_POST_CODE_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_TEL_RE = re.compile(r"TEL[:：]\s*([\d\-()+ 　]+)")
_FAX_RE = re.compile(r"FAX[:：]\s*([\d\-()+ 　]+)")

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


class MonoIprosScraper(StaticCrawler):
    """イプロスものづくり 企業情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["FAX", "主要取引先"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        detail_urls: list[str] = []

        for page in range(1, MAX_PAGES + 1):
            list_url = urljoin(BASE_URL, LIST_PATH) if page == 1 else f"{BASE_URL}{LIST_PATH}?p={page}"
            soup = self.get_soup(list_url)
            if soup is None:
                break

            items = soup.select("section.search-result-company-item")
            if not items:
                break

            new_count = 0
            for item in items:
                a = item.select_one("a.search-result-company-item__name-link")
                if not a or not a.get("href"):
                    continue
                full = urljoin(BASE_URL, a["href"])
                if not full.endswith("/"):
                    full += "/"
                if full in seen:
                    continue
                seen.add(full)
                detail_urls.append(full)
                new_count += 1

            self.logger.info(
                "一覧ページ %d: 企業 %d 件 (累計 %d 件)", page, new_count, len(detail_urls)
            )

            if new_count == 0:
                break

            time.sleep(self.DELAY)

        self.total_items = len(detail_urls)
        self.logger.info("詳細URL収集完了: %d 件", self.total_items)

        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("詳細取得失敗 (スキップ): %s — %s", detail_url, e)
                continue
            time.sleep(self.DELAY)

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {
            Schema.URL: url,
            Schema.NAME: "",
            Schema.POST_CODE: "",
            Schema.PREF: "",
            Schema.ADDR: "",
            Schema.TEL: "",
            Schema.EMP_NUM: "",
            Schema.LOB: "",
            Schema.CAT_SITE: "",
            "FAX": "",
            "主要取引先": "",
        }

        # --- 会社概要テーブル ---
        table = soup.select_one("table.company-detail__table")
        rows: dict[str, str] = {}
        if table:
            for tr in table.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                key = _clean(th.get_text())
                for br in td.find_all("br"):
                    br.replace_with("\n")
                val = _clean(td.get_text(" "))
                if key:
                    rows[key] = val

        data[Schema.NAME] = rows.get("企業名", "")
        data[Schema.EMP_NUM] = rows.get("従業員数", "")
        data[Schema.CAT_SITE] = rows.get("業種", "")
        data["主要取引先"] = rows.get("主要取引先", "")

        contact = rows.get("連絡先", "")
        if contact:
            pc = _POST_CODE_RE.search(contact)
            if pc:
                data[Schema.POST_CODE] = pc.group(1)

            address_line = contact
            if pc:
                address_line = address_line.replace(pc.group(0), "", 1)
            address_line = re.sub(r"地図で見る.*", "", address_line, flags=re.S)
            address_line = re.sub(r"TEL[:：].*", "", address_line, flags=re.S)
            address_line = _clean(address_line)

            pref_m = _PREF_PATTERN.match(address_line)
            if pref_m:
                data[Schema.PREF] = pref_m.group(1)
                data[Schema.ADDR] = _clean(address_line[pref_m.end():])
            else:
                data[Schema.ADDR] = address_line

            tel_m = _TEL_RE.search(contact)
            if tel_m:
                data[Schema.TEL] = _clean(tel_m.group(1))
            fax_m = _FAX_RE.search(contact)
            if fax_m:
                data["FAX"] = _clean(fax_m.group(1))

        # --- 事業内容 (section.company-info__item) ---
        for sec in soup.select("section.company-info__item"):
            heading = sec.select_one("h2")
            heading_text = _clean(heading.get_text()) if heading else ""
            if heading_text == "事業内容":
                p = sec.select_one("p.company-info__text")
                if p:
                    for br in p.find_all("br"):
                        br.replace_with("\n")
                    data[Schema.LOB] = _clean(p.get_text("\n"))
                break

        return data


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = MonoIprosScraper()
    scraper.execute(urljoin(BASE_URL, LIST_PATH))

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
