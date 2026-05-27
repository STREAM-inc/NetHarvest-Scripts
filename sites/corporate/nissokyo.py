"""
日本倉庫協会 (nissokyo.or.jp) — 会員事業者一覧スクレイパー

取得対象:
    - 全国 3,508 件の会員倉庫事業者
    - 会社名・住所（郵便番号・都道府県・市区町村以降）・TEL・FAX・倉庫の種類・HP

取得フロー:
    /member_list/?page=N を 1〜351 ページ巡回し、テーブル各行からデータを抽出

実行方法:
    python scripts/sites/corporate/nissokyo.py
    python bin/run_flow.py --site-id nissokyo
"""

import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


BASE_URL = "https://www.nissokyo.or.jp"
LIST_URL = f"{BASE_URL}/member_list/"

_POST_RE = re.compile(r"〒?\s*(\d{3}[-‐−]\d{4})")
_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_TEL_RE = re.compile(r"TEL\s*([\d\-−‐]+)")
_FAX_RE = re.compile(r"FAX\s*([\d\-−‐]+)")


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"[\s　\xa0]+", " ", text).strip()


def _split_address(raw: str) -> tuple[str, str, str]:
    """住所文字列から (郵便番号, 都道府県, 住所本体) を返す。"""
    text = _clean(raw)
    post = ""
    m = _POST_RE.search(text)
    if m:
        post = m.group(1)
        text = (text[: m.start()] + text[m.end() :]).strip()
    text = re.sub(r"^〒\s*", "", text).strip()
    pref = ""
    pm = _PREF_RE.match(text)
    if pm:
        pref = pm.group(1)
        text = text[pm.end() :].strip()
    return post, pref, text


class NissokyScraper(StaticCrawler):
    """日本倉庫協会 会員事業者一覧スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["FAX番号", "倉庫の種類"]

    def parse(self, url: str):
        soup = self.get_soup(f"{LIST_URL}?page=1")
        if soup is None:
            return

        m = re.search(r"([\d,]+)件中", soup.get_text())
        if m:
            self.total_items = int(m.group(1).replace(",", ""))

        page = 1
        while True:
            if page > 1:
                soup = self.get_soup(f"{LIST_URL}?page={page}")
                if soup is None:
                    break

            rows = self._get_data_rows(soup)
            if not rows:
                break

            for row in rows:
                try:
                    item = self._parse_row(row, f"{LIST_URL}?page={page}")
                    if item:
                        yield item
                except Exception:
                    self.logger.exception("行解析失敗: page=%d", page)

            if not soup.select_one(f'a[href*="page={page + 1}"]'):
                break
            page += 1

    def _get_data_rows(self, soup):
        for table in soup.find_all("table"):
            rows = [tr for tr in table.find_all("tr") if len(tr.find_all("td")) >= 3]
            if rows:
                return rows
        return []

    def _parse_row(self, tr, page_url: str) -> dict | None:
        tds = tr.find_all("td")
        if len(tds) < 3:
            return None

        # td[0]: 会社名 + HP リンク（会社HPテキストのリンク）
        name_td = tds[0]
        hp = ""
        for a in name_td.find_all("a", href=True):
            href = a.get("href", "")
            if href.startswith("http"):
                hp = href.strip()
                a.decompose()
        name = _clean(name_td.get_text(" ", strip=True))
        if not name:
            return None

        # td[1]: 住所（〒付き）
        addr_raw = _clean(tds[1].get_text(" ", strip=True)) if len(tds) > 1 else ""
        post, pref, addr = _split_address(addr_raw)

        # td[2]: TEL / FAX
        tel, fax = "", ""
        if len(tds) > 2:
            tel_fax = _clean(tds[2].get_text(" ", strip=True))
            tm = _TEL_RE.search(tel_fax)
            fm = _FAX_RE.search(tel_fax)
            if tm:
                tel = tm.group(1).strip()
            if fm:
                fax = fm.group(1).strip()

        # td[3]: 倉庫の種類
        warehouse = _clean(tds[3].get_text(" ", strip=True)) if len(tds) > 3 else ""

        return {
            Schema.URL: page_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: hp,
            "FAX番号": fax,
            "倉庫の種類": warehouse,
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = NissokyScraper()
    scraper.execute(LIST_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
