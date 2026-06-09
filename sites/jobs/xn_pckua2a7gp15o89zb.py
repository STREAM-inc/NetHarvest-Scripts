"""
求人ボックス【外国人工場夜勤】スクレイパー

取得対象:
    "外国人 工場 夜勤" の条件で検索した求人結果（全国）

取得フロー:
    /外国人+工場+夜勤の仕事 → 一覧カード (section.p-result_card) を解析
    → data-func-show-arg JSON から基本情報を抽出
    → 内部詳細ページ (/jb|/jbi|/jbn) があれば補完
    → ?pg=N でページネーション、URL重複は除外

取得カラム (Schema のみ、EXTRA_COLUMNS なし):
    - Schema.URL    : 求人URL
    - Schema.NAME   : 企業名
    - Schema.PREF   : 都道府県
    - Schema.ADDR   : 住所 (勤務地)
    - Schema.TEL    : 電話番号 (詳細ページにある場合のみ)
    - Schema.HP     : ホームページURL (詳細ページにある場合のみ)
    - Schema.REP_NM : 代表者名 (詳細ページにある場合のみ)

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/xn_pckua2a7gp15o89zb.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id xn_pckua2a7gp15o89zb
"""

import html
import json
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://xn--pckua2a7gp15o89zb.com"
# "外国人+工場+夜勤の仕事" を URL エンコードした検索結果URL
LIST_URL = (
    "https://xn--pckua2a7gp15o89zb.com/"
    "%E5%A4%96%E5%9B%BD%E4%BA%BA+%E5%B7%A5%E5%A0%B4+%E5%A4%9C%E5%8B%A4"
    "%E3%81%AE%E4%BB%95%E4%BA%8B"
)

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_DETAIL_PATH_RE = re.compile(r"^/(jb|jbi|jbn)/[0-9a-z]+$", re.I)

_URL_IN_TEXT_RE = re.compile(r"https?://[^\s<>\"']+")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def _extract_pref(text: str) -> str:
    text = _clean(text)
    if not text:
        return ""
    m = _PREF_PATTERN.search(text)
    return m.group(1) if m else ""


def _safe_json_loads(text: str) -> dict:
    try:
        return json.loads(text)
    except Exception:
        return {}


def _abs_url(href: str) -> str:
    if not href:
        return ""
    return urljoin(BASE_URL, href)


def _is_internal_detail_url(url: str) -> bool:
    if not url:
        return False
    parsed = urlparse(url)
    if parsed.netloc and parsed.netloc != urlparse(BASE_URL).netloc:
        return False
    return bool(_DETAIL_PATH_RE.match(parsed.path))


class KyujinBoxGaikokujinKoujouYakinScraper(StaticCrawler):
    """求人ボックス【外国人工場夜勤】スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS: list[str] = []

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_keys: set[str] = set()
        seen_tels: set[str] = set()
        page = 1

        while True:
            list_url = f"{LIST_URL}?pg={page}" if page > 1 else LIST_URL
            soup = self.get_soup(list_url)
            if soup is None:
                self.logger.warning("一覧取得失敗: %s", list_url)
                break

            if page == 1:
                num_el = soup.select_one("div.p-resultArea_num")
                if num_el:
                    m = re.search(r"([\d,]+)\s*件", num_el.get_text(" ", strip=True))
                    if m:
                        self.total_items = int(m.group(1).replace(",", ""))
                if not self.total_items:
                    self.total_items = 4661

            cards = soup.select("section.p-result_card")
            if not cards:
                self.logger.info("pg%d: カードなし、終了", page)
                break

            page_count = 0

            for card in cards:
                try:
                    item = self._parse_card(card, list_url)
                    if not item:
                        continue

                    dedupe_key = _clean(item.get(Schema.URL, "")) or _clean(
                        f"{item.get(Schema.NAME, '')}|{item.get(Schema.ADDR, '')}"
                    )
                    if not dedupe_key or dedupe_key in seen_keys:
                        continue

                    tel_key = re.sub(r"\D", "", _clean(item.get(Schema.TEL, "")))
                    if tel_key and tel_key in seen_tels:
                        continue

                    seen_keys.add(dedupe_key)
                    if tel_key:
                        seen_tels.add(tel_key)
                    page_count += 1
                    yield item

                except Exception as e:
                    self.logger.warning("カード解析失敗: %s", e)
                    continue

            self.logger.info("pg%d: %d件出力", page, page_count)

            next_link = soup.select_one("a.c-pager_btn--next")
            if not next_link:
                self.logger.info("pg%d: 次ページなし、終了", page)
                break

            page += 1

    def _parse_card(self, card, list_url: str) -> dict | None:
        a_tag = card.select_one("h2.p-result_title--ver2 a.p-result_title_link")
        if not a_tag:
            return None

        href = a_tag.get("href", "")
        raw_url = _abs_url(href)
        preview = self._extract_preview_json(a_tag)

        company = _clean(
            preview.get("company")
            or self._sel_text(card, "p.p-result_company")
        )

        work_area = _clean(
            preview.get("workArea")
            or self._sel_text(card, "li.p-result_area")
        )

        item: dict = {
            Schema.URL: raw_url or list_url,
            Schema.NAME: company,
            Schema.ADDR: work_area,
        }

        pref = _extract_pref(work_area)
        if pref:
            item[Schema.PREF] = pref

        detail_url = ""
        rd_url = _clean(preview.get("rdUrl"))

        if rd_url:
            rd_abs = _abs_url(rd_url)
            if _is_internal_detail_url(rd_abs):
                detail_url = rd_abs

        if not detail_url and _is_internal_detail_url(raw_url):
            detail_url = raw_url

        if detail_url:
            detail_data = self._scrape_detail(detail_url)
            if detail_data:
                for k, v in detail_data.items():
                    if _clean(v) and not _clean(item.get(k)):
                        item[k] = v

        if not _clean(item.get(Schema.NAME)):
            return None

        if not _clean(item.get(Schema.PREF)):
            item[Schema.PREF] = _extract_pref(item.get(Schema.ADDR, ""))

        return item

    def _extract_preview_json(self, a_tag) -> dict:
        raw = a_tag.get("data-func-show-arg", "")
        if not raw:
            return {}

        outer = _safe_json_loads(raw)
        inner = outer.get("json")
        if not inner:
            return {}

        if isinstance(inner, dict):
            return inner

        try:
            inner = html.unescape(inner)
            return _safe_json_loads(inner)
        except Exception:
            return {}

    def _scrape_detail(self, url: str) -> dict:
        soup = self.get_soup(url)
        if soup is None:
            return {}

        data: dict = {Schema.URL: url}

        company_el = soup.select_one("p.p-detail_head_company")
        if company_el:
            data[Schema.NAME] = _clean(company_el.get_text(" "))

        tel_el = soup.select_one("span.p-detail_tel_num")
        if tel_el:
            tel = _clean(tel_el.get_text(" "))
            if tel:
                data[Schema.TEL] = tel

        for dl in soup.select("dl.p-detail_table"):
            dts = dl.select("dt.p-detail_table_title")
            dds = dl.select("dd.p-detail_table_data")
            for dt, dd in zip(dts, dds):
                key = _clean(dt.get_text(" "))
                val = _clean(dd.get_text(" "))
                if not key or not val:
                    continue

                if "勤務地" in key or "所在地" in key or "住所" in key:
                    data[Schema.ADDR] = val
                    pref = _extract_pref(val)
                    if pref:
                        data[Schema.PREF] = pref

                elif "代表者" in key or "代表取締役" in key or "代表" == key:
                    data[Schema.REP_NM] = val

                elif (
                    "ホームページ" in key
                    or "HP" in key.upper()
                    or "ＨＰ" in key
                    or "URL" in key.upper()
                    or "ＵＲＬ" in key
                    or "ウェブサイト" in key
                    or "Webサイト" in key
                    or "WEBサイト" in key.upper()
                ):
                    hp_link = dd.select_one("a[href^=http]")
                    hp_url = ""
                    if hp_link:
                        hp_url = _clean(hp_link.get("href", ""))
                    if not hp_url:
                        m = _URL_IN_TEXT_RE.search(val)
                        if m:
                            hp_url = m.group(0)
                    if hp_url:
                        data[Schema.HP] = hp_url

        return data

    def _sel_text(self, root, selector: str) -> str:
        el = root.select_one(selector)
        return _clean(el.get_text(" ", strip=True)) if el else ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = KyujinBoxGaikokujinKoujouYakinScraper()
    scraper.execute(LIST_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
