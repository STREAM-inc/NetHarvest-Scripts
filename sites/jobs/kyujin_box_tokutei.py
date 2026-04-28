"""
求人ボックス（特定技能の仕事）スクレイパー 改善版

改善ポイント:
- /jbi/ だけでなく /jb/ /jbn/ /rd/ も対象にする
- 一覧カード内の data-func-show-arg から埋め込みJSONを抽出して取りこぼしを減らす
- 一覧だけで取れる項目は一覧から取り、足りない項目だけ詳細ページで補完する
- ページ内の総件数を self.total_items に設定
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
LIST_URL = "https://xn--pckua2a7gp15o89zb.com/%E7%89%B9%E5%AE%9A%E6%8A%80%E8%83%BD%E3%81%AE%E4%BB%95%E4%BA%8B"

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_DETAIL_PATH_RE = re.compile(r"^/(jb|jbi|jbn)/[0-9a-z]+$", re.I)


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


class KyujinBoxTokuteiScraper(StaticCrawler):
    """求人ボックス（特定技能の仕事）スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "求人タイトル",
        "雇用形態",
        "給与",
        "勤務地",
        "勤務時間",
        "仕事内容",
        "応募資格",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_keys: set[str] = set()
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
                    self.total_items = 42000

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

                    # 重複排除キー
                    dedupe_key = (
                        item.get("取得URL")
                        or item.get(Schema.URL)
                        or f"{item.get(Schema.NAME, '')}|{item.get('求人タイトル', '')}|{item.get('勤務地', '')}"
                    )
                    dedupe_key = _clean(dedupe_key)
                    if not dedupe_key or dedupe_key in seen_keys:
                        continue

                    seen_keys.add(dedupe_key)
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

        # 一覧の埋め込みJSONを優先して読む
        preview = self._extract_preview_json(a_tag)

        title = _clean(
            preview.get("title")
            or preview.get("originalTitle")
            or a_tag.select_one("span.p-result_name").get_text(" ", strip=True)
            if a_tag.select_one("span.p-result_name")
            else a_tag.get_text(" ", strip=True)
        )

        company = _clean(
            preview.get("company")
            or self._sel_text(card, "p.p-result_company")
        )

        work_area = _clean(
            preview.get("workArea")
            or self._sel_text(card, "li.p-result_area")
        )

        payment = _clean(
            preview.get("payment")
            or self._sel_text(card, "li.p-result_pay")
        )

        employ_type = _clean(
            preview.get("employType")
            or self._sel_text(card, "li.p-result_employType")
        )

        description = _clean(
            preview.get("firstYamlContent")
            or self._sel_text(card, "p.p-result_lines")
        )

        item: dict = {
            Schema.URL: raw_url or list_url,
            "取得URL": raw_url or list_url,
            "求人タイトル": title,
            Schema.NAME: company,
            "勤務地": work_area,
            "給与": payment,
            "雇用形態": employ_type,
            "仕事内容": description,
        }

        pref = _extract_pref(work_area)
        if pref:
            item[Schema.PREF] = pref

        # 詳細ページ候補
        detail_url = ""
        rd_url = _clean(preview.get("rdUrl"))
        outer_url = _clean(preview.get("url"))

        if rd_url:
            rd_abs = _abs_url(rd_url)
            if _is_internal_detail_url(rd_abs):
                detail_url = rd_abs

        if not detail_url and _is_internal_detail_url(raw_url):
            detail_url = raw_url

        # 詳細で不足分を補完
        if detail_url:
            detail_data = self._scrape_detail(detail_url)
            if detail_data:
                for k, v in detail_data.items():
                    if _clean(v) and not _clean(item.get(k)):
                        item[k] = v

        # 取得URLは外部遷移先が分かるならそちら優先
        if outer_url:
            item["取得URL"] = outer_url

        # 必須最低限
        if not _clean(item.get("求人タイトル")):
            return None

        # 名称が空なら company fallback をもう一回
        if not _clean(item.get(Schema.NAME)):
            item[Schema.NAME] = company

        # 都道府県がまだ空なら仕事内容やタイトルからは取らない
        if not _clean(item.get(Schema.PREF)):
            item[Schema.PREF] = _extract_pref(item.get("勤務地", ""))

        return item

    def _extract_preview_json(self, a_tag) -> dict:
        """
        data-func-show-arg='{"target":"...", "uid":"...", "json":"{...escaped...}"}'
        の中の json を取り出して dict 化する
        """
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
            # HTML entity 対応
            inner = html.unescape(inner)
            return _safe_json_loads(inner)
        except Exception:
            return {}

    def _scrape_detail(self, url: str) -> dict:
        soup = self.get_soup(url)
        if soup is None:
            return {}

        data: dict = {
            Schema.URL: url,
        }

        # タイトル
        title_el = soup.select_one("p.p-detail_head_title, h1.p-detail_head_title")
        if title_el:
            data["求人タイトル"] = _clean(title_el.get_text(" "))

        # 会社名
        company_el = soup.select_one("p.p-detail_head_company")
        if company_el:
            data[Schema.NAME] = _clean(company_el.get_text(" "))

        # TEL
        tel_el = soup.select_one("span.p-detail_tel_num")
        if tel_el:
            tel = _clean(tel_el.get_text(" "))
            if tel:
                data[Schema.TEL] = tel

        # dl テーブル
        for dl in soup.select("dl.p-detail_table"):
            dts = dl.select("dt.p-detail_table_title")
            dds = dl.select("dd.p-detail_table_data")
            for dt, dd in zip(dts, dds):
                key = _clean(dt.get_text(" "))
                val = _clean(dd.get_text(" "))
                if not key or not val:
                    continue

                if "勤務地" in key:
                    data["勤務地"] = val
                    pref = _extract_pref(val)
                    if pref:
                        data[Schema.PREF] = pref
                elif "給与" in key or "月給" in key or "時給" in key or "年収" in key or "日給" in key:
                    data["給与"] = val
                elif "雇用形態" in key:
                    data["雇用形態"] = val
                elif "勤務時間" in key:
                    data["勤務時間"] = val
                elif "仕事内容" in key:
                    data["仕事内容"] = val
                elif "応募資格" in key or "対象となる方" in key:
                    data["応募資格"] = val

        # セクション見出し
        for sec in soup.select("section.p-detail_panel"):
            heading_el = sec.select_one("h2.p-detail_title")
            if not heading_el:
                continue

            heading = _clean(heading_el.get_text(" "))
            text_parts = []

            for el in sec.select("p.p-detail_line, li, div.p-detail_box, dd, td"):
                txt = _clean(el.get_text(" "))
                if txt:
                    text_parts.append(txt)

            sec_text = _clean(" ".join(text_parts))
            if not sec_text:
                continue

            if "仕事内容" in heading and not data.get("仕事内容"):
                data["仕事内容"] = sec_text
            elif ("応募資格" in heading or "対象となる方" in heading or "必須" in heading) and not data.get("応募資格"):
                data["応募資格"] = sec_text
            elif "勤務時間" in heading and not data.get("勤務時間"):
                data["勤務時間"] = sec_text

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

    scraper = KyujinBoxTokuteiScraper()
    scraper.execute(LIST_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")