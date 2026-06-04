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


def _format_jp_digits(digits: str) -> str:
    """日本の電話番号数字列を簡易的にハイフン整形する"""
    if len(digits) == 11:
        # 070/080/090/050/0120 など
        return f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"
    if len(digits) == 10:
        # 03/06 は 2-4-4、それ以外は 3-3-4 寄せ
        if digits.startswith(("03", "06")):
            return f"{digits[:2]}-{digits[2:6]}-{digits[6:]}"
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    return digits


def _normalize_jp_country_code_tel(s: str) -> str:
    """
    +81 / 81 / 0081 で始まる日本番号を国内表記に戻す。

    例:
      81-70-3148-9655 -> 070-3148-9655
      +81-70-3148-9655 -> 070-3148-9655
      0081-70-3148-9655 -> 070-3148-9655
      81-3-5443-8770 -> 03-5443-8770
      81-06-7166-6821 -> 06-7166-6821
      819086205653 -> 090-8620-5653
    """
    raw = s.strip()
    digits = re.sub(r"\D", "", raw)

    if raw.startswith("+81"):
        rest = re.sub(r"\D", "", raw[3:])
    elif digits.startswith("0081"):
        rest = digits[4:]
    elif digits.startswith("81") and len(digits) in (11, 12, 13):
        rest = digits[2:]
    else:
        return s

    if not rest:
        return s

    # 81-06-... のように、81の後ろに既に0がある場合はそのまま
    if rest.startswith("0"):
        jp_digits = rest
    else:
        jp_digits = "0" + rest

    # 国内番号として10桁または11桁なら整形して返す
    if len(jp_digits) in (10, 11):
        return _format_jp_digits(jp_digits)

    return s


def _clean_tel(text) -> str:
    """TEL専用クリーニング。無効値は空欄化し、日本の国番号付きは国内表記へ修正する。"""
    if text is None:
        return ""

    s = _clean(text)
    if not s:
        return ""

    # ハイフン・ダッシュ類を半角ハイフンに寄せる
    s = s.translate(str.maketrans({
        "－": "-",
        "ー": "-",
        "―": "-",
        "−": "-",
        "‐": "-",
        "–": "-",
        "—": "-",
    }))

    # TEL内の空白は除去
    s = re.sub(r"\s+", "", s)

    # まず日本の国番号付きTELだけ国内表記へ戻す
    s = _normalize_jp_country_code_tel(s)

    # ハイフンだけは無効
    if re.fullmatch(r"-+", s):
        return ""

    digits = re.sub(r"\D", "", s)

    # 数字がない
    if not digits:
        return ""

    # ゼロだけのダミー番号
    if set(digits) == {"0"}:
        return ""

    # 短すぎるものは無効
    if len(digits) < 10:
        return ""

    return s


class MonoIprosScraper(StaticCrawler):
    """イプロスものづくり 企業情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["FAX", "主要取引先"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        yielded = 0

        for page in range(1, MAX_PAGES + 1):
            list_url = urljoin(BASE_URL, LIST_PATH) if page == 1 else f"{BASE_URL}{LIST_PATH}?p={page}"
            soup = self.get_soup(list_url)
            if soup is None:
                break

            items = soup.select("section.search-result-company-item")
            if not items:
                break

            # この一覧ページ内の詳細URLだけを収集する
            page_urls: list[str] = []
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
                page_urls.append(full)

            self.logger.info("一覧ページ %d: 企業 %d 件 (新規)", page, len(page_urls))

            if not page_urls:
                break

            # 収集した詳細URLをすぐ取得し、取れたら即 yield する
            for detail_url in page_urls:
                try:
                    item = self._scrape_detail(detail_url)
                    if item:
                        yielded += 1
                        yield item
                except Exception as e:
                    self.logger.warning("詳細取得失敗 (スキップ): %s — %s", detail_url, e)
                    continue
                time.sleep(self.DELAY)

            self.logger.info("一覧ページ %d まで完了: 累計 yield %d 件", page, yielded)

            time.sleep(self.DELAY)

        self.total_items = yielded
        self.logger.info("全件取得完了: %d 件", yielded)

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
                data[Schema.TEL] = _clean_tel(tel_m.group(1))
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
