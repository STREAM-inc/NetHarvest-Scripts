"""
求人＠works（北海道） — 北海道エリアの求人サイトのクローラー

対象サイト: https://kyujin-at.work/hokkaido/

取得対象:
    - 勤務地=北海道 (rct[area_code][]=01000) の全求人 (2026-11 時点 約83件)

取得フロー:
    1. {url}rct_list.php へ POST (cmn[mode]=top, rct[area_code][]=01000) で検索
       → 検索条件はサーバー側セッションに保存される (Cookie: JBRPTSN1803)
    2. 2ページ目以降は同一セッションで {url}rct_list.php?mode=ssuse&pn=N (20件/ページ)
    3. 一覧 ul.recruit-boxList の各カード → /detail/FJAC{数字}/ 詳細
    4. 詳細の table (募集要項 / 会社情報) を th/td で取得

備考:
    - 詳細の TEL は全求人共通の FREE JOB 受付窓口 (050-3150-0570) が掲載されている。
      掲載値はそのまま EXTRA「掲載TEL」に出し、Schema.TEL は共通窓口以外の番号のみ入れる。
    - 仕事内容・応募資格・職場の雰囲気等の自由記述は著作権リスクのため取得しない。
    - 利用規約ページは存在せず (個人情報保護方針のみ)、スクレイピング禁止条項は確認されない。

実行方法:
    python scripts/sites/jobs/works_2.py
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4
import requests

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_AREA_CODE = "01000"  # 北海道
_MAX_PAGES = 200  # 安全上限 (20件/ページ)
_COMMON_TEL = "050-3150-0570"  # FREE JOB 共通受付窓口

_DETAIL_RE = re.compile(r"/detail/(FJAC\d+)/")
_TEL_RE = re.compile(r"0\d{1,4}-\d{1,4}-\d{3,4}")
_ZIP_RE = re.compile(r"〒?\s*(\d{3}-\d{4})")
_PREF_RE = re.compile(r"^(北海道|東京都|京都府|大阪府|.{2,3}県)")


def _clean(text) -> str:
    if text is None:
        return ""
    return re.sub(r"[\s　]+", " ", str(text)).strip()


def _split_address(raw: str) -> tuple[str, str, str]:
    """'〒098-4582 北海道 稚内市 …' → (郵便番号, 都道府県, 住所)"""
    text = _clean(raw)
    zip_code = ""
    m = _ZIP_RE.search(text)
    if m:
        zip_code = m.group(1)
        text = _clean(text[m.end():])
    pref = ""
    m = _PREF_RE.match(text)
    if m:
        pref = m.group(1)
        text = _clean(text[m.end():])
    return zip_code, pref, text


class Works2Scraper(StaticCrawler):
    """求人＠works（北海道） スクレイパー"""

    DELAY = 1.0  # 備考: アクセス間隔0.5〜1秒

    EXTRA_COLUMNS = [
        "求人ID",
        "求人タイトル",
        "雇用形態",
        "給与",
        "勤務地",
        "掲載TEL",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        list_url = urljoin(url, "rct_list.php")
        seen: set[str] = set()

        soup = self._post_search(list_url)
        for pn in range(1, _MAX_PAGES + 1):
            if pn > 1:
                soup = self.get_soup(f"{list_url}?mode=ssuse&pn={pn}")
            if soup is None:
                break

            cards = self._list_cards(soup)
            new_cards = [c for c in cards if c[0] not in seen]
            if not new_cards:
                break

            for job_id, href, title in new_cards:
                seen.add(job_id)
                item = self._parse_detail(urljoin(url, href), job_id, title)
                if item:
                    yield item

            if not self._has_page(soup, pn + 1):
                break

    # ------------------------------------------------------------------
    # 一覧
    # ------------------------------------------------------------------
    def _post_search(self, list_url: str) -> bs4.BeautifulSoup | None:
        """北海道条件で検索 POST (条件はセッションに保存され、以降の pn=N で引き継がれる)。"""
        data = [("cmn[mode]", "top"), ("rct[area_code][]", _AREA_CODE)]
        try:
            resp = self.session.post(list_url, data=data, timeout=self.TIMEOUT)
            resp.raise_for_status()
            if "charset=" not in resp.headers.get("Content-Type", "").lower():
                resp.encoding = resp.apparent_encoding
            return bs4.BeautifulSoup(resp.text, "html.parser")
        except requests.exceptions.RequestException:
            if self.CONTINUE_ON_ERROR:
                self.error_count += 1
                return None
            raise

    @staticmethod
    def _list_cards(soup: bs4.BeautifulSoup) -> list[tuple[str, str, str]]:
        """一覧カードから (求人ID, href, 求人タイトル) を返す。"""
        cards = []
        for box in soup.select("ul.recruit-boxList li.recruit-box"):
            a = box.select_one(".recruit-box-description a[href*='/detail/']")
            if not a:
                continue
            m = _DETAIL_RE.search(a["href"])
            if not m:
                continue
            title = ""
            txt = a.select_one(".recruit-box-txt")
            if txt:
                lines = [_clean(s) for s in txt.stripped_strings]
                title = lines[0] if lines else ""
            cards.append((m.group(1), a["href"], title))
        return cards

    @staticmethod
    def _has_page(soup: bs4.BeautifulSoup, pn: int) -> bool:
        return soup.select_one(f"a[href*='mode=ssuse'][href$='pn={pn}']") is not None

    # ------------------------------------------------------------------
    # 詳細
    # ------------------------------------------------------------------
    def _parse_detail(self, detail_url: str, job_id: str, list_title: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        fields: dict[str, str] = {}
        rep_label = ""
        for table in soup.select(".details-content table"):
            for tr in table.select("tr"):
                th, td = tr.find("th"), tr.find("td")
                if not th or not td:
                    continue
                label = _clean(th.get_text())
                if "必須" in label or label in fields:
                    continue  # 応募フォーム欄は除外
                fields[label] = td
                if not rep_label and "代表" in label:
                    rep_label = label

        def val(label: str) -> str:
            td = fields.get(label)
            return _clean(td.get_text(" ")) if td is not None else ""

        name = val("会社名")
        if not name:
            h3 = soup.select_one("h3")
            name = _clean(h3.get_text()) if h3 else ""

        title = list_title
        if not title:
            h2 = soup.select_one("h2")
            title = _clean(h2.get_text()).split("募集！")[0] if h2 else ""

        zip_code, pref, addr = _split_address(val("本社所在地"))

        tel_text = val("TEL")
        m = _TEL_RE.search(tel_text)
        posted_tel = m.group(0) if m else ""

        hp = ""
        hp_td = fields.get("企業HP")
        if hp_td is not None:
            a = hp_td.find("a", href=True)
            hp = a["href"].strip() if a else _clean(hp_td.get_text())

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: zip_code,
            Schema.ADDR: addr,
            Schema.TEL: posted_tel if posted_tel != _COMMON_TEL else "",
            Schema.REP_NM: val(rep_label) if rep_label else "",
            Schema.POS_NM: rep_label if rep_label not in ("", "代表者", "代表者名") else "",
            Schema.LOB: val("事業内容"),
            Schema.HP: hp,
            "求人ID": job_id,
            "求人タイトル": title,
            "雇用形態": val("雇用形態"),
            "給与": val("給与"),
            "勤務地": val("勤務地"),
            "掲載TEL": posted_tel,
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Works2Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://kyujin-at.work/hokkaido/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
