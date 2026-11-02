"""
ぐるっと福島 — 福島県北の地域ポータル 全店舗一覧

対象サイト: https://www.gurutto-fukushima.com/digest/

取得対象:
    - /digest/ 全店舗一覧 (約1372件・20件/ページ)

取得フロー:
    一覧 /digest/ → /digest/sort-update/page-N/ (rel=next / 総件数で終端判定)
    → 一覧カード (article.t2) から名称・エリア・カテゴリ・住所・TEL
    → 詳細 /detail/{id}/index.html の基本情報テーブルで郵便番号・営業時間・定休日等を補完
      (公式HP型レイアウトで基本情報テーブルが無い店舗は /detail/{id}/company.html を参照)
    求人/イベント/クーポン/クチコミ/ネットチラシは対象外。

実行方法:
    python scripts/sites/portal/gurutto_fukushima.py
    docker compose exec worker python /app/bin/run_flow.py --site-id gurutto_fukushima
"""

import math
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PER_PAGE = 20
_TEL_RE = re.compile(r"0\d{1,4}-?\d{1,4}-?\d{3,4}")
_ZIP_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_PREF_RE = re.compile(r"^(東京都|北海道|(?:京都|大阪)府|.{2,3}県)")
_KANA_RE = re.compile(r"[\(（]([ァ-ヶー・\s　]+)[\)）]\s*$")

_SNS_MAP = (
    ("instagram.com", Schema.INSTA),
    ("facebook.com", Schema.FB),
    ("twitter.com", Schema.X),
    ("x.com", Schema.X),
    ("line.me", Schema.LINE),
    ("lin.ee", Schema.LINE),
    ("tiktok.com", Schema.TIKTOK),
)


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def _short_name(name: str) -> str:
    """「店名｜福島市・買取・…」のような SEO キーワード接尾辞を落とす。"""
    head = re.split(r"\s*[｜|]\s*", name, maxsplit=1)[0].strip()
    return head or name


class GuruttoFukushimaScraper(StaticCrawler):
    """ぐるっと福島 スクレイパー"""

    DELAY = 3.0
    EXTRA_COLUMNS = ["エリア", "予約専用番号", "駐車場", "座席", "喫煙について", "施設情報"]

    def parse(self, url: str):
        page = 1
        max_page = None
        seen: set[str] = set()
        while True:
            list_url = url if page == 1 else urljoin(url, f"sort-update/page-{page}/")
            soup = self.get_soup(list_url)
            if soup is None:
                break
            if max_page is None:
                m = re.search(r"全\s*([\d,]+)\s*件", soup.get_text(" ", strip=True))
                if m:
                    total = int(m.group(1).replace(",", ""))
                    self.total_items = total
                    max_page = math.ceil(total / _PER_PAGE)

            cards = soup.select(".shopList article.t2")
            if not cards:
                break
            for card in cards:
                item = self._parse_card(card, list_url)
                if not item or item[Schema.URL] in seen:
                    continue
                seen.add(item[Schema.URL])
                self._enrich_detail(item)
                yield item

            has_next = soup.select_one('link[ref="next"], link[rel="next"]') is not None
            page += 1
            if max_page is not None and page > max_page:
                break
            if max_page is None and not has_next:
                break

    # ------------------------------------------------------------------
    def _parse_card(self, card, base_url: str) -> dict | None:
        box = card.select_one(".txtBox") or card
        a = box.select_one('a[href*="/detail/"]') or card.select_one('a[href*="/detail/"]')
        h3 = box.select_one("h3.shop-name") or card.select_one("h3.shop-name")
        if not a or not h3:
            return None
        for span in h3.select("span.new"):
            span.decompose()
        name = _clean(h3.get_text(" ", strip=True))
        detail_url = urljoin(base_url, a["href"])

        area, cat = "", ""
        types = box.select_one("ul.type") or card.select_one("ul.type")
        if types:
            area_li = types.select_one("li.type1")
            text_li = types.select_one("li.text")
            area = _clean(area_li.get_text()) if area_li else ""
            cat = _clean(text_li.get_text()) if text_li else ""

        addr_p = box.select_one("p.address")
        addr = _clean(addr_p.get_text(" ", strip=True)) if addr_p else ""
        tel_p = box.select_one("p.tel")
        tel = ""
        if tel_p:
            m = _TEL_RE.search(tel_p.get_text(" ", strip=True))
            tel = m.group(0) if m else ""

        item = {
            Schema.URL: detail_url,
            Schema.NAME: _short_name(name),
            Schema.NAME_KANA: "",
            Schema.PREF: "",
            Schema.POST_CODE: "",
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.CAT_SITE: cat,
            Schema.TIME: "",
            Schema.HOLIDAY: "",
        }
        item.update({col: "" for col in (
            Schema.HP, Schema.OPEN_DATE, Schema.PAYMENTS,
            Schema.INSTA, Schema.FB, Schema.X, Schema.LINE, Schema.TIKTOK,
        )})
        item.update({col: "" for col in self.EXTRA_COLUMNS})
        item["エリア"] = area
        self._set_pref(item, addr)
        return item

    @staticmethod
    def _set_pref(item: dict, addr: str) -> None:
        m = _PREF_RE.match(addr)
        if m:
            item[Schema.PREF] = m.group(1)

    @staticmethod
    def _info_rows(soup) -> dict[str, object]:
        rows: dict[str, object] = {}
        for tr in soup.select("table tr"):
            th, td = tr.find("th"), tr.find("td")
            if th and td:
                label = _clean(th.get_text(" ", strip=True))
                if label and label not in rows:
                    rows[label] = td
        return rows

    def _enrich_detail(self, item: dict) -> None:
        soup = self.get_soup(item[Schema.URL])
        if soup is None:
            return
        rows = self._info_rows(soup)
        if "住所" not in rows:
            company_url = urljoin(item[Schema.URL], "company.html")
            if soup.select_one('a[href$="/company.html"]'):
                csoup = self.get_soup(company_url)
                if csoup is not None:
                    rows = self._info_rows(csoup)
                    soup = csoup
        if not rows:
            return

        def text(label: str) -> str:
            td = rows.get(label)
            return _clean(td.get_text(" ", strip=True)) if td is not None else ""

        # 名称・カナ
        name_td = text("名称")
        if name_td:
            m = _KANA_RE.search(name_td)
            if m:
                item[Schema.NAME_KANA] = _clean(m.group(1))
        if text("フリガナ"):
            item[Schema.NAME_KANA] = text("フリガナ")

        # 住所・郵便番号
        addr_td = rows.get("住所")
        if addr_td is not None:
            for a in addr_td.find_all("a"):
                a.decompose()
            addr = _clean(addr_td.get_text(" ", strip=True))
            m = _ZIP_RE.search(addr)
            if m:
                item[Schema.POST_CODE] = m.group(1)
                addr = _clean(addr[m.end():])
            if addr:
                item[Schema.ADDR] = addr
                self._set_pref(item, addr)

        # 電話 (予約専用番号とお問合せ番号が併記される)
        tel_txt = text("電話") or text("TEL")
        if tel_txt:
            reserve = re.search(r"予約専用[：:]\s*(" + _TEL_RE.pattern + ")", tel_txt)
            inquiry = re.search(r"お問合せ[：:]\s*(" + _TEL_RE.pattern + ")", tel_txt)
            if reserve:
                item["予約専用番号"] = reserve.group(1)
            if inquiry:
                item[Schema.TEL] = inquiry.group(1)
            elif not reserve:
                m = _TEL_RE.search(tel_txt)
                if m:
                    item[Schema.TEL] = m.group(0)
            elif item[Schema.TEL] == item["予約専用番号"]:
                item[Schema.TEL] = ""

        hp_td = rows.get("公式サイト")
        if hp_td is not None:
            a = hp_td.find("a", href=re.compile(r"^https?://"))
            item[Schema.HP] = a["href"] if a else text("公式サイト")
        item[Schema.TIME] = text("営業時間")
        item[Schema.HOLIDAY] = text("定休日")
        item[Schema.OPEN_DATE] = text("開店日")
        item[Schema.PAYMENTS] = text("支払い方法")
        item["駐車場"] = text("駐車場")
        item["座席"] = text("座席")
        item["喫煙について"] = text("喫煙について")
        item["施設情報"] = text("施設情報")

        # SNS (店舗ナビ内の公式SNSリンク)
        for a in soup.select('.l-navi__sns-list a[href], td a[href]'):
            href = a["href"]
            for key, col in _SNS_MAP:
                if key in href and not item[col]:
                    item[col] = href


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = GuruttoFukushimaScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.gurutto-fukushima.com/digest/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
