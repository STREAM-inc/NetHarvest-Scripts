"""
対象サイト: https://kenchikukensetsu.biz/shops
建築建設biz — 全国の建築・建設業界 企業情報一覧

構造メモ:
- 一覧 /shops?page=N (20件/ページ, 2026-11時点 39,674件 / 1,984ページ)。
  カードは section.box-shadow、詳細リンクは h3 > a[href*=/shop/{id}]。
  掲載エリア (span.company_area) は「大阪市」「名古屋」等の独自区分で都道府県ではない
  → 都道府県は本社所在地の先頭から導出する。
- 詳細 /shop/{id} は2レイアウト:
  * 有料掲載 (div#vip_info): table.tbl02 に 社名/創業/資本金/従業員数/事業内容/本社所在地、
    div.tel_fax に TEL/FAX、h4「リンク」直後の table.tbl02 に HP (ラベルは HP/ホームページリンク等で揺れる)
  * 無料掲載 (section#hellowork_detail): table.tbl01 に 社名/本社所在地/事業内容 のみ
- 代表者は調査サンプル(10件)に出現しなかったが、ラベルがあれば取得する。
- 削除済み ID は 404 (例: /shop/1)。robots.txt は /admin/ のみ Disallow。
- 利用規約ページは存在しない (フッターは会社概要/プライバシーポリシーのみ)。
"""
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

import bs4

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

PREF_RE = re.compile(r"^(東京都|北海道|(?:京都|大阪)府|.{2,3}?県)")
POST_RE = re.compile(r"〒?\s*(\d{3})[-－ー]?(\d{4})\s*")
SHOP_ID_RE = re.compile(r"/shop/(\d+)/?$")
DATE_RE = re.compile(r"(\d{4})\s*年(?:\s*(\d{1,2})\s*月(?:\s*(\d{1,2})\s*日)?)?")
WAREKI_RE = re.compile(r"(明治|大正|昭和|平成|令和)\s*(\d{1,2}|元)\s*年(?:\s*(\d{1,2})\s*月(?:\s*(\d{1,2})\s*日)?)?")
SLASH_DATE_RE = re.compile(r"(\d{4})[/\-.](\d{1,2})(?:[/\-.](\d{1,2}))?")
WAREKI_BASE = {"明治": 1867, "大正": 1911, "昭和": 1925, "平成": 1988, "令和": 2018}

SNS_HOSTS = {
    "instagram.com": Schema.INSTA,
    "twitter.com": Schema.X,
    "x.com": Schema.X,
    "facebook.com": Schema.FB,
    "tiktok.com": Schema.TIKTOK,
    "line.me": Schema.LINE,
    "lin.ee": Schema.LINE,
}


class BizScraper(StaticCrawler):
    """建築建設biz スクレイパー"""

    DELAY = 3.0
    EXTRA_COLUMNS = ["FAX", "創業", "掲載エリア", "こだわり条件"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        page = 1
        last_page = None
        while True:
            list_url = url if page == 1 else self._page_url(url, page)
            soup = self.get_soup(list_url)
            if soup is None:
                break
            if last_page is None:
                last_a = soup.select_one('a[rel="last"]')
                m = re.search(r"page=(\d+)", last_a["href"]) if last_a else None
                last_page = int(m.group(1)) if m else 1

            cards = soup.select("div.company_list section")
            new_count = 0
            for card in cards:
                a = card.select_one(".company_name h3 a[href]")
                if not a:
                    continue
                detail_url = urljoin(list_url, a["href"])
                m = SHOP_ID_RE.search(detail_url)
                if not m or m.group(1) in seen:
                    continue
                seen.add(m.group(1))
                new_count += 1

                area = card.select_one("span.company_area")
                fallback = {
                    "name": a.get_text(strip=True),
                    "area": area.get_text(strip=True) if area else "",
                    "cats": [s.get_text(strip=True) for s in card.select(".area_category span:not(.company_area)")],
                    "addr": self._card_addr(card),
                }
                item = self._parse_detail(detail_url, fallback)
                if item:
                    yield item

            if new_count == 0 or page >= last_page:
                break
            page += 1

    @staticmethod
    def _page_url(url: str, page: int) -> str:
        base = re.sub(r"([?&])page=\d+&?", r"\1", url).rstrip("?&")
        sep = "&" if "?" in base else "?"
        return f"{base}{sep}page={page}"

    @staticmethod
    def _card_addr(card: bs4.Tag) -> str:
        det = card.select_one(".company_details")
        if not det:
            return ""
        m = re.search(r"住所：([^\n]*)", det.get_text("\n"))
        return m.group(1).strip() if m else ""

    def _parse_detail(self, detail_url: str, fb: dict) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        head = soup.select_one("div.contents") or soup
        labels: dict[str, str] = {}
        tds: dict[str, bs4.Tag] = {}
        for tr in head.select("table.tbl01 tr, table.tbl02 tr"):
            th, td = tr.find("th"), tr.find("td")
            if not th or not td:
                continue
            key = self._clean(th.get_text())
            if key and key not in labels:
                val = self._clean(td.get_text(" "))
                labels[key] = "" if val in ("-", "－", "ー") else val
                tds[key] = td

        def pick(*keys: str) -> str:
            for k in keys:
                for label, v in labels.items():
                    if k in label and v:
                        return v
            return ""

        name = pick("社名", "会社名", "企業名")
        if not name:
            h = head.select_one(".company_name span, #hellowork_detail h2")
            name = self._clean(h.get_text()) if h else fb["name"]

        addr_raw = pick("本社所在地", "所在地", "住所") or fb["addr"]
        post_code = ""
        pm = POST_RE.match(addr_raw)
        if pm:
            post_code = f"{pm.group(1)}-{pm.group(2)}"
            addr_raw = addr_raw[pm.end():].strip()
        pref_m = PREF_RE.match(addr_raw)
        pref = pref_m.group(1) if pref_m else ""
        addr = addr_raw[pref_m.end():].strip() if pref_m else addr_raw

        tel, fax = "", ""
        tf = head.select_one(".tel_fax")
        if tf:
            tel_a = tf.select_one('a[href^="tel:"]')
            txt = self._clean(tf.get_text(" "))
            if tel_a:
                tel = self._clean(tel_a.get_text())
            else:
                m = re.search(r"TEL\s*([\d\-－()（）]+)", txt)
                tel = m.group(1) if m else ""
            m = re.search(r"FAX\s*([\d\-－()（）]+)", txt)
            fax = m.group(1) if m else ""

        hp = ""
        sns: dict[str, str] = {}
        for label, td in tds.items():
            for la in td.select("a[href^=http]"):
                href = la["href"].strip()
                host = urlparse(href).netloc.lower().removeprefix("www.")
                col = next((c for h, c in SNS_HOSTS.items() if host == h or host.endswith("." + h)), None)
                if col:
                    sns.setdefault(col, href)
                elif not hp and "kenchikukensetsu.biz" not in host:
                    hp = href

        cat_box = head.select_one("div.category")
        cats = [self._clean(s.get_text()) for s in cat_box.select("span")] if cat_box else []
        cats = cats or fb["cats"]
        conds = [self._clean(s.get_text()) for s in head.select(".company_condition span")]
        founded = pick("設立", "創業")

        item = {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.REP_NM: pick("代表者", "代表"),
            Schema.EMP_NUM: pick("従業員数", "従業員"),
            Schema.LOB: pick("事業内容"),
            Schema.CAP: pick("資本金"),
            Schema.CAT_SITE: "/".join(c for c in cats if c),
            Schema.HP: hp,
            Schema.OPEN_DATE: self._to_date(founded),
            "FAX": fax,
            "創業": founded,
            "掲載エリア": fb["area"],
            "こだわり条件": "/".join(c for c in conds if c),
        }
        item.update(sns)
        return item

    @staticmethod
    def _to_date(s: str) -> str:
        """創業/設立表記を YYYY-MM-DD / YYYY-MM / YYYY に正規化 (不能なら空)。"""
        if not s:
            return ""
        s = s.translate(str.maketrans("０１２３４５６７８９", "0123456789"))
        m = WAREKI_RE.search(s)
        if m:
            n = 1 if m.group(2) == "元" else int(m.group(2))
            parts = [WAREKI_BASE[m.group(1)] + n, m.group(3), m.group(4)]
        else:
            m = DATE_RE.search(s) or SLASH_DATE_RE.search(s)
            if not m:
                return ""
            parts = [int(m.group(1)), m.group(2), m.group(3)]
        out = f"{parts[0]:04d}"
        if parts[1]:
            out += f"-{int(parts[1]):02d}"
            if parts[2]:
                out += f"-{int(parts[2]):02d}"
        return out

    @staticmethod
    def _clean(s: str) -> str:
        return re.sub(r"\s+", " ", (s or "").replace("　", " ")).strip()


if __name__ == "__main__":
    scraper = BizScraper()
    scraper.execute("https://kenchikukensetsu.biz/shops")
