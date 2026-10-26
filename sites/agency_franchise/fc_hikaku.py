"""
対象サイト: https://fc-hikaku.net/ (フランチャイズ比較ネット)

構造メモ:
- https://fc-hikaku.net/ は https://www.fc-hikaku.net/ へ 301。静的 SSR (requests で取得可)。
- 列挙: (1) 予算4区分すべて指定の /search 一覧 (?page=N) → (2) robots.txt の Sitemap
  (S3 上の sitemap.xml) に載る /{slug}_fc を和集合で補完。slug で dedupe。
  ※ 一覧の「全239件」はサイト全体の表示件数で、単独で全件を辿れる一覧は存在しない。
- ブランド詳細 /{slug}_fc : th/td テーブル (ブランド名・業種・加盟店舗数・契約タイプ等)。
  /{slug}_fc/special は同ブランドの特集ページなので基本ページに正規化する。
- 本部企業情報 /{slug}_fc/data : th/td テーブル (会社名・代表者・本社・設立・資本金・
  売上金・従業員数・FC本部URL)。
- 電話番号はサイト上に掲載されていない (問い合わせはフォームのみ) → TEL は空欄。
- 「事業内容」「企業理念・特徴」は自由記述の文章を含むため取得しない (著作権リスク)。
"""
import re
import sys
import urllib.parse
from pathlib import Path
from typing import Generator

import bs4

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_SLUG_RE = re.compile(r"^/([A-Za-z0-9_\-]+_fc)(?:/special)?/?$")
_PREFS = (
    "北海道 青森県 岩手県 宮城県 秋田県 山形県 福島県 茨城県 栃木県 群馬県 埼玉県 千葉県 "
    "東京都 神奈川県 新潟県 富山県 石川県 福井県 山梨県 長野県 岐阜県 静岡県 愛知県 三重県 "
    "滋賀県 京都府 大阪府 兵庫県 奈良県 和歌山県 鳥取県 島根県 岡山県 広島県 山口県 徳島県 "
    "香川県 愛媛県 高知県 福岡県 佐賀県 長崎県 熊本県 大分県 宮崎県 鹿児島県 沖縄県"
).split()
_BUDGETS = ("1", "2", "3", "4")
_MAX_LIST_PAGES = 50


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def _table(soup: bs4.BeautifulSoup) -> dict:
    """th/td テーブルをラベル→td 要素の辞書にする (最初の出現を優先)。"""
    out: dict = {}
    for tr in soup.select("tr"):
        th, td = tr.find("th"), tr.find("td")
        if th is None or td is None:
            continue
        label = _clean(th.get_text())
        if label and label not in out:
            out[label] = td
    return out


def _split_addr(raw: str) -> tuple[str, str, str]:
    """本社表記 → (郵便番号, 都道府県, 住所)。"""
    text = _clean(raw)
    post = ""
    m = re.search(r"〒?\s*(\d{3})[-‐－ー−]?(\d{4})", text)
    if m and text.find(m.group(0)) <= 2:
        post = f"{m.group(1)}-{m.group(2)}"
        text = _clean(text.replace(m.group(0), "", 1))
    pref = ""
    for p in _PREFS:
        if text.startswith(p):
            pref = p
            text = _clean(text[len(p):])
            break
    return post, pref, text


def _open_date(raw: str) -> str:
    m = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月(?:\s*(\d{1,2})\s*日)?", raw)
    if not m:
        return _clean(raw)
    y, mo, d = m.group(1), int(m.group(2)), m.group(3)
    return f"{y}-{mo:02d}-{int(d):02d}" if d else f"{y}-{mo:02d}"


class FcHikakuScraper(StaticCrawler):
    """フランチャイズ比較ネット ブランド(本部)情報スクレイパー"""

    DELAY = 3.0
    EXTRA_COLUMNS = ["ブランド名", "加盟店舗数", "契約タイプ", "契約期間", "設立"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()

        # (1) 一覧: 予算4区分すべて指定の検索結果をページ送り
        query = "&".join(f"q%5Bbudget%5D%5B%5D={b}" for b in _BUDGETS)
        for page in range(1, _MAX_LIST_PAGES + 1):
            list_url = urllib.parse.urljoin(url, f"/search?{query}&page={page}")
            soup = self.get_soup(list_url)
            if soup is None:
                break
            new_slugs = [s for s in self._slugs(soup) if s not in seen]
            if not new_slugs:
                break
            for slug in new_slugs:
                seen.add(slug)
                item = self._scrape_brand(url, slug)
                if item:
                    yield item

        # (2) サイトマップ (robots.txt の Sitemap 行から導出) で補完
        for slug in self._sitemap_slugs(url):
            if slug in seen:
                continue
            seen.add(slug)
            item = self._scrape_brand(url, slug)
            if item:
                yield item

    # ------------------------------------------------------------------
    def _slugs(self, soup: bs4.BeautifulSoup) -> list[str]:
        """一覧ページの検索結果領域からブランド slug を出現順に取得する。"""
        out: list[str] = []
        for a in soup.select("a[href]"):
            path = urllib.parse.urlparse(a["href"]).path
            m = _SLUG_RE.match(path)
            if m and m.group(1) not in out:
                out.append(m.group(1))
        return out

    def _sitemap_slugs(self, url: str) -> list[str]:
        robots = self.get_soup(urllib.parse.urljoin(url, "/robots.txt"))
        if robots is None:
            return []
        sitemaps = re.findall(r"(?im)^\s*Sitemap:\s*(\S+)", robots.get_text("\n"))
        out: list[str] = []
        for sm_url in sitemaps:
            sm = self.get_soup(sm_url)
            if sm is None:
                continue
            for loc in sm.find_all("loc"):
                path = urllib.parse.urlparse(_clean(loc.get_text())).path
                m = _SLUG_RE.match(path)
                if m and m.group(1) not in out:
                    out.append(m.group(1))
        self.logger.info("サイトマップのブランド slug: %d 件", len(out))
        return out

    def _scrape_brand(self, url: str, slug: str) -> dict | None:
        brand_url = urllib.parse.urljoin(url, f"/{slug}")
        soup = self.get_soup(brand_url)
        if soup is None:
            return None
        bt = _table(soup)
        brand = _clean(bt["ブランド名"].get_text(" ")) if "ブランド名" in bt else ""
        if not brand:
            self.logger.warning("ブランド名なし (スキップ): %s", brand_url)
            return None

        # 大業種 (飲食/小売/サービス) は「業種が同じ」内の business_class リンク
        classes: list[str] = []
        same = bt.get("業種が同じ")
        if same is not None:
            for a in same.select("a[href*='business_class_']"):
                t = _clean(a.get_text())
                if t and t not in classes:
                    classes.append(t)
        cat_site = _clean(bt["業種"].get_text(" ")) if "業種" in bt else ""

        data_soup = self.get_soup(urllib.parse.urljoin(url, f"/{slug}/data"))
        dt = _table(data_soup) if data_soup is not None else {}

        def dv(label: str) -> str:
            return _clean(dt[label].get_text(" ")) if label in dt else ""

        def bv(label: str) -> str:
            return _clean(bt[label].get_text(" ")) if label in bt else ""

        hp = ""
        if "FC本部URL" in dt:
            a = dt["FC本部URL"].find("a", href=True)
            hp = _clean(a["href"]) if a else dv("FC本部URL")

        post, pref, addr = _split_addr(dv("本社"))
        established = dv("設立")

        return {
            Schema.URL: brand_url,
            Schema.NAME: dv("会社名") or brand,
            Schema.POST_CODE: post,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: "",  # サイト上に電話番号の掲載なし
            Schema.REP_NM: dv("代表者"),
            Schema.EMP_NUM: dv("従業員数"),
            Schema.CAP: dv("資本金"),
            Schema.SALES: dv("売上金"),
            Schema.OPEN_DATE: _open_date(established) if established else "",
            Schema.CAT_LV1: "/".join(classes),
            Schema.CAT_SITE: cat_site,
            Schema.HP: hp,
            "ブランド名": brand,
            "加盟店舗数": bv("加盟店舗数"),
            "契約タイプ": bv("契約タイプ"),
            "契約期間": bv("契約期間"),
            "設立": established,
        }


if __name__ == "__main__":
    scraper = FcHikakuScraper()
    scraper.execute("https://fc-hikaku.net/")
