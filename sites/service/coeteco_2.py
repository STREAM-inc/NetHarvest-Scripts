"""
対象サイト: https://coeteco.jp/campus/categories/generative-ai-school

コエテコキャンパス (GMOメディア株式会社) の大人向けスクール検索サイト。
9 カテゴリの一覧を全ページ巡回し、各スクール (ブランド) 詳細ページまで潜って取得する。
1 行 = 1 スクール × 1 カテゴリ。同一スクールが複数カテゴリに載る場合は複数行になる。

構造メモ:
  - 全ページ Imperva (Incapsula) WAF 配下。requests では JS チャレンジが返るため
    DynamicCrawler (Playwright) が必須。同一 Cookie で連続アクセスすると 403 になるため、
    goto の直前に context.clear_cookies() を呼ぶ (既存 coeteco.py と同じ対処)。
  - 一覧は 2 系統だが構造は同じ。カテゴリ 8 種は /campus/categories/{slug}、
    プログラミングスクールのみ /campus/brand/list。どちらも 20 件/ページで、
    ブランドは ul.c-list-vertical-alignment > li、ページ送りは a[rel=next]
    (/campus/categories/{slug}/2 形式)。範囲外ページは 404。
  - 詳細 (ブランドトップ) は /brand/{slug} の静的 SSR。table.school-info-list の
    ラベル/値と JSON-LD (LocalBusiness) の aggregateRating で大半が取れる。
  - コース名・料金はブランドトップには全件出ないため /brand/{slug}/courses を辿る。
    1 ページに全件出る (ul.c-adu-school-detail-course-list > li、ページ送り無し)。
  - 「本社所在地」に相当する住所はサイトに存在しない。教室を持つブランドのみ
    /brand/{slug}/schools → /brand/{slug}/schools/{id} の JSON-LD に教室住所があるため、
    代表教室 (一覧の先頭) の住所を掲載住所として採用する。オンライン専業は空になる。
  - 「設立年月」はコエテコキャンパス側には掲載が無い (子ども向けの教室ページにある
    「開設年月」は教室単位の値で、大人向けブランドには存在しない)。Schema.OPEN_DATE は
    常に空で出力される。
  - 「公式サイト」リンクはアフィリエイト計測ドメイン (af.moshimo.com / t.felmat.net /
    ac.affitown.jp) への遷移 URL しか掲載が無く、スクール本来の URL は露出していない。
    計測リンクを踏むと成果計測が汚れるためリダイレクト追跡はせず、掲載されている URL を
    そのまま Schema.HP に格納する。
  - robots.txt: /search? 等は Disallow だが、本クローラーが辿る一覧・ブランド・コース・
    教室パスはいずれも許可されている。
  - 利用規約 (https://coeteco.jp/kiyaku) にスクレイピング/クローリングの明示禁止条項は無い。
    第5条(13) がサーバーへの過負荷を禁じているため DELAY を長めに設定する。
  - コース説明文・口コミ本文・「学べること」「コエテコ編集部おすすめポイント」は運営者/
    投稿者による長文の自由記述のため、著作権リスクを避けて取得しない。
"""

import json
import re
import time
import urllib.parse
from typing import Generator

import bs4

from src.const.schema import Schema
from src.framework.dynamic import DynamicCrawler

# 備考指定の 9 カテゴリ。(カテゴリ名, 一覧パス)。プログラミングスクールのみ別パス体系。
_CATEGORIES = [
    ("生成AIスクール", "/campus/categories/generative-ai-school"),
    ("資格スクール", "/campus/categories/qualification-school"),
    ("通信講座", "/campus/categories/correspondence-course"),
    ("プログラミングスクール", "/campus/brand/list"),
    ("WEBデザインスクール", "/campus/categories/design-school"),
    ("WEBマーケティングスクール", "/campus/categories/marketing-school"),
    ("動画編集スクール", "/campus/categories/movie-creator-school"),
    ("パソコン教室", "/campus/categories/pc-school"),
    ("専門学校", "/campus/categories/vocational-school"),
]

# ブランド詳細 /brand/{slug} (reviews / courses 等のサブパスは除外)
_BRAND_RE = re.compile(r"^/brand/([^/?#]+)$")
# 教室詳細 /brand/{slug}/schools/{id}
_SCHOOL_RE = re.compile(r"^/brand/[^/?#]+/schools/\d+$")
# タブ見出しの件数表記 (例: 「口コミ(142)」「料金・コース(18)」)
_TAB_COUNT_RE = re.compile(r"[(（]\s*(\d+)\s*[)）]")
# 教室一覧の総件数表記 (例: 「全 24 件」)
_TOTAL_COUNT_RE = re.compile(r"全\s*([\d,]+)\s*件")
# 料金表記 (例: 「231,000円」)
_PRICE_RE = re.compile(r"([\d,]+)\s*円")


class CoetecoCampusScraper(DynamicCrawler):
    """コエテコキャンパス (大人向けスクール) スクレイパー (動的)"""

    DELAY = 3.0  # 通信前の待機時間（秒）。規約 第5条(13) の過負荷禁止に配慮して長めに取る
    # WAF チャレンジ時のリトライ上限。get_soup のキャッシュキーを変えるため wait_until を変えて再試行する
    _RETRY_WAITS = ("domcontentloaded", "load", "networkidle")

    EXTRA_COLUMNS = [
        "カテゴリ名",
        "運営会社名",
        "通学スタイル",
        "目的",
        "学べるジャンル",
        "プログラミング言語",
        "ツール・技術",
        "資格",
        "費用／体験授業",
        "フォロー体制",
        "対象／学び方",
        "講師／運営体制",
        "授業",
        "コース数",
        "コース名一覧",
        "コース料金一覧",
        "コース最安料金",
        "コース最高料金",
        "教室数",
        "代表教室名",
    ]

    # ------------------------------------------------------------------ 取得

    @staticmethod
    def _is_blocked(soup: bs4.BeautifulSoup) -> bool:
        """Incapsula のブロック/チャレンジ画面かどうかを判定する。"""
        if soup.select_one("iframe#main-iframe") is not None:
            return True
        return soup.select_one("script[src*='_Incapsula_Resource']") is not None and soup.title is None

    def _fetch(self, url: str) -> bs4.BeautifulSoup | None:
        """WAF のブロックを検知して上限付きでリトライしつつページを取得する。

        同一 Cookie を使い回すと 2 回目以降 403 になるため、毎回 Cookie を破棄してから開く。
        リトライ時は wait_until を変えることで、ブロック画面が fetch キャッシュに
        居座って再取得できなくなるのを避ける。
        """
        for attempt, wait_until in enumerate(self._RETRY_WAITS):
            time.sleep(self.DELAY)
            if self.context is not None:
                self.context.clear_cookies()
            soup = self.get_soup(url, wait_until=wait_until)
            if soup is not None and not self._is_blocked(soup):
                return soup
            self.logger.warning("WAF ブロックを検知 (試行 %d/%d): %s", attempt + 1, len(self._RETRY_WAITS), url)
            time.sleep(min(5 * 2 ** attempt, 30))
        self.logger.error("WAF ブロックを回避できませんでした: %s", url)
        return None

    # ------------------------------------------------------------------ 共通ヘルパ

    @staticmethod
    def _next_url(soup: bs4.BeautifulSoup, base_url: str) -> str | None:
        """ページャの a[rel=next] から次ページ URL を求める。無ければ None。"""
        nxt = soup.select_one("a[rel=next][href]")
        if not nxt:
            return None
        return urllib.parse.urljoin(base_url, nxt["href"])

    @staticmethod
    def _json_ld(soup: bs4.BeautifulSoup, type_name: str) -> dict:
        """指定 @type の JSON-LD を返す (無ければ空 dict)。"""
        for tag in soup.select('script[type="application/ld+json"]'):
            raw = tag.string or tag.get_text()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except ValueError:
                continue
            for node in data if isinstance(data, list) else [data]:
                if isinstance(node, dict) and node.get("@type") == type_name:
                    return node
        return {}

    @staticmethod
    def _text(td: bs4.Tag | None) -> str:
        """セルのテキストを 1 行に正規化する (「-」のみのプレースホルダは未登録扱い)。"""
        if td is None:
            return ""
        value = " ".join(td.get_text(" ", strip=True).split())
        return "" if value in ("-", "ー", "－") else value

    @staticmethod
    def _info_rows(soup: bs4.BeautifulSoup) -> dict:
        """table.school-info-list のラベル→<td> を辞書化する。"""
        rows: dict[str, bs4.Tag] = {}
        for table in soup.select("table.school-info-list"):
            for tr in table.select("tr"):
                th = tr.select_one("th")
                td = tr.select_one("td")
                if th is None or td is None:
                    continue
                rows.setdefault(th.get_text(strip=True), td)
        return rows

    @staticmethod
    def _tab_links(soup: bs4.BeautifulSoup, slug: str) -> dict:
        """ブランド詳細のタブ (スクール/口コミ/料金・コース/教室一覧…) をパス→アンカーで返す。"""
        prefix = f"/brand/{slug}/"
        tabs: dict[str, bs4.Tag] = {}
        for a in soup.select("a[href]"):
            href = a["href"]
            if href.startswith(prefix) and href[len(prefix):].isalpha():
                tabs.setdefault(href[len(prefix):], a)
        return tabs

    @classmethod
    def _tab_count(cls, tabs: dict, key: str) -> str:
        """タブ見出しの「(142)」形式の件数を取り出す。"""
        a = tabs.get(key)
        if a is None:
            return ""
        m = _TAB_COUNT_RE.search(a.get_text(" ", strip=True))
        return m.group(1) if m else ""

    @staticmethod
    def _nav_count(soup: bs4.BeautifulSoup, label: str) -> str:
        """タブ見出しの件数を nav から拾う。

        口コミが 0 件のブランドはタブが <a> ではなく <button> になりリンクから辿れないため、
        nav 要素のテキスト (例: 「口コミ(0)」) を直接読む。
        """
        for li in soup.select("li.c-page-school-nav__item, li.c-page-nav-campus__item"):
            text = li.get_text(" ", strip=True)
            if not text.startswith(label):
                continue
            m = _TAB_COUNT_RE.search(text)
            if m:
                return m.group(1)
        return ""

    # ------------------------------------------------------------------ 一覧

    @staticmethod
    def _brand_slugs(soup: bs4.BeautifulSoup) -> list[str]:
        """一覧ページの ul.c-list-vertical-alignment からブランド slug を出現順で抽出する。"""
        slugs: list[str] = []
        for ul in soup.select("ul.c-list-vertical-alignment"):
            for li in ul.select(":scope > li"):
                for a in li.select("a[href]"):
                    m = _BRAND_RE.match(urllib.parse.urlparse(a["href"]).path)
                    if m and m.group(1) not in slugs:
                        slugs.append(m.group(1))
                        break
        return slugs

    def _category_urls(self, url: str) -> list[tuple[str, str]]:
        """起点 URL を基準に 9 カテゴリの一覧 URL を組み立てる。

        引数 url のカテゴリを先頭に並べ替え、起点がそのまま最初に巡回されるようにする。
        """
        targets = [(name, urllib.parse.urljoin(url, path)) for name, path in _CATEGORIES]
        start = urllib.parse.urlparse(url).path.rstrip("/")
        targets.sort(key=lambda t: urllib.parse.urlparse(t[1]).path.rstrip("/") != start)
        return targets

    # ------------------------------------------------------------------ 詳細

    def _courses(self, brand_url: str) -> dict:
        """コース一覧タブからコース名・料金をまとめる。"""
        soup = self._fetch(f"{brand_url}/courses")
        if soup is None:
            return {}

        names: list[str] = []
        prices: list[str] = []
        amounts: list[int] = []
        for li in soup.select("ul.c-adu-school-detail-course-list > li"):
            title = self._text(li.select_one("h3.c-adu-course-mini__ttl"))
            if not title:
                continue
            names.append(title)
            # 「231,000円 受講料割引中！」のようにラベルが続くため金額表記だけを拾う
            found = _PRICE_RE.findall(self._text(li.select_one("p.c-adu-course-mini__cost")))
            prices.append(f"{found[0]}円" if found else "")
            amounts.extend(int(v.replace(",", "")) for v in found)

        if not names:
            return {}
        return {
            "コース名一覧": " / ".join(names),
            # コース名一覧と同じ並び (料金非掲載のコースは空欄) で対応付けられるようにする
            "コース料金一覧": " / ".join(prices),
            "コース最安料金": f"{min(amounts):,}円" if amounts else "",
            "コース最高料金": f"{max(amounts):,}円" if amounts else "",
        }

    def _representative_school(self, brand_url: str) -> dict:
        """教室一覧の先頭教室を辿り、掲載住所 (本社所在地の代替) を取得する。"""
        soup = self._fetch(f"{brand_url}/schools")
        if soup is None:
            return {}

        detail_section = soup.select_one("section.c-school-detail")
        total = _TOTAL_COUNT_RE.search(
            detail_section.get_text(" ", strip=True) if detail_section else ""
        )
        result = {"教室数": total.group(1).replace(",", "") if total else ""}

        school_url = ""
        for a in soup.select("ul.c-class-list a[href]"):
            path = urllib.parse.urlparse(a["href"]).path
            if _SCHOOL_RE.match(path):
                school_url = urllib.parse.urljoin(brand_url, path)
                break
        if not school_url:
            return result

        detail = self._fetch(school_url)
        if detail is None:
            return result

        biz = self._json_ld(detail, "LocalBusiness")
        addr = biz.get("address") or {}
        rows = self._info_rows(detail)
        result.update({
            Schema.PREF: str(addr.get("addressRegion") or ""),
            Schema.POST_CODE: str(addr.get("postalCode") or ""),
            Schema.ADDR: f"{addr.get('addressLocality') or ''}{addr.get('streetAddress') or ''}",
            "代表教室名": self._text(rows.get("教室名")) or str(biz.get("name") or ""),
        })
        return result

    @staticmethod
    def _official_link(soup: bs4.BeautifulSoup) -> str:
        """「公式サイトで詳細をみる」の遷移先 URL (アフィリエイト計測 URL) を返す。"""
        for a in soup.select("a[href^='http']"):
            if "公式サイト" in a.get_text(" ", strip=True):
                return a["href"]
        return ""

    def _parse_brand(self, brand_url: str, slug: str) -> dict | None:
        """ブランド詳細 1 件分 (カテゴリ名を除く) を辞書にして返す。取得不可なら None。"""
        soup = self._fetch(brand_url)
        if soup is None:
            return None

        rows = self._info_rows(soup)
        name = self._text(rows.get("スクール名"))
        if not name and soup.h1 is not None:
            name = self._text(soup.h1)
        if not name:
            self.logger.warning("スクール名を取得できませんでした: %s", brand_url)
            return None

        rating = (self._json_ld(soup, "LocalBusiness").get("aggregateRating") or {})
        tabs = self._tab_links(soup, slug)

        item = {
            Schema.URL: brand_url,
            Schema.NAME: name,
            Schema.NAME_KANA: self._text(soup.select_one(".c-school-header__phonetic")),
            Schema.PREF: "",
            Schema.POST_CODE: "",
            Schema.ADDR: "",
            Schema.HP: self._official_link(soup),
            Schema.CAT_SITE: self._text(rows.get("スクール種別")),
            Schema.SCORES: str(rating.get("ratingValue") or ""),
            # 口コミ 0 件のスクールは JSON-LD に aggregateRating が出ないためタブ表記で補う
            Schema.REV_SCR: (
                str(rating.get("ratingCount") or "")
                or self._tab_count(tabs, "reviews")
                or self._nav_count(soup, "口コミ")
            ),
            "運営会社名": self._text(rows.get("運営本部")),
            "通学スタイル": self._text(rows.get("通学スタイル")),
            "目的": self._text(rows.get("目的")),
            "学べるジャンル": self._text(rows.get("学べるジャンル")),
            "プログラミング言語": self._text(rows.get("プログラミング言語")),
            "ツール・技術": self._text(rows.get("ツール・技術")),
            "資格": self._text(rows.get("資格")),
            "費用／体験授業": self._text(rows.get("費用／体験授業")),
            "フォロー体制": self._text(rows.get("フォロー体制")),
            "対象／学び方": self._text(rows.get("対象／学び方")),
            "講師／運営体制": self._text(rows.get("講師／運営体制")),
            "授業": self._text(rows.get("授業")),
            "コース数": self._tab_count(tabs, "courses"),
            "コース名一覧": "",
            "コース料金一覧": "",
            "コース最安料金": "",
            "コース最高料金": "",
            "教室数": "",
            "代表教室名": "",
        }

        if "courses" in tabs:
            item.update(self._courses(brand_url))
        # 教室を持つブランドのみ、代表教室の住所を掲載住所として採用する (オンライン専業は空)
        if "schools" in tabs:
            item.update(self._representative_school(brand_url))

        return item

    # ------------------------------------------------------------------ 本体

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 同一スクールが複数カテゴリに載るため、ブランド詳細の取得結果は使い回す
        brands: dict[str, dict | None] = {}

        for category, list_url in self._category_urls(url):
            self.logger.info("カテゴリ巡回開始: %s (%s)", category, list_url)
            seen: set[str] = set()
            page_url: str | None = list_url

            while page_url:
                soup = self._fetch(page_url)
                if soup is None:
                    break

                for slug in self._brand_slugs(soup):
                    if slug in seen:
                        continue
                    seen.add(slug)

                    if slug not in brands:
                        brand_url = urllib.parse.urljoin(url, f"/brand/{slug}")
                        brands[slug] = self._parse_brand(brand_url, slug)
                    item = brands[slug]
                    if item:
                        # 1 行 = 1 スクール × 1 カテゴリ。取得ごとに即 yield する
                        yield {**item, "カテゴリ名": category}

                page_url = self._next_url(soup, page_url)


if __name__ == "__main__":
    scraper = CoetecoCampusScraper()
    scraper.execute("https://coeteco.jp/campus/categories/generative-ai-school")
