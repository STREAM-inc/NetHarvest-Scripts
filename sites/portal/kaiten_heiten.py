import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_SITEMAP_CANDIDATES = [
    "sitemap.xml",
    "sitemap_index.xml",
    "sitemap-index.xml",
    "wp-sitemap.xml",
]

# 店舗ページはルート直下の単一スラッグ（例: /donmaru-yotsuba-shodai/）。
# /category/ や /tag/、sitemap.html 等の非店舗URLを除外する。
_SHOP_PATH = re.compile(r"^/[^/.]+/?$")
_NON_SHOP_SLUGS = {
    "", "category", "tag", "author", "page", "feed",
    "contact", "about", "privacy", "sitemap", "wp-login",
}

# h1 / h3 から開店・閉店フラグを判定する
_OPEN_RE = re.compile(r"開店|オープン|ＯＰＥＮ|OPEN", re.IGNORECASE)
_CLOSE_RE = re.compile(r"閉店|閉業|クローズ|CLOSE", re.IGNORECASE)
# h1 先頭の 【開店】 / 【閉店】 等の角括弧プレフィックス
_BRACKET_PREFIX = re.compile(r"^[【\[（(][^】\]）)]*[】\]）)]\s*")
# 日付 2026年6月29日 → YYYY-MM-DD
_DATE_RE = re.compile(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日")
# 郵便番号 〒573-1152 / 5731152
_POST_RE = re.compile(r"〒?\s*(\d{3})[-－‐\s]?(\d{4})")
# h3 先頭の所在地（都道府県）
_PREF_RE = re.compile(r"^\s*(\S+?[都道府県])")


class KaitenHeitenScraper(StaticCrawler):
    """開店閉店ドットコム・新（kaiten-heiten-24.com）店舗情報スクレイパー

    各記事は新規開店または閉店を1件ずつ告知する WordPress 投稿。
    開店/閉店フラグ（h1 の 【開店】/【閉店】）と開店日（h3 の日付）を必ず取得する。
    店舗詳細（住所・電話・営業時間・定休日・HP）は本文の table から取得する。
    """

    DELAY = 3.0
    EXTRA_COLUMNS = ["開店閉店", "開店閉店日", "アクセス"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        shop_urls = self._collect_shop_urls(url)
        self.total_items = len(shop_urls)
        self.logger.info("店舗URL収集完了: %d 件", len(shop_urls))
        for shop_url in shop_urls:
            item = self._scrape_detail(shop_url)
            if item:
                yield item

    def _collect_shop_urls(self, root_url: str) -> list[str]:
        """sites.yml の root_url を起点にサイトマップを辿り、店舗ページURLを収集する。"""
        urls: list[str] = []
        seen: set[str] = set()
        queue: list[str] = []

        for candidate in _SITEMAP_CANDIDATES:
            sm_url = urljoin(root_url, candidate)
            try:
                r = self.session.get(sm_url, timeout=self.TIMEOUT)
                if r.status_code == 200 and b"<loc>" in r.content:
                    queue.append(sm_url)
            except Exception:
                continue

        if not queue:
            return []

        visited: set[str] = set()
        while queue:
            sm_url = queue.pop(0)
            if sm_url in visited:
                continue
            visited.add(sm_url)
            try:
                r = self.session.get(sm_url, timeout=self.TIMEOUT)
                if r.status_code != 200:
                    continue
                root = ET.fromstring(r.content)
                locs = [el.text.strip() for el in root.iter() if el.tag.endswith("loc") and el.text]
                if root.tag.lower().endswith("sitemapindex"):
                    queue.extend(locs)
                else:
                    for u in locs:
                        if self._is_shop_url(u) and u not in seen:
                            seen.add(u)
                            urls.append(u)
            except Exception as e:
                self.logger.debug("サイトマップスキップ %s: %s", sm_url, e)
        return urls

    @staticmethod
    def _is_shop_url(u: str) -> bool:
        path = urlparse(u).path
        if not _SHOP_PATH.match(path):
            return False
        slug = path.strip("/")
        return slug not in _NON_SHOP_SLUGS

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        # --- 名称・開店/閉店フラグ（h1 の 【開店】/【閉店】プレフィックス） ---
        h1 = soup.select_one("h1.entry-title") or soup.select_one("h1")
        h1_text = h1.get_text(" ", strip=True) if h1 else ""

        # --- 所在地・開店閉店日（h3 例: 大阪府枚方市 2026年6月29日（月）オープン） ---
        h3_text = ""
        for h3 in soup.select("h3"):
            t = h3.get_text(" ", strip=True)
            if _DATE_RE.search(t) and (_OPEN_RE.search(t) or _CLOSE_RE.search(t)):
                h3_text = t
                break

        flag = self._detect_flag(h1_text, h3_text)
        if flag:
            data["開店閉店"] = flag
            data[Schema.STS_NM] = flag

        date = self._extract_date(h3_text, h1_text, soup)
        if date:
            data[Schema.OPEN_DATE] = date
            data["開店閉店日"] = date

        name = _BRACKET_PREFIX.sub("", h1_text).strip()
        if name:
            data[Schema.NAME] = name

        pref_m = _PREF_RE.match(h3_text)
        if pref_m:
            data[Schema.PREF] = pref_m.group(1)

        # --- 店舗詳細テーブル（住所・電話・営業時間・定休日・HP 等） ---
        self._parse_table(soup, data)

        if not data.get(Schema.NAME) and not data.get(Schema.ADDR):
            return None
        return data

    @staticmethod
    def _detect_flag(h1_text: str, h3_text: str) -> str | None:
        for text in (h1_text, h3_text):
            if not text:
                continue
            if _CLOSE_RE.search(text):
                return "閉店"
            if _OPEN_RE.search(text):
                return "開店"
        return None

    @staticmethod
    def _extract_date(h3_text: str, h1_text: str, soup) -> str | None:
        # h3（所在地+日付+オープン/閉店）を最優先。次に h1、最後に <time> 要素。
        for text in (h3_text, h1_text):
            m = _DATE_RE.search(text or "")
            if m:
                return _fmt_date(m)
        t = soup.select_one("time.date") or soup.select_one("time[datetime]")
        if t:
            m = _DATE_RE.search(t.get_text(strip=True))
            if m:
                return _fmt_date(m)
        return None

    def _parse_table(self, soup, data: dict) -> None:
        tables = soup.select("div.detail_text table, div.post_body table") or soup.select("table")
        for table in tables:
            for tr in table.select("tr"):
                cells = tr.find_all(["th", "td"])
                if len(cells) < 2:
                    continue
                key = cells[0].get_text(" ", strip=True)
                val_cell = cells[1]
                val = re.sub(r"\s+", " ", val_cell.get_text(" ", strip=True)).strip()
                if key in ("住所", "所在地"):
                    self._set_address(data, val)
                elif key in ("電話番号", "TEL", "電話"):
                    if val:
                        data[Schema.TEL] = val
                elif key == "営業時間":
                    if val:
                        data[Schema.TIME] = val
                elif key == "定休日":
                    if val:
                        data[Schema.HOLIDAY] = val
                elif key in ("HP", "ホームページ", "URL", "Web", "Webサイト"):
                    a = val_cell.find("a", href=True)
                    href = a["href"].strip() if a else ""
                    if href and not href.startswith(("tel:", "mailto:")):
                        data[Schema.HP] = href
                    elif val and val.upper() != "WEBSITE":
                        data[Schema.HP] = val
                elif key == "アクセス":
                    if val:
                        data["アクセス"] = val

    @staticmethod
    def _set_address(data: dict, val: str) -> None:
        m = _POST_RE.search(val)
        if m:
            data[Schema.POST_CODE] = f"{m.group(1)}-{m.group(2)}"
            val = _POST_RE.sub("", val).strip()
        if val:
            data[Schema.ADDR] = val


def _fmt_date(m: re.Match) -> str:
    y, mo, d = m.groups()
    return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    KaitenHeitenScraper().execute("https://kaiten-heiten-24.com")
