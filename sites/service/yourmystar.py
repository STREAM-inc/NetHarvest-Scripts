import re
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 個別事業者(サービス)ページ: /c0_N/(c1_N/)(c2_N/)d_N/
_DETAIL_PATTERN = re.compile(r"^/c0_\d+/(?:c1_\d+/)?(?:c2_\d+/)?d_\d+/$")

# 所在地で出力対象とする都道府県 (関東1都6県 + 新潟・山梨・長野・岐阜・静岡・愛知)
_TARGET_PREFS = (
    "東京都", "神奈川県", "埼玉県", "千葉県", "茨城県", "栃木県", "群馬県",
    "新潟県", "山梨県", "長野県", "岐阜県", "静岡県", "愛知県",
)
_PREF_RE = re.compile(
    r"(北海道|東京都|大阪府|京都府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|新潟|富山|石川|福井|"
    r"山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|和歌山|鳥取|島根|岡山|広島|山口|徳島|香川|"
    r"愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県)"
)
_DATE_RE = re.compile(r"(\d{4})/(\d{1,2})/(\d{1,2})")
_NEXT_RE = re.compile(r'<link[^>]*rel="next"[^>]*href="([^"]+)"|<link[^>]*href="([^"]+)"[^>]*rel="next"')
_CARD_DATE_RE = re.compile(
    r'class="(?:supplier-review-card_date|heading-block_review-post-date)"[^>]*>([^<]*)<'
)


class YourmystarScraper(StaticCrawler):
    """ユアマイスター 事業者情報スクレイパー

    - 列挙: 引数 url (sitemap.xml) → 404 等で XML でなければ同ホストの robots.txt の
      Sitemap 行から子サイトマップを取得し、/c0_*/c1_*/c2_*/d_N/ 形式の URL を収集する。
    - 所在地 (都道府県) が対象 13 都県の事業者のみ出力。事業者単位で重複除去。
    - 最古の口コミ日: /suppliers/{slug}/reviews/?page=N を最終ページまで巡回して最小日付を取る
      (一覧はサービス単位にまとまっており日付順ではないため、最終ページだけでは不正確)。
    """

    DELAY = 1.0              # robots.txt の Crawl-delay: 1
    REVIEW_PAGE_DELAY = 0.5  # 口コミ一覧 1 リクエストごとの待機 (スレッド毎)
    REVIEW_WORKERS = 3       # 口コミ一覧の並行取得数
    REVIEW_MAX_PAGES = 1000  # 無限ループ防止
    EXTRA_COLUMNS = ["カテゴリー", "最古の口コミ日"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls = self._collect_detail_urls(url)
        self.total_items = len(detail_urls)
        self.logger.info("事業者ページURL収集完了: %d 件", len(detail_urls))
        seen_suppliers: set[str] = set()
        for du in detail_urls:
            item = self._scrape_detail(du, seen_suppliers)
            if item:
                yield item

    # ------------------------------------------------------------------ 列挙
    def _fetch_xml_locs(self, sitemap_url: str) -> tuple[str, list[str]] | None:
        try:
            r = self.session.get(sitemap_url, timeout=self.TIMEOUT)
            if r.status_code != 200 or "xml" not in r.headers.get("Content-Type", "").lower():
                self.logger.info("サイトマップではない応答: %s (%s)", sitemap_url, r.status_code)
                return None
            root = ET.fromstring(r.content)
        except Exception as e:
            self.logger.warning("サイトマップ取得エラー: %s %s", sitemap_url, e)
            return None
        locs = [el.text.strip() for el in root.iter() if el.tag.endswith("loc") and el.text]
        return root.tag.lower(), locs

    def _sitemaps_from_robots(self, url: str) -> list[str]:
        robots_url = urljoin(url, "/robots.txt")
        try:
            r = self.session.get(robots_url, timeout=self.TIMEOUT)
            r.raise_for_status()
        except Exception as e:
            self.logger.warning("robots.txt 取得エラー: %s", e)
            return []
        return [
            urljoin(url, line.split(":", 1)[1].strip())
            for line in r.text.splitlines()
            if line.lower().startswith("sitemap:")
        ]

    def _collect_detail_urls(self, url: str) -> list[str]:
        host = urlparse(url).netloc
        queue: list[str] = []
        res = self._fetch_xml_locs(url)
        if res is None:
            # 正規 URL の sitemap.xml は 404 のため robots.txt 記載のサイトマップ群へ
            queue = self._sitemaps_from_robots(url)
        else:
            tag, locs = res
            if not tag.endswith("sitemapindex"):
                return self._filter_details(locs, host)
            queue = locs

        pages: list[str] = []
        visited: set[str] = set()
        while queue:
            sm = queue.pop(0)
            if sm in visited:
                continue
            visited.add(sm)
            res = self._fetch_xml_locs(sm)
            if res is None:
                continue
            tag, locs = res
            if tag.endswith("sitemapindex"):
                queue.extend(locs)
            else:
                pages.extend(locs)
        return self._filter_details(pages, host)

    @staticmethod
    def _filter_details(pages: list[str], host: str) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for u in pages:
            p = urlparse(u)
            if p.netloc != host or not _DETAIL_PATTERN.match(p.path):
                continue
            if u not in seen:
                seen.add(u)
                out.append(u)
        return out

    # ------------------------------------------------------------------ 詳細
    def _scrape_detail(self, url: str, seen_suppliers: set[str]) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        name_el = soup.select_one("a.supplier-name-block_link") or soup.select_one(".supplier-name-block a")
        if not name_el:
            return None
        name = name_el.get_text(strip=True)
        supplier_href = name_el.get("href") or ""
        supplier_key = supplier_href.strip("/") or name
        if supplier_key in seen_suppliers:
            return None

        # 所在地 → 都道府県で絞り込み (サービス対応エリアではなく所在地で判定)
        loc_el = soup.select_one(".supplier-info-block_address .address-block_text") or soup.select_one(".address-block_text")
        raw_addr = loc_el.get_text(" ", strip=True).replace("\xa0", " ").replace("　", " ").strip() if loc_el else ""
        post_code = ""
        m = re.search(r"〒?\s*(\d{3})-?(\d{4})", raw_addr)
        if m:
            post_code = f"〒{m.group(1)}-{m.group(2)}"
            raw_addr = raw_addr[m.end():].strip()
        pm = _PREF_RE.search(raw_addr)
        pref = pm.group(1) if pm else ""
        seen_suppliers.add(supplier_key)
        if pref not in _TARGET_PREFS:
            self.logger.debug("対象外の所在地: %s (%s)", name, pref or raw_addr)
            return None
        addr = raw_addr[pm.end():].strip() if pm and raw_addr.startswith(pref) else raw_addr

        data = {Schema.URL: url, Schema.NAME: name, Schema.PREF: pref, Schema.ADDR: addr}
        if post_code:
            data[Schema.POST_CODE] = post_code

        rep_el = soup.select_one(".supervisor-detail-block_name")
        if rep_el:
            data[Schema.REP_NM] = rep_el.get_text(strip=True)

        # カテゴリー: パンくずの大カテゴリ > サービス名
        cats = [
            el.get_text(strip=True)
            for el in soup.select(".breadcrumb-link-block_name")
            if el.get_text(strip=True) not in ("トップ", "サービス詳細")
        ]
        title = soup.title.get_text(strip=True) if soup.title else ""
        tm = re.match(r"^" + re.escape(name) + r"の(.+?)\[\d+\]", title)
        if tm:
            cats.append(tm.group(1).strip())
        if cats:
            data["カテゴリー"] = " > ".join(dict.fromkeys(cats))

        # 口コミ件数 (事業者全体)
        count = None
        count_el = soup.select_one('[data-element-id="element-review-count"]')
        txt = count_el.get_text(strip=True) if count_el else ""
        cm = re.search(r"([\d,]+)\s*件", txt) or re.search(r"口コミ\s*([\d,]+)\s*件", soup.get_text(" ", strip=True))
        if cm:
            count = int(cm.group(1).replace(",", ""))
            data[Schema.REV_SCR] = str(count)

        # 最古の口コミ日
        if count != 0:
            reviews_url = urljoin(url, supplier_href.rstrip("/") + "/reviews/") if supplier_href else ""
            if reviews_url:
                oldest = self._oldest_review_date(reviews_url)
                if oldest:
                    data["最古の口コミ日"] = oldest
        return data

    def _oldest_review_date(self, reviews_url: str) -> str:
        """口コミ一覧の全ページから最も古い投稿日を返す (YYYY-MM-DD)。

        一覧はサービス(カテゴリ)単位のまとまりで並び日付順ではないため、全ページを走査する。
        1 ページ目のページャから最終ページ番号を得て、残りページを少数スレッドで並行取得する。
        """
        first = self._fetch_review_page(reviews_url)
        if first is None:
            return ""
        html = first
        dates = self._extract_dates(html)
        page_nums = [int(n) for n in re.findall(r'[?&;]page=(\d+)', html)]
        last_page = min(max(page_nums, default=1), self.REVIEW_MAX_PAGES)
        sep = "&" if "?" in reviews_url else "?"
        targets = [f"{reviews_url}{sep}page={n}" for n in range(2, last_page + 1)]
        last_html = html
        last_url = reviews_url
        if targets:
            with ThreadPoolExecutor(max_workers=self.REVIEW_WORKERS) as ex:
                for u, h in zip(targets, ex.map(self._fetch_review_page, targets)):
                    if h:
                        dates.extend(self._extract_dates(h))
                        last_html, last_url = h, u
        # ページャ表示より後ろが残っていれば rel=next で辿る
        for _ in range(self.REVIEW_MAX_PAGES):
            m = _NEXT_RE.search(last_html or "")
            if not m:
                break
            next_url = urljoin(last_url, (m.group(1) or m.group(2)).replace("&amp;", "&"))
            if next_url in targets or next_url == reviews_url:
                break
            h = self._fetch_review_page(next_url)
            found = self._extract_dates(h or "")
            if not found:
                break
            dates.extend(found)
            last_html, last_url = h, next_url
        if not dates:
            return ""
        y, mo, d = min(dates)
        return f"{y:04d}-{mo:02d}-{d:02d}"

    def _fetch_review_page(self, url: str) -> str | None:
        time.sleep(self.REVIEW_PAGE_DELAY)
        for attempt in range(3):
            try:
                r = self.session.get(url, timeout=self.TIMEOUT)
                if r.status_code == 200:
                    r.encoding = "utf-8"
                    return r.text
                self.logger.debug("口コミページ %s: HTTP %s", url, r.status_code)
            except Exception as e:
                self.logger.debug("口コミページ取得エラー %s: %s", url, e)
            time.sleep(2 * (attempt + 1))
        self.logger.warning("口コミページ取得失敗: %s", url)
        return None

    @staticmethod
    def _extract_dates(html: str) -> list[tuple[int, int, int]]:
        out = []
        for m in _CARD_DATE_RE.finditer(html):
            dm = _DATE_RE.search(m.group(1))
            if dm:
                out.append((int(dm.group(1)), int(dm.group(2)), int(dm.group(3))))
        return out

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    YourmystarScraper().execute("https://yourmystar.jp/sitemap.xml")
