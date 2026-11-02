"""
対象サイト: https://shop.shun.tv/blog/
月刊しゅんWEB 掲載店舗（十勝・帯広圏）

取得フロー:
    1. 店舗一覧 /blog/ → /blog/page/{N}/ (全16ページ・1ページ10件) を巡回
    2. 各記事詳細 (/YYYY/MM/DD/{ID}/) の「店舗情報」ブロック
       (.smb-information__item のラベル/値) から住所・TEL・営業時間・定休日・外部リンクを取得
    3. 住所・TEL のどちらかが無い記事は除外 (備考の指示)

実行方法:
    python scripts/sites/portal/web_6.py
"""
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_PATTERN = re.compile(
    r"^(北海道|東京都|(?:大阪|京都)府|(?:神奈川|和歌山|鹿児島|[^\s　]{2})県)"
)
_DETAIL_PATTERN = re.compile(r"/\d{4}/\d{2}/\d{2}/\d+/?$")

_SNS_DOMAINS = [
    ("instagram.com", Schema.INSTA),
    ("facebook.com", Schema.FB),
    ("twitter.com", Schema.X),
    ("x.com", Schema.X),
    ("line.me", Schema.LINE),
    ("lin.ee", Schema.LINE),
    ("tiktok.com", Schema.TIKTOK),
]


class ShunWebShopScraper(StaticCrawler):
    """月刊しゅんWEB 掲載店舗 スクレイパー"""

    DELAY = 3.0

    EXTRA_COLUMNS = ["詳細ページURL", "投稿日"]

    @staticmethod
    def _clean(text: str) -> str:
        return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()

    def _list_page_url(self, url: str, page: int) -> str:
        if page == 1:
            return url
        return urljoin(url if url.endswith("/") else url + "/", f"page/{page}/")

    def _parse_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        h1 = soup.select_one("h1.c-entry__title")
        name = self._clean(h1.get_text()) if h1 else ""

        info: dict[str, str] = {}
        links: list[str] = []
        for item in soup.select(".p-entry-content .smb-information__item"):
            label_el = item.select_one(".smb-information__item__label")
            body_el = item.select_one(".smb-information__item__body")
            if not label_el or not body_el:
                continue
            label = self._clean(label_el.get_text())
            if label == "外部リンク":
                links.extend(
                    a["href"].strip() for a in body_el.find_all("a", href=True)
                    if a["href"].startswith("http")
                )
                continue
            if label not in info:
                info[label] = self._clean(body_el.get_text(" "))

        addr_raw = info.get("住所", "")
        tel = info.get("電話番号", "") or info.get("TEL", "")
        if not addr_raw or not tel:
            return None

        pref, addr = "北海道", addr_raw
        m = _PREF_PATTERN.match(addr_raw)
        if m:
            pref, addr = m.group(1), addr_raw[m.end():].strip()

        cats = [
            self._clean(a.get_text())
            for a in soup.select(".c-entry__header .c-meta__item--categories a")
        ]
        published = soup.select_one(".c-entry__header .c-meta__item--published time")
        post_date = ""
        if published and published.get("datetime"):
            post_date = published["datetime"][:10]

        item = {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.TIME: info.get("営業時間", "") or info.get("受付時間", ""),
            Schema.HOLIDAY: info.get("定休日", ""),
            Schema.CAT_SITE: "/".join(dict.fromkeys(c for c in cats if c)),
            Schema.HP: "",
            Schema.INSTA: "",
            Schema.FB: "",
            Schema.X: "",
            Schema.LINE: "",
            Schema.TIKTOK: "",
            "詳細ページURL": detail_url,
            "投稿日": post_date,
        }
        for link in links:
            for domain, col in _SNS_DOMAINS:
                if domain in link:
                    if not item[col]:
                        item[col] = link
                    break
            else:
                if not item[Schema.HP]:
                    item[Schema.HP] = link
        return item

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        page = 1
        last_page = 1
        while page <= last_page:
            list_url = self._list_page_url(url, page)
            soup = self.get_soup(list_url)
            if soup is None:
                self.logger.warning("一覧取得失敗: %s", list_url)
                break

            for a in soup.select("a.c-pagination__item-link[href]"):
                m = re.search(r"/page/(\d+)/?", a["href"])
                if m:
                    last_page = max(last_page, int(m.group(1)))

            detail_urls = [
                urljoin(list_url, a["href"])
                for a in soup.select("li.c-entries__item > a[href]")
                if _DETAIL_PATTERN.search(a["href"])
            ]
            if not detail_urls:
                break
            self.logger.info("一覧 %d/%d: %d件", page, last_page, len(detail_urls))

            for detail_url in detail_urls:
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                item = self._parse_detail(detail_url)
                if item is None:
                    self.logger.info("住所/TEL無しのため除外: %s", detail_url)
                    continue
                yield item
            page += 1


if __name__ == "__main__":
    scraper = ShunWebShopScraper()
    scraper.execute("https://shop.shun.tv/blog/")
