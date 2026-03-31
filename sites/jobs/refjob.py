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

BASE_URL = "https://refjob.jp"
_DETAIL_PATTERN = re.compile(r"^https://refjob\.jp/.+/detail/\d+$")


class RefjobScraper(StaticCrawler):
    """リフラクジョブ メンズエステ店舗情報スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["店舗形態", "お仕事内容", "勤務地", "最寄駅", "Twitter", "メール", "LINE"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        """トップページからエリアリンクを取得 → 詳細ページをスクレイプ"""
        area_urls = self._get_area_urls(url)
        self.logger.info("エリア収集完了: %d 件", len(area_urls))
        detail_urls = self._collect_detail_urls(area_urls)
        self.total_items = len(detail_urls)
        self.logger.info("詳細URL収集完了: %d 件", len(detail_urls))
        for detail_url in detail_urls:
            item = self._scrape_detail(detail_url)
            if item:
                yield item

    def _get_area_urls(self, top_url: str) -> list[str]:
        soup = self.get_soup(top_url)
        if soup is None:
            return []
        urls: list[str] = []
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if "/area/" in href or "/list/" in href:
                full = href if href.startswith("http") else urljoin(BASE_URL, href)
                if full.startswith(BASE_URL) and full not in seen:
                    seen.add(full)
                    urls.append(full)
        return urls

    def _collect_detail_urls(self, area_urls: list[str]) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        for area_url in area_urls:
            current = area_url
            visited: set[str] = set()
            while current and current not in visited:
                visited.add(current)
                soup = self.get_soup(current)
                if soup is None:
                    break
                for a in soup.find_all("a", href=True):
                    href = a["href"].strip()
                    if _DETAIL_PATTERN.match(href) and href not in seen:
                        seen.add(href)
                        urls.append(href)
                # 次ページ
                next_btn = soup.select_one("li.page-item:not(.disabled) a.page-link")
                next_url = None
                for li in soup.select("li.page-item:not(.disabled) a.page-link"):
                    text = li.get_text(strip=True)
                    href = li.get("href", "").strip()
                    if "NEXT" in text.upper() and href.startswith("https://refjob.jp/"):
                        next_url = href
                        break
                current = next_url
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url, Schema.CAT_SITE: "メンズエステ"}

        h = soup.select_one("h2.shop-name") or soup.select_one("h1.shop-name")
        if h:
            data[Schema.NAME] = re.sub(r"\s+", " ", h.get_text(strip=True))

        article = soup.select_one("article.shop-recruit#rec")
        if article:
            for dt in article.select("dl dt"):
                key = re.sub(r"\s+", " ", dt.get_text(strip=True))
                dd = dt.find_next_sibling("dd")
                if not dd:
                    continue
                val = dd.get_text(" ", strip=True)

                if "店舗形態" in key:
                    data["店舗形態"] = val
                elif "お仕事内容" in key:
                    data["お仕事内容"] = val.replace("\n", " ")
                elif "勤務地" in key:
                    # テキストのみ（マップリンク除外）
                    parts = []
                    for child in dd.children:
                        from bs4 import NavigableString
                        if isinstance(child, NavigableString):
                            t = str(child).strip()
                            if t:
                                parts.append(t)
                        elif child.name == "a":
                            break
                    data["勤務地"] = " ".join(parts).strip() or val
                elif "最寄駅" in key:
                    data["最寄駅"] = val
                elif "営業時間" in key:
                    data[Schema.TIME] = val
                elif "電話番号" in key:
                    a = dd.select_one('a[href^="tel:"]')
                    data[Schema.TEL] = a.get_text(strip=True) if a else val
                elif "リンク集" in key:
                    hp, tw = "", ""
                    for p in dd.select("p"):
                        lbl = p.get_text(" ", strip=True)
                        pa = p.find("a", href=True)
                        if not pa:
                            continue
                        href = pa["href"].strip()
                        if lbl.startswith("HP"):
                            hp = href
                        elif "Twitter" in lbl:
                            tw = href
                    if not tw:
                        pa = dd.select_one('a[href*="x.com"], a[href*="twitter.com"]')
                        if pa:
                            tw = pa["href"].strip()
                    data[Schema.HP] = hp
                    data["Twitter"] = tw

        # メール
        mail_a = soup.select_one('li.contact_mail a[href^="mailto:"]') or soup.select_one('a[href^="mailto:"]')
        if mail_a:
            href = mail_a.get("href", "")
            data["メール"] = href[7:].split("?")[0].strip()

        # LINE
        line_a = soup.select_one("li.contact_line a[href]")
        if line_a:
            data["LINE"] = line_a["href"].strip()

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    RefjobScraper().execute("https://refjob.jp")
