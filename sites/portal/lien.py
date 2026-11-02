"""
対象サイト: https://www.lien-toyama.net/shop/
富山のフリーマガジンLien 掲載店舗一覧（/shop/page/N/ 1ページ5件）→ 各店舗詳細 /<slug>/ の table.shopInfo
"""
import re
import urllib.parse
from typing import Generator
from src.framework.static import StaticCrawler
from src.const.schema import Schema

_KANA_RE = re.compile(r"^(.*?)[（(]([^（）()]+)[）)]\s*$")


class LienScraper(StaticCrawler):
    """Lien富山 掲載店舗 スクレイパー"""
    DELAY = 3.0  # 待機時間（秒）
    EXTRA_COLUMNS = ["市区町村", "駐車場", "店舗詳細URL"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        base = url.split("?")[0].rstrip("/") + "/"
        seen = set()
        page = 1
        last_page = None
        while True:
            list_url = base if page == 1 else urllib.parse.urljoin(base, f"page/{page}/")
            soup = self.get_soup(list_url)
            if soup is None:
                break
            cards = soup.select("div.LiensNews")
            if not cards:
                break
            if last_page is None:
                nums = [int(m.group(1)) for a in soup.select("a[href]")
                        if (m := re.search(r"/shop/page/(\d+)/", a["href"]))]
                last_page = max(nums) if nums else 1
            new = 0
            for card in cards:
                a = card.select_one("h2 a[href]")
                if not a:
                    continue
                detail_url = urllib.parse.urljoin(list_url, a["href"])
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                new += 1
                city, genre = "", ""
                sd = card.select_one(".shopData")
                if sd:
                    m = re.search(r"[（(]([^/／]*)[/／](.*)[）)]", sd.get_text(" ", strip=True))
                    if m:
                        city, genre = m.group(1).strip(), m.group(2).strip()
                item = self._parse_detail(detail_url, a.get_text(strip=True), city, genre)
                if item:
                    yield item
            if new == 0 or page >= last_page:
                break
            page += 1

    def _parse_detail(self, detail_url: str, list_name: str, city: str, genre: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None
        info = {}
        links = {}
        for tr in soup.select("table.shopInfo tr"):
            th, td = tr.find("th"), tr.find("td")
            if not th or not td:
                continue
            label = th.get_text(strip=True)
            info[label] = td.get_text(" ", strip=True)
            a = td.find("a", href=True)
            if a:
                links[label] = re.sub(r"\s+", "", a["href"])
        for dl in soup.select("#main dl"):
            dt, dd = dl.find("dt"), dl.find("dd")
            if dt and dd and dt.get_text(strip=True) == "エリア" and not city:
                city = dd.get_text(" ", strip=True)

        raw_name = info.get("店舗名", "")
        name, kana = raw_name, ""
        m = _KANA_RE.match(raw_name)
        if m:
            name, kana = m.group(1).strip(), m.group(2).strip()
        if not name:
            h1 = soup.select_one("h1#shopTitle")
            name = h1.get_text(strip=True) if h1 else list_name

        addr = info.get("住所", "")
        addr = re.sub(r"^〒?\s*\d{3}-?\d{4}\s*", "", addr)
        addr = re.sub(r"^富山県\s*", "", addr)

        def sns(*keys):
            for k in keys:
                if k in links:
                    return links[k]
            return ""

        hp = links.get("ホームページ", "") or links.get("HP", "")
        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.NAME_KANA: kana,
            Schema.PREF: "富山県",
            Schema.ADDR: addr,
            Schema.TEL: info.get("電話番号", "") or info.get("TEL", ""),
            Schema.CAT_SITE: info.get("ジャンル", "") or genre,
            Schema.TIME: info.get("営業時間", ""),
            Schema.HOLIDAY: info.get("定休日", ""),
            Schema.HP: hp,
            Schema.INSTA: sns("Instagram", "インスタグラム"),
            Schema.X: sns("X", "Twitter", "X（旧Twitter）"),
            Schema.FB: sns("Facebook", "フェイスブック"),
            Schema.LINE: sns("LINE", "LINE公式アカウント"),
            "市区町村": city,
            "駐車場": info.get("駐車場", ""),
            "店舗詳細URL": detail_url,
        }


if __name__ == "__main__":
    scraper = LienScraper()
    scraper.execute("https://www.lien-toyama.net/shop/")
