"""
対象サイト: EPARKくすりの窓口 https://www.kusurinomadoguchi.com/

巡回: 都道府県エリア一覧 /{pref}/area → 市区町村別一覧 /{pref}/list/{id}
      (30件/頁・「次の30件」リンクで終端判定、範囲外 ?p=N は 404)
      → 店舗詳細 /{pref}/s{id}
詳細は2種類のレイアウト (dl.rich-shop-info / dl.st-shop-info) があるため、
`dl[class*=shop-info]` の dt/dd と JSON-LD (Pharmacy) を併用して取得する。
"""
import json
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

import bs4

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# サイト独自の都道府県スラッグ (北海道=hokkai, 京都=kyouto, 群馬=gumma に注意)
PREF_SLUGS = [
    "tokyo", "hokkai", "aomori", "iwate", "miyagi", "akita", "yamagata", "fukushima",
    "ibaraki", "tochigi", "gumma", "saitama", "chiba", "kanagawa", "niigata",
    "toyama", "ishikawa", "fukui", "yamanashi", "nagano", "gifu", "shizuoka",
    "aichi", "mie", "shiga", "kyouto", "osaka", "hyogo", "nara", "wakayama",
    "tottori", "shimane", "okayama", "hiroshima", "yamaguchi", "tokushima",
    "kagawa", "ehime", "kochi", "fukuoka", "saga", "nagasaki", "kumamoto",
    "oita", "miyazaki", "kagoshima", "okinawa",
]

# 市区町村別一覧は数値IDのみ採用 (nijusanku / sapporo 等の集約ページは重複のため除外)
_LIST_RE = re.compile(r"/([a-z]+)/list/(\d+)$")
_DETAIL_RE = re.compile(r"/([a-z]+)/s(\d+)$")
_DAYS = [
    ("月", Schema.TIME_MON), ("火", Schema.TIME_TUE), ("水", Schema.TIME_WED),
    ("木", Schema.TIME_THU), ("金", Schema.TIME_FRI), ("土", Schema.TIME_SAT),
    ("日", Schema.TIME_SUN),
]
MAX_PAGES_PER_LIST = 500


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("\xa0", " ").replace("　", " ")).strip()


class Epark3Scraper(StaticCrawler):
    """EPARKくすりの窓口 薬局・ドラッグストア店舗スクレイパー"""

    DELAY = 3.0
    EXTRA_COLUMNS = [
        "クレジットカード",
        "電子マネー",
        "スマホ決済",
        "営業時間(祝)",
        "調剤開局時間",
        "ブランド",
        "アクセス",
        "駐車場",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        for slug in PREF_SLUGS:
            area_url = urljoin(url, f"/{slug}/area")
            soup = self.get_soup(area_url)
            if soup is None:
                self.logger.warning("エリアページ取得失敗: %s", area_url)
                continue
            list_urls = []
            for a in soup.find_all("a", href=True):
                full = urljoin(url, a["href"])
                m = _LIST_RE.search(full)
                if m and m.group(1) == slug and full not in list_urls:
                    list_urls.append(full)
            self.logger.info("%s: 市区町村一覧 %d 件", slug, len(list_urls))
            for list_url in list_urls:
                for detail_url in self._iter_detail_urls(list_url, slug):
                    store_id = _DETAIL_RE.search(detail_url).group(2)
                    if store_id in seen:
                        continue
                    seen.add(store_id)
                    item = self._scrape_detail(detail_url)
                    if item:
                        yield item

    def _iter_detail_urls(self, list_url: str, slug: str) -> Generator[str, None, None]:
        current = list_url
        visited: set[str] = set()
        for _ in range(MAX_PAGES_PER_LIST):
            if not current or current in visited:
                return
            visited.add(current)
            soup = self.get_soup(current)
            if soup is None:
                return
            for a in soup.find_all("a", href=True):
                full = urljoin(list_url, a["href"])
                m = _DETAIL_RE.search(full)
                if m and m.group(1) == slug:
                    yield full
            nxt = soup.find("a", string=re.compile(r"次の\d+件"), href=True)
            current = urljoin(list_url, nxt["href"]) if nxt else None

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        ld = self._json_ld(soup)
        info: dict[str, str] = {}
        for dl in soup.select("dl[class*=shop-info]"):
            dt, dd = dl.find("dt"), dl.find("dd")
            if not dt or not dd:
                continue
            key = _clean(dt.get_text(" ", strip=True))
            if key and key not in info:
                info[key] = _clean(dd.get_text(" ", strip=True))

        name = _clean(ld.get("name"))
        if not name:
            h1 = soup.find("h1")
            name = _clean(h1.get_text(" ", strip=True)) if h1 else ""
        if not name:
            return None

        addr_ld = ld.get("address") or {}
        pref = _clean(addr_ld.get("addressRegion"))
        addr = re.sub(r"\s*MAP$", "", info.get("住所", ""))
        if not addr:
            addr = _clean(f"{pref}{addr_ld.get('addressLocality', '')}{addr_ld.get('streetAddress', '')}")
        post = re.sub(r"\D", "", str(addr_ld.get("postalCode") or ""))
        if len(post) == 7:
            post = f"{post[:3]}-{post[3:]}"

        tel_raw = ld.get("telephone") or info.get("電話番号") or info.get("TEL / ネット予約") or ""
        m = re.search(r"0[\d-]{8,12}", str(tel_raw))
        tel = re.sub(r"\D", "", m.group(0)) if m else ""

        item = {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.CAT_SITE: info.get("ジャンル") or _clean(ld.get("keywords")),
            Schema.HOLIDAY: info.get("定休日", ""),
            Schema.HP: info.get("ホームページ", ""),
            "クレジットカード": info.get("クレジットカード", "記載なし"),
            "電子マネー": info.get("電子マネー", ""),
            "スマホ決済": info.get("スマホ決済", ""),
            "ブランド": re.sub(r"の店舗$", "", info.get("ブランド", "")),
            "アクセス": info.get("アクセス", ""),
            "駐車場": info.get("駐車場", ""),
        }
        payments = [
            f"{k}:{info[k]}" for k in ("クレジットカード", "電子マネー", "スマホ決済") if info.get(k)
        ]
        item[Schema.PAYMENTS] = " / ".join(payments)

        schedules = self._schedules(soup)
        biz = schedules.get("営業時間", {})
        for day, col in _DAYS:
            item[col] = biz.get(day, "")
        item["営業時間(祝)"] = biz.get("祝", "")
        item[Schema.TIME] = " / ".join(f"{d} {t}" for d, t in biz.items() if t)
        disp = schedules.get("調剤開局時間", {})
        item["調剤開局時間"] = " / ".join(f"{d} {t}" for d, t in disp.items() if t)
        return item

    @staticmethod
    def _json_ld(soup: bs4.BeautifulSoup) -> dict:
        for sc in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(sc.string or "")
            except (json.JSONDecodeError, TypeError):
                continue
            for d in data if isinstance(data, list) else [data]:
                if isinstance(d, dict) and d.get("@type") in ("Pharmacy", "Store", "LocalBusiness", "DrugStore"):
                    return d
        return {}

    @staticmethod
    def _schedules(soup: bs4.BeautifulSoup) -> dict[str, dict[str, str]]:
        """ul.weekly-schedule / ul.ps-weekly-schedule から {見出し: {曜日: 時間}} を返す"""
        result: dict[str, dict[str, str]] = {}
        for li in soup.select("ul[class*=weekly-schedule] > li"):
            ttl = li.select_one("p.ttl")
            if not ttl:
                continue
            title = _clean(ttl.get_text())
            if title in result:
                continue
            days: dict[str, str] = {}
            for dl in li.select("dl"):
                dt = dl.find("dt")
                if not dt:
                    continue
                times = [_clean(p.get_text()) for p in dl.select("dd p.num")]
                days[_clean(dt.get_text())] = " ".join(t for t in times if t)
            result[title] = days
        return result


if __name__ == "__main__":
    scraper = Epark3Scraper()
    scraper.execute("https://www.kusurinomadoguchi.com/")
