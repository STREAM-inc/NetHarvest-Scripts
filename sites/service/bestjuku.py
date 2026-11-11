# scripts/sites/service/bestjuku.py
"""
塾選（ジュクセン） — 全国の学習塾・予備校 教室一覧

取得対象:
    - 全国の塾の「教室」単位の情報（塾名・教室名・都道府県・住所・最寄駅・対象学年・目的・授業形式 等）

取得フロー:
    1. 起点 url (/search/requirement/city/?prefecture=xxx) から都道府県選択ページ
       (/search/requirement/city-prefecture/) を辿り、47 都道府県スラッグを取得
       （起点 url の都道府県を先頭に巡回）
    2. 都道府県ごとに /search/requirement/city/?prefecture=xxx の市区町村コード
       (address_code_and_upper) を抽出
    3. 市区町村ごとの検索結果 /search/?prefecture=..&address_code_and_upper=..&page=N
       （1ページ30塾）から「{市区町村}のすべての教室を見る」(/juku/{id}/class/{pref}/{code}/) を収集
    4. 塾×市区町村の教室一覧から教室詳細 /juku/{id}/class/{class_id}/ を取得して即 yield
       （教室IDで全体 dedupe）

    robots.txt の Disallow (?s= / ?text= / ?dtl= 等) は使用しない。
    TEL はサイト上に掲載されない（問い合わせフォーム誘導のみ）ため常に空。

実行方法:
    python scripts/sites/service/bestjuku.py
    docker compose exec worker python /app/bin/run_flow.py --site-id bestjuku
"""

import html
import json
import re
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PER_PAGE = 30
_MAX_PAGES = 100
_CLASS_RE = re.compile(r"^/juku/(\d+)/class/(\d+)/$")
_JUKU_CITY_RE = re.compile(r"^/juku/(\d+)/class/[a-z]+/\d+_\d+_\d+/$")


class Bestjuku(StaticCrawler):
    """塾選（ジュクセン） スクレイパー"""

    DELAY = 2.0
    # CloudFront WAF が Chrome 完全形式 UA (Chrome/94.0.4606.61, 131.0.0.0 等) を 403 にする。
    # 正直な Bot UA なら 200 で返る。
    USER_AGENT = "NetHarvestBot/1.0"
    EXTRA_COLUMNS = [
        "塾名",
        "教室名",
        "最寄駅",
        "対象学年",
        "目的",
        "授業形式",
        "教科",
        "特徴",
        "コース",
        "塾ID",
        "教室ID",
    ]

    # ------------------------------------------------------------------ #
    def parse(self, url: str):
        start_pref = (parse_qs(urlparse(url).query).get("prefecture") or [""])[0]
        prefs = self._prefectures(url)
        if start_pref:
            prefs = [start_pref] + [p for p in prefs if p != start_pref]
        if not prefs:
            raise RuntimeError(f"都道府県一覧を取得できませんでした: {url}")

        seen_class: set[str] = set()
        seen_jc: set[str] = set()
        for pref in prefs:
            city_url = urljoin(url, "?" + urlencode({"prefecture": pref}))
            for code in self._cities(city_url):
                for jc_path in self._juku_city_links(url, pref, code):
                    if jc_path in seen_jc:
                        continue
                    seen_jc.add(jc_path)
                    soup = self.get_soup(urljoin(url, jc_path))
                    if soup is None:
                        continue
                    for a in soup.select("a[href]"):
                        m = _CLASS_RE.match(a["href"])
                        if not m or m.group(2) in seen_class:
                            continue
                        seen_class.add(m.group(2))
                        item = self._detail(urljoin(url, a["href"]), m.group(1), m.group(2))
                        if item:
                            yield item

    # ------------------------------------------------------------------ #
    def _prefectures(self, url: str) -> list[str]:
        soup = self.get_soup(urljoin(url, "../city-prefecture/"))
        if soup is None:
            return []
        prefs = []
        for el in soup.select('[data-name="prefectures"][data-value]'):
            v = el["data-value"]
            if v not in prefs:
                prefs.append(v)
        return prefs

    def _cities(self, city_url: str) -> list[str]:
        soup = self.get_soup(city_url)
        if soup is None:
            return []
        raw = html.unescape(str(soup))
        codes = []
        for c in re.findall(r"address_code_and_upper=(\d+_\d+_\d+)", raw):
            if c not in codes:
                codes.append(c)
        if not codes:  # CitySearchBox の props JSON から
            for c in re.findall(r'"addressCode":\[0,"(\d+_\d+_\d+)"\]', raw):
                if c not in codes:
                    codes.append(c)
        return codes

    def _juku_city_links(self, url: str, pref: str, code: str):
        for page in range(1, _MAX_PAGES + 1):
            q = {"prefecture": pref, "address_code_and_upper": code}
            if page > 1:
                q["page"] = page
            soup = self.get_soup(urljoin(url, "/search/?" + urlencode(q)))
            if soup is None:
                return
            links = []
            for a in soup.select("a[href]"):
                h = a["href"]
                if _JUKU_CITY_RE.match(h) and h.rstrip("/").endswith(code) and h not in links:
                    links.append(h)
            if not links:
                return
            yield from links
            total = self._total(soup)
            if total is not None and page * _PER_PAGE >= total:
                return
            if len(links) < _PER_PAGE and total is None:
                return

    @staticmethod
    def _total(soup):
        m = re.search(r"件目\s*/\s*全\s*([\d,]+)\s*件", soup.get_text(" ", strip=True))
        return int(m.group(1).replace(",", "")) if m else None

    # ------------------------------------------------------------------ #
    def _detail(self, detail_url: str, juku_id: str, class_id: str):
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        school = {}
        for sc in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(sc.string or "")
            except (ValueError, TypeError):
                continue
            nodes = data.get("@graph", [data]) if isinstance(data, dict) else data
            for n in nodes:
                if isinstance(n, dict) and n.get("@type") == "School":
                    school = n
        addr = school.get("address") or {}
        brand = (school.get("parentOrganization") or {}).get("name", "")
        full_name = school.get("name", "")
        if not full_name:
            h1 = soup.select_one("h1")
            full_name = re.sub(r"\s+", " ", h1.get_text(" ", strip=True)) if h1 else ""
        class_name = full_name[len(brand):].strip() if brand and full_name.startswith(brand) else ""

        station = ""
        address_text = ""
        for h in soup.select("h3.sub-heading"):
            tx = h.get_text("", strip=True)
            nxt = h.find_next_sibling()
            val = nxt.get_text(" ", strip=True) if nxt else ""
            if tx.endswith("最寄り駅") and not station:
                station = val
            elif tx.endswith("の住所") and not address_text:
                address_text = val

        info = {}
        for h2 in soup.select("h2"):
            if h2.get_text("", strip=True).endswith("の概要"):
                dl_wrap = h2.find_next_sibling()
                dl = dl_wrap.select_one("dl") if dl_wrap else None
                if dl:
                    for dt in dl.find_all("dt"):
                        dd = dt.find_next_sibling("dd")
                        k = dt.get_text(" ", strip=True)
                        if dd and k not in info:
                            info[k] = re.sub(r"\s+", " ", dd.get_text(" ", strip=True))
                break

        pref = addr.get("addressRegion", "")
        street = addr.get("streetAddress", "")
        post = addr.get("postalCode", "")
        if not street and address_text:
            m = re.match(r"〒?\s*(\d{3}-?\d{4})?\s*(.*)", address_text)
            post = post or (m.group(1) or "")
            street = m.group(2)
            if pref and street.startswith(pref):
                street = street[len(pref):]

        return {
            Schema.NAME: full_name,
            Schema.PREF: pref,
            Schema.POST_CODE: post,
            Schema.ADDR: street,
            Schema.TEL: "",
            Schema.CAT_SITE: "学習塾・予備校",
            Schema.URL: detail_url,
            "塾名": brand,
            "教室名": class_name,
            "最寄駅": station,
            "対象学年": info.get("対象学年", ""),
            "目的": info.get("目的", ""),
            "授業形式": info.get("授業形式", ""),
            "教科": info.get("教科", ""),
            "特徴": info.get("特徴", ""),
            "コース": info.get("コース", ""),
            "塾ID": juku_id,
            "教室ID": class_id,
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = Bestjuku()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://bestjuku.com/search/requirement/city/?prefecture=tokyo")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
