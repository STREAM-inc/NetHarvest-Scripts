"""
エステ魂 (estama.jp) — メンズエステ・アロマ店舗ポータルの全国掲載店スクレイパー

取得対象:
    - 全国の全掲載店 (店舗単位 1店舗1行)
    - 名称 / 都道府県 / 住所 / 郵便番号 / エリア / 最寄駅 / TEL (携帯番号含め掲載どおり)
      / 営業時間 / 定休日 / 公式HP / 業態 (店舗タイプ) / 店舗ページURL ほか

取得フロー:
    1. 起点 URL から sitemap index (/sitemap.xml) 配下の
       /sitemaps/sitemap_shop.xml を導出し、/shop/{ID}/ を列挙 (約8,200店)
    2. 詳細 /shop/{ID}/ を取得し、店舗情報 dl.table-dl / dl.shop_info_dl /
       JSON-LD (DaySpa, BreadcrumbList) から各項目を抽出して即 yield

    ※/shop/*/reserve/ は robots.txt で Disallow のため取得しない。
    ※運営のサンプル店舗 (店名に「サンプル店舗」) は除外。
    ※店舗紹介文・キャンペーン文・お知らせ等の自由記述は取得しない。

実行方法:
    python scripts/sites/nightlife/estama.py
    python bin/run_flow.py --site-id estama
"""

import json
import logging
import re
import sys
import warnings
from pathlib import Path
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# sitemap XML を html.parser で読むための警告を抑止
try:
    from bs4 import XMLParsedAsHTMLWarning

    warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
except ImportError:  # pragma: no cover
    pass

SITE_CATEGORY = "メンズエステ"

_SHOP_URL_PATTERN = re.compile(r"/shop/(\d+)/?$")
_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_TEL_PATTERN = re.compile(r"0\d{1,4}-?\d{1,4}-?\d{3,4}")


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


def _first_line(dd) -> str:
    """dd 内の最初のテキスト断片 (補足コメントを除外するため)。"""
    if dd is None:
        return ""
    for s in dd.stripped_strings:
        s = _clean(s)
        if s:
            return s
    return ""


class EstamaScraper(StaticCrawler):
    """エステ魂 (estama.jp) スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "地域",
        "エリア",
        "最寄駅",
        "業態",
        "業種区分",
        "予約受付時間",
        "平均予算",
        "店舗ページURL",
    ]

    def parse(self, url: str):
        shop_urls = self._list_shop_urls(url)
        self.total_items = len(shop_urls)
        logger.info("店舗URL数: %d", len(shop_urls))

        for shop_url in shop_urls:
            try:
                item = self._scrape_detail(shop_url)
            except Exception as e:  # noqa: BLE001 — 1件の失敗で全体を止めない
                logger.warning("詳細の処理に失敗しました (%s): %s", shop_url, e)
                continue
            if item:
                yield item

    # ------------------------------------------------------------------
    # 列挙
    # ------------------------------------------------------------------
    def _list_shop_urls(self, url: str) -> list[str]:
        """sitemap index → sitemap_shop.xml から店舗詳細 URL を列挙する。"""
        index_url = urljoin(url, "/sitemap.xml")
        shop_sitemap = urljoin(url, "/sitemaps/sitemap_shop.xml")
        soup = self.get_soup(index_url)
        if soup is not None:
            for loc in soup.find_all("loc"):
                href = loc.get_text(strip=True)
                if href.endswith("sitemap_shop.xml"):
                    shop_sitemap = href
                    break

        soup = self.get_soup(shop_sitemap)
        if soup is None:
            raise RuntimeError(f"店舗サイトマップを取得できませんでした: {shop_sitemap}")

        host = urlparse(url).netloc
        seen: set[str] = set()
        urls: list[str] = []
        for loc in soup.find_all("loc"):
            href = loc.get_text(strip=True)
            m = _SHOP_URL_PATTERN.search(urlparse(href).path)
            if not m or urlparse(href).netloc != host:
                continue
            if m.group(1) in seen:
                continue
            seen.add(m.group(1))
            urls.append(urljoin(url, f"/shop/{m.group(1)}/"))
        return urls

    # ------------------------------------------------------------------
    # 詳細
    # ------------------------------------------------------------------
    def _scrape_detail(self, shop_url: str) -> dict | None:
        soup = self.get_soup(shop_url)
        if soup is None:
            return None

        info: dict[str, object] = {}
        for dl in soup.select("dl.table-dl"):
            for dt in dl.find_all("dt", recursive=False):
                dd = dt.find_next_sibling("dd")
                label = _clean(dt.get_text(" ", strip=True))
                if label and dd is not None and label not in info:
                    info[label] = dd

        summary: dict[str, str] = {}
        box = soup.select_one("dl.shop_info_dl")
        if box is not None:
            for dt in box.find_all("dt"):
                dd = dt.find_next_sibling("dd")
                if dd is not None:
                    summary[_clean(dt.get_text())] = _clean(dd.get_text(" ", strip=True))

        ld_shop, crumbs = {}, []
        for script in soup.select('script[type="application/ld+json"]'):
            try:
                data = json.loads(script.string or "")
            except (ValueError, TypeError):
                continue
            if not isinstance(data, dict):
                continue
            if data.get("@type") == "BreadcrumbList":
                crumbs = [_clean(i.get("name", "")) for i in data.get("itemListElement", [])]
            elif "address" in data:
                ld_shop = data

        name = _first_line(info.get("店名")) or _clean(ld_shop.get("name", ""))
        if not name:
            return None
        if "サンプル店舗" in name:
            return None

        # TEL: dd 内から電話番号形式のみを抜き出す (補足文言を除外)
        tel = ""
        tel_dd = info.get("TEL")
        if tel_dd is not None:
            m = _TEL_PATTERN.search(tel_dd.get_text(" ", strip=True))
            if m:
                tel = m.group(0)

        # 住所 (「大きな地図で見る」リンク文言を除外)
        addr = _first_line(info.get("住所"))
        address = ld_shop.get("address") or {}
        pref = _clean(address.get("addressRegion", "")) if isinstance(address, dict) else ""
        if not pref:
            m = _PREF_PATTERN.match(addr)
            pref = m.group(1) if m else ""
        if not pref:
            for c in crumbs:
                if _PREF_PATTERN.fullmatch(c):
                    pref = c
                    break
        addr_wo_pref = addr[len(pref):] if pref and addr.startswith(pref) else addr
        postal = ""
        if isinstance(address, dict):
            pc = re.sub(r"\D", "", str(address.get("postalCode", "")))
            if len(pc) == 7:
                postal = f"{pc[:3]}-{pc[3:]}"

        # パンくず: TOP / {地域}一覧 / 都道府県 / エリア / (小エリア) / 店名
        region = ""
        area_parts: list[str] = []
        if len(crumbs) >= 4:
            region = re.sub(r"メンズエステ一覧$", "", crumbs[1])
            area_parts = [c for c in crumbs[3:-1] if c]
        area = " > ".join(area_parts)
        if not area:
            area_dt = soup.find("dt", string=re.compile("掲載エリア"))
            if area_dt is not None:
                area = _clean(area_dt.find_next_sibling("dd").get_text(" ", strip=True))

        station = summary.get("最寄り駅", "")
        if station in ("-", "－", "ー"):
            station = ""

        hours = summary.get("営業時間", "") or _first_line(info.get("営業時間"))
        holiday = summary.get("定休日", "") or _first_line(info.get("定休日"))

        hp = ""
        hp_dd = info.get("公式ホームページ")
        if hp_dd is not None:
            a = hp_dd.find("a", href=True)
            hp = a["href"].strip() if a else _first_line(hp_dd)

        line_id = _first_line(info.get("LINE ID"))
        sns = {"x": "", "insta": "", "fb": "", "tiktok": ""}
        acc_dd = info.get("公式アカウント")
        if acc_dd is not None:
            for a in acc_dd.find_all("a", href=True):
                h = a["href"].strip()
                if re.search(r"//(www\.)?(x|twitter)\.com/", h):
                    sns["x"] = sns["x"] or h
                elif "instagram.com" in h:
                    sns["insta"] = sns["insta"] or h
                elif "facebook.com" in h:
                    sns["fb"] = sns["fb"] or h
                elif "tiktok.com" in h:
                    sns["tiktok"] = sns["tiktok"] or h

        payments = []
        for key in ("クレジットカード", "QRコード決済"):
            dd = info.get(key)
            if dd is not None:
                v = _clean(dd.get_text(" ", strip=True))
                if v:
                    payments.append(f"{key}: {v}")

        budget = _clean(info["平均予算"].get_text(" ", strip=True)) if "平均予算" in info else ""

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: postal,
            Schema.ADDR: addr_wo_pref,
            Schema.TEL: tel,
            Schema.HP: hp,
            Schema.TIME: hours,
            Schema.HOLIDAY: holiday,
            Schema.CAT_SITE: SITE_CATEGORY,
            Schema.LINE: line_id,
            Schema.X: sns["x"],
            Schema.INSTA: sns["insta"],
            Schema.FB: sns["fb"],
            Schema.TIKTOK: sns["tiktok"],
            Schema.PAYMENTS: " / ".join(payments),
            "地域": region,
            "エリア": area,
            "最寄駅": station,
            "業態": _first_line(info.get("店舗タイプ")),
            "業種区分": _first_line(info.get("業種")),
            "予約受付時間": _first_line(info.get("予約受付時間")),
            "平均予算": budget,
            "店舗ページURL": shop_url,
        }


if __name__ == "__main__":
    scraper = EstamaScraper()
    scraper.execute("https://estama.jp/")
