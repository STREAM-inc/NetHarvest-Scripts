"""
カフェるんショップ検索 (shop.caferun.jp) — コンカフェ・メイド喫茶等 店舗情報スクレイパー

取得対象:
    - 全国の店舗検索 (/shop_search/) に掲載されている店舗
    - 店舗名、住所、都道府県、TEL、営業時間、定休日、業種、HP/SNS
    - 平均予算、席数、予約/貸切/団体利用/喫煙/WiFi、決済方法、特徴タグ等

取得フロー:
    1. /shop_search/ と /shop_search/{page} を巡回し、通常店舗枠の /shop/{id}/ を収集
    2. 各詳細ページから店舗基本情報と外部リンクを抽出
    3. 紹介文・口コミ・ブログ本文など文章系コンテンツは保存しない

実行方法:
    python scripts/sites/nightlife/caferun_shop.py
    python bin/run_flow.py --site-id caferun_shop
"""

import json
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler


BASE_URL = "https://shop.caferun.jp"
START_URL = f"{BASE_URL}/shop_search/"
ITEMS_PER_PAGE = 50
MAX_PAGES = 120

_SHOP_PATH_RE = re.compile(r"^/shop/(\d+)/?$")
_TOTAL_RE = re.compile(r"([\d,]+)\s*件(?:の店舗がヒット|を表示|$)")
_POST_RE = re.compile(r"〒?\s*(\d{3})[-‐－ー−]?\s*(\d{4})")
_TEL_RE = re.compile(r"(0\d{1,4}[-‐－ー−]?\d{1,4}[-‐－ー−]?\d{3,4})")
_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_INFO_LABELS = {
    "店名",
    "ジャンル",
    "営業時間",
    "定休日",
    "予算",
    "平均予算",
    "TEL",
    "電話番号",
    "住所",
    "出典",
    "クレジット",
    "QRコード決済",
    "席数",
    "予約",
    "貸切",
    "団体利用",
    "喫煙",
    "WiFi",
}

_CAFERUN_HOSTS = {"shop.caferun.jp", "caferun.jp", "www.caferun.jp"}
_SKIP_EXTERNAL_PATTERNS = re.compile(
    r"moe-navi\.jp/admin|caferun\.jp/(?:regist|jobsearch)|mens\.caferun\.jp|img\.caferun\.jp",
    re.IGNORECASE,
)


def _clean(text) -> str:
    if text is None:
        return ""
    value = str(text).replace("\u3000", " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value.strip("｜|")


def _join_unique(values: list[str], sep: str = ", ") -> str:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        cleaned = _clean(value)
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            out.append(cleaned)
    return sep.join(out)


def _full_url(href: str) -> str:
    return urljoin(BASE_URL, href.strip()) if href else ""


def _split_address(raw: str) -> tuple[str, str, str]:
    """住所から (郵便番号, 都道府県, 住所以降) を返す。"""
    text = _clean(raw)
    text = re.sub(r"(?:アクセスマップ|地図を開く|Google\s*MAPを開く|大きな地図で見る)", "", text)
    post = ""
    m_post = _POST_RE.search(text)
    if m_post:
        post = f"{m_post.group(1)}-{m_post.group(2)}"
        text = _POST_RE.sub("", text, count=1).strip()

    pref = ""
    addr = text
    m_pref = _PREF_RE.search(text)
    if m_pref:
        pref = m_pref.group(1)
        addr = text[m_pref.end():].strip()
        while addr.startswith(pref):
            addr = addr[len(pref):].strip()
    return post, pref, addr


def _extract_tel(raw: str) -> str:
    match = _TEL_RE.search(_clean(raw))
    return match.group(1).replace("ー", "-").replace("－", "-").replace("−", "-").replace("‐", "-") if match else ""


def _extract_table_value(cell) -> str:
    """td/dd の文字列と画像 alt を合わせて値化する。決済ブランド画像に対応。"""
    if cell is None:
        return ""
    pieces = [_clean(s) for s in cell.stripped_strings if _clean(s)]
    for img in cell.find_all("img"):
        alt = _clean(img.get("alt"))
        if alt:
            pieces.append(alt)
    return _join_unique(pieces)


def _parse_info_pairs(soup) -> dict[str, str]:
    """詳細ページ内の基本情報ラベルだけを辞書化する。"""
    info: dict[str, str] = {}

    for label_tag in soup.find_all(["th", "dt"]):
        label = _clean(label_tag.get_text(" ", strip=True))
        if label not in _INFO_LABELS:
            continue
        value_tag = label_tag.find_next_sibling(["td", "dd"])
        value = _extract_table_value(value_tag)
        if value and not info.get(label):
            info[label] = value

    return info


def _extract_breadcrumbs(soup) -> list[str]:
    """JSON-LD BreadcrumbList から TOP 以外の階層名を抽出する。"""
    for script in soup.select('script[type="application/ld+json"]'):
        raw = script.string or script.get_text(strip=True)
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        candidates = payload if isinstance(payload, list) else [payload]
        for data in candidates:
            if not isinstance(data, dict) or data.get("@type") != "BreadcrumbList":
                continue
            names: list[tuple[int, str]] = []
            for node in data.get("itemListElement") or []:
                if not isinstance(node, dict):
                    continue
                name = _clean(node.get("name"))
                pos = int(node.get("position") or 0)
                if name and name != "TOP":
                    names.append((pos, name))
            return [name for _, name in sorted(names)]
    return []


def _industry_levels(site_genre: str) -> dict[str, str]:
    """サイトジャンルから NetHarvest 標準の業種階層へ寄せる。"""
    genre = _clean(site_genre)
    lv1 = "飲食店"
    lv2 = "カフェ・バー"
    lv3 = "コンセプトカフェ"

    if "ガールズバー" in genre or "居酒屋" in genre:
        lv2 = "バー・居酒屋"
        lv3 = "ガールズバー・ガールズ居酒屋"
    elif "メイド" in genre:
        lv3 = "メイドカフェ"
    elif "メンズコンカフェ" in genre:
        lv3 = "メンズコンカフェ"
    elif "男装" in genre or "ギャルソン" in genre:
        lv3 = "男装カフェ"
    elif "コスプレキャバクラ" in genre:
        lv2 = "ナイトレジャー"
        lv3 = "コスプレキャバクラ"

    return {
        Schema.CAT_LV1: lv1,
        Schema.CAT_LV2: lv2,
        Schema.CAT_LV3: lv3,
        Schema.CAT_NM: genre or lv3,
    }


def _is_social_url(href: str) -> bool:
    return bool(
        re.search(
            r"(?:twitter|x|instagram|facebook|fb|line|lin\.ee|page\.line|tiktok|youtube|youtu\.be)\.",
            href,
            re.IGNORECASE,
        )
    )


class CaferunShopScraper(StaticCrawler):
    """カフェるんショップ検索 (shop.caferun.jp) 店舗情報スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "掲載媒体名",
        "店舗ID",
        "地方",
        "エリア",
        "詳細エリア",
        "平均予算",
        "席数",
        "予約",
        "貸切",
        "団体利用",
        "喫煙",
        "WiFi",
        "クレジット",
        "QRコード決済",
        "特徴タグ",
        "Google Maps URL",
        "求人URL",
        "出典URL",
        "YouTube",
    ]

    page_start: int = 1
    page_end: int | None = None

    def parse(self, url: str) -> Generator[dict, None, None]:
        start_url = url if url else START_URL
        seen_urls: set[str] = set()
        failed_pages = 0

        first = self.get_soup(start_url)
        if first is None:
            self.logger.error("一覧ページの取得に失敗: %s", start_url)
            return

        total = self._extract_total_count(first)
        if total:
            self.total_items = total
            self.logger.info("検索結果: %d 件", total)

        max_page = self._max_page(total)
        if self.page_end is not None:
            max_page = min(max_page, self.page_end)

        for page in range(max(1, self.page_start), max_page + 1):
            soup = first if page == 1 else self.get_soup(self._page_url(page))
            if soup is None:
                failed_pages += 1
                self.logger.warning("一覧ページ取得失敗: page=%d", page)
                if failed_pages >= 3:
                    self.logger.error("一覧ページの取得に3回失敗したため打ち切り")
                    break
                continue
            failed_pages = 0

            list_items = self._collect_list_items(soup)
            if not list_items:
                self.logger.info("店舗リンクなし。page=%d で終了", page)
                break

            new_count = 0
            for detail_url, meta in list_items:
                if detail_url in seen_urls:
                    continue
                seen_urls.add(detail_url)
                new_count += 1

                item = self._scrape_detail(detail_url, meta)
                if not item:
                    continue
                yield item

            if new_count == 0:
                self.logger.info("新規店舗なし。page=%d で終了", page)
                break

    def _page_url(self, page: int) -> str:
        return START_URL if page <= 1 else urljoin(BASE_URL, f"/shop_search/{page}")

    def _max_page(self, total: int) -> int:
        if total <= 0:
            return MAX_PAGES
        return min(MAX_PAGES, (total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)

    def _extract_total_count(self, soup) -> int:
        candidates = [
            soup.select_one(".js_search_feature_result"),
            soup.select_one(".hit_total.pc"),
            soup.select_one(".hit_total.sp"),
        ]
        for node in candidates:
            text = _clean(node.get_text(" ", strip=True)) if node else ""
            match = re.search(r"(\d[\d,]*)", text)
            if match:
                return int(match.group(1).replace(",", ""))
        text = soup.get_text(" ", strip=True)
        match = _TOTAL_RE.search(text)
        return int(match.group(1).replace(",", "")) if match else 0

    def _collect_list_items(self, soup) -> list[tuple[str, dict[str, str]]]:
        items: list[tuple[str, dict[str, str]]] = []
        for li in soup.select("ul.search_list > li.detail"):
            # PR枠は検索件数とは別に混在するため除外する。
            if "caferun_ba_pac" in (li.get("class") or []):
                continue

            detail_url = ""
            shop_id = ""
            for a in li.select('a[href^="/shop/"]'):
                href = a.get("href", "").strip()
                m = _SHOP_PATH_RE.match(href)
                if not m:
                    continue
                detail_url = _full_url(href)
                shop_id = m.group(1)
                break
            if not detail_url:
                continue

            head_label = _clean(li.select_one(".list_head > span").get_text(" ", strip=True)) if li.select_one(".list_head > span") else ""
            area_parts = [_clean(x) for x in head_label.split("/") if _clean(x)]
            tags = [_clean(tag.get_text(" ", strip=True)) for tag in li.select(".shop_tags li")]

            meta = {
                "店舗ID": shop_id,
                "地方": area_parts[0] if len(area_parts) >= 1 else "",
                "エリア": area_parts[1] if len(area_parts) >= 2 else "",
                "詳細エリア": area_parts[1] if len(area_parts) >= 2 else "",
                "genre": area_parts[2] if len(area_parts) >= 3 else "",
                "特徴タグ": _join_unique(tags),
                "name": _clean(li.select_one(".name strong").get_text(" ", strip=True)) if li.select_one(".name strong") else "",
            }

            for dl in li.select(".body_right dl"):
                dt = dl.find("dt")
                dd = dl.find("dd")
                label = _clean(dt.get_text(" ", strip=True)) if dt else ""
                value = _extract_table_value(dd)
                if label == "住所":
                    meta["address"] = value
                elif label == "営業時間":
                    meta["hours"] = value
                elif label == "定休日":
                    meta["holiday"] = value
                elif label == "予算":
                    meta["budget"] = value

            items.append((detail_url, meta))
        return items

    def _scrape_detail(self, url: str, meta: dict[str, str]) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            self.logger.warning("詳細ページ取得失敗: %s", url)
            return None

        info = _parse_info_pairs(soup)
        breadcrumbs = _extract_breadcrumbs(soup)

        item: dict[str, str] = {
            Schema.URL: url,
            "掲載媒体名": "カフェるん",
            "店舗ID": meta.get("店舗ID", ""),
            "特徴タグ": meta.get("特徴タグ", ""),
        }

        self._fill_name(item, soup, info, meta)
        self._fill_area(item, breadcrumbs, meta)
        self._fill_address(item, soup, info, meta)
        self._fill_genre(item, soup, info, meta)
        self._fill_basic_fields(item, soup, info, meta)
        self._fill_links(item, soup, info)

        if not item.get(Schema.NAME):
            self.logger.warning("店舗名取得失敗のためスキップ: %s", url)
            return None
        return item

    def _fill_name(self, item: dict[str, str], soup, info: dict[str, str], meta: dict[str, str]) -> None:
        candidates = [
            soup.select_one("h1.shop_name"),
            soup.select_one(".shop_basic_infomation h4"),
            soup.find("h1"),
        ]
        for node in candidates:
            name = _clean(node.get_text(" ", strip=True)) if node else ""
            if name:
                item[Schema.NAME] = name
                return
        if info.get("店名"):
            item[Schema.NAME] = info["店名"]
        elif meta.get("name"):
            item[Schema.NAME] = meta["name"]

    def _fill_area(self, item: dict[str, str], breadcrumbs: list[str], meta: dict[str, str]) -> None:
        if breadcrumbs:
            item["地方"] = breadcrumbs[0] if len(breadcrumbs) >= 1 else meta.get("地方", "")
            # Breadcrumb は [地方, 都道府県, 詳細エリア, 店名] の形が多い。
            if len(breadcrumbs) >= 3:
                item["エリア"] = breadcrumbs[2]
                item["詳細エリア"] = breadcrumbs[2]
            elif len(breadcrumbs) >= 2:
                item["エリア"] = breadcrumbs[1]
        item.setdefault("地方", meta.get("地方", ""))
        item.setdefault("エリア", meta.get("エリア", ""))
        item.setdefault("詳細エリア", meta.get("詳細エリア", ""))

    def _fill_address(self, item: dict[str, str], soup, info: dict[str, str], meta: dict[str, str]) -> None:
        address_node = soup.select_one(".shop_basic_infomation .shop_address") or soup.select_one(".shop_info .shop_address")
        raw_addr = (
            _clean(address_node.get_text(" ", strip=True)) if address_node else ""
        ) or info.get("住所", "") or meta.get("address", "")

        post, pref, addr = _split_address(raw_addr)
        if post:
            item[Schema.POST_CODE] = post
        if pref:
            item[Schema.PREF] = pref
        if addr:
            item[Schema.ADDR] = addr

    def _fill_genre(self, item: dict[str, str], soup, info: dict[str, str], meta: dict[str, str]) -> None:
        genre_node = soup.select_one(".shop_genre")
        genre = (
            _clean(genre_node.get_text(" ", strip=True)) if genre_node else ""
        ) or info.get("ジャンル", "") or meta.get("genre", "")
        if genre:
            item[Schema.CAT_SITE] = genre
            item.update(_industry_levels(genre))

    def _fill_basic_fields(self, item: dict[str, str], soup, info: dict[str, str], meta: dict[str, str]) -> None:
        hours = info.get("営業時間", "") or meta.get("hours", "")
        if hours:
            item[Schema.TIME] = hours
        holiday = info.get("定休日", "") or meta.get("holiday", "")
        if holiday:
            item[Schema.HOLIDAY] = holiday

        budget = info.get("平均予算", "") or info.get("予算", "") or meta.get("budget", "")
        if budget:
            item["平均予算"] = budget

        tel = self._find_tel(soup, info)
        if tel:
            item[Schema.TEL] = tel

        payment_parts: list[str] = []
        for label, extra_col in (("クレジット", "クレジット"), ("QRコード決済", "QRコード決済")):
            value = info.get(label, "")
            if value:
                item[extra_col] = value
                payment_parts.append(f"{label}: {value}")
        if payment_parts:
            item[Schema.PAYMENTS] = " / ".join(payment_parts)

        for label in ("席数", "予約", "貸切", "団体利用", "喫煙", "WiFi"):
            if info.get(label):
                item[label] = info[label]

    def _find_tel(self, soup, info: dict[str, str]) -> str:
        for a in soup.select('a[href^="tel:"]'):
            tel = _extract_tel(a.get("href", "").replace("tel:", ""))
            if tel:
                return tel

        selectors = [".tel_number", ".tel_area", ".shop_header_contact", ".sp_tel_btn"]
        for selector in selectors:
            for node in soup.select(selector):
                tel = _extract_tel(node.get_text(" ", strip=True))
                if tel:
                    return tel

        for key in ("TEL", "電話番号"):
            tel = _extract_tel(info.get(key, ""))
            if tel:
                return tel
        return ""

    def _fill_links(self, item: dict[str, str], soup, info: dict[str, str]) -> None:
        # 「出典」行の a[href] を先に控える。簡易掲載ページの名寄せ補助になる。
        for label in ("出典",):
            if info.get(label, "").startswith("http"):
                item["出典URL"] = info[label]

        for a in soup.select("a[href]"):
            href = a.get("href", "").strip()
            if not href:
                continue
            full = _full_url(href)
            if not full.startswith(("http://", "https://")):
                continue
            parsed = urlparse(full)
            host = parsed.netloc.lower()

            if "google." in host and "/maps" in parsed.path and not item.get("Google Maps URL"):
                item["Google Maps URL"] = full
                continue

            if host in _CAFERUN_HOSTS:
                if host in {"caferun.jp", "www.caferun.jp"} and re.search(r"/shop/\d+/?$", parsed.path):
                    item.setdefault("求人URL", full)
                continue
            if _SKIP_EXTERNAL_PATTERNS.search(full):
                continue

            lower = full.lower()
            if ("twitter.com/" in lower or "x.com/" in lower) and not item.get(Schema.X):
                item[Schema.X] = full
            elif "instagram.com/" in lower and not item.get(Schema.INSTA):
                item[Schema.INSTA] = full
            elif ("facebook.com/" in lower or "fb.com/" in lower) and not item.get(Schema.FB):
                item[Schema.FB] = full
            elif re.search(r"(?:line\.me|lin\.ee|page\.line\.me)/", lower) and not item.get(Schema.LINE):
                item[Schema.LINE] = full
            elif "tiktok.com/" in lower and not item.get(Schema.TIKTOK):
                item[Schema.TIKTOK] = full
            elif ("youtube.com/" in lower or "youtu.be/" in lower) and not item.get("YouTube"):
                item["YouTube"] = full
            elif not _is_social_url(full) and not item.get(Schema.HP):
                item[Schema.HP] = full


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    scraper = CaferunShopScraper()
    scraper.execute(START_URL)
    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
