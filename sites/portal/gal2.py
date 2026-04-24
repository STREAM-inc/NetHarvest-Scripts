# scripts/sites/portal/gal_colle_net.py
"""
ギャルコレネット — 愛媛県ナイト系店舗情報スクレイパー

改善点:
- sitemap.xml だけでなく検索一覧ページも巡回
- 一覧ページにある TEL / 住所 / 平均予算 / OPEN / 定休日 / 設備 を取得
- 詳細ページは補完用途として利用
- 相対URL, tel:, 改行混在, 全角記号混在に強くする
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://gal-colle.net"
SITEMAP_URL = urljoin(BASE_URL, "/sitemap.xml")
DEFAULT_PREF = "愛媛県"

# 愛媛県の検索導線
SEARCH_URLS = [
    # ガールズ系
    urljoin(BASE_URL, "/gc_search.php?sel_shopcate[]=1"),  # クラブ＆キャバクラ
    urljoin(BASE_URL, "/gc_search.php?sel_shopcate[]=2"),  # ラウンジ＆スナック
    urljoin(BASE_URL, "/gc_search.php?sel_shopcate[]=3"),  # ガールズバー
    urljoin(BASE_URL, "/gc_search.php?sel_shopcate[]=4"),  # セクキャバ
    # ボーイズ系
    urljoin(BASE_URL, "/gc_search.php?sel_shopcate[]=5"),  # ホスト
]

_PREF_RE = re.compile(r"^(愛媛県|北海道|東京都|京都府|大阪府|.{2,3}県)")
_SHOPNO_RE = re.compile(r"SHOPNO=(\d+)")
_TEL_RE = re.compile(r"(0\d{1,4}-\d{1,4}-\d{3,4}|0\d{9,10})")
_OPEN_HOLIDAY_RE = re.compile(
    r"OPEN[:：]?\s*(.*?)\s*/\s*定休日[:：]?\s*(.*)$",
    re.IGNORECASE,
)
_PRICE_RE = re.compile(r"(¥\s*[\d,]+|要問合せ|要確認|[0-9,]+円)")
_STATUS_TEXTS = {"要確認", "待ち時間なし", "30分以内"}

FEATURE_LABELS = {
    "カード": Schema.PAY,     # 値は「カード」
    "カラオケ": "カラオケ",
    "ダーツ": "ダーツ",
    "VIP": "VIPルーム",
    "WiFi": "WiFi",
    "飲み放題": "飲み放題",
}

CATEGORY_MAP = {
    "クラブ＆キャバクラ": "クラブ＆キャバクラ",
    "ラウンジ＆スナック": "ラウンジ＆スナック",
    "ガールズバー": "ガールズバー",
    "セクキャバ": "セクキャバ",
    "ホスト": "ホスト",
}


def _clean(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n+", "\n", text)
    return text.strip()


def _clean_inline(text: str) -> str:
    return re.sub(r"\s+", " ", _clean(text)).strip()


def _abs_url(href: str) -> str:
    if not href:
        return ""
    return urljoin(BASE_URL, href.strip())


def _is_valid_http_url(href: str) -> bool:
    href = (href or "").strip()
    return href.startswith("http://") or href.startswith("https://")


def _normalize_tel(text: str) -> str:
    if not text:
        return ""
    text = _clean_inline(text)
    m = _TEL_RE.search(text.replace("−", "-").replace("ー", "-"))
    if not m:
        return ""
    tel = m.group(1)
    if "-" not in tel and len(tel) in (10, 11):
        # 末尾をそのままにして最低限の正規化だけ
        return tel
    return tel


def _split_pref_addr(addr: str) -> tuple[str, str]:
    addr = _clean_inline(addr)
    if not addr:
        return DEFAULT_PREF, ""
    m = _PREF_RE.match(addr)
    if m:
        return m.group(1), addr[m.end():].strip()
    return DEFAULT_PREF, addr


def _extract_shopno(url: str) -> str:
    m = _SHOPNO_RE.search(url or "")
    return m.group(1) if m else ""


def _first_text_lines(el) -> list[str]:
    if not el:
        return []
    text = el.get_text("\n", strip=True)
    lines = [_clean_inline(x) for x in text.split("\n")]
    return [x for x in lines if x and x != "-"]


class GalColleNetScraper(StaticCrawler):
    DELAY = 1.5
    EXTRA_COLUMNS = [
        "店舗名カナ",
        "平均予算",
        "VIPルーム",
        "カラオケ",
        "ダーツ",
        "WiFi",
        "飲み放題",
        "システム",
        "待ち時間",
        "説明",
        "エリア",
        "画像URL",
    ]

    def parse(self, url: str):
        shops = {}

        # 1) 一覧ページから広く収集（情報量が多い）
        for search_url in SEARCH_URLS:
            try:
                self.logger.info("一覧収集中: %s", search_url)
                for item in self._collect_from_search(search_url):
                    shopno = _extract_shopno(item.get(Schema.URL, ""))
                    if not shopno:
                        continue
                    shops.setdefault(shopno, {}).update(
                        {k: v for k, v in item.items() if v not in ("", None)}
                    )
            except Exception as e:
                self.logger.warning("一覧取得失敗: %s (%s)", search_url, e)

        # 2) sitemap から詳細URLを追加
        for detail_url in self._collect_shop_urls_from_sitemap():
            shopno = _extract_shopno(detail_url)
            if not shopno:
                continue
            shops.setdefault(shopno, {})
            shops[shopno].setdefault(Schema.URL, detail_url)

        self.total_items = len(shops)
        self.logger.info("対象店舗数: %d 件", len(shops))

        # 3) 詳細ページで補完
        for shopno, base_item in shops.items():
            url = base_item.get(Schema.URL)
            if not url:
                continue
            try:
                detail_item = self._scrape_detail(url)
                merged = self._merge_items(base_item, detail_item or {})
                if merged.get(Schema.NAME):
                    yield merged
            except Exception as e:
                self.logger.warning("詳細補完失敗: %s (%s)", url, e)
                if base_item.get(Schema.NAME):
                    yield base_item

    def _merge_items(self, base: dict, extra: dict) -> dict:
        merged = dict(base)
        for k, v in (extra or {}).items():
            if v not in ("", None):
                merged[k] = v
        return merged

    def _collect_shop_urls_from_sitemap(self) -> list[str]:
        soup = self.get_soup(SITEMAP_URL)
        urls = []
        for loc in soup.find_all("loc"):
            href = _clean_inline(loc.get_text())
            if "gc_shop.php" in href and "SHOPNO=" in href:
                urls.append(_abs_url(href))
        return list(dict.fromkeys(urls))

    def _collect_from_search(self, search_url: str):
        """
        検索結果一覧をページングしつつ全店舗取得
        """
        page = 1
        seen_urls = set()

        while True:
            url = search_url if page == 1 else f"{search_url}&page={page}"
            soup = self.get_soup(url)

            cards = self._find_search_cards(soup)
            if not cards:
                break

            new_count = 0
            for card in cards:
                item = self._parse_search_card(card)
                shop_url = item.get(Schema.URL)
                if not shop_url or shop_url in seen_urls:
                    continue
                seen_urls.add(shop_url)
                new_count += 1
                yield item

            # 次ページがなければ終了
            if not self._has_next_page(soup, page):
                break
            if new_count == 0:
                break
            page += 1

    def _find_search_cards(self, soup):
        cards = []
        for box in soup.select("div.col-xs-12[style*='border:solid 1px pink']"):
            if box.select_one("a[href*='gc_shop.php?SHOPNO=']"):
                cards.append(box)
        return cards

    def _has_next_page(self, soup, current_page: int) -> bool:
        next_page = current_page + 1
        for a in soup.select("ul.pagination a[href]"):
            href = a.get("href", "")
            text = _clean_inline(a.get_text())
            if f"page={next_page}" in href or text == str(next_page):
                return True
        return False

    def _parse_search_card(self, card) -> dict:
        data = {}

        # 詳細URL / 店名 / カナ
        detail_a = card.select_one("a[href*='gc_shop.php?SHOPNO=']")
        if detail_a:
            data[Schema.URL] = _abs_url(detail_a.get("href", ""))

        h3 = detail_a.select_one("h3") if detail_a else None
        if h3:
            name_lines = _first_text_lines(h3)
            if name_lines:
                data[Schema.NAME] = name_lines[0]
            span = h3.select_one("span")
            if span:
                data["店舗名カナ"] = _clean_inline(span.get_text())

        # カテゴリ
        head_text = _clean_inline(card.select_one("div[style*='background-color']").get_text(" ", strip=True)) \
            if card.select_one("div[style*='background-color']") else ""
        for k, v in CATEGORY_MAP.items():
            if k in head_text:
                data[Schema.CAT_SITE] = v
                break

        # 待ち時間/状態
        for badge in card.select("div.icon-boxes"):
            txt = _clean_inline(badge.get_text())
            if txt in _STATUS_TEXTS:
                data["待ち時間"] = txt
                break

        # TEL
        tel_a = card.select_one("a[href^='tel:']")
        if tel_a:
            tel_href = tel_a.get("href", "")
            tel = tel_href.replace("tel:", "").strip()
            if tel:
                data[Schema.TEL] = tel

        # 説明・OPEN/定休日・住所・平均予算
        info_wrap = None
        for h3_tag in card.select("h3"):
            if "[Infomation]" in h3_tag.get_text():
                info_wrap = h3_tag.find_parent("div")
                break

        if info_wrap:
            ps = info_wrap.select("p")
            if len(ps) >= 1:
                desc = _clean(ps[0].get_text("\n", strip=True))
                if desc and desc != "-":
                    data["説明"] = desc[:500]

            if len(ps) >= 2:
                open_holiday = _clean(ps[1].get_text("\n", strip=True))
                m = _OPEN_HOLIDAY_RE.search(open_holiday.replace("<br />", " "))
                if m:
                    open_text = _clean_inline(m.group(1))
                    holiday = _clean_inline(m.group(2))
                    if open_text:
                        data[Schema.TIME] = open_text
                    if holiday and holiday != "-":
                        data[Schema.HOLIDAY] = holiday
                else:
                    # OPENだけでも拾う
                    line = _clean_inline(open_holiday)
                    if line:
                        data[Schema.TIME] = line

            if len(ps) >= 3:
                addr_text = _clean_inline(ps[2].get_text(" ", strip=True))
                addr_text = re.sub(r"^住所[：:]\s*", "", addr_text)
                pref, addr = _split_pref_addr(addr_text)
                data[Schema.PREF] = pref
                data[Schema.ADDR] = addr

            if len(ps) >= 4:
                price_text = _clean_inline(ps[3].get_text(" ", strip=True))
                price_text = re.sub(r"^平均予算[：:]\s*", "", price_text)
                m = _PRICE_RE.search(price_text)
                if m:
                    data["平均予算"] = m.group(1).replace(" ", "")

        # 設備
        feature_divs = card.select("div.kadomaru")
        for div in feature_divs:
            txt = _clean_inline(div.get_text())
            if txt not in FEATURE_LABELS:
                continue
            key = FEATURE_LABELS[txt]
            is_on = "fc5f85" in (div.get("style", "") or "").lower() or "color:#FC7AC1".lower() in (div.get("style", "") or "").lower()
            if key == Schema.PAY and is_on:
                data[Schema.PAY] = "カード"
            elif key != Schema.PAY:
                data[key] = "あり" if is_on else "なし"

        # 画像
        img = card.select_one("img[src*='img-shop/']")
        if img:
            data["画像URL"] = _abs_url(img.get("src", ""))

        return data

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        data = {Schema.URL: url}

        # タイトル部分
        h2 = soup.select_one("h2.title")
        if h2:
            h2_clone = h2.__copy__() if hasattr(h2, "__copy__") else h2
            span = h2.select_one("span")
            if span:
                data[Schema.CAT_SITE] = _clean_inline(span.get_text())
            small = h2.select_one("small")
            if small:
                data["店舗名カナ"] = _clean_inline(small.get_text())

            title_text = _clean_inline(h2.get_text(" ", strip=True))
            # カテゴリ/カナを除去して店名候補化
            for txt in filter(None, [
                data.get(Schema.CAT_SITE),
                data.get("店舗名カナ"),
            ]):
                title_text = title_text.replace(txt, "").strip()
            if title_text and not data.get(Schema.NAME):
                data[Schema.NAME] = title_text

        # テーブル形式の詳細
        for cell in soup.select("div.col-xs-12.cell"):
            label_el = cell.select_one("div.htd")
            value_el = cell.select_one("div.dtd")
            if not label_el or not value_el:
                continue

            for tag in value_el.select("iframe, script, style"):
                tag.decompose()

            label = _clean_inline(label_el.get_text())
            raw_text = _clean(value_el.get_text("\n", strip=True))
            raw_inline = _clean_inline(raw_text)
            links = [a.get("href", "").strip() for a in value_el.select("a[href]")]
            links = [_abs_url(x) if not x.startswith("mailto:") and not x.startswith("tel:") else x for x in links]

            if label == "店舗名":
                if raw_inline and raw_inline != "-":
                    data[Schema.NAME] = raw_inline

            elif label == "住所":
                pref, addr = _split_pref_addr(raw_inline)
                data[Schema.PREF] = pref
                data[Schema.ADDR] = addr

            elif label in ("TEL", "電話番号"):
                tel = _normalize_tel(raw_text)
                if not tel:
                    for href in links:
                        if href.startswith("tel:"):
                            tel = href.replace("tel:", "").strip()
                            break
                if tel:
                    data[Schema.TEL] = tel

            elif label == "営業時間":
                if raw_inline and raw_inline != "-":
                    data[Schema.TIME] = raw_inline

            elif label == "URL":
                http_links = [x for x in links if _is_valid_http_url(x)]
                if http_links:
                    data[Schema.HP] = http_links[0]

            elif label in ("BLOG", "ブログ"):
                http_links = [x for x in links if _is_valid_http_url(x)]
                for href in http_links:
                    low = href.lower()
                    if "facebook" in low:
                        data[Schema.FB] = href
                    elif "instagram" in low:
                        data[Schema.INSTA] = href
                    elif not data.get(Schema.HP):
                        data[Schema.HP] = href

            elif label == "Facebook":
                http_links = [x for x in links if _is_valid_http_url(x)]
                if http_links:
                    data[Schema.FB] = http_links[0]

            elif label == "Instagram":
                http_links = [x for x in links if _is_valid_http_url(x)]
                if http_links:
                    data[Schema.INSTA] = http_links[0]

            elif label == "定休日":
                if raw_inline and raw_inline != "-":
                    data[Schema.HOLIDAY] = raw_inline

            elif label == "クレジットカード":
                if raw_inline and raw_inline != "-":
                    data[Schema.PAY] = raw_inline

            elif label == "平均予算":
                if raw_inline and raw_inline != "-":
                    data["平均予算"] = raw_inline

            elif label == "VIPルーム":
                data["VIPルーム"] = raw_inline or "-"

            elif label == "カラオケ":
                data["カラオケ"] = raw_inline or "-"

            elif label == "ダーツ":
                data["ダーツ"] = raw_inline or "-"

            elif label == "システム":
                if raw_text and raw_inline != "-":
                    data["システム"] = raw_text[:1000]

        # パンくずやメタから補完
        if not data.get(Schema.NAME):
            og_title = soup.title.get_text(strip=True) if soup.title else ""
            og_title = og_title.replace("|ギャルコレネット", "").strip()
            if og_title:
                data[Schema.NAME] = og_title.split("|")[0].strip()

        # 店舗画像
        main_img = soup.select_one("img[src*='img-shop/']")
        if main_img:
            data["画像URL"] = _abs_url(main_img.get("src", ""))

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = GalColleNetScraper()
    scraper.execute(BASE_URL + "/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")