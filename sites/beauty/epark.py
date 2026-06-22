"""
EPARKリラク＆エステ (mitsuraku.jp) — リラク・エステ・フィットネス店舗情報スクレイパー

取得対象:
    - 3ジャンル (リラク・マッサージ / エステ / フィットネス) × 全47都道府県
    - 店舗名, 名称カナ, 住所, TEL, 電話番号, 営業時間, 定休日, 最寄駅, アクセス,
      駐車場, 店舗設備, 備考, 施術内容, サロンの特徴, 料金, 口コミ情報 等

取得フロー:
    1. 各ジャンル × 各都道府県の一覧ページを全ページ巡回して詳細URLを収集
    2. 各店舗の詳細ページから情報を取得して yield

実行方法:
    python scripts/sites/beauty/epark.py
    python bin/run_flow.py --site-id epark
"""

import json
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


_PREF_RE = re.compile(
    r"^(北海道|東京都|大阪府|京都府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 店舗名末尾の読み仮名 「（アビコ…）」を分離する
_KANA_RE = re.compile(r"[（(]([ァ-ヶ゛゜ー\s・]+)[）)]\s*$")
# 電話番号抽出 (注釈テキストが混入していても先頭の番号のみ取り出す)
_TEL_RE = re.compile(r"0\d{1,3}[\-\d]{6,12}\d")

# ジャンル: (カテゴリ名, URLプレフィックス)
_GENRES = [
    ("リラク・マッサージ", ""),
    ("エステ", "esthe/"),
    ("フィットネス", "fitness/"),
]

_PREFS = [
    "hokkaido", "aomori", "iwate", "miyagi", "akita", "yamagata", "fukushima",
    "ibaraki", "tochigi", "gumma", "saitama", "chiba", "tokyo", "kanagawa",
    "niigata", "toyama", "ishikawa", "fukui", "yamanashi", "nagano",
    "gifu", "shizuoka", "aichi", "mie", "shiga", "kyoto", "osaka", "hyogo",
    "nara", "wakayama", "tottori", "shimane", "okayama", "hiroshima",
    "yamaguchi", "tokushima", "kagawa", "ehime", "kochi", "fukuoka", "saga",
    "nagasaki", "kumamoto", "oita", "miyazaki", "kagoshima", "okinawa",
]

# 詳細ページの店舗情報テーブル (ul.panel-list の dt/dd 相当) ラベル → 出力カラム
_PANEL_MAP = {
    "サロン名":         Schema.NAME,
    "店舗名":           Schema.NAME,
    "最寄り駅":         "最寄駅",
    "最寄駅":           "最寄駅",
    "住所":             Schema.ADDR,
    "アクセス":         "アクセス",
    "予約専用電話番号": Schema.TEL,
    "予約電話番号":     Schema.TEL,
    "電話番号":         Schema.PHONE,
    "TEL":              Schema.TEL,
    "Tel":              Schema.TEL,
    "営業時間":         Schema.TIME,
    "受付時間":         Schema.TIME,
    "定休日":           Schema.HOLIDAY,
    "駐車場":           "駐車場",
    "店舗設備":         "店舗設備",
    "備考":             "備考",
    "施術内容":         "施術内容",
    "サロンの特徴":     "サロンの特徴",
    "支払方法":         Schema.PAYMENTS,
    "支払い方法":       Schema.PAYMENTS,
}

# 電話番号系カラム (注釈混入を除去して番号のみ整形する対象)
_TEL_COLUMNS = {Schema.TEL, Schema.PHONE}


class EparkScraper(StaticCrawler):
    """EPARKリラク＆エステ (mitsuraku.jp) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "最寄駅", "アクセス", "駐車場", "店舗設備", "備考",
        "施術内容", "サロンの特徴", "料金",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        base = url.rstrip("/")
        seen: set[str] = set()

        for cat_name, gprefix in _GENRES:
            for pref in _PREFS:
                list_url = f"{base}/{gprefix}{pref}/"
                for detail_url in self._iter_list(list_url, seen):
                    try:
                        item = self._scrape_detail(detail_url, cat_name)
                        if item:
                            yield item
                    except Exception as e:
                        self.logger.warning("詳細取得エラー: %s — %s", detail_url, e)

    def _iter_list(self, list_url: str, seen: set) -> Generator[str, None, None]:
        """一覧ページをページネーションして新規詳細URLを yield する"""
        page = 1
        while True:
            paged_url = list_url if page == 1 else f"{list_url}?page={page}"
            soup = self.get_soup(paged_url)
            if soup is None:
                break

            links = soup.select("h2.search_shopname a.js-salon-link[href]")
            if not links:
                break

            found_new = False
            for a in links:
                href = a.get("href", "").strip()
                if not href:
                    continue
                full = href if href.startswith("http") else urljoin(list_url, href)
                # 詳細ページのみ (一覧ページURLは除く)
                if full not in seen:
                    seen.add(full)
                    found_new = True
                    yield full

            if not found_new:
                break
            page += 1

    def _scrape_detail(self, url: str, cat_name: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        item: dict = {
            Schema.URL: url,
            Schema.CAT_SITE: cat_name,
        }

        # --- 店舗情報テーブル (ul.panel-list: ラベルli / 値li) を横断取得 ---
        for label, value in self._collect_panel(soup).items():
            col = _PANEL_MAP.get(label)
            if not col:
                continue
            if col == Schema.NAME:
                # 「店舗名 （ヨミガナ）」を名称と名称カナに分離する
                name, kana = self._split_name_kana(value)
                item.setdefault(Schema.NAME, name)
                if kana:
                    item.setdefault(Schema.NAME_KANA, kana)
            else:
                item.setdefault(col, value)

        # --- microdata (itemprop=address) から住所を整形取得 ---
        addr_root = soup.find(attrs={"itemprop": "address"})
        if addr_root:
            region = self._itemprop_text(addr_root, "addressRegion")
            locality = self._itemprop_text(addr_root, "addressLocality")
            street = self._itemprop_text(addr_root, "streetAddress")
            full_addr = (locality + street).strip()
            if region and not item.get(Schema.PREF):
                item[Schema.PREF] = region
            if full_addr:
                item[Schema.ADDR] = full_addr

        # --- JSON-LD 構造化データでフォールバック補完 ---
        _LD_TYPES = {"LocalBusiness", "HealthAndBeautyBusiness", "BeautySalon",
                     "SportsActivityLocation", "ExerciseGym"}
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                if isinstance(data, list):
                    data = next(
                        (d for d in data if isinstance(d, dict) and d.get("@type") in _LD_TYPES),
                        data[0] if data else None,
                    )
                if not isinstance(data, dict):
                    continue
                if data.get("name") and not item.get(Schema.NAME):
                    name, kana = self._split_name_kana(data["name"])
                    item[Schema.NAME] = name
                    if kana and not item.get(Schema.NAME_KANA):
                        item[Schema.NAME_KANA] = kana
                if data.get("telephone") and not item.get(Schema.TEL):
                    item[Schema.TEL] = data["telephone"]
                addr = data.get("address", {})
                if isinstance(addr, dict):
                    pref = addr.get("addressRegion", "")
                    locality = addr.get("addressLocality", "")
                    street = addr.get("streetAddress", "")
                    full_addr = (locality + street).strip()
                    if full_addr and not item.get(Schema.ADDR):
                        item[Schema.ADDR] = full_addr
                    if pref and not item.get(Schema.PREF):
                        item[Schema.PREF] = pref
                oh = data.get("openingHours")
                if oh and not item.get(Schema.TIME):
                    item[Schema.TIME] = ", ".join(oh) if isinstance(oh, list) else str(oh)
                url_val = data.get("url") or data.get("sameAs")
                if url_val and not item.get(Schema.HP):
                    item[Schema.HP] = url_val if isinstance(url_val, str) else url_val[0] if url_val else ""
            except Exception:
                pass

        # --- 店舗名フォールバック ---
        if not item.get(Schema.NAME):
            h1 = soup.select_one(
                "h1.salon-detail__name, h1.shopdetail_title, h1.shop_name, "
                "[class*='salon-name'], [class*='shop-name'], h1"
            )
            if h1:
                name, kana = self._split_name_kana(h1.get_text(strip=True))
                item[Schema.NAME] = name
                if kana and not item.get(Schema.NAME_KANA):
                    item[Schema.NAME_KANA] = kana

        # --- 電話番号フォールバック: data-phone-number 属性 ---
        if not item.get(Schema.TEL):
            phone_el = soup.select_one(".js-shop-phone-number[data-phone-number]")
            if phone_el:
                item[Schema.TEL] = phone_el.get("data-phone-number", "")
        # --- 電話番号フォールバック: tel: リンク ---
        if not item.get(Schema.TEL):
            for a in soup.find_all("a", href=re.compile(r"^tel:")):
                num = a["href"].replace("tel:", "").strip()
                if num:
                    item[Schema.TEL] = num
                    break

        # --- 電話番号系カラムを整形 (注釈混入を除去) ---
        for col in _TEL_COLUMNS:
            if item.get(col):
                m = _TEL_RE.search(item[col])
                item[col] = m.group(0) if m else re.sub(r"[^\d\-\+\(\)]", "", item[col])
                if not item[col]:
                    item.pop(col, None)

        # --- 住所フォールバック & 都道府県分離 ---
        if not item.get(Schema.ADDR):
            for sel in ("address", "[class*='address']", "[class*='addr']"):
                el = soup.select_one(sel)
                if el:
                    val = re.sub(r"\s+", " ", el.get_text(" ", strip=True))
                    if val:
                        item[Schema.ADDR] = val
                        break
        addr = item.get(Schema.ADDR, "")
        if addr:
            m = _PREF_RE.match(addr)
            if m:
                if not item.get(Schema.PREF):
                    item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = addr[m.end():].strip()

        # --- 料金 (人気メニュー名 + 価格) ---
        menus = []
        for box in soup.select("div.menu_box01"):
            name_el = box.select_one(".js-menu-name, .menu-name, h4")
            if not name_el:
                continue
            menu_txt = re.sub(r"\s+", " ", name_el.get_text(" ", strip=True))
            if menu_txt and menu_txt not in menus:
                menus.append(menu_txt)
        if menus:
            item["料金"] = " / ".join(menus)

        # --- 口コミ採点・件数 (microdata) ---
        rv = soup.select_one("[itemprop='ratingValue']")
        if rv:
            val = (rv.get("content") or rv.get_text(strip=True)).strip()
            if re.search(r"\d", val):
                item[Schema.SCORES] = val
        rc = soup.select_one("[itemprop='ratingCount'], [itemprop='reviewCount']")
        if rc:
            val = (rc.get("content") or rc.get_text(strip=True)).strip()
            if re.search(r"\d", val):
                item[Schema.REV_SCR] = val

        if not item.get(Schema.NAME):
            return None
        return item

    @staticmethod
    def _split_name_kana(text: str) -> tuple[str, str]:
        """「店舗名 （ヨミガナ）」を (名称, カナ) に分離する。カナが無ければ ("", "")。"""
        text = re.sub(r"\s+", " ", text or "").strip()
        m = _KANA_RE.search(text)
        if m:
            kana = re.sub(r"\s+", " ", m.group(1)).strip()
            name = text[:m.start()].strip()
            return name, kana
        return text, ""

    @staticmethod
    def _itemprop_text(root, prop: str) -> str:
        el = root.find(attrs={"itemprop": prop})
        if not el:
            return ""
        val = el.get("content") or el.get_text(" ", strip=True)
        return re.sub(r"\s+", " ", val or "").strip()

    @staticmethod
    def _collect_panel(soup) -> dict:
        """店舗情報テーブル ul.panel-list (ラベルli=col-lg-2 / 値li=col-lg-10) を辞書化する。

        mitsuraku の <li> は明示的に閉じられず html.parser では値liがラベルliに
        ネストされるため、クラス指定で各liを取得し、ラベルは直下テキストのみを使う。
        """
        labels: dict[str, str] = {}

        def _has(token: str):
            return lambda c: c and token in c

        for ul in soup.select("ul.panel-list"):
            label_li = ul.find("li", class_=_has("col-lg-2"))
            value_li = ul.find("li", class_=_has("col-lg-10"))
            if not label_li or not value_li:
                continue
            # ラベルliの直下テキストノードのみ (ネストした値liを除外)
            label = (label_li.find(string=True, recursive=False) or "").strip()
            if not label:
                continue
            value = re.sub(r"\s+", " ", value_li.get_text(" ", strip=True)).strip()
            if value and label not in labels:
                labels[label] = value

        return labels


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = EparkScraper()
    scraper.execute("https://mitsuraku.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
