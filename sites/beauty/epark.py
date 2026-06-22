"""
EPARKリラク＆エステ (mitsuraku.jp) — リラク・エステ・フィットネス店舗情報スクレイパー

取得対象:
    - 3ジャンル (リラク・マッサージ / エステ / フィットネス) × 全47都道府県
    - 店舗名, 住所, TEL, 営業時間, 定休日, ジャンル, 口コミ情報 等

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


class EparkScraper(StaticCrawler):
    """EPARKリラク＆エステ (mitsuraku.jp) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["最寄駅", "アクセス", "駐車場", "個室", "キャンセル料"]

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

        # --- JSON-LD 構造化データから優先取得 ---
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
                    item[Schema.NAME] = data["name"]
                if data.get("telephone") and not item.get(Schema.TEL):
                    item[Schema.TEL] = re.sub(r"[^\d\-\+\(\)]", "", data["telephone"])
                addr = data.get("address", {})
                if isinstance(addr, dict):
                    pref = addr.get("addressRegion", "")
                    locality = addr.get("addressLocality", "")
                    street = addr.get("streetAddress", "")
                    full_addr = pref + locality + street
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

        # --- 店舗名 ---
        if not item.get(Schema.NAME):
            h1 = soup.select_one(
                "h1.salon-detail__name, h1.shopdetail_title, h1.shop_name, "
                "[class*='salon-name'], [class*='shop-name'], h1"
            )
            if h1:
                item[Schema.NAME] = h1.get_text(strip=True)

        # --- itemprop 属性から TEL / 住所 ---
        if not item.get(Schema.TEL):
            tel_el = soup.select_one("[itemprop='telephone']")
            if tel_el:
                val = tel_el.get("content") or tel_el.get_text(strip=True)
                if val:
                    item[Schema.TEL] = re.sub(r"[^\d\-\+\(\)]", "", val)
        if not item.get(Schema.ADDR):
            addr_el = soup.select_one("[itemprop='streetAddress']")
            if addr_el:
                val = addr_el.get("content") or addr_el.get_text(strip=True)
                if val:
                    item[Schema.ADDR] = re.sub(r"\s+", " ", val).strip()

        # --- dl / table / div の構造化フィールドを横断的に取得 ---
        labels = self._collect_labels(soup)

        _MAP = {
            "住所":           Schema.ADDR,
            "TEL":            Schema.TEL,
            "Tel":            Schema.TEL,
            "電話番号":       Schema.TEL,
            "電話":           Schema.TEL,
            "予約専用電話番号": Schema.TEL,
            "予約電話番号":   Schema.TEL,
            "お問い合わせ":   Schema.TEL,
            "営業時間":       Schema.TIME,
            "受付時間":       Schema.TIME,
            "定休日":         Schema.HOLIDAY,
            "支払方法":       Schema.PAYMENTS,
            "支払い方法":     Schema.PAYMENTS,
            "最寄駅":         "最寄駅",
            "アクセス":       "アクセス",
            "駐車場":         "駐車場",
            "個室":           "個室",
            "キャンセル料":   "キャンセル料",
        }
        for label, col in _MAP.items():
            if label in labels and labels[label]:
                item.setdefault(col, labels[label])

        # TEL 正規化: ラベル値に注釈テキストが混入している場合、先頭の電話番号のみ抽出
        if item.get(Schema.TEL):
            raw = item[Schema.TEL]
            m = re.search(r"0\d{1,3}[\-\d]{6,12}\d", raw)
            if m:
                item[Schema.TEL] = m.group(0)
            else:
                item[Schema.TEL] = re.sub(r"[^\d\-\+\(\)]", "", raw)

        # --- 予約ボタンエリアの tel: リンクから電話番号を優先取得 ---
        if not item.get(Schema.TEL):
            _RESERVE_SEL = (
                "[class*='reserve'] a[href^='tel:'], [class*='yoyaku'] a[href^='tel:'], "
                "[class*='booking'] a[href^='tel:'], [class*='tel-reserve'] a[href^='tel:'], "
                "[class*='call'] a[href^='tel:'], [id*='reserve'] a[href^='tel:'], "
                "[id*='yoyaku'] a[href^='tel:']"
            )
            reserve_tel = soup.select_one(_RESERVE_SEL)
            if reserve_tel is None:
                # ボタン/リンクに「予約」「電話」テキストを持つ要素の近傍を探す
                for btn in soup.find_all(["a", "button", "span", "div"],
                                         string=re.compile(r"電話|予約電話|TEL|tel")):
                    parent = btn.parent
                    if parent is None:
                        continue
                    reserve_tel = parent.find("a", href=re.compile(r"^tel:"))
                    if reserve_tel:
                        break
            if reserve_tel:
                num = re.sub(r"[^\d\-\+\(\)]", "", reserve_tel["href"].replace("tel:", ""))
                if num:
                    item[Schema.TEL] = num

        # --- data-phone-number 属性から電話番号を取得 (js-shop-phone-number ボタン) ---
        if not item.get(Schema.TEL):
            phone_el = soup.select_one(".js-shop-phone-number[data-phone-number]")
            if phone_el:
                num = re.sub(r"[^\d\-\+\(\)]", "", phone_el.get("data-phone-number", ""))
                if num:
                    item[Schema.TEL] = num
        if not item.get(Schema.TEL):
            sms_btn = soup.select_one("a.sms-balloon")
            if sms_btn:
                m = re.search(r"0\d{1,3}[\-\d]{6,12}\d", sms_btn.get_text(strip=True))
                if m:
                    item[Schema.TEL] = m.group(0)

        # --- tel: リンクから電話番号をフォールバック取得 ---
        if not item.get(Schema.TEL):
            for a in soup.find_all("a", href=re.compile(r"^tel:")):
                num = re.sub(r"[^\d\-\+\(\)]", "", a["href"].replace("tel:", ""))
                if num:
                    item[Schema.TEL] = num
                    break

        # --- address 要素から住所フォールバック ---
        if not item.get(Schema.ADDR):
            for sel in ("address", "[class*='address']", "[class*='addr']"):
                el = soup.select_one(sel)
                if el:
                    val = re.sub(r"\s+", " ", el.get_text(" ", strip=True))
                    if val:
                        item[Schema.ADDR] = val
                        break

        # --- 住所から都道府県を分離 ---
        addr = item.get(Schema.ADDR, "")
        if addr and not item.get(Schema.PREF):
            m = _PREF_RE.match(addr)
            if m:
                item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = addr[m.end():].strip()
        elif addr and item.get(Schema.PREF):
            # JSON-LD で PREF が既に入っている場合は ADDR から都道府県部分だけ除去
            m = _PREF_RE.match(addr)
            if m:
                item[Schema.ADDR] = addr[m.end():].strip()

        # --- 口コミ ---
        score_el = soup.select_one("[class*='score'], [class*='rating']")
        if score_el:
            score_text = score_el.get_text(strip=True)
            if re.search(r"\d", score_text):
                item[Schema.SCORES] = score_text

        count_el = soup.select_one("[class*='review-count'], [class*='kuchikomi']")
        if count_el:
            count_text = count_el.get_text(strip=True)
            if re.search(r"\d", count_text):
                item[Schema.REV_SCR] = count_text

        if not item.get(Schema.NAME):
            return None
        return item

    @staticmethod
    def _collect_labels(soup) -> dict:
        """dl dt→dd, table th→td, div系ラベル/値ペアからラベル:値辞書を構築する"""
        labels: dict[str, str] = {}

        def _set(key: str, val: str) -> None:
            key = key.strip()
            val = re.sub(r"\s+", " ", val).strip()
            if key and val and key not in labels:
                labels[key] = val

        # dl/dt/dd パターン
        for dl in soup.find_all("dl"):
            for dt in dl.find_all("dt"):
                dd = dt.find_next_sibling("dd")
                if dd is None:
                    continue
                _set(dt.get_text(strip=True), dd.get_text(" ", strip=True))

        # table th→td パターン
        for tr in soup.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if th and td:
                _set(th.get_text(strip=True), td.get_text(" ", strip=True))

        # div ベースのラベル/値ペア (EPARKサイト等で多用)
        # 例: <div class="item"><span class="label">TEL</span><span class="value">03-xxxx</span></div>
        _LABEL_WORDS = re.compile(r"label|heading|title|term|key", re.I)
        _VALUE_WORDS = re.compile(r"value|content|body|data|desc", re.I)
        for label_el in soup.find_all(True, class_=_LABEL_WORDS):
            tag = label_el.name
            if tag in ("dt", "th", "script", "style"):
                continue
            parent = label_el.parent
            if parent is None:
                continue
            val_el = parent.find(True, class_=_VALUE_WORDS)
            if val_el is None or val_el is label_el:
                continue
            _set(label_el.get_text(strip=True), val_el.get_text(" ", strip=True))

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
