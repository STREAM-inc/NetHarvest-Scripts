"""
対象サイト: https://www.urunavi.jp/shop/
売るナビ (urunavi_2) — 買取専門店「売るナビ」 店舗情報スクレイパー (12 都県・閉店除外版)

取得対象:
    - 買取専門店 売るナビ (www.urunavi.jp) の買取店舗一覧 (/shop/) に掲載された店舗のうち
      東京都・神奈川県・埼玉県・千葉県・茨城県・栃木県・群馬県・山梨県・長野県・
      岐阜県・静岡県・愛知県 の店舗 (依頼仕様)
    - 一覧/詳細で「閉店:日付」表記のある店舗は除外 (依頼仕様)
    - 店舗名 / 都道府県 / 郵便番号 / 住所 / 電話番号 / 営業時間 / 店舗詳細URL
      に加え、詳細ページの メールアドレス / 施設名 / 施設内フロア / 駐車場 /
      エリア / 市区町村

取得フロー:
    1. ルート URL (買取店舗一覧) を 1 回取得する。ページネーションは無く、
       全店舗 (2026-11 時点 239 店) が地域別セクション (dl.shopList の dt=地域 /
       dd=店舗群) で 1 ページに列挙されている。
    2. 一覧の各店舗 (div.shopWrap) から 店舗名 / 住所 / 電話番号 / 営業時間 /
       詳細URL を取得し、対象都県・閉店表記でフィルタする。
    3. 対象店舗の詳細ページを 1 件取得するたびに即 yield する (早期 yield)。
       詳細取得に失敗した場合でも一覧由来の情報だけで yield する。

注意:
    - ルート URL は引数 `url` を唯一の起点 (SSOT) とし、配下 URL はすべて
      urljoin(url, ...) で派生させる。別 URL はハードコードしない。
    - 詳細ページの div.comment は店舗紹介の長文 (自由記述プロース) のため取得しない。
    - 利用規約ページ (/policy/) は個人情報保護方針のみで、スクレイピング禁止条項は無い。

実行方法:
    python scripts/sites/service/urunavi_2.py
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


# 住所先頭から都道府県を切り出すためのパターン (長い名称を先に並べて誤マッチを防ぐ)
_PREF_PATTERN = re.compile(
    r"(北海道|東京都|大阪府|京都府|"
    r"神奈川県|和歌山県|鹿児島県|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|沖縄県)"
)

# 〒123-4567 形式の郵便番号
_POST_PATTERN = re.compile(r"〒\s*(\d{3}-?\d{4})")

# 依頼仕様: 下記 12 都県の店舗のみを抽出する
_TARGET_PREFS = {
    "東京都", "神奈川県", "埼玉県", "千葉県", "茨城県", "栃木県", "群馬県",
    "山梨県", "長野県", "岐阜県", "静岡県", "愛知県",
}

# 閉店表記 (例: "※閉店:2026年8月27日(木)")。"閉店時間" 等の誤検知を避けるため区切り記号付きで判定
_CLOSED_PATTERN = re.compile(r"閉店\s*[:：]|閉店しました|閉店いたしました|【閉店】")

# Schema に無いサイト固有の構造化項目 (いずれも短いラベル/値。自由記述は含めない)
_COL_FLOOR = "施設内フロア"
_COL_PARKING = "駐車場"
_COL_AREA = "エリア"
_COL_CITY = "市区町村"


class Urunavi2Scraper(StaticCrawler):
    """買取専門店 売るナビ 店舗情報スクレイパー"""

    DELAY = 3.0
    EXTRA_COLUMNS = [_COL_FLOOR, _COL_PARKING, _COL_AREA, _COL_CITY]

    def parse(self, url: str):
        # 1. 一覧ページ (ページネーション無しの単一ページ) を取得
        soup = self.get_soup(url)
        if soup is None:
            return

        shop_list = soup.select_one("dl.shopList")
        if shop_list is None:
            self.logger.warning("店舗一覧 (dl.shopList) が見つかりません: %s", url)
            return

        # 2. 地域セクション (dt=地域名 / dd=店舗群) を走査して一覧情報を収集
        listings = []
        area_name = ""
        for child in shop_list.find_all(["dt", "dd"], recursive=False):
            if child.name == "dt":
                # 例: "北陸・甲信越" (末尾の空 span は get_text で無視される)
                area_name = child.get_text(strip=True)
                continue
            for wrap in child.select("div.shopWrap"):
                listing = self._parse_listing(wrap, url, area_name)
                # 対象都県のみ・閉店表記の店舗は除外 (依頼仕様のフィルタ)
                if listing is None or listing["pref"] not in _TARGET_PREFS:
                    continue
                if _CLOSED_PATTERN.search(listing["name"] + " " + listing["time"]):
                    continue
                listings.append(listing)

        self.total_items = len(listings)

        # 3. 詳細ページを 1 件取得するたびに即 yield (早期 yield)
        for listing in listings:
            item = self._build_item(listing)
            # 詳細側にのみ閉店表記がある場合も除外
            if item is not None:
                yield item

    def _parse_listing(self, wrap, url: str, area_name: str) -> dict | None:
        """一覧の 1 店舗ブロック (div.shopWrap) から基本情報を取り出す。"""
        link = wrap.select_one(".shopInfo h4 a")
        if link is None:
            return None

        name = link.get_text(strip=True)
        href = link.get("href") or ""
        if not name or not href:
            return None

        # 住所 / 電話番号 / 営業時間 は dl.shopData の dt ラベルと dd 値の対
        values: dict[str, str] = {}
        data_dl = wrap.select_one("dl.shopData")
        if data_dl is not None:
            for dt in data_dl.find_all("dt"):
                dd = dt.find_next_sibling("dd")
                if dd is None:
                    continue
                values[dt.get_text(strip=True)] = dd.get_text(" ", strip=True)

        address = values.get("住所", "")
        pref_match = _PREF_PATTERN.search(address)

        return {
            "name": name,
            "detail_url": urljoin(url, href),
            "address": address,
            "pref": pref_match.group(1) if pref_match else "",
            "tel": values.get("電話番号", ""),
            "time": values.get("営業時間", ""),
            "area": area_name,
        }

    def _build_item(self, listing: dict) -> dict | None:
        """一覧情報に詳細ページの情報をマージして 1 アイテムを組み立てる (閉店なら None)。"""
        address = listing["address"]
        detail = self._scrape_detail(listing["detail_url"])
        if _CLOSED_PATTERN.search(detail.get("営業時間", "")):
            return None

        # 詳細ページ側の住所の方が正 (一覧と同一だが、欠落時のみ一覧にフォールバック)
        address = detail.get("住所") or address
        pref_match = _PREF_PATTERN.search(address)
        pref = pref_match.group(1) if pref_match else listing["pref"]

        post_match = _POST_PATTERN.search(address)
        post_code = post_match.group(1) if post_match else ""

        # 住所は郵便番号を除き、都道府県以降 (市区町村以降) を残す
        addr = _POST_PATTERN.sub("", address).strip()
        if pref_match:
            addr = addr[addr.find(pref) + len(pref):].strip()

        return {
            Schema.URL: listing["detail_url"],
            Schema.NAME: listing["name"],
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: detail.get("電話番号") or listing["tel"],
            Schema.TIME: detail.get("営業時間") or listing["time"],
            Schema.EMAIL: detail.get("メールアドレス", ""),
            Schema.FAC_NAME: detail.get("施設名", ""),
            _COL_FLOOR: detail.get("施設内フロア", ""),
            _COL_PARKING: detail.get("駐車場", ""),
            _COL_AREA: listing["area"],
            _COL_CITY: detail.get("_city", ""),
        }

    def _scrape_detail(self, detail_url: str) -> dict:
        """詳細ページの基本情報テーブルをラベル → 値の辞書にして返す。

        取得に失敗した場合は空の辞書を返し、一覧由来の情報だけで出力させる。
        """
        try:
            soup = self.get_soup(detail_url)
        except Exception as e:  # 個別店舗の失敗は握りつぶして継続
            self.logger.warning("詳細取得失敗 %s — %s", detail_url, e)
            return {}
        if soup is None:
            return {}

        detail: dict[str, str] = {}

        detail_dl = soup.select_one("dl.shopDetailList")
        if detail_dl is not None:
            for dt in detail_dl.find_all("dt"):
                dd = dt.find_next_sibling("dd")
                if dd is None:
                    continue
                detail[dt.get_text(strip=True)] = dd.get_text(" ", strip=True)

        # 例: "北海道エリア　函館市" → 市区町村 = "函館市"
        area_p = soup.select_one(".shopName p.area")
        if area_p is not None:
            parts = area_p.get_text(" ", strip=True).replace("　", " ").split()
            if len(parts) >= 2:
                detail["_city"] = parts[-1]

        return detail


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    scraper = Urunavi2Scraper()
    scraper.execute("https://www.urunavi.jp/shop/")
