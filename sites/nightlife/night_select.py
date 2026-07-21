"""
サンキュー案内所 (night-select.jp) — 中洲のナイトビジネス店舗ポータル

取得対象:
    - 掲載店舗 (キャバクラ / ガールズバー / スナック / ホスト / ボーイズバー) の
      基本情報
      (店舗名 / 読み仮名 / ジャンル / 住所 / 営業時間 / 定休日 / TEL /
       公式サイト(HP) / SNS(Instagram / X / TikTok / LINE) /
       平均在籍数 / テーブル席 / 個室 / セット料金 / TAX / YouTube)

取得フロー:
    1. ルート URL から sitemap.xml を取得し、店舗詳細ページ
       (/shops/{id} — 末尾に /reviews や /casts が付かないもの) を列挙する。
    2. 詳細ページを 1 件取得するごとに即 yield する (Pattern B)。

備考 (取得方針):
    - ジャンルをはじめとする構造化フィールドのみを対象とする。
    - 口コミ本文・店舗紹介文などの自由記述プロースは著作権リスクのため取得しない。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/night_select.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id night_select
"""

import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 47 都道府県 (住所先頭マッチ用)
_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 政令指定都市など、住所が「◯◯市」で始まる場合の都道府県補完
_CITY_TO_PREF = {
    "札幌市": "北海道", "仙台市": "宮城県", "さいたま市": "埼玉県",
    "千葉市": "千葉県", "横浜市": "神奈川県", "川崎市": "神奈川県",
    "相模原市": "神奈川県", "新潟市": "新潟県", "静岡市": "静岡県",
    "浜松市": "静岡県", "名古屋市": "愛知県", "京都市": "京都府",
    "大阪市": "大阪府", "堺市": "大阪府", "神戸市": "兵庫県",
    "岡山市": "岡山県", "広島市": "広島県", "北九州市": "福岡県",
    "福岡市": "福岡県", "熊本市": "熊本県",
}

# /shops/{id} (末尾に /reviews /casts /cast/N /recruits が付かないもの) のみ
_SHOP_URL_PATTERN = re.compile(r"^https?://[^/]+/shops/\d+/?$")

# bottom_btns の <img alt> → Schema 定数 / EXTRA カラム名
_SOCIAL_ALT_MAP = {
    "Instagram": Schema.INSTA,
    "エックス": Schema.X,
    "ティックトック": Schema.TIKTOK,
    "LINE": Schema.LINE,
    "YouTube": "YouTube",
}


class NightSelect(StaticCrawler):
    """サンキュー案内所 (night-select.jp) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["平均在籍数", "テーブル席", "個室", "セット料金", "TAX", "YouTube"]

    def parse(self, url: str):
        shop_urls = self._collect_shop_urls(url)
        self.total_items = len(shop_urls)
        logger.info("店舗詳細ページ %d 件を検出", len(shop_urls))

        for shop_url in shop_urls:
            try:
                item = self._scrape_detail(shop_url)
            except Exception as e:  # 個別ページのエラーはスキップ
                logger.warning("詳細ページ取得失敗 %s: %s", shop_url, e)
                continue
            if item:
                yield item

    def _collect_shop_urls(self, url: str) -> list[str]:
        """sitemap.xml から店舗詳細ページ URL を列挙する。"""
        sitemap_url = urljoin(url, "/sitemap.xml")
        soup = self.get_soup(sitemap_url)
        if soup is None:
            logger.warning("sitemap を取得できませんでした: %s", sitemap_url)
            return []

        urls: list[str] = []
        seen: set[str] = set()
        for loc in soup.find_all("loc"):
            loc_url = loc.get_text(strip=True)
            if _SHOP_URL_PATTERN.match(loc_url):
                normalized = loc_url.rstrip("/")
                if normalized not in seen:
                    seen.add(normalized)
                    urls.append(normalized)
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        item = {Schema.URL: url}

        # --- 店舗名 / 読み仮名 ---
        title_block = soup.select_one(".title_block")
        name = ""
        if title_block:
            h1 = title_block.find("h1")
            name = h1.get_text(strip=True) if h1 else ""
            kana_el = title_block.find("span")
            if kana_el:
                kana = kana_el.get_text(strip=True)
                if kana and kana != name:
                    item[Schema.NAME_KANA] = kana
        if not name:  # フォールバック
            h1 = soup.find("h1")
            name = h1.get_text(strip=True) if h1 else ""
        item[Schema.NAME] = name

        # --- shop_info_item (dt ラベル -> dd 値) ---
        info = self._parse_info_items(soup)

        genre = info.get("ジャンル", ("", ""))[0]
        if genre:
            item[Schema.CAT_SITE] = genre

        addr_text, _ = info.get("場所", ("", ""))
        if addr_text:
            pref, rest = self._split_pref(addr_text)
            if pref:
                item[Schema.PREF] = pref
            item[Schema.ADDR] = addr_text

        time_text = info.get("営業時間", ("", ""))[0]
        if time_text:
            item[Schema.TIME] = time_text

        holiday = info.get("定休日", ("", ""))[0]
        if holiday:
            item[Schema.HOLIDAY] = holiday

        tel_text, tel_href = info.get("TEL", ("", ""))
        tel = tel_text or tel_href.replace("tel:", "")
        tel = re.sub(r"\s+", "", tel)
        if tel:
            item[Schema.TEL] = tel

        # 公式サイト (HP) は dd 内の a[href]
        hp = info.get("公式サイト", ("", ""))[1]
        if hp:
            item[Schema.HP] = hp

        # EXTRA (短い構造化値のみ)
        for label in ("平均在籍数", "テーブル席", "個室", "セット料金", "TAX"):
            val = info.get(label, ("", ""))[0]
            if val and val != "〜":
                item[label] = val

        # --- SNS (bottom_btns 内の shop 固有リンク) ---
        for a in soup.select(".bottom_btns a[href]"):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            img = a.find("img")
            alt = img.get("alt", "").strip() if img else ""
            key = _SOCIAL_ALT_MAP.get(alt)
            if key:
                item[key] = href

        return item

    def _parse_info_items(self, soup) -> dict:
        """.shop_info_item を {ラベル: (テキスト, 内部リンク href)} に変換する。"""
        result: dict[str, tuple[str, str]] = {}
        for it in soup.select(".shop_info_item"):
            label_el = it.select_one("dt span")
            label = label_el.get_text(strip=True) if label_el else ""
            if not label:
                continue
            dd = it.find("dd")
            if dd is None:
                result[label] = ("", "")
                continue
            a = dd.find("a")
            href = (a.get("href") or "").strip() if a else ""
            text = dd.get_text(" ", strip=True)
            result[label] = (text, href)
        return result

    @staticmethod
    def _split_pref(address: str) -> tuple[str, str]:
        """住所先頭から都道府県を抽出。政令市始まりは市名から補完。"""
        m = _PREF_PATTERN.match(address)
        if m:
            return m.group(1), address[m.end():].strip()
        for city, pref in _CITY_TO_PREF.items():
            if address.startswith(city):
                return pref, address
        return "", address


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = NightSelect()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://night-select.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
