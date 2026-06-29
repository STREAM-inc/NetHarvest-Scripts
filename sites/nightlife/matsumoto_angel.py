"""
長野ナイトナビ — 松本・塩尻エリアのキャバクラ/スナック等ナイト店舗総合情報サイト

取得対象:
    - 店舗一覧 (/night/shop/) に掲載される全店舗の基本情報

取得フロー:
    1. ルート url から店舗一覧ページ (shop/) を導出して取得
    2. 一覧から店舗詳細ページ (/night/shop/area-XX/YYYY/) のリンクを重複排除で収集
    3. 各詳細ページを取得し、1件ずつ即 yield (途中中断に強い Pattern B)

備考対応:
    - 「取れるカラムは全部」: 静的HTMLから取得可能な構造化フィールドを網羅
    - 料金システム等の自由記述プロースは著作権リスクのため除外
    - SNS は店舗自身のもの (data-gtm-cate="お店") のみ抽出し、在籍キャストの
      Instagram 等が混入しないようにする

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/matsumoto_angel.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id matsumoto_angel
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

# 店舗詳細ページのパス: /night/shop/area-43/8028/ 形式
_DETAIL_PATTERN = re.compile(r"/shop/area-\d+/\d+/$")

# 住所先頭の都道府県を抽出 (このサイトは長野県内のみだが、明記されない住所も多い)
_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# data-gtm-action (店舗SNS) → Schema/EXTRA カラムの対応
_SNS_MAP = {
    "instagram": Schema.INSTA,
    "twitter": Schema.X,
    "x": Schema.X,
    "facebook": Schema.FB,
    "tiktok": Schema.TIKTOK,
    "line": Schema.LINE,
    "youtube": "YouTube",
}


class MatsumotoAngel(StaticCrawler):
    """長野ナイトナビ スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["エリア", "YouTube"]

    def parse(self, url: str):
        # ルート url (sites.yml の url = SSOT) から店舗一覧 URL を導出する
        list_url = urljoin(url, "shop/")
        soup = self.get_soup(list_url)
        if soup is None:
            return

        # 詳細ページリンクを出現順を保ったまま重複排除
        seen = set()
        detail_urls = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if _DETAIL_PATTERN.search(href):
                full = urljoin(url, href)
                if full not in seen:
                    seen.add(full)
                    detail_urls.append(full)

        self.total_items = len(detail_urls)
        logger.info("店舗詳細リンク: %d件", len(detail_urls))

        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as e:  # noqa: BLE001 — 個別店舗の失敗で全体を止めない
                logger.warning("詳細ページ解析に失敗 (スキップ): %s — %s", detail_url, e)
                continue

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        item = {Schema.URL: detail_url}

        wrapper = soup.select_one(".shop-title-wrapper")

        # 名称
        name_el = wrapper.select_one("strong") if wrapper else None
        if name_el:
            item[Schema.NAME] = name_el.get_text(strip=True)

        # 名称_カナ
        kana_el = soup.select_one(".shop-kana")
        if kana_el:
            item[Schema.NAME_KANA] = kana_el.get_text(strip=True)

        # 業種ジャンル / エリア (.shop-info 内のアンカー href で判別)
        if wrapper:
            for a in wrapper.select(".shop-info a[href]"):
                href = a.get("href", "")
                text = a.get_text(strip=True)
                if "businessType" in href:
                    item[Schema.CAT_SITE] = text
                elif re.search(r"/area-\d+/", href):
                    item["エリア"] = text

        # TEL
        tel_el = soup.select_one(".shop-tel")
        if tel_el:
            item[Schema.TEL] = tel_el.get_text(strip=True)

        # 基本情報テーブル (.wrap-shop-info): th のアイコンで行を判別
        info_table = soup.select_one(".wrap-shop-info")
        if info_table:
            for tr in info_table.find_all("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                td_text = td.get_text(" ", strip=True)
                img = th.find("img")
                icon = (img.get("src") or "") if img else ""

                if "time.png" in icon:
                    if td_text:
                        item[Schema.TIME] = td_text
                elif "map.png" in icon:
                    if td_text:
                        self._set_address(item, td_text)
                elif "web.png" in icon:
                    if td_text:
                        item[Schema.HP] = td_text
                elif th.select_one(".shop_rest"):
                    # 定休日 (th 内に「休」アイコン div)
                    if td_text:
                        item[Schema.HOLIDAY] = td_text

        # 店舗own SNS のみ (在籍キャストの SNS は除外)
        for a in soup.select('a.gtm-cnt__sns[href]'):
            if a.get("data-gtm-cate") != "お店":
                continue
            action = (a.get("data-gtm-action") or "").strip().lower()
            col = _SNS_MAP.get(action)
            if col and col not in item:
                item[col] = a["href"]

        return item

    @staticmethod
    def _set_address(item: dict, address: str):
        m = _PREF_PATTERN.match(address)
        if m:
            item[Schema.PREF] = m.group(1)
            item[Schema.ADDR] = address[m.end():].strip()
        else:
            # 都道府県が明記されない住所は長野県固定 (松本・塩尻エリアの地域サイト)
            item[Schema.PREF] = "長野県"
            item[Schema.ADDR] = address


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = MatsumotoAngel()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.matsumoto-angel.net/night/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
