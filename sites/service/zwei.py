# scripts/sites/service/zwei.py
"""
ツヴァイ — 全国結婚相談所店舗スクレイパー

取得対象:
    - 全国54店舗の基本情報（店舗名・住所・TEL・営業時間・定休日・アクセス）

取得フロー:
    地域インデックス7ページ → 各店舗詳細ページ（計54件）

実行方法:
    # ローカルテスト
    python scripts/sites/service/zwei.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id zwei
"""

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

BASE_URL = "https://www.zwei.com"

REGION_PATHS = [
    "/branch/hokkaido/",
    "/branch/kantou/",
    "/branch/hokuriku/",
    "/branch/chubu/",
    "/branch/kinki/",
    "/branch/chugoku_shikoku/",
    "/branch/kyusyu/",
]

_POST_RE = re.compile(r"〒(\d{3}-\d{4})")
_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
# 店舗詳細URLパターン: /branch/{region}/{pref}/{city}/ (3セグメント)
_STORE_URL_RE = re.compile(
    r"^https://www\.zwei\.com/branch/[^/]+/[^/]+/[^/]+/?$"
)


class ZweiCrawler(StaticCrawler):
    """ツヴァイ 全国店舗スクレイパー（zwei.com）"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["アクセス"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        store_urls: list[str] = []
        seen: set[str] = set()

        for region_path in REGION_PATHS:
            region_url = BASE_URL + region_path
            self.logger.info("地域ページ取得: %s", region_url)
            for store_url in self._get_store_urls(region_url):
                if store_url not in seen:
                    seen.add(store_url)
                    store_urls.append(store_url)

        self.total_items = len(store_urls)
        self.logger.info("店舗URL件数: %d", self.total_items)

        for store_url in store_urls:
            item = self._scrape_detail(store_url)
            if item:
                yield item

    def _get_store_urls(self, region_url: str) -> list[str]:
        soup = self.get_soup(region_url)
        if soup is None:
            return []
        urls = []
        for a in soup.find_all("a", href=True):
            full = urljoin(BASE_URL, a["href"])
            if _STORE_URL_RE.match(full) and full not in urls:
                urls.append(full)
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        item: dict = {Schema.URL: url}

        # 店舗名: h1.mdl-ttl-xxl の最後の text node ("ツヴァイ渋谷店")
        # 構造: <h1>東京都渋谷区の結婚相談所<br>ツヴァイ渋谷店</h1>
        name_el = soup.find("h1", class_="mdl-ttl-xxl")
        if name_el:
            strings = [s.strip() for s in name_el.strings if s.strip()]
            item[Schema.NAME] = strings[-1] if strings else name_el.get_text(strip=True)
        else:
            # フォールバック: title タグからサービス名部分を取り出す
            title_el = soup.find("title")
            if title_el:
                m = re.search(r"ツヴァイ\S*?店", title_el.get_text(strip=True))
                if m:
                    item[Schema.NAME] = m.group(0)

        if Schema.NAME not in item:
            return None

        # dl.branch-info-store-row の dt/dd: 所在地・電話番号・営業時間
        dl = soup.find("dl", class_="branch-info-store-row")
        if dl:
            for dt in dl.find_all("dt"):
                dd = dt.find_next_sibling("dd")
                if not dd:
                    continue
                label = dt.get_text(strip=True)
                # dd 内の p があればそこからテキスト取得（リンク等を除外）
                p_el = dd.find("p")
                value = p_el.get_text(" ", strip=True) if p_el else dd.get_text(" ", strip=True)

                if "所在地" in label:
                    post_m = _POST_RE.search(value)
                    if post_m:
                        item[Schema.POST_CODE] = post_m.group(1)
                        addr_text = value[post_m.end():].strip()
                        pref_m = _PREF_RE.search(addr_text)
                        if pref_m:
                            item[Schema.PREF] = pref_m.group(1)
                            item[Schema.ADDR] = addr_text[pref_m.start():].strip()
                elif "電話番号" in label:
                    tel_a = dd.find("a", href=re.compile(r"^tel:"))
                    item[Schema.TEL] = tel_a.get_text(strip=True) if tel_a else value
                elif "営業時間" in label:
                    item[Schema.TIME] = value

        # h3.branch-info-ttl の次兄弟から 定休日・アクセス を取得
        for h3 in soup.find_all("h3", class_="branch-info-ttl"):
            label = h3.get_text(strip=True)
            nxt = h3.find_next_sibling()
            if not nxt:
                continue
            if "定休日" in label and nxt.name == "p":
                item[Schema.HOLIDAY] = nxt.get_text(" ", strip=True)
            elif "アクセス" in label:
                item["アクセス"] = nxt.get_text(" ", strip=True)[:300]

        return item if item.get(Schema.ADDR) else None


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = ZweiCrawler()
    scraper.execute("https://www.zwei.com/branch/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
