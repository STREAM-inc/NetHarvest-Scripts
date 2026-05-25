"""
フィットネスサーチ (fitness-search.net) — 全国パーソナルジム情報スクレイパー

取得対象:
    - 全国 23 都道府県のパーソナルジム情報

取得フロー:
    1. トップページから都道府県URL収集
    2. BFS で都道府県 → 区市 → エリアの全リストページを巡回
    3. 各リストページから /kuchikomi/{slug}/ の詳細URLを収集
    4. 各詳細ページから情報を抽出

実行方法:
    # ローカルテスト
    python scripts/sites/portal/fitness_search.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id fitness_search
"""

import re
import sys
from collections import deque
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


BASE_URL = "https://www.fitness-search.net"
INDEX_URL = f"{BASE_URL}/"

# 巡回対象のリストページURL（都道府県/区市/エリア）
_LIST_RE = re.compile(
    r'^https://www\.fitness-search\.net/'
    r'(?!kuchikomi/|html/|lp_|jump/|images/)([a-z][^/?#]*/)+$'
)

# ジム詳細ページURL
_GYM_RE = re.compile(r'^https://www\.fitness-search\.net/kuchikomi/[^/?#]+/')

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


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def _extract_pref_addr(raw: str) -> tuple[str, str]:
    if not raw:
        return "", ""
    m = _PREF_PATTERN.match(raw)
    if m:
        return m.group(1), raw[m.end():].strip()
    return "", raw.strip()


def _normalize_href(href: str) -> str:
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return BASE_URL + href
    return href


class FitnessSearchScraper(StaticCrawler):
    """フィットネスサーチ (fitness-search.net) 全国パーソナルジムスクレイパー"""

    DELAY = 1.5

    def parse(self, url: str):
        # Step 1: トップページから都道府県URLを収集
        index_soup = self.get_soup(INDEX_URL)
        if index_soup is None:
            self.logger.error("トップページ取得失敗: %s", INDEX_URL)
            return

        queue: deque[str] = deque()
        visited_lists: set[str] = set()
        gym_urls: set[str] = set()

        for a in index_soup.select("a[href]"):
            href = _normalize_href(a.get("href", ""))
            if _LIST_RE.match(href) and href not in visited_lists:
                visited_lists.add(href)
                queue.append(href)

        # Step 2: BFS で全リストページを巡回
        while queue:
            list_url = queue.popleft()
            soup = self.get_soup(list_url)
            if soup is None:
                continue

            for a in soup.select("a[href]"):
                href = _normalize_href(a.get("href", ""))
                if _GYM_RE.match(href):
                    gym_urls.add(href)
                elif _LIST_RE.match(href) and href not in visited_lists:
                    visited_lists.add(href)
                    queue.append(href)

        self.total_items = len(gym_urls)
        self.logger.info(
            "収集したジム数: %d (リストページ巡回: %d)",
            self.total_items, len(visited_lists)
        )

        # Step 3: 詳細ページスクレイピング
        for gym_url in gym_urls:
            item = self._scrape_detail(gym_url)
            if item:
                yield item

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        try:
            name = _clean(soup.select_one("h1").get_text(strip=True)) if soup.select_one("h1") else ""

            info: dict[str, str] = {}
            for tr in soup.select("table tr"):
                th = tr.select_one("th")
                td = tr.select_one("td")
                if th and td:
                    k = _clean(th.get_text(strip=True))
                    if k and "必須" not in k and k not in info:
                        info[k] = _clean(td.get_text(strip=True))

            pref, addr = _extract_pref_addr(info.get("住所", ""))

            return {
                Schema.URL:     url,
                Schema.NAME:    name,
                Schema.PREF:    pref,
                Schema.ADDR:    addr,
                Schema.TIME:    info.get("営業時間", ""),
                Schema.HOLIDAY: info.get("定休日", ""),
            }
        except Exception as e:
            self.logger.error("詳細取得失敗 %s: %s", url, e)
            return None


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = FitnessSearchScraper()
    scraper.execute(INDEX_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
