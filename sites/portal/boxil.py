"""
BOXIL — SaaS比較・口コミサイト スクレイパー

取得対象:
    - 全カテゴリのSaaSサービス一覧 (467カテゴリ、推定6,800件超)

取得フロー:
    1. /categories/ から全カテゴリURL・名称を収集
    2. 各カテゴリで一覧ページをページネーション (page=N) で全件取得
    3. サービスカードから必要フィールドを取得し、詳細ページで住所を補完
    4. サービスURLで重複排除 (同一サービスが複数カテゴリに登録されている場合あり)

実行方法:
    # ローカルテスト
    python scripts/sites/portal/boxil.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id boxil
"""

import json
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class BoxilScraper(StaticCrawler):
    """BOXIL SaaS比較サイト スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["住所"]

    def parse(self, url: str):
        base = url.rstrip("/")

        cats_soup = self.get_soup(f"{base}/categories/")
        if cats_soup is None:
            return

        categories = self._extract_categories(cats_soup, base)
        self.logger.info("カテゴリ数: %d", len(categories))

        seen_urls: set[str] = set()

        for cat_url, cat_name in categories:
            self.logger.info("カテゴリ: %s", cat_name)
            page = 1
            while True:
                page_url = cat_url if page == 1 else f"{cat_url}?page={page}"
                soup = self.get_soup(page_url)
                if soup is None:
                    break

                cards = soup.find_all("section", class_=re.compile(r"CategoryServiceCard"))
                if not cards:
                    break

                last_page = self._extract_last_page(soup)

                for card in cards:
                    try:
                        item = self._parse_card(card, base, cat_name, seen_urls)
                        if item:
                            item["住所"] = self._fetch_address(item[Schema.URL])
                            yield item
                    except Exception as e:
                        self.logger.warning("カード解析失敗: %s", e)

                if page >= last_page:
                    break
                page += 1

    def _extract_categories(self, soup, base: str) -> list[tuple[str, str]]:
        result = []
        seen = set()
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            if not re.match(r"^/sc-[^/?]+/$", href):
                continue
            if href in seen:
                continue
            seen.add(href)
            raw_text = link.get_text(strip=True)
            name = re.sub(r"\(\d+\)", "", raw_text).strip()
            if name:
                result.append((urljoin(base + "/", href), name))
        return result

    def _extract_last_page(self, soup) -> int:
        nums = []
        for link in soup.find_all("a", href=re.compile(r"[?&]page=\d+")):
            m = re.search(r"page=(\d+)", link.get("href", ""))
            if m:
                nums.append(int(m.group(1)))
        return max(nums) if nums else 1

    def _parse_card(self, card, base: str, cat_name: str, seen_urls: set) -> dict | None:
        name_link = card.find("a", href=re.compile(r"/service/\d+/"))
        if not name_link:
            return None

        href = name_link.get("href", "")
        service_path = re.sub(r"\?.*$", "", href)
        service_url = urljoin(base + "/", service_path)

        if service_url in seen_urls:
            return None
        seen_urls.add(service_url)

        name = name_link.get_text(strip=True)
        if not name:
            return None

        return {
            Schema.NAME: name,
            Schema.URL: service_url,
            Schema.CAT_SITE: cat_name,
        }

    def _fetch_address(self, service_url: str) -> str:
        soup = self.get_soup(service_url)
        if soup is None:
            return ""

        # JSON-LD の Product.brand.address から取得（最も信頼性が高い）
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                if data.get("@type") == "Product":
                    brand = data.get("brand", {})
                    if isinstance(brand, dict) and brand.get("address"):
                        return brand["address"]
            except Exception:
                pass

        # フォールバック: 「住所」ラベルを持つ th/dt の隣接要素
        for label_el in soup.find_all(["th", "dt"]):
            if label_el.get_text(strip=True) == "住所":
                sibling = label_el.find_next_sibling(["td", "dd"])
                if sibling:
                    return sibling.get_text(strip=True)

        return ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BoxilScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://boxil.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
