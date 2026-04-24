"""
アイブリー (ivry.jp) — IVRy導入事例の企業一覧クローラー

取得対象:
    https://ivry.jp/case-list/ に掲載された全導入事例 (約179件)

取得フロー:
    1. /case-list/ を1ページ取得 (ページネーション無し, 全件1ページ表示)
    2. カード <a href="/case/{slug}"> から 社名・業界カテゴリ・事例タイトル・サムネイル・詳細URL を抽出
    3. 各詳細ページの JSON-LD (Article) および meta description から
       公開日・更新日・事例説明 を補完

実行方法:
    python scripts/sites/corporate/ivry.py
    python bin/run_flow.py --site-id ivry
"""

import json
import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://ivry.jp"
LIST_URL = f"{BASE_URL}/case-list/"


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


class IvryScraper(StaticCrawler):
    """アイブリー導入事例 (ivry.jp/case-list/) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "事例タイトル",
        "サムネイル画像URL",
        "公開日",
        "更新日",
        "事例説明",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)
        if soup is None:
            self.logger.error("一覧ページ取得失敗: %s", url)
            return

        cards = self._extract_cards(soup)
        self.total_items = len(cards)
        self.logger.info("導入事例カード: %d 件", self.total_items)

        for card in cards:
            try:
                item = self._scrape_detail(card)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning(
                    "詳細取得失敗 (スキップ): %s — %s", card.get("detail_url"), e
                )
                continue
            time.sleep(self.DELAY)

    def _extract_cards(self, soup) -> list[dict]:
        main = soup.find("main") or soup
        seen: set[str] = set()
        cards: list[dict] = []
        for a in main.select('a[href^="/case/"]'):
            href = a.get("href", "").strip()
            if not href or href in seen:
                continue
            # ヘッダー/フッターのナビゲーションリンクは main 外だが、
            # 念のため h2 を持つカードのみを対象にする。
            h2 = a.find("h2")
            if not h2:
                continue
            seen.add(href)

            name = _clean(h2.get_text())
            headline_el = a.find("p")
            headline = _clean(headline_el.get_text()) if headline_el else ""

            category = ""
            # 業界ラベルは先頭の div 配下にある内側 div
            outer_div = a.find("div")
            if outer_div:
                inner_divs = outer_div.find_all("div")
                for d in inner_divs:
                    t = _clean(d.get_text())
                    if t and d.find("img") is None:
                        category = t
                        break

            img = a.find("img")
            thumb = img.get("src", "") if img else ""

            cards.append({
                "detail_url": urljoin(BASE_URL, href),
                "name": name,
                "category": category,
                "headline": headline,
                "thumbnail": thumb,
            })
        return cards

    def _scrape_detail(self, card: dict) -> dict | None:
        detail_url = card["detail_url"]
        data: dict = {
            Schema.URL: detail_url,
            Schema.NAME: card["name"],
            Schema.CAT_SITE: card["category"],
            "事例タイトル": card["headline"],
            "サムネイル画像URL": card["thumbnail"],
            "公開日": "",
            "更新日": "",
            "事例説明": "",
        }

        soup = self.get_soup(detail_url)
        if soup is None:
            return data

        meta = soup.find("meta", attrs={"name": "description"})
        if meta and meta.get("content"):
            data["事例説明"] = _clean(meta["content"])

        for s in soup.find_all("script", attrs={"type": "application/ld+json"}):
            raw = s.string or s.get_text()
            if not raw:
                continue
            try:
                parsed = json.loads(raw)
            except (ValueError, TypeError):
                continue
            entries = parsed if isinstance(parsed, list) else [parsed]
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                if entry.get("@type") == "Article":
                    if entry.get("datePublished"):
                        data["公開日"] = _clean(entry["datePublished"])
                    if entry.get("dateModified"):
                        data["更新日"] = _clean(entry["dateModified"])

        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = IvryScraper()
    scraper.execute(LIST_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
