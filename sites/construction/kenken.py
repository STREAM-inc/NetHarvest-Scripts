"""
KenKen! (建築系検索エンジン KenKen!) — 建築設計事務所一覧スクレイパー

取得対象:
    - 全国版 建築設計事務所一覧 (category/0k0k0k{page}.html) 約 1,373 社
    - 各 office-card 内に埋め込まれた JSON-LD (schema.org/LocalBusiness) を解析
    - 名称・HP・都道府県・市区町村・サイト定義業種・口コミ採点/件数・タグ 等

取得フロー:
    一覧ページ (category/0k0k0k{N}.html) を 1 ページ目から順に巡回。
    各ページに 10 件の div.office-card があり、それぞれに LocalBusiness の
    JSON-LD が埋め込まれているのでこれをパースし、1 件ごとに即 yield する
    (list-only / Pattern A)。カードが 0 件になったら終了。

備考:
    - 詳細ページ (/status/{id}, /works/{id}/) に TEL・住所の追加情報は無いため
      一覧のみで完結する。住所は addressRegion (都道府県) + addressLocality
      (市区町村) のみで、番地・郵便番号・TEL はサイト上に存在しない。
    - 著作権リスク回避のため、長文の自由記述 (事業紹介 description /
      キャッチコピー office-catch) は取得しない。

実行方法:
    python scripts/sites/construction/kenken.py
    python bin/run_flow.py --site-id kenken
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


BASE_URL = "https://www.kenchikukenken.co.jp"
LIST_URL = f"{BASE_URL}/category/0k0k0k1.html"
# 全国版 建築設計事務所一覧。末尾の連番がページ番号 (0k0k0k{page}.html)
PAGE_URL_TMPL = f"{BASE_URL}/category/0k0k0k{{page}}.html"

MAX_PAGES = 300  # 安全弁 (約 138 ページ想定)
_TOTAL_RE = re.compile(r"([0-9,]+)\s*件")


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"[\s　\xa0]+", " ", text).strip()


class KenkenScraper(StaticCrawler):
    """KenKen! 建築設計事務所一覧スクレイパー"""

    DELAY = 1.5

    def parse(self, url: str) -> Generator[dict, None, None]:
        page = 1
        while page <= MAX_PAGES:
            page_url = PAGE_URL_TMPL.format(page=page)
            soup = self.get_soup(page_url)
            if soup is None:
                self.logger.warning("ページ取得失敗: %s", page_url)
                break

            cards = soup.select("div.office-card")
            if not cards:
                self.logger.info("ページ %d: カード無し、終了", page)
                break

            # 初回ページで総件数を拾って進捗表示を有効化
            if page == 1:
                m = _TOTAL_RE.search(soup.get_text())
                if m:
                    try:
                        self.total_items = int(m.group(1).replace(",", ""))
                    except ValueError:
                        pass

            self.logger.info("ページ %d: %d 件", page, len(cards))
            for card in cards:
                try:
                    item = self._parse_card(card, page_url)
                    if item:
                        yield item
                except Exception:
                    self.logger.exception("カード解析失敗 (page %d)", page)
                    continue

            page += 1

    def _parse_card(self, card, page_url: str) -> dict | None:
        data = self._extract_jsonld(card)

        # 名称: JSON-LD 優先、無ければ h2
        name = _clean(data.get("name", ""))
        if not name:
            h2 = card.select_one("h2")
            name = _clean(h2.get_text(" ", strip=True)) if h2 else ""
        if not name:
            return None

        # 住所 (都道府県 + 市区町村のみ。番地・郵便番号は存在しない)
        addr = data.get("address") or {}
        pref = _clean(addr.get("addressRegion", "")) if isinstance(addr, dict) else ""
        locality = _clean(addr.get("addressLocality", "")) if isinstance(addr, dict) else ""

        # 口コミ採点・件数
        rating = ""
        review_count = ""
        agg = data.get("aggregateRating") or {}
        if isinstance(agg, dict):
            rating = _clean(str(agg.get("ratingValue", "")))
            rc = agg.get("reviewCount", "")
            review_count = _clean(str(rc)) if rc != "" else ""

        # KenKen ID (div#num_{id})
        kenken_id = ""
        card_id = card.get("id") or ""
        mid = re.search(r"num_(\d+)", card_id)
        if mid:
            kenken_id = mid.group(1)

        # KenKen 上のプロフィール (クチコミ) ページを取得 URL とする
        page_kenken = page_url
        if kenken_id:
            page_kenken = f"{BASE_URL}/status/{kenken_id}"

        return {
            Schema.URL: page_kenken,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: locality,
            Schema.HP: _clean(data.get("url", "")),
            Schema.CAT_SITE: _clean(data.get("category", "")),
            Schema.SCORES: rating,
            Schema.REV_SCR: review_count,
        }

    @staticmethod
    def _extract_jsonld(card) -> dict:
        """office-card 内の LocalBusiness JSON-LD を返す (無ければ空 dict)。"""
        for script in card.select('script[type="application/ld+json"]'):
            raw = script.string or script.get_text() or ""
            if not raw.strip():
                continue
            try:
                obj = json.loads(raw)
            except (json.JSONDecodeError, ValueError):
                continue
            candidates = obj if isinstance(obj, list) else [obj]
            for c in candidates:
                if isinstance(c, dict) and c.get("@type") == "LocalBusiness":
                    return c
            # @type が無くても name を持つ dict なら採用
            for c in candidates:
                if isinstance(c, dict) and c.get("name"):
                    return c
        return {}


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = KenkenScraper()
    scraper.execute(LIST_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
