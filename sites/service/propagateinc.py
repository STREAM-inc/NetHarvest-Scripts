"""
プロパゲート制作実績 — 株式会社プロパゲートの WEB 制作実績一覧

取得対象:
    - 制作実績 (全33件): 企業名(事例名)、制作サイトURL、業種、プラン、対象(BtoB/BtoC)、掲載ページURL

取得フロー:
    /works は Next.js (App Router) の SSR ページ。カード一覧のページ送り (6件/ページ) は
    クライアント側のみで、全件が初期 HTML の RSC フライトデータ
    (self.__next_f.push) 内の "works":[{name, slug, domain, industry, businessType,
    pageCount, href, ...}] に埋め込まれている。1 リクエストで全件を取得する。
    詳細ページは存在しない (カードは制作先の外部サイトへ直リンク)。

    プランはサイトの JS と同じ規則で pageCount から導出する:
        pageCount <= 1 → Basic（月額9,800円）
        pageCount <= 6 → Standard（月額14,800円）
        それ以外        → Advance（月額19,800円）

実行方法:
    python scripts/sites/service/propagateinc.py
    docker compose exec worker python /app/bin/run_flow.py --site-id propagateinc
"""

import json
import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PUSH_RE = re.compile(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', re.S)

_BUSINESS_TYPE = {"b2b": "BtoB（法人向け）", "b2c": "BtoC（個人向け）"}


def _plan_label(page_count) -> str:
    try:
        n = int(page_count)
    except (TypeError, ValueError):
        return ""
    if n <= 1:
        return "Basic（月額9,800円）"
    if n <= 6:
        return "Standard（月額14,800円）"
    return "Advance（月額19,800円）"


class Propagateinc(StaticCrawler):
    """プロパゲート制作実績 スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["プラン", "対象", "ページ数", "ドメイン", "掲載ページURL"]

    def _extract_works(self, html: str) -> list:
        flight = "".join(json.loads(f'"{c}"') for c in _PUSH_RE.findall(html))
        key = '"works":['
        works = []
        pos = 0
        decoder = json.JSONDecoder()
        while True:
            i = flight.find(key, pos)
            if i < 0:
                break
            try:
                arr, end = decoder.raw_decode(flight, i + len(key) - 1)
            except json.JSONDecodeError:
                pos = i + len(key)
                continue
            works.extend(w for w in arr if isinstance(w, dict) and w.get("name"))
            pos = end
        return works

    def parse(self, url: str):
        soup = self.get_soup(url)
        html = "".join(s.string or "" for s in soup.find_all("script"))
        works = self._extract_works(html)
        if not works:
            raise RuntimeError("制作実績データ (works) が見つかりません。ページ構造が変わった可能性があります")

        seen = set()
        unique = []
        for w in works:
            key = w.get("slug") or w.get("href") or w.get("name")
            if key in seen:
                continue
            seen.add(key)
            unique.append(w)
        self.total_items = len(unique)

        for w in unique:
            yield {
                Schema.NAME: (w.get("name") or "").strip(),
                Schema.HP: w.get("href") or (f"https://{w['domain']}/" if w.get("domain") else ""),
                Schema.CAT_SITE: (w.get("industry") or "").strip(),
                Schema.URL: url,
                "プラン": _plan_label(w.get("pageCount")),
                "対象": _BUSINESS_TYPE.get(w.get("businessType", ""), w.get("businessType", "") or ""),
                "ページ数": str(w.get("pageCount", "") or ""),
                "ドメイン": w.get("domain", "") or "",
                "掲載ページURL": url,
            }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = Propagateinc()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.propagateinc.com/works")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
