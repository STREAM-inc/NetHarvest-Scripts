"""
Beautyworld Japan Tokyo — 全出展者情報取得クローラー

取得対象:
    - Beautyworld Japan Tokyo 2026 の全出展者（約832件）

取得フロー:
    公開REST API を pageNumber=1〜N でループし、全出展者を取得。
    詳細ページへのアクセスは不要（検索APIが全フィールドを含む）。

実行方法:
    # ローカルテスト
    python scripts/sites/beauty/beautyworld_japan_tokyo.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id beautyworld_japan_tokyo
"""

import re
import sys
import time
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


_HTML_TAG_RE = re.compile(r"<[^>]+>")

_DETAIL_URL = (
    "https://beautyworld-japan.jp.messefrankfurt.com"
    "/tokyo/ja/exhibitor-search.detail.html/{slug}.html#exhibitorheadline"
)
_API_URL = (
    "https://api.messefrankfurt.com/service/esb_api/exhibitor-service"
    "/api/2.1/public/exhibitor/search"
    "?language=ja-JP&q=&orderBy=name&pageNumber={page}&pageSize=30"
    "&orSearchFallback=false&findEventVariable=BEAUTYWORLDJAPANTOKYO"
)
_API_KEY = "LXnMWcYQhipLAS7rImEzmZ3CkrU033FMha9cwVSngG4vbufTsAOCQQ=="
_API_HEADERS = {
    "apikey": _API_KEY,
    "accept": "application/json",
    "referer": "https://beautyworld-japan.jp.messefrankfurt.com/",
}


class BeautyworldJapanTokyoScraper(StaticCrawler):
    """Beautyworld Japan Tokyo 全出展者スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "ホール",
        "ブース番号",
        "ゾーン",
        "PRテキスト",
        "キーワード",
        "特別検索条件",
        "YouTube",
        "メールアドレス",
        "国",
    ]

    def parse(self, url: str):
        page = 1
        while True:
            time.sleep(self.DELAY)
            resp = self.session.get(
                _API_URL.format(page=page),
                headers=_API_HEADERS,
                timeout=self.TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()

            result = data.get("result", {})
            hits = result.get("hits", [])
            meta = result.get("metaData", {})

            if page == 1:
                self.total_items = meta.get("hitsTotal", 0)
                self.logger.info("総件数: %d件", self.total_items)

            if not hits:
                break

            for hit in hits:
                ex = hit.get("exhibitor", {})
                try:
                    yield self._extract(ex)
                except Exception as exc:
                    self.logger.warning(
                        "抽出エラーのためスキップ: %s (%s)", ex.get("name", ""), exc
                    )

            hits_total = meta.get("hitsTotal", 0)
            hits_per_page = meta.get("hitsPerPage", 30)
            if page * hits_per_page >= hits_total:
                break
            page += 1

    def _extract(self, ex: dict) -> dict:
        slug = ex.get("rewriteId", "")
        detail_url = _DETAIL_URL.format(slug=slug) if slug else ""

        addr_rdm = ex.get("addressrdm") or {}
        addr_raw = addr_rdm.get("formatedAddress", "") or ""
        addr = _HTML_TAG_RE.sub(" ", addr_raw).strip()

        address = ex.get("address") or {}

        social = {
            s["network"]: s["url"]
            for s in (ex.get("social") or [])
            if s.get("network") and s.get("url")
        }

        halls = (ex.get("exhibition") or {}).get("exhibitionHall") or []
        hall_id = halls[0].get("id", "") if halls else ""
        stands = halls[0].get("stand", []) if halls else []
        booth = stands[0].get("name", "") if stands else ""
        zones = stands[0].get("zones", []) if stands else []
        zone = zones[0].get("name", "") if zones else ""

        categories = ", ".join(
            c.get("name", "") for c in (ex.get("categories") or [])
        )
        keywords = ", ".join(ex.get("keyWords") or [])
        programs = ", ".join(
            p.get("name", "") for p in (ex.get("exhibitorPrograms") or [])
        )

        return {
            Schema.URL: detail_url,
            Schema.NAME: ex.get("name", ""),
            Schema.ADDR: addr,
            Schema.POST_CODE: address.get("zip", "") or "",
            Schema.TEL: address.get("tel", "") or "",
            Schema.HP: ex.get("homepage", "") or "",
            Schema.INSTA: social.get("instagram", ""),
            Schema.FB: social.get("facebook", ""),
            Schema.X: social.get("twitter", ""),
            Schema.TIKTOK: social.get("tiktok", ""),
            Schema.CAT_SITE: categories,
            "ホール": hall_id,
            "ブース番号": booth,
            "ゾーン": zone,
            "PRテキスト": ex.get("teaser", "") or "",
            "キーワード": keywords,
            "特別検索条件": programs,
            "YouTube": social.get("youtube", ""),
            "メールアドレス": address.get("email", "") or "",
            "国": (address.get("country") or {}).get("label", ""),
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BeautyworldJapanTokyoScraper()
    scraper.execute(
        "https://beautyworld-japan.jp.messefrankfurt.com/tokyo/ja/exhibitor-search.html"
    )

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
