"""
リジョブ (relax-job.com) — 美容・治療・リラクゼーション・歯科・介護の求人ポータル

取得対象:
    - 全業種 (美容師/アイリスト/ネイリスト/エステ/セラピスト/歯科/介護 等) の求人詳細

取得フロー:
    1. robots.txt から sitemap index (S3) を発見する
    2. sitemap index から業種別求人サイトマップ ({業種}_shop_jobs_sitemap.xml.gz) を抽出
       (応募不可 non_enterable は除外)
    3. 各サイトマップの求人 URL (/job/{id}/sid/{sid}) を1件ずつ詳細取得
    4. 詳細ページ内の JSON-LD (schema.org JobPosting) から構造化フィールドを抽出して即 yield

    ※ 求人本文 (description / PR / 仕事内容) は自由記述プロースのため著作権リスクで取得しない。
    ※ TEL はリジョブ共通の応募ダイヤル (0120-248-101) のみで店舗固有番号が無いため取得しない。

備考の URL 構造 `/{業種}/{area}/{pref}` の SEO 一覧ページは求人をサーバサイドレンダリングせず
(JS/検索フォーム経由) 0 件のため、業種=サイトマップ名、area/pref=各求人の住所 から取得して尊重する。

実行方法:
    python scripts/sites/jobs/relax_job.py
    docker compose exec worker python /app/bin/run_flow.py --site-id relax_job
"""

import gzip
import json
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

# 業種別求人サイトマップ ({業種}_shop_jobs_sitemap.xml.gz) を判定する。
# 応募不可 (non_enterable) は重複/外部応募のため除外。
_JOB_SITEMAP_PATTERN = re.compile(r"/([a-z0-9_-]+)_shop_jobs_sitemap\.xml")
_LOC_PATTERN = re.compile(r"<loc>([^<]+)</loc>")

# JSON-LD employmentType コード → 日本語ラベル
_EMP_TYPE_MAP = {
    "FULL_TIME": "正社員",
    "PART_TIME": "アルバイト・パート",
    "CONTRACTOR": "業務委託",
    "TEMPORARY": "派遣社員",
    "INTERN": "インターン",
    "OTHER": "その他",
}

class RelaxJob(StaticCrawler):
    """リジョブ スクレイパー"""

    DELAY = 1.5

    EXTRA_COLUMNS = [
        "職種",            # occupationalCategory (例: 施術者)
        "雇用形態",        # employmentType (日本語)
    ]

    def prepare(self):
        """詳細ページは Accept ヘッダが無いと 404 を返すため補強し、Cookie を温める。"""
        self.session.headers.update(
            {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            }
        )

    # ------------------------------------------------------------------
    # メイン
    # ------------------------------------------------------------------
    def parse(self, url: str):
        # Cookie ウォームアップ (引数 url = サイトのルート)
        self.get_soup(url)

        # 1. robots.txt から sitemap index を発見 (url を唯一のルートとして派生)
        sitemap_index_url = self._discover_sitemap(url)
        if not sitemap_index_url:
            logger.error("sitemap index を発見できませんでした")
            return

        # 2. 業種別求人サイトマップを列挙
        index_xml = self._fetch_sitemap(sitemap_index_url)
        if not index_xml:
            return
        job_sitemaps = []
        for loc in _LOC_PATTERN.findall(index_xml):
            if "non_enterable" in loc:
                continue
            m = _JOB_SITEMAP_PATTERN.search(loc)
            if m:
                job_sitemaps.append((loc, m.group(1)))
        logger.info("業種別求人サイトマップ: %d 件", len(job_sitemaps))

        # 3. 各サイトマップの求人を 1 件ずつ詳細取得 → 即 yield (Pattern B)
        for sitemap_url, business_type in job_sitemaps:
            sm_xml = self._fetch_sitemap(sitemap_url)
            if not sm_xml:
                continue
            job_urls = _LOC_PATTERN.findall(sm_xml)
            logger.info("業種 %s: 求人 %d 件", business_type, len(job_urls))
            for job_url in job_urls:
                item = self._scrape_detail(job_url)
                if item:
                    yield item

    # ------------------------------------------------------------------
    # 詳細ページ
    # ------------------------------------------------------------------
    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)  # 期限切れ求人は 404 → CONTINUE_ON_ERROR で None
        if soup is None:
            return None

        jp = self._find_jobposting(soup)
        if not jp:
            return None

        item = {
            Schema.URL: url,
        }

        org = jp.get("hiringOrganization") or {}
        item[Schema.NAME] = (org.get("name") or "").strip()

        # 住所
        place = jp.get("jobLocation") or {}
        addr = place.get("address") or {}
        item[Schema.PREF] = (addr.get("addressRegion") or "").strip()
        locality = (addr.get("addressLocality") or "").strip()
        street = (addr.get("streetAddress") or "").strip()
        item[Schema.ADDR] = f"{locality} {street}".strip()
        item[Schema.POST_CODE] = self._format_postcode(addr.get("postalCode"))

        # 勤務時間
        item[Schema.TIME] = (jp.get("workHours") or "").strip()

        # サイト定義業種 (パンくず: リジョブ > 業種 > 都道府県 > 市区 > タイトル)
        item[Schema.CAT_SITE] = self._breadcrumb_business(soup) or (jp.get("occupationalCategory") or "").strip()

        # EXTRA 構造化フィールド
        item["職種"] = (jp.get("occupationalCategory") or "").strip()
        item["雇用形態"] = self._map_employment_types(jp.get("employmentType"))

        return item

    # ------------------------------------------------------------------
    # ヘルパー
    # ------------------------------------------------------------------
    def _discover_sitemap(self, root_url: str) -> str | None:
        """robots.txt の `Sitemap:` 行から sitemap index URL を取得する。"""
        robots_url = urljoin(root_url, "/robots.txt")
        try:
            resp = self.session.get(robots_url, timeout=self.TIMEOUT)
            resp.raise_for_status()
        except Exception as e:  # noqa: BLE001
            logger.warning("robots.txt 取得失敗: %s", e)
            return None
        m = re.search(r"Sitemap:\s*(\S+)", resp.text)
        return m.group(1) if m else None

    def _fetch_sitemap(self, url: str) -> str | None:
        """gzip 圧縮された sitemap を取得して XML 文字列を返す。"""
        try:
            resp = self.session.get(url, timeout=self.TIMEOUT)
            resp.raise_for_status()
        except Exception as e:  # noqa: BLE001
            logger.warning("sitemap 取得失敗 (スキップ): %s — %s", url, e)
            return None
        content = resp.content
        try:
            return gzip.decompress(content).decode("utf-8", "replace")
        except (OSError, EOFError):
            # 既に展開済み / 非 gzip の場合
            return content.decode("utf-8", "replace")

    @staticmethod
    def _find_jobposting(soup) -> dict | None:
        for script in soup.find_all("script", type="application/ld+json"):
            raw = script.string or script.get_text()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except (ValueError, TypeError):
                continue
            for entry in data if isinstance(data, list) else [data]:
                if isinstance(entry, dict) and entry.get("@type") == "JobPosting":
                    return entry
        return None

    @staticmethod
    def _breadcrumb_business(soup) -> str:
        """パンくず JSON-LD の 2 番目 (業種) を返す。"""
        for script in soup.find_all("script", type="application/ld+json"):
            raw = script.string or script.get_text()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except (ValueError, TypeError):
                continue
            for entry in data if isinstance(data, list) else [data]:
                if isinstance(entry, dict) and entry.get("@type") == "BreadcrumbList":
                    elems = entry.get("itemListElement") or []
                    if len(elems) >= 2:
                        item = elems[1].get("item")
                        name = item.get("name") if isinstance(item, dict) else elems[1].get("name")
                        return (name or "").strip()
        return ""

    @staticmethod
    def _format_postcode(value) -> str:
        digits = re.sub(r"\D", "", str(value or ""))
        if len(digits) == 7:
            return f"{digits[:3]}-{digits[3:]}"
        return digits

    @staticmethod
    def _map_employment_types(value) -> str:
        if not value:
            return ""
        codes = value if isinstance(value, list) else [value]
        return "、".join(_EMP_TYPE_MAP.get(str(c), str(c)) for c in codes)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = RelaxJob()
    # 🔒 この URL は sites.yml の url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://relax-job.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
