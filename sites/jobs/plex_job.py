"""
PLEX JOB(プレックスジョブ) — 物流・ドライバー系求人サイトの求人企業情報スクレイパー

取得対象:
    - sitemap.xml に掲載された求人詳細ページ (/{職種}/job/{id}/) の企業・求人情報
    - 対象職種: driver(ドライバー) / forklift(フォークリフト) / operation_manager(運行管理者)

取得フロー:
    1. sitemap.xml (トップから派生) を取得し、/{cat}/job/{id}/ 形式の求人詳細URLを列挙
    2. 各詳細ページの __NEXT_DATA__ (Next.js) から構造化 JSON を抽出
    3. 会社名・所在地・職種・雇用形態・給与などの「構造化フィールドのみ」を取得して即 yield
       ※ 求人本文・アピール文・担当者コメント等の自由記述(プロース)は著作権配慮で取得しない
    ※ 電話番号はサイト上に掲載されないため TEL は空 (応募はサイト経由)

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/plex_job.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id plex_job
"""

import json
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree as ET

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 求人詳細URL: /{職種}/job/{数値ID}/
_JOB_PATH = re.compile(r"^/[a-z_]+/job/\d+/?$")
_TAG = re.compile(r"<[^>]+>")


def _clean(value) -> str:
    """None 安全に文字列化し、HTMLタグ(<mark>等)と余分な空白を除去する。"""
    if value is None:
        return ""
    text = _TAG.sub("", str(value))
    return re.sub(r"\s+", " ", text.replace("　", " ")).strip()


def _format_zip(zip_code) -> str:
    """郵便番号を 7桁 → NNN-NNNN 形式に整形する。"""
    if zip_code in (None, ""):
        return ""
    digits = re.sub(r"\D", "", str(zip_code))
    if len(digits) == 7:
        return f"{digits[:3]}-{digits[3:]}"
    return digits


class PlexJobScraper(StaticCrawler):
    """PLEX JOB(プレックスジョブ) スクレイパー"""

    DELAY = 1.0  # robots.txt の Crawl-delay: 1 に準拠
    EXTRA_COLUMNS = [
        "求人タイトル",
        "雇用形態",
        "給与下限",
        "給与上限",
        "市区町村",
        "特徴",
        "掲載日",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url を唯一のルート(SSOT)とし、sitemap を派生させる
        sitemap_url = urljoin(url, "/sitemap.xml")
        detail_urls = self._collect_detail_urls(sitemap_url)
        self.total_items = len(detail_urls)
        self.logger.info("求人詳細URL収集完了: %d 件", len(detail_urls))

        for detail_url in detail_urls:
            item = self._scrape_detail(detail_url)
            if item and item.get(Schema.NAME):
                yield item

    def _collect_detail_urls(self, sitemap_url: str) -> list[str]:
        """sitemap.xml から求人詳細URL (/{cat}/job/{id}/) を列挙する。"""
        resp = self.session.get(sitemap_url, timeout=self.TIMEOUT)
        resp.raise_for_status()
        try:
            root = ET.fromstring(resp.content)
        except ET.ParseError as e:
            self.logger.error("sitemap パース失敗: %s", e)
            return []

        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        detail_urls: list[str] = []
        seen: set[str] = set()
        for loc_el in root.findall(".//sm:url/sm:loc", ns):
            loc = loc_el.text and loc_el.text.strip()
            if not loc:
                continue
            path = urlparse(loc).path
            if _JOB_PATH.match(path) and loc not in seen:
                seen.add(loc)
                detail_urls.append(loc)
        return detail_urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        script = soup.find("script", id="__NEXT_DATA__")
        if script is None or not script.string:
            self.logger.warning("__NEXT_DATA__ が見つかりません: %s", url)
            return None
        try:
            data = json.loads(script.string)["props"]["pageProps"].get("data")
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            self.logger.warning("__NEXT_DATA__ 解析失敗: %s — %s", url, e)
            return None
        if not data:
            return None

        # 会社名: company.name が最もクリーン (title は車種プレフィックスや
        # SEO タイトルが混入するため fallback とする)
        company = data.get("company") or {}
        name = _clean(company.get("name")) or _clean(data.get("title"))
        job_detail = data.get("jobDetail") or {}
        catchphrase = _clean(job_detail.get("catchphrase"))
        if not name:
            name = catchphrase

        prefecture = (data.get("prefecture") or {}).get("name", "")
        municipality = (data.get("municipality") or {}).get("name", "")
        addr = _clean(f"{municipality}{_clean(data.get('addressLine'))}")

        occupation = (data.get("occupation") or {}).get("name", "")

        # 特徴タグ (チェックボックス由来の構造化タグ)
        features = " / ".join(
            _clean(f.get("name")) for f in (data.get("features") or []) if f.get("name")
        )

        created_at = _clean(data.get("createdAt"))
        posted_date = created_at[:10] if created_at else ""

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: _clean(prefecture),
            Schema.POST_CODE: _format_zip(data.get("zipCode")),
            Schema.ADDR: addr,
            Schema.TEL: "",  # サイト上に電話番号の掲載なし (応募はサイト経由)
            Schema.CAT_SITE: _clean(occupation),
            "求人タイトル": catchphrase,
            "雇用形態": _clean(data.get("hiringType")),
            "給与下限": _clean(data.get("lowestSalary")),
            "給与上限": _clean(data.get("highestSalary")),
            "市区町村": _clean(municipality),
            "特徴": features,
            "掲載日": posted_date,
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = PlexJobScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.plex-job.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
