# -*- coding: utf-8 -*-
"""
READYFOR — クラウドファンディングポータル (https://readyfor.jp/projects)

取得対象:
    - 公開中プロジェクト一覧 (/projects, ?page=N でページ送り) から
      各プロジェクト詳細を巡回し、実行者・支援状況などの構造化情報を取得する。

取得フロー:
    1. 一覧ページ (?page=N) から /projects/{slug} のリンクを収集
    2. 各詳細ページの <script data-component-name="ProjectPageTemplate">
       に埋め込まれた JSON (react-on-rails) から構造化データを抽出
    3. 詳細を 1 件取得するごとに即 yield (Pattern B / 早期 yield)
    4. 一覧ページにプロジェクトカードが無くなったら終了

実行方法:
    # ローカルテスト
    python scripts/sites/portal/readyfor.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id readyfor
"""

import json
import logging
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

root_path = Path(__file__).resolve()
while not (root_path / "src").exists() and root_path != root_path.parent:
    root_path = root_path.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

# /projects/{slug} の一覧リンク (詳細ページのみ。/watch 等のサブパスは除外)
_PROJECT_HREF = re.compile(r"^/projects/([A-Za-z0-9_\-]+)/?$")
# 都道府県タグ判定 (タグ名が「〇〇県/都/道/府」で終わるもの)
_PREF_SUFFIX = re.compile(r".+(都|道|府|県)$")


class ReadyforCrawler(StaticCrawler):
    """READYFOR クラウドファンディング スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "実行者名",
        "プロジェクトID",
        "識別子",
        "目標金額",
        "支援総額",
        "達成率",
        "支援者数",
        "募集ステータス",
        "プロジェクト種別",
        "資金調達モデル",
        "公開日",
        "終了日",
        "完了予定日",
        "ウォッチ数",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        page = 1

        while True:
            list_soup = self.get_soup(f"{url}?page={page}")
            if list_soup is None:
                break

            # このページのプロジェクト slug を出現順に収集 (ページ内重複排除)
            slugs: list[str] = []
            page_seen: set[str] = set()
            for a in list_soup.select('a[href^="/projects/"]'):
                m = _PROJECT_HREF.match(a.get("href", ""))
                if not m:
                    continue
                slug = m.group(1)
                if slug in page_seen:
                    continue
                page_seen.add(slug)
                slugs.append(slug)

            # カードが無ければ最終ページを越えたとみなして終了
            if not slugs:
                break

            for slug in slugs:
                if slug in seen:  # 注目枠などで複数ページに跨る重複を排除
                    continue
                seen.add(slug)
                detail_url = urljoin(url, f"/projects/{slug}")
                try:
                    item = self._scrape_detail(detail_url)
                except Exception as e:  # 個別失敗はスキップして継続
                    logger.warning("詳細取得に失敗 (スキップ): %s — %s", detail_url, e)
                    continue
                if item:
                    yield item

            page += 1

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        blob = soup.find("script", attrs={"data-component-name": "ProjectPageTemplate"})
        if blob is None or not blob.string:
            logger.warning("ProjectPageTemplate JSON が見つかりません: %s", url)
            return None

        data = json.loads(blob.string)
        proj = data.get("project") or {}
        user = proj.get("user") or {}
        tags = proj.get("tags") or []
        tag_names = [t.get("name", "") for t in tags if t.get("name")]

        # 都道府県タグがあれば PREF に採用
        pref = ""
        for name in tag_names:
            if _PREF_SUFFIX.match(name):
                pref = name
                break

        def _date(value) -> str:
            # ISO8601 文字列は日付部分のみに切り詰める
            if isinstance(value, str) and len(value) >= 10 and value[4] == "-":
                return value[:10]
            return value or ""

        return {
            Schema.NAME: proj.get("title", ""),
            Schema.URL: proj.get("url") or url,
            Schema.REP_NM: data.get("ownerDisplayName") or "",
            Schema.PREF: pref,
            Schema.CAT_SITE: " / ".join(tag_names),
            Schema.OPEN_DATE: data.get("corporationEstablishmentDate") or "",
            Schema.EMP_NUM: data.get("numberOfStaffMembers") or "",
            "実行者名": user.get("name", ""),
            "プロジェクトID": proj.get("id", ""),
            "識別子": proj.get("keyword", ""),
            "目標金額": proj.get("goalPrice", ""),
            "支援総額": proj.get("amount", ""),
            "達成率": proj.get("fundedPercent", ""),
            "支援者数": proj.get("purchasesCount", ""),
            "募集ステータス": proj.get("fundraisingStatus", ""),
            "プロジェクト種別": proj.get("projectType", ""),
            "資金調達モデル": proj.get("fundingModel", ""),
            "公開日": _date(proj.get("publishedAt")),
            "終了日": _date(proj.get("expiredAt")),
            "完了予定日": data.get("completionDate") or "",
            "ウォッチ数": proj.get("numberOfWatchlists", ""),
        }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = ReadyforCrawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://readyfor.jp/projects")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
