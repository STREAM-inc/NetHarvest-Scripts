# scripts/sites/portal/pepperlikes.py
"""
PEPPER LIKES (pepperlikes.com) — インフルエンサー募集プロジェクトスクレイパー

取得対象:
    - 掲載中の全プロジェクト (約288件) のブランド・案件情報

取得フロー:
    /api/projects?page=N (JSON API, 15件/ページ)
        → 各プロジェクトの詳細ページ (/project/{id}) から要件を補完

実行方法:
    # ローカルテスト
    python scripts/sites/portal/pepperlikes.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id pepperlikes
"""

import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

API_URL = "https://www.pepperlikes.com/api/projects"
PROJECT_URL = "https://www.pepperlikes.com/project/{id}"

_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


class PepperlikesScraper(StaticCrawler):
    """PEPPER LIKES プロジェクト/ブランドスクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "プロジェクトID",
        "プロジェクトタイトル",
        "報酬スタイル",
        "投稿形式",
        "プロジェクト期間",
        "スキル",
        "対応SNS",
        "タグ",
        "フォロワー数",
        "エリア",
        "使用言語",
        "投稿日時",
    ]

    def parse(self, url: str):
        resp = self.session.get(url, params={"page": 1}, timeout=self.TIMEOUT)
        resp.raise_for_status()
        first = resp.json()
        pagination = first.get("data", {}).get("pagination", {})
        total = pagination.get("total", 0)
        total_pages = pagination.get("totalPages", 1)
        self.total_items = total
        self.logger.info("全%d件 / %dページ", total, total_pages)

        page = 1
        while page <= total_pages:
            if page == 1:
                payload = first
            else:
                r = self.session.get(url, params={"page": page}, timeout=self.TIMEOUT)
                r.raise_for_status()
                payload = r.json()

            items = payload.get("data", {}).get("list", [])
            for proj in items:
                try:
                    item = self._build_item(proj)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.warning("プロジェクト処理失敗 id=%s: %s", proj.get("id"), e)
            page += 1

    def _build_item(self, proj: dict) -> dict | None:
        pid = proj.get("id")
        if not pid:
            return None

        author = proj.get("project_author") or {}
        item: dict = {
            Schema.URL: PROJECT_URL.format(id=pid),
            Schema.NAME: author.get("full_name", "").strip(),
            "プロジェクトID": str(pid),
            "プロジェクトタイトル": (proj.get("project_title") or "").strip(),
            "報酬スタイル": proj.get("project_type", "") or "",
            "投稿形式": proj.get("project_location", "") or "",
            "プロジェクト期間": proj.get("projectDuration", "") or "",
            "投稿日時": proj.get("posted_at", "") or "",
        }

        # 代表者名 (last_name + first_name)
        first = (author.get("first_name") or "").strip()
        last = (author.get("last_name") or "").strip()
        rep = f"{last}{first}".strip()
        if rep:
            item[Schema.REP_NM] = rep

        # スキル配列
        skills = proj.get("skills") or []
        if isinstance(skills, list) and skills:
            item["スキル"] = " / ".join(str(s) for s in skills)

        # 住所 (API)
        addr = (proj.get("address") or "").strip()
        if addr:
            m = _PREF_RE.match(addr)
            if m:
                item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = addr[m.end():].strip()
            else:
                item[Schema.ADDR] = addr

        # 事業内容 (説明HTMLをプレーンテキスト化、先頭500文字)
        desc = proj.get("project_description") or ""
        if desc:
            text = re.sub(r"<[^>]+>", " ", desc)
            text = re.sub(r"\s+", " ", text).strip()
            item[Schema.LOB] = text[:500]

        # 詳細ページから要件を補完
        self._enrich_from_detail(pid, item)

        if not item.get(Schema.NAME):
            return None
        return item

    def _enrich_from_detail(self, pid: int, item: dict) -> None:
        """詳細ページのプロジェクト要件セクションから追加フィールドを取得する"""
        try:
            soup = self.get_soup(PROJECT_URL.format(id=pid))
        except Exception as e:
            self.logger.debug("詳細ページ取得失敗 id=%s: %s", pid, e)
            return

        req_ul = soup.select_one("ul.tk-project-requirement")
        if not req_ul:
            return

        for li in req_ul.select("li"):
            em = li.select_one("em")
            if not em:
                continue
            label = em.get_text(strip=True)
            spans = [s.get_text(strip=True) for s in li.select(".tk-requirement-tags span")]
            value = ", ".join(s for s in spans if s)
            if not value:
                continue

            if label == "ジャンル":
                item[Schema.CAT_SITE] = value
            elif label == "エリア":
                item["エリア"] = value
            elif label == "対応SNS":
                item["対応SNS"] = value
            elif label == "住所" and not item.get(Schema.ADDR):
                m = _PREF_RE.match(value)
                if m:
                    item[Schema.PREF] = m.group(1)
                    item[Schema.ADDR] = value[m.end():].strip()
                else:
                    item[Schema.ADDR] = value
            elif label == "タグ":
                item["タグ"] = value
            elif label == "フォロワー数":
                item["フォロワー数"] = value
            elif label == "使用言語":
                item["使用言語"] = value
            elif label == "プロジェクト期間" and not item.get("プロジェクト期間"):
                item["プロジェクト期間"] = value


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = PepperlikesScraper()
    scraper.execute(API_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
