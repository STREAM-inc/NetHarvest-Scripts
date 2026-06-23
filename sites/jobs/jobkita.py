"""
ジョブキタ (jobkita.jp) — 北海道の求人・転職情報サイト スクレイパー

取得対象:
    - 求人・転職情報一覧 (/job/list/?page=N) → 各求人詳細 (/job/detail/{id}/) の
      募集企業名・所在地・連絡先・求人メタ情報

取得フロー:
    1. 一覧ページ (?page=N) を 1 → 末尾 (約81ページ / 約1,618件) まで巡回
    2. 各カードの詳細ページリンク (/job/detail/{id}/) を抽出
    3. 詳細ページの JSON-LD (JobPosting) と #jobContact から構造化データを抽出し、
       1件ごとに即 yield (Pattern B)

備考カラム対応 (jobkita は求人ボードのため企業プロフィール系は出典に存在しない):
    取得可: 名称 / TEL / エリア / 都道府県 / 郵便番号 / 住所
    出典に無し (空文字): 法人番号 / 代表者役職 / 代表者 / 資本金 / 売上 / 従業員数 /
                          設立日 / 事業内容 / FAX / メール / HP / Instagram / Facebook / X / LINE公式

実行方法:
    python scripts/sites/jobs/https_www_jobkita_jp.py
    docker compose exec worker python /app/bin/run_flow.py --site-id https_www_jobkita_jp
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


# 雇用形態 (schema.org employmentType) → 日本語表記
_EMPLOYMENT_MAP = {
    "FULL_TIME": "正社員",
    "PART_TIME": "アルバイト・パート",
    "CONTRACTOR": "業務委託",
    "TEMPORARY": "派遣社員",
    "INTERN": "インターン",
    "OTHER": "その他",
}

_TOTAL_PATTERN = re.compile(r"検索結果.*?([\d,]+)\s*件", re.S)
_TEL_PATTERN = re.compile(r"Tel\s*[:：]\s*([0-9０-９][\d０-９\-－ｰ]+)")
_JOB_ID_PATTERN = re.compile(r"/job/detail/(\d+)")


def _clean(s) -> str:
    """全角空白・連続空白を整理して trim する。"""
    if s is None:
        return ""
    return re.sub(r"[\s　]+", " ", str(s)).strip()


def _as_dict(v) -> dict:
    """JSON-LD の値が list / dict いずれでも先頭の dict を返す。"""
    if isinstance(v, list):
        v = next((x for x in v if isinstance(x, dict)), {})
    return v if isinstance(v, dict) else {}


class HttpsWwwJobkitaJpScraper(StaticCrawler):
    """ジョブキタ (jobkita.jp) 求人スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "求人ID",
        "求人タイトル",
        "雇用形態",
        "エリア",
        "最寄り駅",
        "掲載開始日",
        "掲載終了日",
    ]

    def _get_soup_with_retry(self, url: str, retries: int = 3):
        """一覧ページ取得用リトライ"""
        for attempt in range(1, retries + 1):
            try:
                soup = self.get_soup(url)

                if soup is not None:
                    return soup

                self.logger.warning(
                    "Failed to fetch %s (attempt %d/%d)",
                    url,
                    attempt,
                    retries,
                )

            except Exception as e:
                self.logger.warning(
                    "Error fetching %s (attempt %d/%d): %s",
                    url,
                    attempt,
                    retries,
                    e,
                )

        self.logger.error(
            "Giving up after %d attempts: %s",
            retries,
            url,
        )
        return None

    def parse(self, url: str):
        first_soup = self._get_soup_with_retry(f"{url}?page=1")

        if first_soup is None:
            self.logger.error("Failed to fetch first page")
            return

        last_page = self._get_last_page(first_soup)
        self.logger.info("Detected last page: %d", last_page)

        for page in range(1, last_page + 1):
            list_url = f"{url}?page={page}"

            if page == 1:
                soup = first_soup
            else:
                soup = self._get_soup_with_retry(list_url)

            if soup is None:
                self.logger.warning(
                    "Skipping page %d after 3 failed attempts: %s",
                    page,
                    list_url,
                )
                continue

            seen = set()
            detail_paths = []

            for a in soup.select('a[href*="/job/detail/"]'):
                href = a.get("href", "")
                m = _JOB_ID_PATTERN.search(href)

                if not m:
                    continue

                job_id = m.group(1)

                if job_id in seen:
                    continue

                seen.add(job_id)
                detail_paths.append(href)

            for href in detail_paths:
                detail_url = urljoin(url, href)

                try:
                    item = self._scrape_detail(detail_url)

                except Exception as e:
                    self.logger.warning(
                        "Detail parse failed %s: %s",
                        detail_url,
                        e,
                    )
                    continue

                if item:
                    yield item


    def _get_last_page(self, soup) -> int:
        last_link = soup.select_one("li.last a")

        if not last_link:
            return 1

        href = last_link.get("href", "")
        _LAST_PAGE_PATTERN = re.compile(r"page=(\d+)")
        m = _LAST_PAGE_PATTERN.search(href)
        if not m:
            return 1

        return int(m.group(1))

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        # --- JSON-LD (JobPosting) から構造化データ ---
        posting = None
        for s in soup.select('script[type="application/ld+json"]'):
            raw = s.string or s.get_text()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue
            candidates = data if isinstance(data, list) else [data]
            for d in candidates:
                if isinstance(d, dict) and d.get("@type") == "JobPosting":
                    posting = d
                    break
            if posting:
                break

        if not posting:
            return None

        org = _as_dict(posting.get("hiringOrganization"))
        name = _clean(org.get("name"))
        if not name:
            return None  # 必須フィールド (名称) が無ければスキップ

        addr = _as_dict(_as_dict(posting.get("jobLocation")).get("address"))
        pref = _clean(addr.get("addressRegion"))
        locality = _clean(addr.get("addressLocality"))
        street = _clean(addr.get("streetAddress"))
        post_code = _clean(addr.get("postalCode"))
        full_addr = _clean(f"{locality} {street}")

        # 雇用形態 (list / str いずれもありうる)
        emp = posting.get("employmentType")
        if isinstance(emp, list):
            emp_codes = emp
        elif emp:
            emp_codes = [emp]
        else:
            emp_codes = []
        emp_label = "・".join(_EMPLOYMENT_MAP.get(c, c) for c in emp_codes)

        # 掲載期間 (日付部分のみ)
        date_posted = _clean(posting.get("datePosted"))[:10]
        valid_through = _clean(posting.get("validThrough"))[:10]

        # --- HTML から TEL (#jobContact 内 "Tel:...") ---
        tel = ""
        contact = soup.select_one("#jobContact")
        if contact:
            tm = _TEL_PATTERN.search(contact.get_text(" ", strip=True))
            if tm:
                tel = tm.group(1)

        # --- 募集要項テーブルから [最寄り駅] ---
        nearest_station = ""
        for tr in soup.select("table tr"):
            th = tr.select_one("th")
            td = tr.select_one("td")
            if not th or not td:
                continue
            label = th.get_text(strip=True).strip("[]［］")
            if label == "最寄り駅":
                nearest_station = _clean(td.get_text(" ", strip=True))
                break

        # 求人ID は URL から (JSON-LD identifier.value は出典側で重複するため不使用)
        jm = _JOB_ID_PATTERN.search(url)
        job_id = jm.group(1) if jm else ""

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: full_addr,
            Schema.TEL: tel,
            Schema.URL: url,
            "求人ID": job_id,
            "求人タイトル": _clean(posting.get("title")),
            "雇用形態": emp_label,
            "エリア": locality,
            "最寄り駅": nearest_station,
            "掲載開始日": date_posted,
            "掲載終了日": valid_through,
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = HttpsWwwJobkitaJpScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.jobkita.jp/job/list/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
