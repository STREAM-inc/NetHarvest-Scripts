"""
ビズコミ (bizcomm) — 職種で探す求人メディア

取得対象:
    - 全国の求人情報 (job-detail) を sitemap から全件列挙し、公開 JSON API から
      構造化フィールドを取得する。

取得フロー:
    1. {url}sitemap.xml (gzip, Content-Encoding 自動解凍) を取得し、
       <loc> から sitemap_joblist*.xml を抽出
    2. 各 joblist サイトマップを取得し、/job-detail/{id} の求人 ID を抽出
    3. 求人 ID ごとに公開 API (api.bizcomm.net/user-biz/job-postings/{id}) を叩き、
       1 件取得するたびに即 yield (Pattern B)

備考:
    - サイト全体の一覧ページは無く、sitemap で全件 (約 93,000 件) を探索する。
    - 自由記述プロース (求人本文・キャッチコピー・各種 description 等) は
      著作権リスク回避のため取得しない。構造化フィールドのみを対象とする。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/bizcomm.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id bizcomm
"""

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

# SSR/フロントが利用する公開バックエンド API。
# サイトルート (url) と同一サービスのデータソースであり、求人詳細 JSON はここからのみ
# 構造化された形で取得できる (HTML 側は Nuxt の難読化 state のみ)。ホストは固定定数。
_API_BASE = "https://api.bizcomm.net/user-biz/job-postings/"

# /job-detail/{数字} から求人 ID を抽出
_JOB_ID_PATTERN = re.compile(r"/job-detail/(\d+)")


class BizcommScraper(StaticCrawler):
    """ビズコミ スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "求人ID",
        "募集企業名",
        "募集企業所在地",
        "募集企業郵便番号",
        "給与種別",
        "給与下限",
        "給与上限",
        "最寄駅",
        "特徴タグ",
        "担当者名",
    ]

    @staticmethod
    def _clean(value) -> str:
        """文字列化して前後空白・改行を除去。None/数値も安全に扱う。"""
        if value is None:
            return ""
        return str(value).strip()

    def _build_hours(self, d: dict) -> str:
        """working_hour_from_N / to_N の構造化ペアから営業(勤務)時間を組み立てる。"""
        parts = []
        for n in range(1, 5):
            frm = self._clean(d.get(f"working_hour_from_{n}"))
            to = self._clean(d.get(f"working_hour_to_{n}"))
            if frm and to:
                parts.append(f"{frm}~{to}")
            elif frm:
                parts.append(frm)
        return " / ".join(parts)

    def _fetch_loc_list(self, xml_url: str) -> list[str]:
        """サイトマップ XML を取得し <loc> の値を返す。"""
        resp = self.session.get(xml_url, timeout=self.TIMEOUT)
        resp.raise_for_status()
        return re.findall(r"<loc>([^<]+)</loc>", resp.text)

    def _scrape_detail(self, job_id: str, page_url: str) -> dict | None:
        """公開 API から 1 求人の構造化データを取得して 1 レコードを構築する。"""
        api_url = f"{_API_BASE}{job_id}"
        resp = self.session.get(api_url, timeout=self.TIMEOUT)
        resp.raise_for_status()
        d = (resp.json() or {}).get("data") or {}
        if not d:
            return None

        # 勤務地住所 = 市区町村 + 建物/番地 (都道府県は PREF に分離)
        addr = " ".join(
            p for p in (self._clean(d.get("city")), self._clean(d.get("job_building"))) if p
        )

        stations = [
            self._clean(s.get("station_name"))
            for s in (d.get("stations") or [])
            if self._clean(s.get("station_name"))
        ]
        tags = [
            self._clean(t.get("name"))
            for t in (d.get("tags") or [])
            if self._clean(t.get("name"))
        ]
        companies = d.get("companies") or []
        postal = self._clean(companies[0].get("postal_code")) if companies else ""

        return {
            Schema.URL: page_url,
            Schema.NAME: self._clean(d.get("company_name")),
            Schema.NAME_KANA: self._clean(d.get("company_katakana_name")),
            Schema.PREF: self._clean(d.get("prefecture_name")),
            Schema.ADDR: addr,
            Schema.TEL: self._clean(d.get("phone_apply_other")),
            Schema.CAT_LV1: self._clean(d.get("job_main_category_name")),
            Schema.CAT_LV2: self._clean(d.get("job_sub_category_name")),
            Schema.TIME: self._build_hours(d),
            Schema.HP: self._clean(d.get("url_application")),
            "求人ID": self._clean(d.get("code")) or job_id,
            "募集企業名": self._clean(d.get("recruiting_company_name")),
            "募集企業所在地": self._clean(d.get("address_recruiting_other")),
            "募集企業郵便番号": postal,
            "給与種別": self._clean(d.get("salary_type_name")),
            "給与下限": self._clean(d.get("lower_salary")),
            "給与上限": self._clean(d.get("upper_salary")),
            "最寄駅": " / ".join(stations),
            "特徴タグ": " / ".join(tags),
            "担当者名": self._clean(d.get("personal_in_charge_name")),
        }

    def parse(self, url: str):
        # サイトマップインデックス (gzip。requests が Content-Encoding で自動解凍)
        index_url = urljoin(url, "sitemap.xml")
        try:
            locs = self._fetch_loc_list(index_url)
        except Exception as e:
            logger.error("サイトマップインデックス取得失敗: %s — %s", index_url, e)
            return

        joblist_sitemaps = [loc for loc in locs if "joblist" in loc]
        logger.info("joblist サイトマップ %d 件を処理します", len(joblist_sitemaps))

        seen = set()
        for sm_url in joblist_sitemaps:
            try:
                detail_locs = self._fetch_loc_list(sm_url)
            except Exception as e:
                logger.warning("サイトマップ取得失敗 (スキップ): %s — %s", sm_url, e)
                continue

            for loc in detail_locs:
                m = _JOB_ID_PATTERN.search(loc)
                if not m:
                    continue
                job_id = m.group(1)
                if job_id in seen:
                    continue
                seen.add(job_id)

                # 詳細ページ URL はルート url から派生 (SSOT = sites.yml の url)
                page_url = urljoin(url, f"job-detail/{job_id}")
                try:
                    item = self._scrape_detail(job_id, page_url)
                except Exception as e:
                    logger.warning("求人取得失敗 (スキップ): %s — %s", page_url, e)
                    continue

                if item and item.get(Schema.NAME):
                    yield item


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BizcommScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://bizcomm.net/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
