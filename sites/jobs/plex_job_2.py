"""
プレックスジョブ_整備士 (PLEX JOB) — 自動車整備士求人の詳細スクレイパー

取得対象:
    整備士カテゴリ (/mechanic/) 配下の以下の特集ページに掲載された求人
      - 自動車整備士         /mechanic/feature/272/  (= sites.yml の正規 URL)
      - 1級自動車整備士      /mechanic/feature/215/
      - 2級自動車整備士      /mechanic/feature/216/
      - 3級自動車整備士      /mechanic/feature/217/
      - 自動車検査員         /mechanic/feature/218/
    ※ 機械整備・修理工 (/mechanic/feature/273/) は対象外 (依頼者指示)

取得フロー:
    1. 引数 url (特集ページ) を起点に、資格別特集ページ URL を派生させる
    2. 各特集ページを `?page=N` でページ送りし、__NEXT_DATA__ (Next.js) の
       searchJobs.jobs から求人 ID を列挙 (10件/ページ)
    3. 求人 ID から詳細ページ /{職種}/job/{id}/ を組み立てて取得し、
       __NEXT_DATA__ の構造化 JSON から 1 件ずつ即 yield (重複 ID は除外)

備考:
    - 電話番号・企業HP はサイト上に掲載がないため TEL / HP は常に空
      (応募はサイト経由。掲載終了予定日も項目自体が存在しない)
    - 「求人本文」(仕事内容・応募資格・特徴) は依頼者の明示指示により取得する
    - robots.txt は /mechanic/feature/ と /mechanic/job/ を許可 (Crawl-delay: 1)

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/plex_job_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id plex_job_2
"""

import json
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 巡回対象の特集 ID (272=自動車整備士, 215/216/217=1〜3級, 218=自動車検査員)
# ※ 273 (機械整備・修理工) は対象外
_TARGET_FEATURE_IDS = ["272", "215", "216", "217", "218"]

_TAG = re.compile(r"<[^>]+>")          # <mark> / <b> などの装飾タグ
_FEATURE_URL = re.compile(r"^(?P<base>.*/feature/)(?P<fid>\d+)/?$")
_OCCUPATION = re.compile(r"^/(?P<occ>[a-z_]+)/feature/")
_ANNUAL = re.compile(r"年収[：:]\s*([^\n]+)")


def _clean(value) -> str:
    """None 安全に文字列化し、HTML タグと余分な空白を除去する。"""
    if value is None:
        return ""
    text = _TAG.sub("", str(value))
    return re.sub(r"[ \t　]+", " ", text).strip()


def _clean_multiline(value) -> str:
    """改行を保ったままタグ除去する (求人本文用)。"""
    if value is None:
        return ""
    text = _TAG.sub("", str(value))
    text = re.sub(r"[ \t　]+", " ", text)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def _format_zip(zip_code) -> str:
    """郵便番号を 7桁 → NNN-NNNN 形式に整形する。"""
    if zip_code in (None, ""):
        return ""
    digits = re.sub(r"\D", "", str(zip_code))
    return f"{digits[:3]}-{digits[3:]}" if len(digits) == 7 else digits


class PlexJobMechanicScraper(StaticCrawler):
    """プレックスジョブ_整備士 スクレイパー"""

    DELAY = 1.0  # robots.txt の Crawl-delay: 1 に準拠
    EXTRA_COLUMNS = [
        "媒体名",
        "市区町村",
        "特集",
        "求人タイトル",
        "雇用形態",
        "給与下限",
        "給与上限",
        "年収",
        "掲載日",
        "掲載終了予定日",
        "求人本文",
    ]

    MEDIA_NAME = "プレックスジョブ"

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url を唯一のルート(SSOT)とし、資格別特集ページを派生させる
        feature_urls = self._build_feature_urls(url)
        occupation_seg = self._occupation_segment(url)
        seen_ids: set[str] = set()
        total = 0

        for feature_url in feature_urls:
            page = 1
            total_pages = 1
            while page <= total_pages:
                data = self._fetch_next_data(f"{feature_url}?page={page}")
                if data is None:
                    break

                search_jobs = data.get("searchJobs") or {}
                pagination = search_jobs.get("pagination") or {}
                if page == 1:
                    total_pages = int(pagination.get("totalPages") or 1)
                    total += int(pagination.get("totalCount") or 0)
                    self.total_items = total
                    self.logger.info(
                        "特集 %s (%s): 全 %s 件 / %s ページ",
                        feature_url,
                        _clean((data.get("feature") or {}).get("name")),
                        pagination.get("totalCount"),
                        total_pages,
                    )

                jobs = search_jobs.get("jobs") or []
                if not jobs:
                    break

                for job in jobs:
                    job_id = str(job.get("id") or "").strip()
                    if not job_id or job_id in seen_ids:
                        continue
                    seen_ids.add(job_id)
                    detail_url = urljoin(url, f"/{occupation_seg}/job/{job_id}/")
                    item = self._scrape_detail(detail_url)
                    if item:
                        yield item  # 1 件取得ごとに即 yield (全件バッファしない)

                page += 1

    # ------------------------------------------------------------------
    # URL 組み立て
    # ------------------------------------------------------------------
    def _build_feature_urls(self, url: str) -> list[str]:
        """引数 url から対象の特集ページ URL 一覧を派生させる (引数 url が先頭)。"""
        match = _FEATURE_URL.match(url)
        if not match:
            self.logger.warning("特集ページ URL 形式ではありません。単独で巡回します: %s", url)
            return [url if url.endswith("/") else url + "/"]

        base, current = match.group("base"), match.group("fid")
        urls = [f"{base}{current}/"]
        urls += [f"{base}{fid}/" for fid in _TARGET_FEATURE_IDS if fid != current]
        return urls

    def _occupation_segment(self, url: str) -> str:
        """URL パスから職種セグメント (例: mechanic) を取り出す。"""
        match = _OCCUPATION.match(urlparse(url).path)
        return match.group("occ") if match else "mechanic"

    # ------------------------------------------------------------------
    # ページ取得
    # ------------------------------------------------------------------
    def _fetch_next_data(self, url: str) -> dict | None:
        """ページの __NEXT_DATA__ から pageProps.data を取り出す。"""
        soup = self.get_soup(url)
        if soup is None:
            return None
        script = soup.find("script", id="__NEXT_DATA__")
        if script is None or not script.string:
            self.logger.warning("__NEXT_DATA__ が見つかりません: %s", url)
            return None
        try:
            return json.loads(script.string)["props"]["pageProps"].get("data")
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            self.logger.warning("__NEXT_DATA__ 解析失敗: %s — %s", url, e)
            return None

    def _scrape_detail(self, url: str) -> dict | None:
        data = self._fetch_next_data(url)
        if not data:
            return None

        company = data.get("company") or {}
        # 企業名: company.name が最もクリーン。非公開求人は company が空になる
        name = _clean(company.get("name")) or _clean(data.get("title"))
        if not name and data.get("hiddenCompany"):
            name = "（企業名非公開）"

        prefecture = _clean((data.get("prefecture") or {}).get("name"))
        municipality = _clean((data.get("municipality") or {}).get("name"))
        address = municipality + _clean(data.get("addressLine"))
        building = _clean(data.get("buildingName"))
        if building:
            address = f"{address} {building}".strip()

        job_detail = data.get("jobDetail") or {}
        salary_option = _clean_multiline(data.get("salaryOption"))
        annual_match = _ANNUAL.search(salary_option)
        annual_salary = _clean(annual_match.group(1)) if annual_match else ""

        created_at = _clean(data.get("createdAt"))

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: prefecture,
            Schema.POST_CODE: _format_zip(data.get("zipCode")),
            Schema.ADDR: address,
            Schema.TEL: "",  # サイト上に電話番号の掲載なし (応募はサイト経由)
            Schema.CAT_SITE: _clean((data.get("occupation") or {}).get("name")),
            Schema.HP: "",   # 企業HP の掲載項目なし
            "媒体名": self.MEDIA_NAME,
            "市区町村": municipality,
            "特集": self._feature_labels(data),
            "求人タイトル": _clean(job_detail.get("catchphrase")),
            "雇用形態": _clean(data.get("hiringType")),
            "給与下限": _clean(data.get("lowestSalary")),
            "給与上限": _clean(data.get("highestSalary")),
            "年収": annual_salary,
            "掲載日": created_at[:10] if created_at else "",
            "掲載終了予定日": "",  # サイトに掲載終了日の項目なし
            "求人本文": self._build_body(data),
        }

    # ------------------------------------------------------------------
    # 整形
    # ------------------------------------------------------------------
    def _feature_labels(self, data: dict) -> str:
        """対象特集 (仕事内容カテゴリ) のうち、この求人が持つものを列挙する。"""
        labels = [
            _clean(f.get("name"))
            for f in (data.get("features") or [])
            if str(f.get("id")) in _TARGET_FEATURE_IDS
        ]
        return " / ".join(label for label in labels if label)

    def _build_body(self, data: dict) -> str:
        """求人本文 (仕事内容・応募資格・特徴) を結合する。"""
        job_detail = data.get("jobDetail") or {}
        features = " / ".join(
            _clean(f.get("name")) for f in (data.get("features") or []) if f.get("name")
        )
        sections = [
            ("【仕事内容】", _clean_multiline(job_detail.get("jobDescriptionOption"))),
            ("【応募資格】", _clean_multiline(job_detail.get("qualificationOption"))),
            ("【特徴】", features),
        ]
        return "\n\n".join(f"{title}\n{body}" for title, body in sections if body)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = PlexJobMechanicScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.plex-job.com/mechanic/feature/272/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
