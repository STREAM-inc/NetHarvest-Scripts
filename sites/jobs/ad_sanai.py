"""
アドサンアイ (ad-sanai.co.jp) — 求人企業情報スクレイパー

取得対象:
    - 求人一覧 (/job-search) に掲載される全求人の掲載企業情報

取得フロー:
    一覧ページ (?...&page=N, 0始まり) を巡回 → 各 /job/{id} 詳細ページへ遷移
    → 1 件取得するごとに即 yield (途中 break しても無駄な通信が起きない)

サイト構造:
    - Drupal Views。一覧は `.views-row .job-list-title a[href^=/job/]`
    - ページネーションは `&page=N` (0 始まり)。pager の「最終ページ」リンクで総ページ数を把握
    - 詳細ページの本体は `#block-vwork-front-ad-sanai-content` 配下
      (これでスコープしないと末尾の「おすすめ求人」teaser が混入する)

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/ad_sanai.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ad_sanai
"""

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

# 詳細ページ本体のコンテナ (末尾のおすすめ求人 teaser を除外するためのスコープ)
_MAIN_SCOPE = "#block-vwork-front-ad-sanai-content"
_ITEMS_PER_PAGE = 16


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


class AdSanaiScraper(StaticCrawler):
    """アドサンアイ 求人企業情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "求人タイトル",
        "雇用形態",
        "職種",
        "給与",
        "勤務時間",
        "待遇",
        "こだわり条件",
        "勤務地",
        "担当者",
        "掲載開始日",
        "掲載終了日",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url を唯一のルート(SSOT)とし、ページングは &page=N で派生させる
        page = 0
        last_page = None
        seen: set[str] = set()

        while True:
            page_url = f"{url}&page={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                break

            # 初回ページで総ページ数を把握し進捗表示を有効化
            if last_page is None:
                last_page = self._detect_last_page(soup)
                if last_page is not None:
                    self.total_items = (last_page + 1) * _ITEMS_PER_PAGE
                    self.logger.info(
                        "総ページ数: %d (推定 %d 件)", last_page + 1, self.total_items
                    )

            detail_urls = []
            for a in soup.select(".job-list-title a[href^='/job/']"):
                href = a.get("href")
                if href:
                    detail_urls.append(urljoin(url, href))

            if not detail_urls:
                break

            for detail_url in detail_urls:
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                try:
                    item = self._scrape_detail(detail_url)
                except Exception as e:  # 個別ページのエラーはスキップして継続
                    self.logger.warning("詳細取得失敗 (スキップ): %s — %s", detail_url, e)
                    continue
                if item and item.get(Schema.NAME):
                    yield item

            if last_page is not None and page >= last_page:
                break
            page += 1

    @staticmethod
    def _detect_last_page(soup) -> int | None:
        """pager の「最終ページ」/「次ページ」以外のリンクから最大 page 番号を取得する"""
        max_page = None
        for a in soup.select(".pager a[href], nav.pager a[href], ul.pagination a[href]"):
            href = a.get("href") or ""
            m = re.search(r"[?&]page=(\d+)", href)
            if m:
                p = int(m.group(1))
                if max_page is None or p > max_page:
                    max_page = p
        return max_page

    def _field(self, scope, field_name: str, sep: str = " ") -> str:
        """`.field--name-{field_name}` の値を取り出す。

        Drupal の field は (a) 要素自身が field__item の場合と、
        (b) 内部に field__label + field__item を持つ場合があるため両対応する。
        """
        el = scope.select_one(f".field--name-{field_name}")
        if el is None:
            return ""
        items = el.select(".field__item")
        if items:
            return sep.join(_clean(i.get_text(" ")) for i in items if _clean(i.get_text(" ")))
        return _clean(el.get_text(" "))

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None
        scope = soup.select_one(_MAIN_SCOPE) or soup

        data = {Schema.URL: url}

        # --- Schema マッピング ---
        data[Schema.NAME] = self._field(scope, "field-job-location-name")
        data[Schema.PREF] = self._field(scope, "field-job-prefectures")
        city = self._field(scope, "field-job-city")
        addr = self._field(scope, "field-job-address")
        data[Schema.ADDR] = _clean(f"{city}{addr}")
        data[Schema.TEL] = self._field(scope, "field-jobex-contact-tel")
        data[Schema.CAT_SITE] = self._field(scope, "field-job-occupation")

        # HP は location-url の <a href> を優先
        hp_el = scope.select_one(".field--name-field-job-location-url a[href]")
        if hp_el:
            data[Schema.HP] = _clean(hp_el.get("href"))
        else:
            data[Schema.HP] = self._field(scope, "field-job-location-url")

        # --- EXTRA_COLUMNS ---
        # 求人タイトル: h1 / title フィールドから " | サイト名" の接尾辞を除去
        title_el = soup.select_one(".field--name-title") or soup.select_one("h1")
        title = _clean(title_el.get_text(" ")) if title_el else ""
        data["求人タイトル"] = title.split("|")[0].strip() if title else ""

        data["雇用形態"] = self._field(scope, "field-job-employment-type")
        data["職種"] = self._field(scope, "field-jobex-occupation-text")
        data["給与"] = self._field(scope, "field-job-salary-text")
        data["勤務時間"] = self._field(scope, "field-job-worktime")
        data["待遇"] = self._field(scope, "field-jobex-treatment")
        data["こだわり条件"] = self._field(scope, "field-job-kodawari", sep=" / ")
        data["勤務地"] = self._field(scope, "field-job-place-detail")
        data["担当者"] = self._field(scope, "field-jobex-entry-person")
        data["掲載開始日"] = self._field(scope, "field-job-public-period-start")
        data["掲載終了日"] = self._field(scope, "field-job-public-period-end")

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = AdSanaiScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute(
        "https://www.ad-sanai.co.jp/job-search?area=All&pref=All&city=All&field_job_salary_system_value=All&field_job_salary_time=All&field_job_salary_day=All&field_job_salary_month=All&field_job_salary_year=All&station_name=&keyword="
    )

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
