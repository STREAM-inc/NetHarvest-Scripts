"""
工場ワークス — 工場・製造業求人サイトのクローラー

取得対象:
    - 04510.jp/areas/ から全都道府県の求人一覧を巡回（約 18,500 件）
    - 各求人詳細ページから会社情報・募集要項を取得

取得フロー:
    1. トップ /areas/ から都道府県別一覧 URL を抽出
    2. 各都道府県を ?page=N でページネーション巡回し、求人詳細 URL を収集
    3. 各詳細ページ /jobs/{jobId}/?companyId={cid} を訪問
    4. 求人情報セクション (h3) と掲載企業情報セクション (h3) をパース

実行方法:
    python scripts/sites/jobs/works.py
"""

import csv
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

BASE_URL = "https://04510.jp"
START_URL = f"{BASE_URL}/areas/"

# /jobs/{jobId}/?companyId={cid}  のリンク検出
_JOB_HREF_RE = re.compile(r"/jobs/(\d+)/?\?[^\"']*companyId=(\d+)")
# /jobs/areas/{region}/{prefecture}/  の都道府県リンク検出
_PREF_HREF_RE = re.compile(r"^/jobs/areas/[a-z]+/[a-z]+/?$")
# 都道府県名抽出用
_PREF_NAME_RE = re.compile(r"(東京都|北海道|京都府|大阪府|\S{2,3}[県])")


def _clean(text) -> str:
    if text is None:
        return ""
    return re.sub(r"[\s　]+", " ", str(text)).strip()


class FactoryWorksScraper(StaticCrawler):
    """工場ワークス (04510.jp) スクレイパー"""

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ① DELAY
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    DELAY = 1.0

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ② EXTRA_COLUMNS
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    EXTRA_COLUMNS = [
        "jobId",
        "companyId",
        "求人タイトル",
        "職種",
        "雇用形態",
        "給与",
        "交通費",
        "勤務時間",
        "勤務期間",
        "勤務曜日",
        "休日・休暇",
        "応募資格",
        "ここがポイント",
        "待遇",
        "特徴",
        "仕事内容",
        "勤務先",
    ]

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ③ parse()
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    def parse(self, url: str) -> Generator[dict, None, None]:
        """
        Phase 1: 全都道府県を巡回して求人URLを収集
        Phase 2: 収集したURLを順次詳細取得して yield
        途中再開対応: 既に取得済みのURLを除外
        """
        top_soup = self.get_soup(url)
        if top_soup is None:
            self.logger.error("トップページ取得失敗 → 中止")
            return

        pref_urls = self._extract_prefecture_urls(top_soup)
        self.logger.info("都道府県リンク: %d 件", len(pref_urls))

        # 既取得URL を先に読み込み（Phase 1 で除外するため）
        already_scraped = self._load_already_scraped_urls()
        self.logger.info("✓ 既取得URL: %d 件（Phase 1 で除外）", len(already_scraped))

        # Phase 1: 新規URL のみを収集（既取得分は除外）
        all_job_urls = self._collect_all_job_urls(pref_urls, already_scraped)
        self.logger.info(
            "📍 Phase 1 完了: 新規 %d 件のURL を収集しました", len(all_job_urls)
        )

        self.total_items = len(all_job_urls)
        self.logger.info("📍 Phase 2 開始: %d 件の詳細を取得します", self.total_items)

        # Phase 2: 詳細取得 → yield
        for job_url in all_job_urls:
            item = self._scrape_detail(job_url)
            if item:
                yield item

    # ------------------------------------------------------------------
    # 内部メソッド
    # ------------------------------------------------------------------
    def _load_already_scraped_urls(self) -> set[str]:
        """前回実行の出力CSVから既取得URLを読み込み。存在しなければ空集合を返す。"""
        output_dir = _project_root / "output"

        # output/ ディレクトリから *_FactoryWorksScraper_*.csv を探す
        if not output_dir.exists():
            return set()

        csv_files = sorted(output_dir.glob("*_FactoryWorksScraper_*.csv"), reverse=True)
        if not csv_files:
            return set()

        output_file = csv_files[0]  # 最新のファイルを使用

        try:
            urls = set()
            with open(output_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    url = row.get(Schema.URL, "").strip()
                    if url:
                        urls.add(url)
            self.logger.info(
                "✓ 既取得ファイル読み込み: %s（%d 件）", output_file.name, len(urls)
            )
            return urls
        except Exception as e:
            self.logger.warning("既取得ファイル読み込みエラー: %s — %s", output_file, e)
            return set()

    def _collect_all_job_urls(self, pref_urls: list[str], already_scraped: set[str]) -> list[str]:
        """全都道府県から求人URLを収集。既取得分は除外。進捗ログを出力。"""
        all_urls: list[str] = []
        global_seen: set[str] = already_scraped.copy()  # 既取得URL から開始

        for i, pref_url in enumerate(pref_urls, 1):
            urls = self._collect_pref_job_urls(pref_url, global_seen)
            all_urls.extend(urls)
            pref_name = pref_url.split("/")[-2]
            self.logger.info(
                "  [%d/%d] %s: %d 件 → 累計 %d 件",
                i,
                len(pref_urls),
                pref_name,
                len(urls),
                len(all_urls),
            )

        return all_urls

    def _collect_pref_job_urls(self, pref_url: str, global_seen: set[str]) -> list[str]:
        """1 都道府県分: 一覧ページを順に取得し、求人URLを収集。1ページ失敗で打ち切り。"""
        results: list[str] = []
        page = 1

        while True:
            page_url = pref_url if page == 1 else f"{pref_url}?page={page}"
            soup = self.get_soup(page_url)

            # 一覧ページ取得失敗時: 1 ページ失敗で打ち切り
            if soup is None:
                self.logger.warning("一覧取得失敗で都道府県を打ち切り: %s", pref_url)
                break

            page_links = self._extract_job_urls(soup)
            new_urls = [u for u in page_links if u not in global_seen]

            self.logger.info(
                "%s page=%d: %d 件 (新規 %d)",
                pref_url,
                page,
                len(page_links),
                len(new_urls),
            )

            for url in new_urls:
                global_seen.add(url)
                results.append(url)

            # 新規URLが無く2ページ目以降なら終了
            if not new_urls and page > 1:
                break

            if not self._has_next_page(soup, page):
                break
            page += 1

        return results

    def _extract_prefecture_urls(self, soup) -> list[str]:
        """トップから /jobs/areas/{region}/{prefecture}/ 形式のリンクを抽出。"""
        results: list[str] = []
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            path = a["href"].replace(BASE_URL, "")
            if not path.startswith("/"):
                continue
            normalized = path.rstrip("/") + "/"
            if not _PREF_HREF_RE.match(normalized):
                continue
            full = urljoin(BASE_URL, normalized)
            if full in seen:
                continue
            seen.add(full)
            results.append(full)
        return results

    def _extract_job_urls(self, soup) -> list[str]:
        """一覧ページから /jobs/{id}/?companyId={cid} 形式のリンクを抽出。"""
        results: list[str] = []
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            m = _JOB_HREF_RE.search(href)
            if not m:
                continue
            job_id, company_id = m.group(1), m.group(2)
            url = f"{BASE_URL}/jobs/{job_id}/?companyId={company_id}"
            if url in seen:
                continue
            seen.add(url)
            results.append(url)
        return results

    def _has_next_page(self, soup, current_page: int) -> bool:
        target = f"?page={current_page + 1}"
        for a in soup.select("a[href]"):
            if target in a.get("href", ""):
                return True
        return False

    def _scrape_detail(self, url: str) -> dict | None:
        """詳細ページから求人情報・掲載企業情報を抽出。"""
        soup = self.get_soup(url)
        if soup is None:
            return None

        m = _JOB_HREF_RE.search(url)
        job_id, company_id = (m.group(1), m.group(2)) if m else ("", "")

        # 求人タイトル: h2（一番最初のもの）
        title_el = soup.select_one("h2")
        title = _clean(title_el.get_text()) if title_el else ""

        # 求人情報セクション (h3「求人情報」配下の h4 → 値)
        info = self._extract_h4_pairs(soup, ["求人情報"])
        # 掲載企業情報セクション (h3「掲載企業情報」配下の h4 → 値)
        company = self._extract_h4_pairs(soup, ["掲載企業情報"])

        # 勤務先 → 都道府県を抽出
        workplace = info.get("勤務先", "")
        pref_m = _PREF_NAME_RE.search(workplace)
        pref = pref_m.group(1) if pref_m else ""

        company_name = company.get("求人掲載企業名", "")
        company_addr = company.get("所在地", "")

        data = {
            Schema.URL: url,
            Schema.NAME: company_name,
            Schema.ADDR: company_addr,
            Schema.PREF: pref,
            Schema.CAT_SITE: info.get("業種", ""),
            "jobId": job_id,
            "companyId": company_id,
            "求人タイトル": title,
            "職種": info.get("職種", ""),
            "雇用形態": info.get("雇用形態", ""),
            "給与": info.get("給与", ""),
            "交通費": info.get("交通費", ""),
            "勤務時間": info.get("勤務時間", ""),
            "勤務期間": info.get("勤務期間", ""),
            "勤務曜日": info.get("勤務曜日", ""),
            "休日・休暇": info.get("休日・休暇", ""),
            "応募資格": info.get("応募資格", ""),
            "ここがポイント": info.get("ここがポイント", ""),
            "待遇": info.get("待遇", ""),
            "特徴": info.get("特徴", ""),
            "仕事内容": info.get("仕事内容", ""),
            "勤務先": workplace,
        }

        if not data[Schema.NAME] and not title:
            return None
        return data

    def _extract_h4_pairs(self, soup, target_h3_keywords: list[str]) -> dict[str, str]:
        """h3セクション内の <h4>キー</h4>本文 を辞書化。"""
        result: dict[str, str] = {}
        for h3 in soup.find_all("h3"):
            h3_text = _clean(h3.get_text())
            if not any(kw in h3_text for kw in target_h3_keywords):
                continue
            current_key: str | None = None
            buf: list[str] = []
            for el in h3.find_all_next():
                if el.name == "h3" and el is not h3:
                    break
                if el.name == "h4":
                    if current_key:
                        result[current_key] = _clean("\n".join(buf))
                    current_key = _clean(el.get_text())
                    buf = []
                elif current_key and el.name in ("ul", "ol"):
                    items = [
                        _clean(li.get_text())
                        for li in el.find_all("li", recursive=False)
                    ]
                    if items:
                        buf.append(" / ".join(items))
                elif current_key and el.name in ("p", "div", "dl", "dd"):
                    txt = _clean(el.get_text(separator="\n"))
                    if txt:
                        buf.append(txt)
            if current_key:
                result[current_key] = _clean("\n".join(buf))
            break
        return result


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = FactoryWorksScraper()
    scraper.execute(START_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
