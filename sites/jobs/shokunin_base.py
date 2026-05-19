"""
職人BASE 求人情報スクレイパー

対象サイト: https://shokunin-base.com/

公開ページ上で取得できる情報:
    - /cases/ およびカテゴリ別一覧の求人カード
    - /cases/job/... の詳細ページが公開リンクとして渡された場合の企業名・求人詳細

注意:
    公開求人一覧には企業名/TEL/住所/HP が表示されないため、一覧カードだけの行は
    名寄せキー不足として出力する。詳細ページから企業名を取得できた場合のみ Schema.NAME
    に企業名を設定する。
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
for _candidate in (_project_root, _project_root / "NetHarvest"):
    if (_candidate / "src").exists() and str(_candidate) not in sys.path:
        sys.path.insert(0, str(_candidate))

from src.const.schema import Schema
from src.framework.static import StaticCrawler


BASE_URL = "https://shokunin-base.com"
START_URL = "https://shokunin-base.com/cases/"
CATEGORY_URLS = [
    START_URL,
    f"{START_URL}?category=technical",
    f"{START_URL}?category=planner",
    f"{START_URL}?category=carpenter",
    f"{START_URL}?category=designer",
]

PREF_PATTERN = re.compile(
    r"(北海道|東京都|大阪府|京都府|(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|"
    r"埼玉|千葉|神奈川|新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|"
    r"滋賀|兵庫|奈良|和歌山|鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|"
    r"福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県)"
)


def _clean(text) -> str:
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text).replace("\u3000", " ")).strip()


def _first_text(root, selector: str) -> str:
    el = root.select_one(selector)
    return _clean(el.get_text(" ")) if el else ""


def _extract_prefs(area: str) -> str:
    prefs = []
    for pref in PREF_PATTERN.findall(area or ""):
        if pref not in prefs:
            prefs.append(pref)
    return "、".join(prefs)


class ShokuninBaseScraper(StaticCrawler):
    """職人BASE 求人情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "案件名",
        "仕事概要",
        "案件カテゴリ",
        "求人タイトル",
        "エリア",
        "業種",
        "職種",
        "稼働日数",
        "雇用形態",
        "給与",
        "最終更新日",
        "募集背景",
        "契約期間",
        "業務内容",
        "応募資格・必須スキル",
        "勤務地",
        "受動喫煙防止措置",
        "勤務時間",
        "時間外労働",
        "取得元区分",
        "企業名取得状況",
        "名寄せ可否",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        target = url or START_URL
        seen: set[str] = set()

        if "/cases/job/" in target:
            item = self._scrape_detail(target)
            if item:
                yield item
            return

        for source_url in self._listing_urls(target):
            soup = self._get_soup_polite(source_url)
            if soup is None:
                continue

            detail_urls = self._extract_detail_urls(soup, source_url)
            for detail_url in detail_urls:
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                item = self._scrape_detail(detail_url)
                if item:
                    yield item

            for case in soup.select("li.case"):
                item = self._parse_case_card(case, source_url)
                if not item:
                    continue
                key = self._dedupe_key(item)
                if key in seen:
                    continue
                seen.add(key)
                yield item

    def _listing_urls(self, start_url: str) -> list[str]:
        parsed = urlparse(start_url)
        if parsed.path.rstrip("/") == "/cases" and not parsed.query:
            return CATEGORY_URLS
        return [start_url]

    def _get_soup_polite(self, url: str):
        if self.DELAY > 0:
            time.sleep(self.DELAY)
        return self.get_soup(url)

    def _extract_detail_urls(self, soup, source_url: str) -> list[str]:
        urls = []
        for a in soup.select('a[href*="/cases/job/"]'):
            href = a.get("href", "").strip()
            if not href:
                continue
            detail_url = urljoin(source_url, href).split("#", 1)[0]
            if detail_url not in urls:
                urls.append(detail_url)
        return urls

    def _parse_case_card(self, case, source_url: str) -> dict | None:
        title = _first_text(case, ".job-category-container .title, .title")
        if not title:
            return None

        job_category = _first_text(case, ".job-category")
        tags = [_clean(tag.get_text(" ")) for tag in case.select(".tag") if _clean(tag.get_text(" "))]
        area = tags[0] if tags else ""
        employment = tags[1] if len(tags) > 1 else ""
        salary = _first_text(case, ".salary-container-wrapper")
        company = _first_text(case, ".company-name")
        prefs = _extract_prefs(area)

        item = {
            Schema.URL: source_url,
            Schema.NAME: company,
            Schema.PREF: prefs,
            Schema.CAT_SITE: job_category,
            "案件名": title,
            "仕事概要": "",
            "案件カテゴリ": job_category,
            "求人タイトル": title,
            "エリア": area,
            "業種": job_category,
            "職種": job_category,
            "稼働日数": "",
            "雇用形態": employment,
            "給与": salary,
            "取得元区分": "公開求人一覧",
            "企業名取得状況": "取得済み" if company else "一覧非掲載",
            "名寄せ可否": "可" if company else "不可（一覧に企業名・住所・HP・TELなし）",
        }
        return item

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self._get_soup_polite(detail_url)
        if soup is None:
            return None

        company = _first_text(soup, ".detail-company")
        title = _first_text(soup, ".detail-title")
        if not company and not title:
            return None

        tags = [_clean(tag.get_text(" ")) for tag in soup.select(".detail-tag") if _clean(tag.get_text(" "))]
        job_category = tags[0] if tags else ""
        area = tags[1] if len(tags) > 1 else ""
        summary = _first_text(soup, ".detail-summary")

        updated = ""
        updated_text = _first_text(soup, ".detail-updated-date")
        if updated_text:
            updated = re.sub(r"^最終更新日[:：]\s*", "", updated_text)
        if not updated:
            page_text = soup.get_text(" ", strip=True)
            m = re.search(r"最終更新日[:：]\s*([0-9]{4}年[0-9]{1,2}月[0-9]{1,2}日)", page_text)
            if m:
                updated = m.group(1)

        item = {
            Schema.URL: detail_url,
            Schema.NAME: company,
            Schema.PREF: _extract_prefs(area),
            Schema.CAT_SITE: job_category,
            "案件名": title,
            "仕事概要": summary,
            "案件カテゴリ": job_category,
            "求人タイトル": title,
            "エリア": area,
            "業種": job_category,
            "職種": job_category,
            "稼働日数": "",
            "最終更新日": updated,
            "取得元区分": "公開求人詳細",
            "企業名取得状況": "取得済み" if company else "詳細ページ非掲載",
            "名寄せ可否": "企業名のみ（住所・HP・TELなし）" if company else "不可",
        }

        for section in soup.select(".detail-section"):
            key = _first_text(section, ".detail-section-subtitle")
            val = _first_text(section, ".detail-section-content")
            if key in self.EXTRA_COLUMNS and val:
                item[key] = val
            if key == "雇用形態" and val:
                item["雇用形態"] = self._extract_employment(val)
            if key in ("稼働日数", "勤務日数") and val:
                item["稼働日数"] = val

        return item

    def _extract_employment(self, text: str) -> str:
        lines = [_clean(line) for line in re.split(r"[■\n]", text) if _clean(line)]
        for i, line in enumerate(lines):
            if line == "雇用形態" and i + 1 < len(lines):
                return lines[i + 1]
        for candidate in ("正社員", "契約社員", "業務委託", "アルバイト", "パート"):
            if candidate in text:
                return candidate
        return _clean(text)

    def _dedupe_key(self, item: dict) -> str:
        return "|".join([
            item.get(Schema.NAME, ""),
            item.get("求人タイトル", ""),
            item.get("エリア", ""),
            item.get("雇用形態", ""),
            item.get("給与", ""),
        ])


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    ShokuninBaseScraper().execute(START_URL)
