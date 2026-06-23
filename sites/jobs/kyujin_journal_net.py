"""
求人ジャーナルネット (job-j.net) スクレイパー

取得対象:
    - 全国の求人一覧 (https://www.job-j.net/zenkoku/search/) を巡回し、各求人カード
      および詳細ページから求人情報を取得する。

取得フロー:
    1. ルート URL (https://www.job-j.net) から一覧ページ (/zenkoku/search/?page=N) を
       派生させ、順に巡回する。
    2. 各カード (div.c-job__item) から 会社名・給与・勤務地・詳細ページ URL を取得。
    3. 詳細ページ (/{pref}/job/J{id}/) の contentbox から 仕事内容・給与・勤務地・
       最寄駅 を補完する。
    4. 1 件取得するごとに即 yield (途中中断に強い)。

実行方法:
    python scripts/sites/jobs/kyujin_journal_net.py
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

# scripts/sites/jobs/<file>.py → root は .parent x4
_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

# 一覧パス (ルート URL からの相対)
_LIST_PATH = "/zenkoku/search/"


def _clean(value) -> str:
    """空白・改行・全角スペースを 1 つの半角スペースに畳む。"""
    if value is None:
        return ""
    if not isinstance(value, str):
        value = value.get_text(" ") if hasattr(value, "get_text") else str(value)
    return re.sub(r"\s+", " ", value.replace("　", " ")).strip()


class KyujinJournalNetScraper(DynamicCrawler):
    """求人ジャーナルネット (job-j.net) スクレイパー"""

    DELAY = 1.5
    CONTINUE_ON_ERROR = True
    EXTRA_COLUMNS = ["仕事内容", "給与", "勤務地", "最寄駅"]

    # 詳細ページ contentbox ラベル → 出力先カラム
    _DETAIL_LABEL_MAP = {
        "仕事内容": "仕事内容",
        "給与": "給与",
        "勤務地": "勤務地",
        "勤務先": "勤務地",
        "最寄駅": "最寄駅",
        "アクセス": "最寄駅",
    }

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url を唯一のルート(起点)とし、一覧/ページネーション/詳細 URL は
        # すべて url から派生させる。
        list_root = urljoin(url.rstrip("/") + "/", _LIST_PATH.lstrip("/"))

        page = 1
        seen: set[str] = set()
        while True:
            list_url = f"{list_root}?page={page}"
            soup = self.get_soup(list_url, wait_until="networkidle")
            if soup is None:
                break

            if page == 1:
                self.total_items = self._extract_total(soup)

            cards = soup.select("div.c-job__item")
            if not cards:
                break

            for card in cards:
                try:
                    item = self._parse_card(card, list_root)
                except Exception as e:
                    self.logger.warning("カード解析失敗: %s", e)
                    continue

                detail_url = item.get(Schema.URL)
                if not detail_url or detail_url in seen:
                    continue
                seen.add(detail_url)

                # 詳細ページ取得は失敗しても一覧由来情報だけで yield して継続。
                try:
                    item.update(self._scrape_detail(detail_url))
                except Exception as e:
                    self.logger.warning("詳細取得失敗 %s: %s", detail_url, e)

                if item.get(Schema.NAME):
                    yield item

            page += 1

    # ------------------------------------------------------------------ #
    # 一覧カード
    # ------------------------------------------------------------------ #
    def _parse_card(self, card, list_root: str) -> dict:
        data: dict = {}

        link = card.select_one("a.c-job__catchcopy-link[href]")
        if link:
            data[Schema.URL] = urljoin(list_root, link["href"])

        name_el = card.select_one(".c-heading-list__title-text, .js-heading-list-inner")
        if name_el:
            name = re.sub(r"の求人情報\s*$", "", _clean(name_el.get_text()))
            if name:
                data[Schema.NAME] = name

        salary = card.select_one(".c-job__salary-detail")
        if salary:
            data["給与"] = _clean(salary.get_text())

        # 勤務地ブロック (detail-title が「勤務地」のもの)
        for detail in card.select(".c-job__detail"):
            title_el = detail.select_one(".c-job__detail-title")
            if title_el and _clean(title_el.get_text()) == "勤務地":
                loc = " ".join(
                    _clean(p.get_text()) for p in detail.select(".c-job__detail-text")
                ).strip()
                if loc:
                    data["勤務地"] = loc
                break

        return data

    # ------------------------------------------------------------------ #
    # 詳細ページ
    # ------------------------------------------------------------------ #
    def _scrape_detail(self, detail_url: str) -> dict:
        soup = self.get_soup(detail_url, wait_until="networkidle")
        if soup is None:
            return {}

        data: dict = {Schema.URL: detail_url}

        # H1 の括弧内から法人名を抽出 (例: "店舗名 (株式会社○○)/...の求人情報")
        h1 = soup.select_one("h1")
        if h1:
            m = re.search(r"[（(]([^（）()]+)[）)]", _clean(h1.get_text()))
            if m:
                data[Schema.NAME] = _clean(m.group(1))

        for box in soup.select(".c-contentbox__item"):
            title_el = box.select_one(".c-contentbox__item-title")
            content_el = box.select_one(".c-contentbox__item-content")
            if not title_el or not content_el:
                continue
            label = _clean(title_el.get_text())
            field = self._DETAIL_LABEL_MAP.get(label)
            if field is None:
                continue
            value = _clean(content_el.get_text(" "))
            if value:
                data[field] = value

        return data

    # ------------------------------------------------------------------ #
    # ヘルパ
    # ------------------------------------------------------------------ #
    def _extract_total(self, soup) -> int | None:
        el = soup.select_one('[class*="result__num-text"]')
        if el:
            digits = re.sub(r"[^\d]", "", el.get_text())
            if digits:
                return int(digits)
        return None


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = KyujinJournalNetScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.job-j.net")
