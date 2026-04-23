"""
BIZTEL — クラウド型コールセンターシステム「BIZTEL」の導入事例クローラー

取得対象:
    - https://biztel.jp/case/ に掲載されている全導入事例 (約116件)
    - 各事例ページ (/case/cs/{id}/, /case/shouin/{id}/) から
      企業名・事業内容・公式サイト URL・利用サービス・業種・企業規模・課題タグを収集

取得フロー:
    1. /case/ 一覧ページから事例詳細 URL を抽出
       (div.caseGroup.origin div.item を使い view 側の重複を排除)
    2. 各詳細ページを取得し、table の 社名 / 事業内容 / URL と
       a[href*="/case/?filter="] のタグから情報を抽出

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/biztel.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id biztel
"""

import logging
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

logger = logging.getLogger(__name__)

BASE_URL = "https://biztel.jp"
LISTING_PATH = "/case/"

_NAME_SUFFIX_RE = re.compile(r"さま$")


def _clean(text) -> str:
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


class Biztel(StaticCrawler):
    """BIZTEL 導入事例スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "事例タイトル",
        "利用サービス",
        "業種",
        "企業規模",
        "課題タグ",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        listing_url = urljoin(BASE_URL, LISTING_PATH)
        soup = self.get_soup(listing_url)
        if soup is None:
            logger.error("一覧ページを取得できませんでした: %s", listing_url)
            return

        detail_urls: list[str] = []
        seen: set[str] = set()
        # caseGroup.origin だけを見て view 側の重複を排除
        for a in soup.select(
            'div.caseGroup.origin div.item a[href*="/case/cs/"], '
            'div.caseGroup.origin div.item a[href*="/case/shouin/"]'
        ):
            href = a.get("href")
            if not href:
                continue
            full = urljoin(BASE_URL, href)
            if full in seen:
                continue
            seen.add(full)
            detail_urls.append(full)

        # フォールバック: origin が無い場合は通常のセレクタで重複除外
        if not detail_urls:
            for a in soup.select(
                'div.item a[href*="/case/cs/"], div.item a[href*="/case/shouin/"]'
            ):
                href = a.get("href")
                if not href:
                    continue
                full = urljoin(BASE_URL, href)
                if full in seen:
                    continue
                seen.add(full)
                detail_urls.append(full)

        self.total_items = len(detail_urls)
        logger.info("事例 URL を %d 件検出しました", self.total_items)

        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as e:
                logger.warning("詳細ページの解析に失敗: %s — %s", detail_url, e)
                continue

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        # 企業名: h1 から「さま」を除去
        h1 = soup.find("h1")
        name = _NAME_SUFFIX_RE.sub("", _clean(h1.get_text() if h1 else ""))

        # 事例タイトル: 最初の h2
        h2 = soup.find("h2")
        title = _clean(h2.get_text() if h2 else "")

        # 会社情報テーブル (社名 / 事業内容 / URL)
        company_name = ""
        lob = ""
        hp = ""
        table = soup.find("table")
        if table:
            for tr in table.find_all("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                key = _clean(th.get_text())
                if key == "社名":
                    company_name = _clean(td.get_text())
                elif key == "事業内容":
                    # 事業内容は改行を含むので空白圧縮ではなく改行→ / に変換
                    lob_raw = td.get_text("\n").strip()
                    lob = re.sub(r"\s*\n+\s*", " / ", lob_raw)
                elif key == "URL":
                    a = td.find("a")
                    hp = (a.get("href") if a and a.get("href") else _clean(td.get_text()))

        # 社名がテーブルから取れた場合はそちらを優先 (h1 に「さま」以外の装飾が混ざるケース対策)
        if company_name:
            name = company_name

        # タグ (利用サービス / 業種 / 企業規模 / 課題) を filter プレフィックスで分類
        services: list[str] = []
        industries: list[str] = []
        sizes: list[str] = []
        tasks: list[str] = []
        for a in soup.select('a[href*="/case/?filter="]'):
            href = a.get("href", "")
            text = _clean(a.get_text())
            if not text:
                continue
            m = re.search(r"filter=([a-z]+)_", href)
            if not m:
                continue
            kind = m.group(1)
            if kind == "service":
                services.append(text)
            elif kind == "type":
                industries.append(text)
            elif kind == "size":
                # "企業規模：100〜499名" → "100〜499名"
                sizes.append(re.sub(r"^企業規模[:：]\s*", "", text))
            elif kind == "task":
                tasks.append(text)

        # 重複除去 (順序維持)
        def _uniq(seq: list[str]) -> list[str]:
            seen: set[str] = set()
            out: list[str] = []
            for s in seq:
                if s not in seen:
                    seen.add(s)
                    out.append(s)
            return out

        services = _uniq(services)
        industries = _uniq(industries)
        sizes = _uniq(sizes)
        tasks = _uniq(tasks)

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.LOB: lob,
            Schema.HP: hp,
            "事例タイトル": title,
            "利用サービス": ", ".join(services),
            "業種": ", ".join(industries),
            "企業規模": ", ".join(sizes),
            "課題タグ": ", ".join(tasks),
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Biztel()
    scraper.execute(urljoin(BASE_URL, LISTING_PATH))

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
