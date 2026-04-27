"""
kyozon — 法人向けクラウドサービス・SaaS・IT製品の比較・資料請求サイト

取得対象:
    kyozon.net に掲載された全サービス詳細ページ (/service/{id}/)

取得フロー:
    /service-list/ → 68 の課題カテゴリ (/issue/service_issue_N) を収集
      → 各カテゴリページから /service/{id}/ URL を収集 (グローバル重複排除)
      → 各サービス詳細ページを解析

実行方法:
    python scripts/sites/corporate/kyozon.py
    python bin/run_flow.py --site-id kyozon
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

BASE_URL = "https://kyozon.net"
START_URL = "https://kyozon.net/service-list/"

_PREF_PATTERN = re.compile(
    r"^\s*(北海道|東京都|(?:大阪|京都)府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|"
    r"新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|"
    r"和歌山|鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|"
    r"長崎|熊本|大分|宮崎|鹿児島|沖縄)県)"
)


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class KyozonScraper(DynamicCrawler):
    """kyozon.net SaaS 製品情報スクレイパー (Stealthモード対応)"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["運営会社", "設立", "サービス特徴", "キャッチコピー"]

    def parse(self, url: str):
        soup = self.get_soup(url)
        if soup is None:
            return

        issue_urls: list[str] = []
        seen_issue: set[str] = set()
        for a in soup.select('a[href*="/issue/service_issue_"]'):
            href = a.get("href", "")
            if not href:
                continue
            full = urljoin(BASE_URL, href).rstrip("/") + "/"
            if full not in seen_issue:
                seen_issue.add(full)
                issue_urls.append(full)
        self.logger.info("課題カテゴリ: %d 件", len(issue_urls))

        service_urls: list[str] = []
        seen_service: set[str] = set()
        for idx, issue_url in enumerate(issue_urls, 1):
            self.logger.info("[%d/%d] カテゴリ解析: %s", idx, len(issue_urls), issue_url)
            try:
                s = self.get_soup(issue_url)
            except Exception as e:
                self.logger.warning("課題ページ取得失敗 %s: %s", issue_url, e)
                continue
            if s is None:
                continue
            for a in s.select('a[href*="/service/"]'):
                href = a.get("href", "")
                m = re.search(r"/service/([a-f0-9]{16,})/?", href)
                if not m:
                    continue
                full = f"{BASE_URL}/service/{m.group(1)}/"
                if full not in seen_service:
                    seen_service.add(full)
                    service_urls.append(full)

        self.total_items = len(service_urls)
        self.logger.info("サービス詳細: %d 件", self.total_items)

        for detail_url in service_urls:
            try:
                item = self._scrape_detail(detail_url)
            except Exception as e:
                self.logger.warning("詳細取得失敗 %s: %s", detail_url, e)
                continue
            if item:
                yield item

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        h1 = soup.select_one("h1")
        if not h1:
            return None
        data[Schema.NAME] = _clean(h1.get_text())

        bc = [_clean(a.get_text()) for a in soup.select('[class*="breadcrumb"] a')]
        bc = [b for b in bc if b and b != "TOPページ"]
        if len(bc) >= 1:
            data[Schema.CAT_LV1] = bc[0]
        if len(bc) >= 2:
            data[Schema.CAT_LV2] = bc[1]
        if bc:
            data[Schema.CAT_SITE] = " / ".join(bc)

        for block in soup.select(".service_office_list .service_office_content"):
            k_el = block.select_one(".c-service-company__main-title")
            v_el = block.select_one(".c-service-company__main-text")
            if not k_el or not v_el:
                continue
            k = _clean(k_el.get_text())
            v = _clean(v_el.get_text())
            if k == "会社名":
                data["運営会社"] = v
            elif k == "所在地":
                addr = v
                m = _PREF_PATTERN.match(addr)
                if m:
                    data[Schema.PREF] = m.group(1)
                    data[Schema.ADDR] = addr[m.end():].strip()
                else:
                    data[Schema.ADDR] = addr
            elif k == "代表者名":
                data[Schema.REP_NM] = v
            elif k == "資本金":
                data[Schema.CAP] = v
            elif k == "設立年月日":
                data[Schema.OPEN_DATE] = v
                data["設立"] = v

        desc_el = soup.select_one("#toc-desc")
        if desc_el:
            text = _clean(desc_el.get_text(" "))
            text = re.sub(r"^サービス概要\s*", "", text)
            data[Schema.LOB] = text[:1000]

        lead_el = soup.select_one(".p-service-single-head__main-text, .p-service-single-head__lead")
        if lead_el:
            data["キャッチコピー"] = _clean(lead_el.get_text(" "))[:500]

        head = soup.select_one("#js-service-single-head, .p-service-single-head")
        if head:
            points = [_clean(e.get_text()) for e in head.select(".p-service-single-head__points-item-text")]
            points = [p for p in points if p]
            if points:
                data["サービス特徴"] = " / ".join(points)

        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = KyozonScraper()
    scraper.stealth = True  # 直接実行時も Patchright + 行動分析対策を有効化
    scraper.execute(START_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
