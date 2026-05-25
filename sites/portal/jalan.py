"""
じゃらんnet — 宿泊施設情報 全件回収スクレイパー

取得対象:
    - 宿名、都道府県、住所、エリア、キャッチフレーズ、最安値、評価スコア、
      口コミ件数、高評価項目、チェックイン、チェックアウト、総部屋数、施設内容

取得フロー:
    yado.html (ハブ) → WID_XX.HTML (地方別) → LRG_XXXXXX/ (エリア別一覧, ?p=N)
    → 各宿の詳細ページ

実行方法:
    # ローカルテスト
    python scripts/sites/portal/jalan.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id jalan
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

BASE_URL = "https://www.jalan.net"
HUB_URL = f"{BASE_URL}/yado.html"

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|"
    r"三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_WID_RE = re.compile(r"/WID_\d+\.HTML", re.I)
_LRG_RE = re.compile(r"/\d{6}/LRG_\d+/")
_YAD_RE = re.compile(r"/yad\d+/")
_YAD_AD_RE = re.compile(r"doYadDetail(?:Ad)?\('(\d+)'")


def _abs(href: str) -> str:
    return href if href.startswith("http") else BASE_URL + href


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class JalanScraper(StaticCrawler):
    """じゃらんnet 宿泊施設スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "エリア", "キャッチフレーズ", "最安値",
        "評価スコア", "口コミ件数", "高評価項目",
        "チェックイン", "チェックアウト", "総部屋数", "施設内容",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)
        if soup is None:
            return

        seen: set[str] = set()

        wid_urls = list({
            _abs(a["href"])
            for a in soup.find_all("a", href=_WID_RE)
        })
        self.logger.info("WIDページ数: %d", len(wid_urls))

        for wid_url in wid_urls:
            self.logger.info("=== WID: %s ===", wid_url)
            yield from self._parse_wid(wid_url, seen)

    # ------------------------------------------------------------------
    # WIDページ → LRGリンク収集
    # ------------------------------------------------------------------

    def _parse_wid(self, wid_url: str, seen: set[str]) -> Generator[dict, None, None]:
        soup = self.get_soup(wid_url)
        if soup is None:
            return

        lrg_urls = list({
            _abs(a["href"])
            for a in soup.find_all("a", href=_LRG_RE)
        })
        self.logger.info("  LRGエリア数: %d", len(lrg_urls))

        for lrg_url in lrg_urls:
            self.logger.info("  LRG: %s", lrg_url)
            yield from self._parse_lrg_listing(lrg_url, seen)
            time.sleep(self.DELAY)

    # ------------------------------------------------------------------
    # LRG一覧ページ（ページネーション）
    # ------------------------------------------------------------------

    def _parse_lrg_listing(self, lrg_url: str, seen: set[str]) -> Generator[dict, None, None]:
        page = 1

        while True:
            current_url = lrg_url if page == 1 else f"{lrg_url}?p={page}"
            soup = self.get_soup(current_url)
            if soup is None:
                break

            items = soup.select(".p-yadoCassette.js-searchResultItem")
            if not items:
                break

            for item in items:
                detail_url = self._extract_detail_url(item)
                if not detail_url or detail_url in seen:
                    continue
                seen.add(detail_url)

                list_data = self._parse_list_item(item)

                try:
                    record = self._scrape_detail(detail_url, list_data)
                    if record:
                        yield record
                except Exception as e:
                    self.logger.warning("詳細ページエラー (%s): %s", detail_url, e)

                time.sleep(self.DELAY)

            page += 1

    # ------------------------------------------------------------------
    # 詳細URLの抽出
    # ------------------------------------------------------------------

    def _extract_detail_url(self, item) -> str | None:
        a = item.select_one("a.jlnpc-yadoCassette__link")
        if a is None:
            return None
        href = a.get("href", "")

        if _YAD_RE.search(href):
            return _abs(href)

        # PRホテル: javascript:doYadDetailAd('336161',...)
        m = _YAD_AD_RE.search(href)
        if m:
            return f"{BASE_URL}/yad{m.group(1)}/"

        return None

    # ------------------------------------------------------------------
    # 一覧カードのパース
    # ------------------------------------------------------------------

    def _parse_list_item(self, item) -> dict:
        def _text(sel):
            el = item.select_one(sel)
            return _clean(el.get_text()) if el else ""

        review_raw = _text(".p-searchResultItem__summarykuchikomi__totalNumber")
        review_count = review_raw.replace("件", "").strip()

        return {
            Schema.NAME: _text(".p-searchResultItem__facilityName"),
            "エリア": _text(".p-searchResultItem__areaValues"),
            "キャッチフレーズ": _text(".p-searchResultItem__catchPhrase"),
            "最安値": _text(".p-searchResultItem__lowestPriceValue"),
            "評価スコア": _text(".p-searchResultItem__summaryaverage-num"),
            "口コミ件数": review_count,
            "高評価項目": _text(".p-searchResultItem__highlyRated strong"),
        }

    # ------------------------------------------------------------------
    # 詳細ページのスクレイピング
    # ------------------------------------------------------------------

    def _scrape_detail(self, url: str, list_data: dict) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        name = list_data.get(Schema.NAME, "")
        if not name:
            return None

        checkin = ""
        checkout = ""
        total_rooms = ""
        facility = ""
        addr_full = ""

        for tr in soup.select("table tr"):
            th = tr.select_one("th")
            td = tr.select_one("td")
            if not th or not td:
                continue
            label = _clean(th.get_text())
            value = _clean(td.get_text(" "))

            if label == "住所":
                addr_full = value
            elif "チェックイン時間" in label:
                checkin = value
            elif "チェックアウト時間" in label:
                checkout = value
            elif label == "施設内容":
                facility = value
            elif label == "総部屋数":
                total_rooms = value

        pref = ""
        addr = addr_full
        m = _PREF_PATTERN.match(addr_full)
        if m:
            pref = m.group(1)
            addr = addr_full[m.end():].strip()

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            "エリア": list_data.get("エリア", ""),
            "キャッチフレーズ": list_data.get("キャッチフレーズ", ""),
            "最安値": list_data.get("最安値", ""),
            "評価スコア": list_data.get("評価スコア", ""),
            "口コミ件数": list_data.get("口コミ件数", ""),
            "高評価項目": list_data.get("高評価項目", ""),
            "チェックイン": checkin,
            "チェックアウト": checkout,
            "総部屋数": total_rooms,
            "施設内容": facility,
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = JalanScraper()
    scraper.execute(HUB_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
