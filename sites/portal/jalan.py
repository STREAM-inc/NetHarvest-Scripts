"""
version 1.0.0 niwai パフォーマンス改善

じゃらんnet — 宿泊施設情報 全件回収スクレイパー

取得対象:
    - 宿名、都道府県、住所、エリア、キャッチフレーズ、最安値、評価スコア、
      口コミ件数、高評価項目、チェックイン、チェックアウト、総部屋数、施設内容

取得フロー:
    yado.html (ハブ) → WID_XX.HTML (地方別) → LRG_XXXXXX/ (エリア別一覧, pageN.html)
    → エリアごとに詳細URLをリスト化 → 詳細ページ取得

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
from urllib.parse import urlparse, urlunparse

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
_SELECT_PAGE_RE = re.compile(r"selectPage\([^,]+,\s*'(\d+)'\)")

# クチコミ評価（詳細ページ下部）: 抽出カラム → 正規表現
# 値は N.N（1桁小数）または未評価の「-」
_REVIEW_SCORE_RE = {
    "総合評価": re.compile(r"総合\s*([0-9]\.[0-9]|-)"),
    "部屋評価": re.compile(r"部屋\s*([0-9]\.[0-9]|-)"),
    "風呂評価": re.compile(r"風呂\s*([0-9]\.[0-9]|-)"),
    "朝食評価": re.compile(r"料理（朝食）\s*([0-9]\.[0-9]|-)"),
    "夕食評価": re.compile(r"料理（夕食）\s*([0-9]\.[0-9]|-)"),
    "接客評価": re.compile(r"接客・サービス\s*([0-9]\.[0-9]|-)"),
    "清潔感評価": re.compile(r"清潔感\s*([0-9]\.[0-9]|-)"),
}


def _abs(href: str) -> str:
    return href if href.startswith("http") else BASE_URL + href


def _normalize_lrg_url(url: str) -> str:
    """LRG一覧URLをパスのみに正規化する（不要なクエリ・pageN.html を除去）。"""
    parsed = urlparse(url)
    path = parsed.path
    path = re.sub(r"page\d+\.html$", "", path, flags=re.I)
    if not path.endswith("/"):
        path += "/"
    return urlunparse((parsed.scheme or "https", parsed.netloc, path, "", "", ""))


def _listing_page_url(base_url: str, page_num: int) -> str:
    """一覧ページURL（サイトの getPageUrlHtmlStr に準拠）。

    1ページ目: LRG_xxxxxx/ のみ
    2ページ目以降: LRG_xxxxxx/page{N}.html
    """
    if page_num <= 1:
        return base_url
    return f"{base_url}page{page_num}.html"


def _get_max_listing_page(soup) -> int:
    """ページャの selectPage から最大ページ番号を取得する。"""
    max_page = 1
    for a in soup.select("a.page[onclick], a.next[onclick], a.last[onclick]"):
        m = _SELECT_PAGE_RE.search(a.get("onclick", ""))
        if m:
            max_page = max(max_page, int(m.group(1)))
    return max_page


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class JalanScraper(StaticCrawler):
    """じゃらんnet 宿泊施設スクレイパー"""

    DELAY = 1.5
    SAMPLE_AREAS_FOR_ESTIMATE = 3  # total_items 予測用のサンプルエリア数
    EXTRA_COLUMNS = [
        "エリア", "キャッチフレーズ", "最安値",
        "評価スコア", "口コミ件数", "高評価項目",
        "チェックイン", "チェックアウト", "総部屋数", "施設内容",
        # 部屋・部屋施設 / アメニティ・施設・サービス
        "部屋補足", "部屋設備", "インターネット", "温泉",
        "サービス&レジャー", "クレジットカード", "補足",
        # クチコミ評価（項目別スコア）
        "総合評価", "部屋評価", "風呂評価", "朝食評価",
        "夕食評価", "接客評価", "清潔感評価",
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

        lrg_urls: list[str] = []
        for wid_url in wid_urls:
            self.logger.info("=== WID: %s ===", wid_url)
            lrg_urls.extend(self._collect_lrg_urls(wid_url))
        lrg_urls = list(dict.fromkeys(lrg_urls))
        self.logger.info("LRGエリア数: %d", len(lrg_urls))

        sample_counts: list[int] = []
        for lrg_url in lrg_urls:
            self.logger.info("  LRG: %s", lrg_url)
            entries = self._collect_lrg_entries(lrg_url, seen)
            self.logger.info("    詳細URL数: %d", len(entries))

            if len(sample_counts) < self.SAMPLE_AREAS_FOR_ESTIMATE:
                sample_counts.append(len(entries))
                if len(sample_counts) >= min(
                    self.SAMPLE_AREAS_FOR_ESTIMATE, len(lrg_urls)
                ):
                    avg = sum(sample_counts) / len(sample_counts)
                    self.total_items = int(avg * len(lrg_urls))
                    self.logger.info(
                        "取得予測件数: %d (平均 %.1f × %d エリア)",
                        self.total_items,
                        avg,
                        len(lrg_urls),
                    )

            for detail_url, list_data in entries:
                try:
                    record = self._scrape_detail(detail_url, list_data)
                    if record:
                        yield record
                except Exception as e:
                    self.logger.warning("詳細ページエラー (%s): %s", detail_url, e)

                time.sleep(self.DELAY)

            time.sleep(self.DELAY)

    # ------------------------------------------------------------------
    # WIDページ → LRGリンク収集
    # ------------------------------------------------------------------

    def _collect_lrg_urls(self, wid_url: str) -> list[str]:
        soup = self.get_soup(wid_url)
        if soup is None:
            return []

        return list({
            _normalize_lrg_url(_abs(a["href"]))
            for a in soup.find_all("a", href=_LRG_RE)
        })

    # ------------------------------------------------------------------
    # LRG一覧ページ（ページネーション）→ 詳細URLリスト化
    # ------------------------------------------------------------------

    def _collect_lrg_entries(
        self, lrg_url: str, seen: set[str]
    ) -> list[tuple[str, dict]]:
        """エリアの全一覧ページから (詳細URL, 一覧データ) のリストを構築する。"""
        entries: list[tuple[str, dict]] = []
        for soup, page_num, current_url in self._iter_listing_pages(lrg_url):
            items = soup.select(".p-yadoCassette.js-searchResultItem")
            if not items:
                self.logger.warning("一覧0件 (page=%d): %s", page_num, current_url)
                break

            for item in items:
                detail_url = self._extract_detail_url(item)
                if not detail_url or detail_url in seen:
                    continue
                seen.add(detail_url)
                entries.append((detail_url, self._parse_list_item(item)))

        return entries

    def _iter_listing_pages(self, lrg_url: str):
        """LRG一覧の各ページ (soup, page_num, url) を順に返す。"""
        base_url = _normalize_lrg_url(lrg_url)
        soup = self.get_soup(base_url)
        if soup is None:
            return

        max_page = _get_max_listing_page(soup)
        self.logger.info("    一覧ページ数: %d", max_page)

        for page_num in range(1, max_page + 1):
            current_url = _listing_page_url(base_url, page_num)
            if page_num > 1:
                soup = self.get_soup(current_url)
                if soup is None:
                    return
            yield soup, page_num, current_url

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
        room_note = ""
        room_equip = ""
        internet = ""
        onsen = ""
        service = ""
        credit = ""
        note = ""

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
            elif label == "部屋補足":
                room_note = value
            elif "標準的な部屋設備" in label:
                room_equip = value
            elif "インターネット" in label:
                internet = value
            elif label == "温泉":
                onsen = value
            elif "サービス" in label and "レジャー" in label:
                service = value
            elif "クレジットカード" in label:
                credit = value
            elif label == "補足":
                note = value

        # 総部屋数 は洋室/和室/… と並ぶ横並びテーブルの「列見出し」で、
        # 値は見出し行の次行の同じ列位置(td)に入る。th/td 同一行では拾えない。
        total_rooms = self._extract_total_rooms(soup)

        pref = ""
        addr = addr_full
        m = _PREF_PATTERN.match(addr_full)
        if m:
            pref = m.group(1)
            addr = addr_full[m.end():].strip()

        record = {
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
            "部屋補足": room_note,
            "部屋設備": room_equip,
            "インターネット": internet,
            "温泉": onsen,
            "サービス&レジャー": service,
            "クレジットカード": credit,
            "補足": note,
        }
        record.update(self._parse_reviews(soup))
        return record

    def _extract_total_rooms(self, soup) -> str:
        """横並びテーブル(洋室/和室/和洋室/その他/総部屋数)から総部屋数を抽出する。

        「総部屋数」は見出し行の th、値は次行の同じ列位置の td にある。
        """
        for th in soup.find_all("th"):
            if _clean(th.get_text()) != "総部屋数":
                continue
            header_tr = th.find_parent("tr")
            if header_tr is None:
                continue
            headers = header_tr.find_all("th", recursive=False)
            if th not in headers:
                headers = header_tr.find_all("th")
            try:
                idx = headers.index(th)
            except ValueError:
                continue
            value_tr = header_tr.find_next_sibling("tr")
            if value_tr is None:
                continue
            tds = value_tr.find_all("td", recursive=False)
            if not tds:
                tds = value_tr.find_all("td")
            if idx < len(tds):
                value = _clean(tds[idx].get_text(" "))
                if value:
                    return value
        return ""

    def _parse_reviews(self, soup) -> dict:
        """詳細ページ下部のクチコミ評価（項目別スコア）を抽出する。"""
        text = _clean(soup.get_text(" "))
        return {
            col: (m.group(1) if (m := pat.search(text)) else "")
            for col, pat in _REVIEW_SCORE_RE.items()
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = JalanScraper()
    scraper.execute("https://www.jalan.net/yado.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
