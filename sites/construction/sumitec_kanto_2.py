"""
リンクスアシスト (東日本リフォーム情報サイト / sumitec-kanto.com) — 施工店一覧クローラー

取得対象:
    - サイト内検索一覧 (/page/{n}?s) に掲載される全施工店の会社概要

取得フロー:
    1. 検索一覧ページ (/page/{n}?s) を 1 ページずつ巡回
    2. 各ページから施工店詳細ページの URL (/contractor/.../{id}) を抽出
    3. 詳細ページを 1 件取得するごとに即 yield (Pattern B)
       ※全件収集してから一括 yield はしない (テスト実行のタイムアウト回避)

備考:
    - 一覧アイテムには店舗紹介の自由記述 (プロース) があるが、著作権リスクのため取得しない。
    - フィルター指示は無いため全件取得する。

実行方法:
    # ローカルテスト
    python scripts/sites/construction/sumitec_kanto_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id sumitec_kanto_2
"""

import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

root_path = Path(__file__).resolve().parent.parent.parent.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


class SumitecKanto2Scraper(StaticCrawler):
    """リンクスアシスト (sumitec-kanto) 検索一覧 → 施工店詳細クローラー"""

    DELAY = 1.5
    MAX_LIST_PAGES = 300
    EXTRA_COLUMNS = []  # 一覧の店舗紹介は自由記述プロースのため取得しない

    def parse(self, url: str) -> Generator[dict, None, None]:
        # sites.yml の url (= /page/1?s) を唯一のルートとして使う。
        # ページ番号だけを差し替えて後続ページの URL を派生させる (?s クエリは保持)。
        max_list_pages = int(os.getenv("NH_MAX_LIST_PAGES", "0")) or self.MAX_LIST_PAGES
        max_details = int(os.getenv("NH_MAX_DETAILS", "0"))

        self.logger.info("一覧ページのクロールを開始します (上限: %d ページ)", max_list_pages)
        seen_urls: set[str] = set()
        yielded = 0

        for page in range(1, max_list_pages + 1):
            page_url = url if page == 1 else self._page_url(url, page)
            self.logger.info("[page %d] 一覧取得: %s", page, page_url)
            soup = self.get_soup(page_url)
            if soup is None:
                self.logger.info("一覧ページが存在しないため終了します: page=%d", page)
                break

            detail_urls: list[str] = []
            for link in soup.select("a[href]"):
                href = (link.get("href") or "").strip()
                if not href:
                    continue
                detail_url = urljoin(page_url, href)
                if not self._is_detail_url(detail_url):
                    continue
                if detail_url in seen_urls:
                    continue
                seen_urls.add(detail_url)
                detail_urls.append(detail_url)

            if not detail_urls:
                self.logger.info("詳細リンクが 0 件のため終了します: page=%d", page)
                break

            for detail_url in detail_urls:
                if max_details and yielded >= max_details:
                    self.logger.info("NH_MAX_DETAILS=%d に到達したため終了します", max_details)
                    return
                time.sleep(self.DELAY)
                item = self._scrape_detail(detail_url)
                if item:
                    yielded += 1
                    self.total_items = yielded
                    yield item

        self.logger.info("取得完了: 合計 %d 件", yielded)

    def _scrape_detail(self, detail_url: str) -> dict | None:
        self.logger.info("詳細ページ取得: %s", detail_url)
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        name_el = soup.select_one("header.m_sec_h h2.h.mainTxt")
        name_text = name_el.get_text(" ", strip=True) if name_el else ""
        name_main, name_kana = self._split_name_and_kana(name_text)

        row_map: dict[str, str] = {}
        for row in soup.select("table.p_table.c_tableBlue tr"):
            th = row.select_one("th")
            td = row.select_one("td")
            if not th or not td:
                continue
            label = th.get_text(" ", strip=True)
            value = td.get_text(" ", strip=True)
            if label == "ホームページ":
                link = td.select_one("a[href]")
                if link and (link.get("href") or "").strip():
                    value = (link.get("href") or "").strip()
            row_map[label] = value

        address = row_map.get("住所", "")
        pref, addr_rest = self._split_pref(address)

        tags = soup.select("div.tags ul li a")
        lob = ",".join(a.get_text(strip=True) for a in tags)

        return {
            Schema.URL: detail_url,
            Schema.NAME: name_main,
            Schema.NAME_KANA: name_kana,
            Schema.PREF: pref,
            Schema.ADDR: address,
            Schema.TEL: row_map.get("電話番号", ""),
            Schema.HP: row_map.get("ホームページ", ""),
            Schema.TIME: row_map.get("営業時間", ""),
            Schema.HOLIDAY: row_map.get("定休日", ""),
            Schema.LOB: lob,
        }

    def _page_url(self, url: str, page: int) -> str:
        """引数 url のページ番号だけを差し替える (/page/N を維持しつつ ?s も保持)。"""
        new_url, n = re.subn(r"/page/\d+", f"/page/{page}", url, count=1)
        if n:
            return new_url
        # url に /page/N が無い場合のフォールバック (SSOT を尊重しつつ末尾に付与)
        base = url.split("?", 1)[0].rstrip("/")
        query = url[len(url.split("?", 1)[0]):]
        return f"{base}/page/{page}{query}"

    def _split_pref(self, address: str) -> tuple[str, str]:
        text = (address or "").strip()
        if not text:
            return "", ""
        m = _PREF_PATTERN.match(text)
        if m:
            return m.group(1), text[m.end():].strip()
        return "", text

    def _split_name_and_kana(self, raw_name: str) -> tuple[str, str]:
        text = re.sub(r"\s+", " ", (raw_name or "").replace("　", " ")).strip()
        if not text:
            return "", ""

        paren_match = re.search(r"[\(（]([ァ-ヶー・･\s]+)[\)）]$", text)
        if paren_match:
            kana = re.sub(r"\s+", "", paren_match.group(1)).strip()
            name = re.sub(r"[\(（][ァ-ヶー・･\s]+[\)）]$", "", text).strip()
            return name or text, kana

        tokens = text.split(" ")
        kana_tokens: list[str] = []
        while tokens and re.fullmatch(r"[ァ-ヶー・･]+", tokens[-1]):
            kana_tokens.insert(0, tokens.pop())

        if kana_tokens:
            return " ".join(tokens).strip(), "".join(kana_tokens).strip()

        return text, ""

    def _is_detail_url(self, target_url: str) -> bool:
        path = target_url.split("?", 1)[0]
        if "/contractor/page/" in path:
            return False
        return re.search(r"/contractor/.+/\d+/?$", path) is not None


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = SumitecKanto2Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://sumitec-kanto.com/page/1?s")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
