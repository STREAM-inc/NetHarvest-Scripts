# scripts/sites/portal/dartslive.py
"""
DARTSLIVE SEARCH — ダーツライブ設置店舗スクレイパー

取得対象:
    - 全国のDARTSLIVE設置店舗 (約3,809件 / 191ページ)

取得フロー:
    一覧ページ (?page=N, 最大191ページ) → 各店舗の詳細ページ (静的HTML) + 基本情報API

実行方法:
    # ローカルテスト (1ページ分のみ)
    python scripts/sites/portal/dartslive.py

    # Prefect Flow 経由 (全件)
    python bin/run_flow.py --site-id dartslive
"""

import re
import sys
import time
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://search.dartslive.com"
API_BASE = "https://search.dartslive.com/shop/shop-basicdata/"

_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_DOW = {1: "日", 2: "月", 3: "火", 4: "水", 5: "木", 6: "金", 7: "土", 10: "祝"}


class DartsliveScraper(StaticCrawler):
    """DARTSLIVE ダーツバー・ダーツ場店舗スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["最寄り駅", "料金帯", "設備情報"]
    _test_max_pages: int | None = None  # テスト時にオーバーライド

    def parse(self, url: str):
        base = re.sub(r"[?&]page=\d+", "", url)
        page = 1
        seen: set[str] = set()
        max_page: int | None = None

        while True:
            list_url = f"{base}?page={page}"
            soup = self.get_soup(list_url)

            if page == 1:
                max_page = self._init_pagination(soup)

            page_links: list[str] = []
            for a in soup.select('a[href^="/jp/shop/"]'):
                href = a["href"]
                if href not in seen:
                    seen.add(href)
                    page_links.append(href)

            if not page_links:
                self.logger.info("ページ %d: リンクなし、終了", page)
                break

            for href in page_links:
                detail_url = f"{BASE_URL}{href}"
                try:
                    item = self._scrape_detail(detail_url)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.warning("詳細ページ取得失敗: %s (%s)", detail_url, e)

            if self._test_max_pages and page >= self._test_max_pages:
                self.logger.info("テスト上限ページ %d に到達", self._test_max_pages)
                break
            if max_page and page >= max_page:
                self.logger.info("最終ページ %d に到達", max_page)
                break
            page += 1

    def _init_pagination(self, soup) -> int:
        """初回ページで総件数・最終ページ数を初期化する"""
        text = soup.get_text(" ", strip=True)
        m = re.search(r"検索結果\s*[\d,]+-[\d,]+件目/\s*([\d,]+)件", text)
        if m:
            self.total_items = int(m.group(1).replace(",", ""))
            self.logger.info("総件数: %d 件", self.total_items)

        last = 0
        for a in soup.select('a[href^="?page="]'):
            pm = re.search(r"page=(\d+)", a.get("href", ""))
            if pm:
                last = max(last, int(pm.group(1)))
        if last:
            self.logger.info("最終ページ: %d", last)
            return last
        return 191

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)

        # --- 店舗名 ---
        name_el = soup.select_one("h2.cls-safe-title span")
        if not name_el:
            return None
        name = name_el.get_text(strip=True)
        if not name:
            return None

        item: dict = {Schema.URL: url, Schema.NAME: name}

        # --- 住所 (都道府県を分離) ---
        addr_el = soup.select_one("p.address")
        if addr_el:
            full = addr_el.get_text(strip=True)
            m = _PREF_RE.match(full)
            if m:
                item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = full[m.end():].strip()
            else:
                item[Schema.ADDR] = full

        # --- 電話番号 ---
        tel_el = soup.select_one("p.shop_telPhone")
        if tel_el:
            item[Schema.TEL] = tel_el.get_text(strip=True)

        # --- 営業時間 (サーバーサイドレンダリング済) ---
        hours = soup.select("ol#business-open-days li")
        if hours:
            item[Schema.TIME] = " / ".join(li.get_text(strip=True) for li in hours)

        # --- basicinfo-tbody テーブル行をパース ---
        for tr in soup.select("tbody.basicinfo-tbody tr"):
            th = tr.select_one("th h4")
            td = tr.select_one("td")
            if not th or not td:
                continue
            label = th.get_text(strip=True)
            if label == "最寄り駅":
                p = td.select_one("p")
                if p:
                    item["最寄り駅"] = p.get_text(strip=True)
            elif label == "料金帯":
                p = td.select_one("p")
                if p:
                    item["料金帯"] = p.get_text(strip=True)
            elif label == "設備情報":
                p = td.select_one("p")
                if p:
                    item["設備情報"] = re.sub(r"\s+", " ", p.get_text(strip=True))
            elif label == "店舗HP":
                a_el = td.select_one("a[href]")
                if a_el:
                    item[Schema.HP] = a_el["href"].strip()

        # --- 定休日・SNS (API補完) ---
        enc_id = url.rstrip("/").rsplit("/", 1)[-1]
        self._enrich_from_api(enc_id, item)

        return item

    def _enrich_from_api(self, enc_id: str, item: dict) -> None:
        """定休日・SNS情報をJSONAPIから補完する"""
        time.sleep(0.3)
        try:
            resp = self.session.get(
                API_BASE,
                params={"country_code": "jp", "shop_enc_id": enc_id},
                timeout=self.TIMEOUT,
            )
            resp.raise_for_status()
            shop = resp.json().get("data", {}).get("basicdata", {}).get("shop", {})

            # 定休日
            closed_info = shop.get("closedDay", {})
            closed = closed_info.get("closedDay", {})
            if closed.get("isOpen365"):
                item[Schema.HOLIDAY] = "年中無休"
            else:
                dow = closed.get("dayOfWeek")
                dom = closed.get("dayOfMonth")
                parts = []
                if dom:
                    parts.append(f"第{dom}週")
                if dow and dow in _DOW:
                    parts.append(f"{_DOW[dow]}曜日")
                if parts:
                    item[Schema.HOLIDAY] = "".join(parts)
            comment = closed_info.get("comment", "")
            if comment:
                base = item.get(Schema.HOLIDAY, "")
                item[Schema.HOLIDAY] = f"{base} {comment}".strip() if base else comment

            # SNS
            sns = shop.get("sns", {})
            if sns.get("twitterAccount"):
                item[Schema.X] = sns["twitterAccount"]
            if sns.get("InstagramAccount"):
                item[Schema.INSTA] = sns["InstagramAccount"]

        except Exception as e:
            self.logger.debug("API取得失敗 enc_id=%s: %s", enc_id, e)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = DartsliveScraper()
    scraper.execute("https://search.dartslive.com/jp/shops/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
