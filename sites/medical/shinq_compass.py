# scripts/sites/shinq_compass.py
"""
しんきゅうコンパス (shinq-compass.jp) — 鍼灸院スクレイパー

取得対象:
    - 詳細ページの 店舗名、店舗名カナ
    - 詳細ページの「店舗詳細情報」テーブル
        住所, TEL, 営業時間(曜日別), 定休日, HP, LINE,
        スタッフ数, 支払い方法, ジャンル, アクセス, こだわり
    - 一覧ページの TEL (div.tel > span)

取得フロー:
    一覧ページ (pagination, 全1703ページ) → 詳細ページリンク収集 → 各詳細ページからデータ取得

実行方法:
    # ローカルテスト（1ページ分のみ）
    python scripts/sites/shinq_compass.py

    # Prefect Flow 経由（全件）
    python bin/run_flow.py --site-id shinq_compass
"""

import re
import sys
import time
import urllib.parse
from datetime import datetime
from pathlib import Path

# プロジェクトルートを sys.path に追加（ローカル実行用）
root_path = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(root_path))

from bs4 import BeautifulSoup
from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

# 曜日マッピング: HTML の "月："→ Schema 定数
_DAY_MAP = {
    "月": Schema.TIME_MON,
    "火": Schema.TIME_TUE,
    "水": Schema.TIME_WED,
    "木": Schema.TIME_THU,
    "金": Schema.TIME_FRI,
    "土": Schema.TIME_SAT,
    "日": Schema.TIME_SUN,
}

BASE_URL = "https://www.shinq-compass.jp"


class ShinqCompassScraper(DynamicCrawler):
    """しんきゅうコンパス 鍼灸院スクレイパー"""

    DELAY = 2.0  # サーバー負荷軽減
    EXTRA_COLUMNS = ["アクセス", "対応しているこだわり"]
    _max_pages = None  # ページ上限 (None = 全ページ、テスト時にオーバーライド)

    def get_soup(self, url: str):
        """Playwright経由でページを取得し、BeautifulSoupオブジェクトを返す"""
        # 動的サイトの読み込み完了を待機 (WAF回避後は domcontentloaded で十分高速に取得可能)
        self.page.goto(url, wait_until="domcontentloaded")
        return BeautifulSoup(self.page.content(), "html.parser")

    def parse(self, url):
        """
        一覧ページを順次めくり、そのページの店舗URLを取得 → 即座に詳細ページをスクレイピングする
        """
        page = 1
        clean_url = re.sub(r"[&?]page=\d+", "", url)
        scraped_urls = set()  # 重複防止用

        while True:
            sep = "&" if "?" in clean_url else "?"
            list_url = f"{clean_url}{sep}page={page}"
            self.logger.info("一覧ページ取得: page=%d (累計取得済: %d件)", page, len(scraped_urls))
            
            try:
                if self.DELAY > 0:
                    time.sleep(self.DELAY)
                soup = self.get_soup(list_url)
            except Exception as e:
                self.logger.warning("一覧ページ %d の取得に失敗しました: %s。スキップします。", page, e)
                page += 1
                continue

            # 初回ページで全体件数と最終ページ数を取得
            if page == 1:
                self._extract_pagination_info(soup)

            # 一覧ページ内の詳細リンクを抽出
            salon_links = soup.select("a.searchSalon__link")
            if not salon_links:
                self.logger.warning("ページ %d に店舗リンクがありません。スキップします", page)
                
                # 最終ページ判定
                if hasattr(self, "_site_max_pages") and page >= self._site_max_pages:
                    self.logger.info("最終ページ (%d) に達しました。スクレイピングを完了します。", self._site_max_pages)
                    break
                    
                # 最終ページが取れていなくても、空ページが連続した場合はフェイルセーフで抜けるためのロジックは必要なら追加
                page += 1
                continue

            # --- そのページ内の各サロンを取得 ---
            for link in salon_links:
                href = link.get("href", "")
                if "/salon/detail/" not in href:
                    continue

                detail_url = href if href.startswith("http") else BASE_URL + href
                
                # 重複チェック
                if detail_url in scraped_urls:
                    continue

                # TEL は一覧ページからも取得
                tel = ""
                parent = link.parent
                if parent:
                    tel_div = parent.select_one("div.tel span")
                    if tel_div:
                        tel = tel_div.get_text(strip=True)

                # --- 詳細ページへアクセス ---
                try:
                    item = self._scrape_detail(detail_url, tel)
                    if item:
                        scraped_urls.add(detail_url)
                        yield item
                except Exception as e:
                    self.logger.warning("詳細ページ取得失敗: %s (%s)", detail_url, e)

                # デバッグ/テスト用の全件数制限
                if self._max_pages and page >= self._max_pages:
                    break

            # テスト制限チェック
            if self._max_pages and page >= self._max_pages:
                self.logger.info("テスト用ページ上限 (%d) に達しました", self._max_pages)
                break

            # 最終ページチェック
            if hasattr(self, "_site_max_pages") and page >= self._site_max_pages:
                self.logger.info("最終ページ (%d) に達しました。全 %d 件のURLを収集完了", self._site_max_pages, len(scraped_urls))
                break

            page += 1

    def _extract_pagination_info(self, soup):
        """初回ページで全体の件数と最終ページ数を取得する"""
        h1box_p = soup.select_one(".h1box p")
        if h1box_p:
            total_text = h1box_p.get_text(strip=True)
            digits = "".join(c for c in total_text if c.isdigit())
            if digits:
                self.total_items = int(digits)
                self.logger.info("全 %d 件", self.total_items)

        last_page_link = soup.select_one('li.forward a[title="last page"]')
        if last_page_link:
            href = last_page_link.get("href", "")
            m = re.search(r"page=(\d+)", href)
            if m:
                self._site_max_pages = int(m.group(1))
                self.logger.info("最終ページは %d ページ目です", self._site_max_pages)
        else:
            self._site_max_pages = 1

    def _collect_entries(self, detail_url: str, tel: str) -> dict | None:
        """
        詳細ページにアクセスし、該当の1店舗分のデータを抽出・yieldする関数。
        """
        self.logger.info("詳細ページ取得: %s", detail_url)
        if self.DELAY > 0:
            time.sleep(self.DELAY)
        soup = self.get_soup(detail_url)

        item = {Schema.URL: detail_url}

        # --- 店舗名 & カナ ---
        name_el = soup.select_one(".salon__name")
        if name_el:
            item[Schema.NAME] = name_el.get_text(strip=True)
        kana_el = soup.select_one(".salon__nameKana")
        if kana_el:
            item[Schema.NAME_KANA] = kana_el.get_text(strip=True)

        # 店舗名が取れなかったらスキップ
        if Schema.NAME not in item:
            return None

        # --- TEL ---
        # 優先順位: 詳細ページ div.tel > span → 一覧ページから取得した値
        tel_span = soup.select_one("div.tel span")
        if tel_span:
            tel_text = tel_span.get_text(strip=True)
            if tel_text:
                item[Schema.TEL] = tel_text
        if Schema.TEL not in item and tel:
            item[Schema.TEL] = tel

        # --- 代表者名（「当店からのご挨拶」セクションの最初のスタッフ） ---
        self._parse_staff(soup, item)

        # --- 店舗詳細情報テーブル ---
        table = soup.select_one("table.detail_tb")
        if table:
            self._parse_detail_table(table, item)

        return item

    def _parse_detail_table(self, table, item: dict) -> None:
        """<table class="detail_tb"> の各行を解析して item に格納する"""
        for row in table.select("tr"):
            th = row.select_one("th")
            td = row.select_one("td")
            if not th or not td:
                continue

            label = th.get_text(strip=True)

            if label == "住所":
                # 〒コードと住所テキストだけを取得（iframe 等を除外）
                p_tag = td.select_one("p")
                if p_tag:
                    addr_text = p_tag.get_text(strip=True)
                    item[Schema.ADDR] = addr_text

            elif label == "アクセス":
                item["アクセス"] = td.get_text(strip=True)

            elif label == "TEL":
                # TEL が一覧ページで取得できなかった場合のフォールバック
                if Schema.TEL not in item:
                    tel_text = td.get_text(strip=True)
                    if tel_text and tel_text != "電話で予約・お問い合わせ":
                        item[Schema.TEL] = tel_text

            elif label == "営業時間":
                self._parse_business_hours(td, item)

            elif label == "定休日":
                item[Schema.HOLIDAY] = re.sub(r"\s+", " ", td.get_text(strip=True))

            elif label == "URL":
                a_tag = td.select_one("a")
                if a_tag:
                    item[Schema.HP] = a_tag.get("href", "").strip()

            elif label == "LINE公式アカウント URL":
                a_tag = td.select_one("a[href]")
                if a_tag:
                    href = a_tag.get("href", "").strip()
                    # URL のみ格納（HTML タグの混入を防ぐ）
                    if href.startswith("http"):
                        item[Schema.LINE] = href

            elif label == "スタッフ数":
                item[Schema.EMP_NUM] = td.get_text(strip=True)

            elif label == "利用可能なクレジットカード":
                item[Schema.PAY] = td.get_text(strip=True)

            elif label == "ジャンル":
                # "鍼灸治療 / 美容鍼灸 / スポーツ鍼" → まとめて取得
                genre_text = td.get_text(strip=True)
                # 余分な空白・改行を正規化
                item[Schema.CAT_SITE] = re.sub(r"\s*/\s*", " / ", genre_text)

            elif label == "対応しているこだわり":
                self._parse_kodawari(td, item)

    def _parse_business_hours(self, td, item: dict) -> None:
        """営業時間セクションを曜日別に解析する"""
        time_lists = td.select("dl.businessTime__list")
        for dl in time_lists:
            day_el = dl.select_one("dt.businessTime__day")
            hour_el = dl.select_one("dd.businessTime__hour")
            if not day_el or not hour_el:
                continue

            day_text = day_el.get_text(strip=True).rstrip("：").rstrip(":")
            hours = re.sub(r"\s+", " ", hour_el.get_text(strip=True))
            # 「、」を「, 」に統一
            hours = hours.replace("、", ", ")

            # 曜日別カラムにセット
            schema_key = _DAY_MAP.get(day_text)
            if schema_key:
                item[schema_key] = hours

    def _parse_staff(self, soup, item: dict) -> None:
        """「当店からのご挨拶」セクションから代表者名を取得する

        最初の staff_fix ブロックの <strong> から解析。
        例:
            "院長　立松　栄二"              → POS_NM="院長",              REP_NM="立松　栄二"
            "鍼処SHIRAKAWA京都院　林英孝"   → (店名スキップ)              REP_NM="林英孝"
            "石川 美絵　meilong 代表／鍼灸師" → REP_NM="石川 美絵",        POS_NM="meilong 代表／鍼灸師"
        """
        first_staff = soup.select_one("div.staff_fix strong")
        if not first_staff:
            return

        raw = first_staff.get_text(strip=True)
        if not raw:
            return

        salon_name = item.get(Schema.NAME, "")
        role_keywords = ["院長", "代表", "オーナー"]

        parts = re.split(r"[　\t]+", raw)  # 全角スペース or タブで分割
        # 店名と一致・類似するパートを除外
        parts = [p for p in parts if p and p not in salon_name and salon_name not in p]

        if not parts:
            return

        if len(parts) >= 2:
            if any(kw in parts[0] for kw in role_keywords):
                # "院長　立松　栄二" → 役職 + 名前
                item[Schema.POS_NM] = parts[0]
                item[Schema.REP_NM] = "　".join(parts[1:])
            else:
                # "石川 美絵　meilong 代表／鍼灸師" → 名前 + 肩書き
                item[Schema.REP_NM] = parts[0]
                item[Schema.POS_NM] = "　".join(parts[1:])
        else:
            item[Schema.REP_NM] = parts[0]

    def _parse_kodawari(self, td, item: dict) -> None:
        """「対応しているこだわり」セクションを解析する"""
        parts = []
        for dl in td.select("dl"):
            dt = dl.select_one("dt")
            dd = dl.select_one("dd")
            if dt and dd:
                category = dt.get_text(strip=True)
                values = re.sub(r"\s*/\s*", " / ", dd.get_text(strip=True))
                parts.append(f"【{category}】{values}")
        if parts:
            item["対応しているこだわり"] = " ｜ ".join(parts)


# =============================================================================
# ローカル実行用エントリーポイント
# =============================================================================
if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = ShinqCompassScraper()
    scraper._max_pages = 10  # テスト: 1ページ分だけ (20件)
    scraper.execute("https://www.shinq-compass.jp/area/list/?page=1")

    print("\n" + "=" * 30)
    print("📊 実行結果サマリ")
    print("=" * 30)
    print(f"  出力ファイル:     {scraper.output_filepath}")
    print(f"  取得件数:         {scraper.item_count}")
    print(f"  観測カラム数:     {len(scraper.observed_columns)}")
    print(f"  観測カラム:       {scraper.observed_columns}")
    print(f"  EXTRA カラム:     {scraper.extra_columns}")
    print("=" * 30)

    # CSV の先頭5行を表示
    if scraper.output_filepath:
        print("\n CSV 先頭5行:")
        print("-" * 30)
        with open(scraper.output_filepath, encoding="utf-8-sig") as f:
            for i, line in enumerate(f):
                if i >= 6:  # ヘッダー + 5行
                    break
                print(line.rstrip())
