"""
居抜き本舗 — 居抜き物件（クラブ・キャバクラ等）一覧

取得対象:
    - 居抜き物件（クラブ・キャバクラ）の物件情報（住所・賃料・広さ・沿線・PR説明等）

取得フロー:
    1. 検索結果一覧ページを巡回（.afterLink a で次ページへ、offset=25 刻み）
    2. 各物件の詳細ページ (.detailBukkenTable) から全フィールドを抽出

実行方法:
    # ローカルテスト
    python scripts/sites/realestate/inuki_honpo.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id inuki_honpo
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://www.inuki-honpo.jp"
_TOTAL_PATTERN = re.compile(r"(\d+)件中")
_BUKKEN_NO_PATTERN = re.compile(r"物件NO\.(\S+)")


def _clean(el) -> str:
    if el is None:
        return ""
    return re.sub(r"\s+", " ", el.get_text(separator=" ", strip=True)).strip()


class InukiHonpoScraper(StaticCrawler):
    """居抜き本舗 スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "物件NO",
        "最寄り駅_概要",
        "沿線_駅",
        "徒歩_分",
        "広さ_m2",
        "広さ_坪",
        "階数",
        "賃料_円",
        "坪単価",
        "保証金_敷金",
        "アイコン",
        "PR説明",
    ]

    def parse(self, url: str):
        page_url = url
        while True:
            soup = self.get_soup(page_url)
            if soup is None:
                break

            # 初回ページで総件数をセット
            if not self.total_items:
                for text in soup.find_all(string=_TOTAL_PATTERN):
                    m = _TOTAL_PATTERN.search(text)
                    if m:
                        self.total_items = int(m.group(1))
                        break

            items = soup.select(".bukkenListTable")
            for item in items:
                a = item.select_one(".detailBtn a")
                if not a:
                    continue
                href = a.get("href", "")
                if not href:
                    continue
                detail_url = urljoin(BASE_URL, href)
                try:
                    row = self._scrape_detail(detail_url)
                    if row:
                        yield row
                except Exception as e:
                    self.logger.warning("詳細取得失敗 (スキップ): %s — %s", detail_url, e)

            # 次ページへ
            next_a = soup.select_one(".afterLink a")
            if not next_a:
                break
            next_href = next_a.get("href", "")
            if not next_href:
                break
            page_url = urljoin(BASE_URL, next_href)

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        tbl = soup.select_one(".detailBukkenTable")
        if not tbl:
            return None

        # 物件NO
        num_el = tbl.select_one(".detailBukkenNum")
        bukken_no = ""
        if num_el:
            m = _BUKKEN_NO_PATTERN.search(num_el.get_text())
            bukken_no = m.group(1) if m else _clean(num_el)

        # 住所（都道府県プレフィックス付与）
        add_el = tbl.select_one(".detailBukkenAdd")
        raw_addr = _clean(add_el) if add_el else ""
        addr = ("東京都" + raw_addr) if raw_addr and not raw_addr.startswith("東京都") else raw_addr

        # 賃料
        price_el = tbl.select_one(".detailBukkenPrice")
        price_raw = price_el.get_text(strip=True).replace(",", "") if price_el else ""

        # 坪単価
        price2_el = tbl.select_one(".detailBukkenPrice2")
        price2 = price2_el.get_text(strip=True) if price2_el else ""

        # 広さ・階数
        m2_el = tbl.select_one(".detailBukkenM2")
        lis = m2_el.select("li") if m2_el else []
        area_m2 = re.sub(r"[^\d.]", "", lis[0].get_text(strip=True)) if len(lis) > 0 else ""
        area_tsubo = re.sub(r"[^\d.]", "", lis[1].get_text(strip=True)) if len(lis) > 1 else ""
        floor_p = m2_el.select_one("p") if m2_el else None
        floor = floor_p.get_text(strip=True).lstrip("／ ").strip() if floor_p else ""

        # 沿線・駅 / 徒歩
        sta_el = tbl.select_one(".detailBukkenSta")
        sta_min_el = tbl.select_one(".detailBukkenStaMin")
        sta = re.sub(r"\s+", "／", _clean(sta_el)) if sta_el else ""
        sta_min = re.sub(r"\s+", "／", _clean(sta_min_el)) if sta_min_el else ""

        # 保証金・敷金（最初の .bukkenListshiki）
        shiki_els = tbl.select(".bukkenListshiki")
        shiki = _clean(shiki_els[0]) if shiki_els else ""
        # 2番目は造作価格（会員専用）→ 除外

        # アイコン
        icons = [img.get("alt", "") for img in tbl.select(".detailBukkenIcon img") if img.get("alt")]
        icon_str = ",".join(icons)

        # PRタイトル・説明
        name_el = soup.select_one(".prTitle2 strong") or soup.select_one(".prTitle strong")
        pr_desc_el = soup.select_one(".prDesc")

        name = _clean(name_el) if name_el else ""
        pr_desc = _clean(pr_desc_el) if pr_desc_el else ""

        # 業態
        cat_el = tbl.select_one(".detailBukkenCat")
        cat = _clean(cat_el) if cat_el else ""

        # 最寄り駅概要
        station_name_el = tbl.select_one(".detailBukkenName")
        station_summary = _clean(station_name_el) if station_name_el else ""

        if not name and not raw_addr:
            return None

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: "東京都",
            Schema.ADDR: addr,
            Schema.CAT_SITE: cat,
            "物件NO": bukken_no,
            "最寄り駅_概要": station_summary,
            "沿線_駅": sta,
            "徒歩_分": sta_min,
            "広さ_m2": area_m2,
            "広さ_坪": area_tsubo,
            "階数": floor,
            "賃料_円": price_raw,
            "坪単価": price2,
            "保証金_敷金": shiki,
            "アイコン": icon_str,
            "PR説明": pr_desc,
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    START_URL = (
        "https://www.inuki-honpo.jp/rent/"
        "?s_prf_cd%5B%5D=13&s_inuki_type%5B%5D=1&s_inuki_category%5B%5D=26"
        "&s_city_cd%5B%5D=13101&s_city_cd%5B%5D=13102&s_city_cd%5B%5D=13103"
        "&s_city_cd%5B%5D=13104&s_city_cd%5B%5D=13109&s_city_cd%5B%5D=13110"
        "&s_city_cd%5B%5D=13112&s_city_cd%5B%5D=13113&s_city_cd%5B%5D=13114"
        "&s_city_cd%5B%5D=13115&s_city_cd%5B%5D=13116&s_city_cd%5B%5D=13203"
        "&s_walk=&s_tsubo_max=&s_kakaku_m=&s_freeword="
    )

    scraper = InukiHonpoScraper()
    scraper.execute(START_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
