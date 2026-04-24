"""
楽天ビューティ — 都道府県→エリア→ページネーション経由で全店舗詳細を取得 (rakuten_beauty_2)

取得対象:
    - 全都道府県(/preXX/)から全エリア(/areaNNNN/)を抽出し、
      各エリアをページネーション(/sort4/pageN/)で巡回して店舗URL(/sNNNNNN/)を収集。
    - 各店舗詳細ページから 17 カラムを取得。

実行方法:
    python scripts/sites/beauty/rakuten_beauty_2.py
    python bin/run_flow.py --site-id rakuten_beauty_2
"""

import re
import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class RakutenBeauty2Scraper(StaticCrawler):
    """楽天ビューティ ホームページ起点クローラー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "エリア",
        "最寄り駅",
        "アクセス",
        "得意メニュー",
        "設備・サービス",
        "口コミ評価",
        "口コミ件数",
    ]

    BASE = "https://beauty.rakuten.co.jp"
    _PREF_RE = re.compile(r"^https?://beauty\.rakuten\.co\.jp/pre\d+/?$")
    _AREA_RE = re.compile(r"^https?://beauty\.rakuten\.co\.jp/area\d+/?$")
    _SHOP_RE = re.compile(r"^https?://beauty\.rakuten\.co\.jp/s\d+/?$")
    _POST_RE = re.compile(r"〒?\s*(\d{3}-\d{4})")
    _REVIEW_RE = re.compile(r"(\d+\.\d+)\s*\((\d+)件\)")

    def parse(self, url: str) -> Generator[dict, None, None]:
        pref_urls = self._collect_pref_urls(url)
        self.logger.info("都道府県URL: %d 件", len(pref_urls))

        area_urls = []
        for pref_url in pref_urls:
            for area in self._collect_area_urls(pref_url):
                if area not in area_urls:
                    area_urls.append(area)
        self.logger.info("エリアURL: %d 件", len(area_urls))

        seen_shops: set[str] = set()
        for area_url in area_urls:
            for shop_url in self._collect_shop_urls(area_url):
                if shop_url in seen_shops:
                    continue
                seen_shops.add(shop_url)
                item = self._scrape_detail(shop_url)
                if item:
                    yield item

        self.total_items = len(seen_shops)

    # ------------------------------------------------------------------
    # ナビゲーション収集
    # ------------------------------------------------------------------
    def _collect_pref_urls(self, home_url: str) -> list[str]:
        soup = self.get_soup(home_url)
        if soup is None:
            return []
        urls = []
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            full = self._absolutize(href)
            if self._PREF_RE.match(full) and full not in urls:
                urls.append(full)
        return urls

    def _collect_area_urls(self, pref_url: str) -> list[str]:
        soup = self.get_soup(pref_url)
        if soup is None:
            return []
        urls = []
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            full = self._absolutize(href)
            if self._AREA_RE.match(full) and full not in urls:
                urls.append(full)
        return urls

    def _collect_shop_urls(self, area_url: str) -> list[str]:
        """エリアの全ページを巡回して店舗URLを集める"""
        base_listing = area_url.rstrip("/") + "/sort4/"
        urls: list[str] = []
        page = 1
        while True:
            page_url = base_listing if page == 1 else f"{base_listing}page{page}/"
            soup = self.get_soup(page_url)
            if soup is None:
                break

            page_shops: list[str] = []
            for a in soup.select("a[href]"):
                full = self._absolutize(a.get("href", ""))
                if self._SHOP_RE.match(full) and full not in page_shops:
                    page_shops.append(full)

            if not page_shops:
                break

            urls.extend(page_shops)

            next_link = soup.select_one(".c-pagination__next a, a.c-pagination__next")
            if next_link is None:
                # フォールバック: 次のページ番号リンクが存在するか
                next_num = soup.find("a", class_="c-pagination__link", string=str(page + 1))
                if next_num is None:
                    break
            page += 1
        return urls

    # ------------------------------------------------------------------
    # 詳細ページ
    # ------------------------------------------------------------------
    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}

        # 名称
        h1 = soup.select_one("h1")
        if h1:
            data[Schema.NAME] = h1.get_text(strip=True)

        # 電話
        tel = soup.select_one("span.m-shopDetailInfo__tel")
        if tel:
            data[Schema.TEL] = tel.get_text(strip=True)

        # パンくず: 美容院・美容室 / 都道府県 / エリア / 駅
        crumbs = [c.get_text(strip=True) for c in soup.select(".c-breadcrumbs__link")]
        if crumbs:
            data[Schema.CAT_SITE] = crumbs[0] if len(crumbs) >= 1 else ""
            data[Schema.PREF] = crumbs[1] if len(crumbs) >= 2 else ""
            data["エリア"] = crumbs[2] if len(crumbs) >= 3 else ""
            data["最寄り駅"] = crumbs[3] if len(crumbs) >= 4 else ""

        # テーブル th/td 抽出
        for tr in soup.select("table tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            label = th.get_text(strip=True)
            value = td.get_text(" ", strip=True)
            value = re.sub(r"地図を見る", "", value).strip()
            if "定休日" == label:
                data[Schema.HOLIDAY] = value
            elif "営業時間" == label:
                data[Schema.TIME] = value
            elif "住所" == label:
                m = self._POST_RE.search(value)
                if m:
                    data[Schema.POST_CODE] = m.group(1)
                    addr = self._POST_RE.sub("", value).strip()
                else:
                    addr = value
                data[Schema.ADDR] = addr
            elif "アクセス" == label:
                data["アクセス"] = value
            elif "支払方法" == label:
                data[Schema.PAYMENTS] = value
            elif "得意メニュー" == label:
                data["得意メニュー"] = value
            elif "設備・サービス" == label:
                data["設備・サービス"] = value

        # 口コミ
        body_text = soup.get_text(" ", strip=True)
        m = self._REVIEW_RE.search(body_text)
        if m:
            data["口コミ評価"] = m.group(1)
            data["口コミ件数"] = m.group(2)

        if not data.get(Schema.NAME):
            return None
        return data

    # ------------------------------------------------------------------
    def _absolutize(self, href: str) -> str:
        if not href:
            return ""
        if href.startswith("http"):
            return href.split("?")[0].split("#")[0]
        if href.startswith("/"):
            return self.BASE + href.split("?")[0].split("#")[0]
        return ""


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    scraper = RakutenBeauty2Scraper()
    scraper.execute("https://beauty.rakuten.co.jp/")
    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
