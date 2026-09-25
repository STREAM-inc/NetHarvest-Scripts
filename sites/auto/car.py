"""
楽天Car車検 — 全国の車検掲載店舗スクレイパー

取得対象:
    - 全国の楽天Car車検 掲載店舗 (想定 約5,600〜6,000店舗)
    - 店舗名 / 都道府県 / 住所 / 郵便番号 / 業態記述 (店舗紹介文) /
      ブランド(FC)名 / 運営会社 / 楽天Car口コミ件数 / 口コミ評価 /
      WEB見積もり予約ボタン有無 / 受付(営業)時間 / 定休日 / GoogleMapsリンク

取得フロー (2階層 + 補完):
    1. トップ `{root}` から 47 都道府県のエリア一覧 `{root}area/{slug}/` を収集
    2. 各エリア一覧の `a[data-shop="shop-name"]` から店舗詳細 ID を収集
       (都道府県ページは 1 ページに全店舗を掲載しておりページ送りは無い)
    3. 店舗詳細 `{root}shop/{id}/` を 1 件取得するたびに即 yield する (Pattern B)
    4. 補完としてブランド一覧 `{root}brand/` → `{root}brand/{slug}/shop/` を巡回し、
       エリア一覧に出てこなかった店舗 ID のみ追加取得する (備考のブランド別起点に対応)

robots.txt 遵守:
    Disallow 対象 (`/shaken/search/`, `/shaken/shop/0/`, `/shaken/geo/`,
    `/shaken/tag/`, `/shaken/carmodel/`, `/shaken/brand/*/search/`,
    `sort=` / `carKind=` 付きのエリア URL) には一切アクセスしない。
    Crawl-delay: 1 に合わせて DELAY = 1.0 とする。

実行方法:
    # ローカルテスト
    python scripts/sites/auto/car.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id car
"""

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

# エリア一覧リンク: /shaken/area/{都道府県スラッグ}/ (市区町村ページは 2 セグメントなので除外)
_AREA_HREF = re.compile(r"^/shaken/area/([a-z]+)/?$")
# ブランド一覧リンク: /shaken/brand/{ブランドスラッグ}/ (ranking/review/shop 等の下層は除外)
_BRAND_HREF = re.compile(r"^/shaken/brand/([a-z0-9\-]+)/?(?:\?|$)")
# 店舗詳細リンク: /shaken/shop/{ID}/ — ID=0 は robots.txt で Disallow のため除外する
_SHOP_HREF = re.compile(r"^/shaken/shop/([1-9]\d*)/")

# 住所先頭の都道府県
_PREF_PATTERN = re.compile(r"^(北海道|東京都|京都府|大阪府|.{2,3}県)")
# 郵便番号 (〒869-1108)
_POST_PATTERN = re.compile(r"〒?\s*(\d{3}-?\d{4})")
# 口コミ件数 "口コミ（226件）" / "口コミ（1,058件）"
_REVIEW_PATTERN = re.compile(r"([\d,]+)\s*件")

# 店舗情報テーブルのラベル → 出力先
_INFO_LABELS = {
    "店舗名": "name",
    "運営会社": "company",
    "住所": "address",
    "受付時間": "time",
    "営業時間": "time",
    "定休日": "holiday",
}


class CarScraper(StaticCrawler):
    """楽天Car車検 スクレイパー"""

    # robots.txt の Crawl-delay: 1 に合わせる
    DELAY = 1.0
    EXTRA_COLUMNS = [
        "ブランド名",
        "運営会社",
        "WEB見積もり予約ボタン",
        "GoogleMapsリンク",
        "店舗ID",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        root = url if url.endswith("/") else url + "/"
        seen: set[str] = set()

        # --- 1. 都道府県エリア一覧を起点に巡回 ---------------------------------
        area_urls = self._collect_area_urls(root)
        self.logger.info("都道府県エリアURL: %d 件", len(area_urls))

        for area_url in area_urls:
            for shop_id in self._collect_shop_ids(area_url, list_page=True):
                if shop_id in seen:
                    continue
                seen.add(shop_id)
                item = self._parse_detail(root, shop_id)
                if item:
                    yield item

        # --- 2. ブランド別一覧で取りこぼしを補完 --------------------------------
        brand_urls = self._collect_brand_shop_urls(root)
        self.logger.info("ブランド一覧URL: %d 件", len(brand_urls))

        for brand_url in brand_urls:
            for shop_id in self._collect_shop_ids(brand_url, list_page=False):
                if shop_id in seen:
                    continue
                seen.add(shop_id)
                item = self._parse_detail(root, shop_id)
                if item:
                    yield item

    # ------------------------------------------------------------------ 一覧
    def _collect_area_urls(self, root: str) -> list[str]:
        """トップページから 47 都道府県のエリア一覧 URL を収集する。"""
        soup = self.get_soup(root)
        if soup is None:
            return []
        urls: list[str] = []
        for a in soup.select("a[href]"):
            m = _AREA_HREF.match(a["href"].split("?")[0])
            if not m:
                continue
            full = urljoin(root, f"area/{m.group(1)}/")
            if full not in urls:
                urls.append(full)
        return urls

    def _collect_brand_shop_urls(self, root: str) -> list[str]:
        """ブランド索引から各ブランドの店舗一覧 URL を収集する。"""
        soup = self.get_soup(urljoin(root, "brand/"))
        if soup is None:
            return []
        urls: list[str] = []
        for a in soup.select("a[href]"):
            m = _BRAND_HREF.match(a["href"])
            if not m:
                continue
            full = urljoin(root, f"brand/{m.group(1)}/shop/")
            if full not in urls:
                urls.append(full)
        return urls

    def _collect_shop_ids(self, list_url: str, list_page: bool) -> list[str]:
        """一覧ページから店舗 ID を抽出する。

        都道府県エリア一覧は 1 ページに全店舗を掲載しており、店舗リンクには
        `data-shop="shop-name"` が付く。ブランド別一覧はマークアップが異なるため
        店舗詳細 URL の形をした全リンクを対象にする。
        """
        soup = self.get_soup(list_url)
        if soup is None:
            return []
        selector = 'a[data-shop="shop-name"][href]' if list_page else "a[href]"
        ids: list[str] = []
        for a in soup.select(selector):
            m = _SHOP_HREF.match(a["href"])
            if not m:
                continue
            shop_id = m.group(1)
            if shop_id not in ids:
                ids.append(shop_id)
        return ids

    # ------------------------------------------------------------------ 詳細
    def _parse_detail(self, root: str, shop_id: str) -> dict | None:
        detail_url = urljoin(root, f"shop/{shop_id}/")
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        info = self._parse_info_table(soup)

        name = info.get("name", "")
        if not name:
            h1 = soup.select_one(".shaken-pc-shop-header__name")
            name = h1.get_text(strip=True) if h1 else ""
        if not name:
            return None

        addr_full = info.get("address", "")
        if not addr_full:
            p = soup.select_one(".shaken-pc-shop-header__address p")
            addr_full = p.get_text(strip=True) if p else ""

        post_code = ""
        m = _POST_PATTERN.search(addr_full)
        if m:
            post_code = m.group(1)
            addr_full = addr_full[m.end():].strip()

        pref = ""
        m = _PREF_PATTERN.match(addr_full)
        if m:
            pref = m.group(1)
            addr = addr_full[m.end():].strip()
        else:
            addr = addr_full

        score_el = soup.select_one(".shaken-rate__score")
        score = score_el.get_text(strip=True) if score_el else ""

        review_count = ""
        review_el = soup.select_one(".shaken-rate__review")
        if review_el:
            m = _REVIEW_PATTERN.search(review_el.get_text())
            if m:
                review_count = m.group(1).replace(",", "")

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.CAT_SITE: "車検",
            Schema.DESCRIPTION: self._parse_description(soup),
            Schema.TIME: info.get("time", ""),
            Schema.HOLIDAY: info.get("holiday", ""),
            Schema.SCORES: score,
            Schema.REV_SCR: review_count,
            "ブランド名": self._parse_brand(soup),
            "運営会社": info.get("company", ""),
            "WEB見積もり予約ボタン": self._parse_booking(soup),
            "GoogleMapsリンク": self._parse_map_link(soup),
            "店舗ID": shop_id,
        }

    def _parse_info_table(self, soup) -> dict:
        """「店舗情報」テーブルの th/td をラベル→値で取り出す。

        1 行に th/td が 2 組並ぶ行 (受付時間 / 定休日) があるため、
        行内を順に走査して直前の th をラベルとして扱う。
        """
        result: dict[str, str] = {}
        section = soup.select_one(".shaken-section-top-info")
        if section is None:
            return result
        for tr in section.select("tr"):
            label = ""
            for cell in tr.find_all(["th", "td"]):
                if cell.name == "th":
                    label = cell.get_text(strip=True)
                    continue
                key = _INFO_LABELS.get(label)
                if key and key not in result:
                    result[key] = cell.get_text(" ", strip=True)
                label = ""
        return result

    def _parse_description(self, soup) -> str:
        """店舗紹介文 (業態記述)。備考で取得指示があるため DESCRIPTION に格納する。"""
        block = soup.select_one(".shaken-section-top-shop-comment")
        if block is None:
            return ""
        parts = [p.get_text(" ", strip=True) for p in block.select("p")]
        return " ".join(p for p in parts if p)

    def _parse_brand(self, soup) -> str:
        """FC/ブランド名。ロゴ画像の alt、無ければブランドリンクのスラッグを使う。"""
        link = soup.select_one("#shop-brand-link")
        if link is None:
            return ""
        img = link.select_one("img[alt]")
        if img and img.get("alt", "").strip():
            return img["alt"].strip()
        m = _BRAND_HREF.match(link.get("href", ""))
        return m.group(1) if m else ""

    def _parse_booking(self, soup) -> str:
        """WEB見積もり予約ボタンの有無 (あり / なし)。"""
        for a in soup.select("a[href]"):
            if "/shaken/reserve/" in a["href"]:
                return "あり"
        return "なし"

    def _parse_map_link(self, soup) -> str:
        """「大きな地図で見る」の GoogleMaps リンク。"""
        for a in soup.select(".shaken-section-top-map a[href]"):
            if "maps.google" in a["href"] or "google.com/maps" in a["href"]:
                return a["href"]
        return ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = CarScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://car.rakuten.co.jp/shaken/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
