"""
CLUB JT【喫煙所・喫煙可能なカフェ検索】

取得対象:
    - CLUB JT の「スポット検索」(全国の喫煙所・喫煙可能な飲食店/カフェ/バー等) の全スポット
    - 名称・住所・都道府県・TEL・HP・ジャンル・営業時間 等の構造化情報

取得フロー:
    1. ルート (/place/spot/) から 47 都道府県ページのリンクを取得
    2. 各都道府県 → 市区町村ページ → エリア(駅/ランドマーク)ページへと辿る
    3. エリアページ (静的 HTML) の一覧から各スポットの spotId を取得
       (ページネーションは .../area-{id}/page-{n}/ 形式で巡回)
    4. spotId ごとに公開 JSON API (user-api.clubjt.jp/spot/{id}) を 1 件叩いて
       構造化データを取得し、その場で即 yield する (取得即 yield / Pattern B)
    5. spotId をキーに全国でグローバル重複排除する
       (同一スポットが複数エリアの「周辺」一覧に重複出現するため)

著作権への配慮:
    - 文章 (自由記述プロース) は取得しない。API の message / comment /
      introduceContents / warningMessage 等の自由記述フィールドは保存対象外。
    - 保存するのは構造化された短いラベル・数値・コード・住所・電話・URL のみ。

規約:
    - robots.txt は Disallow 指定なし (Sitemap 宣言のみ) のためクロール可。
    - 詳細は公開 JSON API から取得 (アプリが利用する公開エンドポイント)。

実行方法:
    # ローカルテスト
    python scripts/sites/service/club_jt.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id club_jt
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

# スポット詳細を返す公開 JSON API のベース URL。
# クロールの「起点 (root)」は引数 url (= sites.yml の url) で固定だが、
# 詳細データはこの別ホストの公開 API から取得する (調査で確認済み)。
_API_BASE = "https://user-api.clubjt.jp"

# 各階層リンクの判定用パターン
_PREF_HREF = re.compile(r"/place/spot/pref-\d+/$")
_CITY_HREF = re.compile(r"/place/spot/pref-\d+/city-\d+/$")
_AREA_HREF = re.compile(r"/place/spot/pref-\d+/city-\d+/area-\d+/$")
_SPOT_ID = re.compile(r"/map/spot/(\d+)")

# API genre(英語コード) → 日本語ジャンル
_GENRE_JA = {
    "SMOKING_SPOT": "喫煙所",
    "RESTAURANTS": "レストラン・飲食店",
    "CAFE": "カフェ",
    "BAR_SNACK": "バー・スナック",
    "CLUB_LOUNGE_SNACK": "クラブ・ラウンジ・スナック",
    "IZAKAYA": "居酒屋",
}

# 住所先頭の都道府県を抽出
_PREF_PATTERN = re.compile(r"^(北海道|東京都|京都府|大阪府|.{2,3}県)")


class ClubJtScraper(StaticCrawler):
    """CLUB JT【喫煙所・喫煙可能なカフェ検索】スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "spot_id",
        "ジャンル細",
        "喫煙タイプ",
        "喫煙エリアタイプ",
        "アクセス",
        "予算",
        "料金タイプ",
        "WiFi有無",
        "電源有無",
        "駐車場有無",
        "個室有無",
        "屋根有無",
        "席数",
        "緯度",
        "経度",
        "外部サービスURL",
        "提供元コード",
    ]

    @staticmethod
    def _clean(value) -> str:
        """空白・改行を 1 個のスペースに畳んで前後を除去する (構造化値の整形のみ)。"""
        if value is None:
            return ""
        return re.sub(r"\s+", " ", str(value)).strip()

    @staticmethod
    def _yesno(value) -> str:
        """真偽値の設備フラグを「有 / 無」へ変換する (列名が「○○有無」のため)。"""
        if value is None:
            return ""
        return "有" if value else "無"

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_ids: set[str] = set()

        # --- 1. 都道府県リンク ---
        root_soup = self.get_soup(url)
        if root_soup is None:
            return
        pref_urls = self._collect_links(root_soup, url, _PREF_HREF)
        self.logger.info("都道府県ページ数: %d", len(pref_urls))

        for pref_url in pref_urls:
            # --- 2. 市区町村リンク ---
            pref_soup = self.get_soup(pref_url)
            if pref_soup is None:
                continue
            city_urls = self._collect_links(pref_soup, url, _CITY_HREF)

            for city_url in city_urls:
                # --- 3. エリア(駅/ランドマーク)リンク ---
                city_soup = self.get_soup(city_url)
                if city_soup is None:
                    continue
                area_urls = self._collect_links(city_soup, url, _AREA_HREF)

                for area_url in area_urls:
                    yield from self._crawl_area(area_url, seen_ids)

    def _crawl_area(self, area_url: str, seen_ids: set) -> Generator[dict, None, None]:
        """1 エリアをページ送りしながら、未取得スポットを取得即 yield する。"""
        page = 1
        while True:
            page_url = area_url if page == 1 else f"{area_url}page-{page}/"
            soup = self.get_soup(page_url)
            if soup is None:
                break

            spot_ids: list[str] = []
            for a in soup.select("a.search-spot-link[href]"):
                m = _SPOT_ID.search(a.get("href", ""))
                if m:
                    spot_ids.append(m.group(1))

            if not spot_ids:
                break

            for spot_id in spot_ids:
                if spot_id in seen_ids:
                    continue
                seen_ids.add(spot_id)
                try:
                    record = self._fetch_spot(spot_id, area_url)
                except Exception as e:  # noqa: BLE001
                    self.logger.warning("スポット取得失敗 id=%s: %s", spot_id, e)
                    continue
                if record:
                    yield record

            page += 1

    def _fetch_spot(self, spot_id: str, root_for_detail: str) -> dict | None:
        """公開 API からスポット詳細を取得し、構造化レコードを組み立てる。"""
        api_url = f"{_API_BASE}/spot/{spot_id}"
        # session.get はテストランナーのソフトタイムアウト対象 (中断可能)
        resp = self.session.get(
            api_url, timeout=self.TIMEOUT, headers={"Accept": "application/json"}
        )
        resp.raise_for_status()
        d = resp.json()

        name = self._clean(d.get("name"))
        if not name:
            # 削除済み / 非公開スポットは name が null → スキップ
            return None

        address = self._clean(d.get("address"))

        # 都道府県: 住所先頭を優先し、無ければ API の prefectureName
        pref = ""
        m = _PREF_PATTERN.match(address)
        if m:
            pref = m.group(1)
        if not pref:
            pref = self._clean(d.get("prefectureName"))

        genre = d.get("genre") or ""
        cat_site = _GENRE_JA.get(genre, self._clean(genre))

        # 詳細ページ URL は root (引数 url) と同一ホストから派生させる
        detail_url = urljoin(root_for_detail, f"/map/spot/{spot_id}")

        record = {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.TEL: self._clean(d.get("phoneNumber")),
            Schema.PREF: pref,
            Schema.POST_CODE: self._clean(d.get("zipCode")),
            Schema.ADDR: address,
            Schema.HP: self._clean(d.get("hostUrl")),
            Schema.CAT_SITE: cat_site,
            Schema.TIME: self._clean(d.get("openingHours")),
            # --- EXTRA ---
            "spot_id": spot_id,
            "ジャンル細": self._clean(d.get("subGenre")),
            "喫煙タイプ": self._clean(d.get("smokingType")),
            "喫煙エリアタイプ": self._clean(d.get("smokingAreaType")),
            "アクセス": self._clean(d.get("access")),
            "予算": self._clean(d.get("budget")),
            "料金タイプ": self._clean(d.get("chargeType")),
            "WiFi有無": self._yesno(d.get("hasWifi")),
            "電源有無": self._yesno(d.get("hasPower")),
            "駐車場有無": self._yesno(d.get("hasParking")),
            "個室有無": self._yesno(d.get("hasPrivateRoom")),
            "屋根有無": self._yesno(d.get("hasRoof")),
            "席数": d.get("seatCount"),
            "緯度": d.get("lat"),
            "経度": d.get("lng"),
            "外部サービスURL": self._clean(d.get("externalServiceUrl")),
            "提供元コード": self._clean(d.get("spotManagerCode")),
        }
        return record

    @staticmethod
    def _collect_links(soup, base_url: str, pattern: re.Pattern) -> list[str]:
        """soup 内の <a href> から pattern に一致するものを base_url で絶対化し重複排除して返す。"""
        urls: list[str] = []
        seen: set[str] = set()
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if pattern.search(href):
                absolute = urljoin(base_url, href)
                if absolute not in seen:
                    seen.add(absolute)
                    urls.append(absolute)
        return urls


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = ClubJtScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.clubjt.jp/place/spot/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
