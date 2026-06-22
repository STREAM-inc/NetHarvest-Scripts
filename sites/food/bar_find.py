"""
バーファインド (Bar-find) — 新宿区バー検索・情報サイト

取得対象:
    - 新宿区のバー店舗情報 (約1,900件)
    - 店名 / フリガナ / 郵便番号 / 都道府県 / 住所 / TEL /
      ジャンル / 営業時間 / 定休日 / Instagram / LINE / X

取得フロー:
    1. GET /main/search でページ1を取得 (10件/ページ)
    2. POST /main/search?page=N&... で追加ページを順次取得
    3. 各店舗の詳細ページ (/main/store/public_id/{id}) を即 yield

実行方法:
    # ローカルテスト
    python scripts/sites/food/bar_find.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id bar_find
"""

import re
import sys
import urllib.parse
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup
from src.framework.static import StaticCrawler
from src.const.schema import Schema

_POSTAL_RE = re.compile(r"^(\d{3}-\d{4})\s*")
_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|"
    r"三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_WS_RE = re.compile(r"\s+")

# 検索用クエリパラメータ (フィルターなし全件取得)
_SEARCH_QS = (
    "q=&open_time=&avg_price_json=%5B%5D&area_json=%5B%5D"
    "&scene=&kodawari_json=%5B%5D&genre_json=%5B%5D"
    "&drink_json=%5B%5D&etc_json=%5B%5D"
)


class BarFindScraper(StaticCrawler):
    """バーファインド スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = []

    def parse(self, url: str):
        list_url = urllib.parse.urljoin(url, "main/search")
        seen: set[str] = set()
        page = 1

        while True:
            if page == 1:
                soup = self.get_soup(list_url)
            else:
                post_url = f"{list_url}?page={page}&{_SEARCH_QS}"
                try:
                    resp = self.session.post(
                        post_url, data={"context": ""}, timeout=self.TIMEOUT
                    )
                    resp.raise_for_status()
                    if "charset=" not in resp.headers.get("Content-Type", "").lower():
                        resp.encoding = resp.apparent_encoding
                    soup = BeautifulSoup(resp.text, "html.parser")
                except Exception as exc:
                    self.logger.warning("ページ取得エラー page=%d: %s", page, exc)
                    break

            if soup is None:
                break

            links = soup.select("a.search-result-link")
            if not links:
                break

            for link in links:
                href = link.get("href", "")
                if not href or href in seen:
                    continue
                seen.add(href)
                detail_url = urllib.parse.urljoin(url, href)
                try:
                    item = self._scrape_detail(detail_url)
                    if item:
                        yield item
                except Exception as exc:
                    self.logger.warning("詳細取得エラー %s: %s", detail_url, exc)

            page += 1

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}

        # ---- 店名・カナ ----
        name_el = soup.select_one(".store-name")
        kana_el = soup.select_one(".store-kana")
        data[Schema.NAME] = name_el.get_text(strip=True) if name_el else ""
        data[Schema.NAME_KANA] = kana_el.get_text(strip=True) if kana_el else ""
        if not data[Schema.NAME]:
            return None

        # ---- 住所 (郵便番号・都道府県・住所を分離) ----
        addr_el = soup.select_one(".store-address")
        if addr_el:
            raw = _WS_RE.sub(" ", addr_el.get_text(strip=True))
            m_postal = _POSTAL_RE.match(raw)
            if m_postal:
                data[Schema.POST_CODE] = m_postal.group(1)
                rest = raw[m_postal.end():].strip()
            else:
                rest = raw
            m_pref = _PREF_RE.match(rest)
            if m_pref:
                data[Schema.PREF] = m_pref.group(1)
                data[Schema.ADDR] = rest[m_pref.end():].strip()
            else:
                data[Schema.ADDR] = rest

        # ---- TEL ----
        tel_el = soup.select_one("a.store-tel[href]")
        if tel_el:
            data[Schema.TEL] = tel_el["href"].replace("tel:", "").strip()

        # ---- ジャンル ----
        genre_items = soup.select(".store-genre .genre-item")
        if genre_items:
            data[Schema.CAT_SITE] = "/".join(
                g.get_text(strip=True) for g in genre_items if g.get_text(strip=True)
            )

        # ---- システム情報: 営業時間・定休日 ----
        for sys_item in soup.select(".system-item"):
            title_el = sys_item.select_one(".title")
            value_el = sys_item.select_one(".value")
            if not title_el or not value_el:
                continue
            title = title_el.get_text(strip=True)
            value_text = _WS_RE.sub(" ", value_el.get_text(separator=" ", strip=True)).strip()
            if title == "営業時間":
                data.setdefault(Schema.TIME, value_text)
            elif title == "定休日":
                data.setdefault(Schema.HOLIDAY, value_text)

        # ---- SNS ----
        insta_el = soup.find(id="ga-store-instagram-click")
        if insta_el:
            data[Schema.INSTA] = insta_el.get("href", "")

        line_el = soup.find(id="ga-store-line-click")
        if line_el:
            data[Schema.LINE] = line_el.get("href", "")

        # X/Twitter: リクルートセクション内の x.com / twitter.com リンク
        for a in soup.select(".recruit-social-item-container a[href]"):
            href = a.get("href", "")
            if "x.com" in href or "twitter.com" in href:
                data[Schema.X] = href
                break

        return data


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BarFindScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://bar-find.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
