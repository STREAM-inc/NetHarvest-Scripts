"""
AQUA MART — 熱帯魚店・アクアショップ検索サイト (全国のアクアリウムショップ・観賞魚専門店ディレクトリ)

取得対象:
    - 全国47都道府県のアクアリウムショップ (観賞魚・熱帯魚専門店) 情報
    - 店舗名 / 郵便番号 / 住所 / 都道府県 / 業種(サイト表記) / 公式サイトURL /
      電話番号 / 定休日 / 営業時間

取得フロー:
    ルート (/) から派生した 47 都道府県一覧ページ (/{pref}/) を巡回し、
    各ページに列挙された店舗詳細ページ URL (/{pref}/{area}/{shop}.html) を収集。
    ページ送りは無く、都道府県内の全店舗が 1 ページに列挙される。
    各詳細ページの dt/dd から情報を抽出し、1 件ずつ即 yield する (Pattern B)。
    同一店舗が「区・エリア別」と「ジャンル別」の両方に列挙されるため URL で重複排除する。

実行方法:
    # ローカルテスト
    python scripts/sites/service/aqua_mart.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id aqua_mart
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

# 全国 47 都道府県のローマ字スラッグ (一覧ページ /{slug}/ を構成する。ルート url から派生)
_PREF_SLUGS = [
    "hokkaido", "aomori", "iwate", "miyagi", "akita", "yamagata", "fukushima",
    "ibaraki", "tochigi", "gunma", "saitama", "chiba", "tokyo", "kanagawa",
    "niigata", "toyama", "ishikawa", "fukui", "yamanashi", "nagano", "gifu",
    "shizuoka", "aichi", "mie", "shiga", "kyoto", "osaka", "hyogo", "nara",
    "wakayama", "tottori", "shimane", "okayama", "hiroshima", "yamaguchi",
    "tokushima", "kagawa", "ehime", "kochi", "fukuoka", "saga", "nagasaki",
    "kumamoto", "oita", "miyazaki", "kagoshima", "okinawa",
]

# 都道府県抽出 (住所先頭)
_PREF_PATTERN = re.compile(r"(北海道|東京都|(?:京都|大阪)府|.{2,3}県)")
# 郵便番号 (先頭の 〒 を除去した数値部分)
_POST_PATTERN = re.compile(r"〒?\s*(\d{3}-?\d{4})\s*(.*)$")


class AquaMart(StaticCrawler):
    """AQUA MART (熱帯魚店検索サイト) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = []  # 備考指定の 9 カラムはすべて Schema にマッピング済み

    def parse(self, url: str):
        seen = set()
        for slug in _PREF_SLUGS:
            list_url = urljoin(url, f"{slug}/")
            soup = self.get_soup(list_url)
            if soup is None:
                continue

            # 詳細ページ URL: /{pref}/{area}/{shop}.html の形のみを対象に抽出
            detail_urls = []
            for a in soup.find_all("a", href=True):
                href = urljoin(list_url, a["href"])
                if re.search(rf"/{slug}/[^/]+/[^/]+\.html$", href) and href not in seen:
                    seen.add(href)
                    detail_urls.append(href)

            for detail_url in detail_urls:
                try:
                    item = self._scrape_detail(detail_url)
                    if item:
                        yield item
                except Exception as e:  # 個別店舗の失敗はログして継続
                    self.logger.warning("詳細ページ解析失敗: %s — %s", detail_url, e)

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        # 店舗名: 本文見出し h2
        name = soup.h2.get_text(" ", strip=True) if soup.h2 else ""

        # dt/dd から各項目を取得するヘルパ
        def dd_text(label: str) -> str:
            for dt in soup.find_all("dt"):
                if label in dt.get_text():
                    dd = dt.find_next_sibling("dd")
                    return dd.get_text(" ", strip=True) if dd else ""
            return ""

        # 住所 (〒郵便番号 + 都道府県 + 以降)
        raw_addr = dd_text("住所")
        post_code, address, pref = "", raw_addr, ""
        m = _POST_PATTERN.match(raw_addr)
        if m:
            post_code = m.group(1)
            address = m.group(2).strip()
        pm = _PREF_PATTERN.match(address)
        if pm:
            pref = pm.group(1)

        # 業種 (サイト表記): 「お取り扱いジャンル」のアイコン img の alt を収集
        genres = []
        for dt in soup.find_all("dt"):
            if "ジャンル" in dt.get_text():
                dd = dt.find_next_sibling("dd")
                if dd:
                    genres = [img.get("alt", "").strip() for img in dd.find_all("img")]
                    genres = [g for g in genres if g]
                break
        cat_site = " / ".join(genres)

        # 公式サイト URL: 「関連サイト」内の外部リンク (SNS/ブログ/自サイト内リンクを除く)
        hp = self._find_official_url(soup)

        return {
            Schema.NAME: name,
            Schema.POST_CODE: post_code,
            Schema.ADDR: address,
            Schema.PREF: pref,
            Schema.CAT_SITE: cat_site,
            Schema.HP: hp,
            Schema.TEL: dd_text("TEL"),
            Schema.HOLIDAY: dd_text("定休日"),
            Schema.TIME: dd_text("営業時間"),
            Schema.URL: url,
        }

    @staticmethod
    def _find_official_url(soup) -> str:
        """関連サイトセクションから店舗公式ホームページの外部 URL を 1 件返す。"""
        # 「関連サイト」見出し以降のリンクを優先的に走査
        anchors = soup.find_all("a", href=True)
        for h3 in soup.find_all("h3"):
            if "関連サイト" in h3.get_text():
                anchors = h3.find_all_next("a", href=True)
                break

        for a in anchors:
            href = a["href"].strip()
            text = a.get_text(strip=True)
            if not href.startswith("http"):
                continue
            if "aqua-mart.jp" in href:  # サイト内リンク (他店舗など) は除外
                continue
            if re.search(r"twitter|instagram|facebook|x\.com|tiktok|line\.me|youtube|/blog", href, re.I):
                continue  # SNS / ブログは公式サイトとしない
            if ("ホームページ" in text) or ("オフィシャル" in text) or ("公式" in text):
                return href
        return ""


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = AquaMart()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.aqua-mart.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
