"""
浜松ナイトガイド — 浜松市のナイトスポット(スナック・バー・ラウンジ等)店舗情報

取得対象:
    浜松 night Guide (guide-jp.com/hamamatsu-night) に掲載された全店舗の
    基本情報 (店名・ジャンル・住所・TEL・営業時間・定休日・カード・駐車場・
    リンク(HP/SNS)・備考・エリア)

取得フロー:
    1. ルート URL から sitemap.xml を取得し、店舗ページ URL を列挙
       (single-segment slug のみ。category/page/固定ページ等は除外)
    2. 店舗ページごとに table.information を解析し、取得即 yield (Pattern B)

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/guide_jp.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id guide_jp
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup
from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PHONE_RE = re.compile(r"0\d{1,4}[-(]?\d{1,4}[-)]?\d{3,4}")
_PREF_RE = re.compile(
    r"^(北海道|東京都|大阪府|京都府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|新潟|富山|"
    r"石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|和歌山|鳥取|島根|"
    r"岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|"
    r"沖縄)県?)"
)

# sitemap の <loc> のうち店舗ページではないもの (ルート直下の単一スラッグ)
_NON_SHOP_SLUGS = {"contact", "guideline"}


class GuideJpCrawler(StaticCrawler):
    """浜松ナイトガイド スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["エリア", "アクセス", "駐車場", "備考"]

    def parse(self, url: str):
        base = url.rstrip("/")
        root_prefix = base + "/"
        sitemap_url = urljoin(root_prefix, "sitemap.xml")

        shop_urls = self._list_shop_urls(sitemap_url, root_prefix)
        self.total_items = len(shop_urls)
        self.logger.info("店舗ページ数: %d", len(shop_urls))

        for shop_url in shop_urls:
            try:
                record = self._scrape_detail(shop_url)
            except Exception as e:
                self.logger.warning("詳細パース失敗 %s: %s", shop_url, e)
                continue
            if record:
                yield record

    # ------------------------------------------------------------------
    # 一覧 (sitemap)
    # ------------------------------------------------------------------

    def _list_shop_urls(self, sitemap_url: str, root_prefix: str) -> list[str]:
        soup = self.get_soup(sitemap_url)
        if soup is None:
            return []

        urls: list[str] = []
        seen: set[str] = set()
        for loc in soup.find_all("loc"):
            href = loc.get_text(strip=True)
            if not href.startswith(root_prefix):
                continue
            rel = href[len(root_prefix):].strip("/")
            # 店舗ページはルート直下の単一スラッグ。
            # category/<...>, page/<n>, wp-content/<...> 等はスラッシュを含むため除外。
            if not rel or "/" in rel:
                continue
            if rel in _NON_SHOP_SLUGS:
                continue
            if rel.endswith((".xml", ".xsl", ".jpg", ".jpeg", ".png", ".gif")):
                continue
            if href not in seen:
                seen.add(href)
                urls.append(href)
        return urls

    # ------------------------------------------------------------------
    # 詳細
    # ------------------------------------------------------------------

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        table = soup.select_one("table.information")
        if table is None:
            return None

        # th(ラベル) -> td(セル) のマップ
        rows: dict[str, "BeautifulSoup"] = {}
        for tr in table.select("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if th and td:
                rows[th.get_text(strip=True)] = td

        name = self._text(rows.get("店舗名"))
        if not name:
            return None

        # 住所 → 都道府県 / 住所 を分離
        addr_raw = self._text(rows.get("住所"))
        pref = ""
        addr = addr_raw
        m = _PREF_RE.match(addr_raw)
        if m:
            pref = m.group(1)
            addr = addr_raw[m.end():].strip()

        # 電話番号 (案内文が後続するため番号のみ抽出)
        tel = ""
        if rows.get("電話番号"):
            tm = _PHONE_RE.search(rows["電話番号"].get_text(" ", strip=True))
            if tm:
                tel = tm.group(0)

        # リンク (店舗の HP / SNS)。サイト自身の共有ボタン等は td 内に無いが念のため除外。
        hp = line = insta = x = fb = ""
        link_td = rows.get("リンク")
        if link_td:
            for a in link_td.find_all("a", href=True):
                href = a["href"].strip()
                low = href.lower()
                if "guide-jp.com" in low:
                    continue
                if "instagram.com" in low:
                    insta = insta or href
                elif "facebook.com" in low:
                    fb = fb or href
                elif "twitter.com" in low or "//x.com" in low or low.startswith("https://x.com"):
                    x = x or href
                elif "line.me" in low or "//line." in low:
                    line = line or href
                else:
                    hp = hp or href

        # エリア (投稿カテゴリ: 浜松市 > 区 > エリア)。先頭の市名は冗長なので除く。
        area_terms = [
            a.get_text(strip=True)
            for a in soup.select('span.entry-meta-items a[href*="/category/"]')
            if a.get_text(strip=True)
        ]
        if area_terms and area_terms[0] == "浜松市":
            area_terms = area_terms[1:]
        area = " ".join(area_terms)

        record = {
            Schema.NAME: name,
            Schema.URL: detail_url,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.CAT_SITE: self._text(rows.get("ジャンル")),
            Schema.TIME: self._text(rows.get("営業時間")),
            Schema.HOLIDAY: self._text(rows.get("定休日")),
            Schema.PAYMENTS: self._clean_dash(self._text(rows.get("カード"))),
            Schema.HP: hp,
            Schema.LINE: line,
            Schema.INSTA: insta,
            Schema.X: x,
            Schema.FB: fb,
            "エリア": area,
            "アクセス": self._text(rows.get("アクセス")),
            "駐車場": self._clean_dash(self._text(rows.get("駐車場"))),
            "備考": self._text(rows.get("備考")),
        }
        return record

    # ------------------------------------------------------------------
    # ユーティリティ
    # ------------------------------------------------------------------

    @staticmethod
    def _text(node) -> str:
        if node is None:
            return ""
        return node.get_text(" ", strip=True)

    @staticmethod
    def _clean_dash(value: str) -> str:
        # 「-」のみのセルは未設定とみなし空文字に
        return "" if value.strip() in {"-", "ー", "−", "—"} else value


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = GuideJpCrawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://guide-jp.com/hamamatsu-night/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
