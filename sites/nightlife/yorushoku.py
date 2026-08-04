"""
夜職倶楽部 (yorushoku.jp) — 全国ナイトワーク店舗情報スクレイパー

取得対象:
    - 全国の店舗情報（キャバクラ・ラウンジ・スナック・ガールズバー・コンカフェ等）

取得フロー:
    1. /shops を ?page=N でページネーション全件収集（1ページ200件 · 約79ページ · 約15,677件）
    2. 各店舗の詳細ページ（/shops/{slug}）から情報を抽出
       - 名称/住所/都道府県/電話番号は JSON-LD (LocalBusiness) から
       - 業種はページ見出しのカテゴリ表示から

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/yorushoku.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id yorushoku
"""

import json
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


# sites.yml に登録済みの正規ルート URL（parse() へ渡される）
ROOT_URL = "https://yorushoku.jp/job-list/"

# 店舗詳細 URL: /shops/{slug}
_DETAIL_RE = re.compile(r"^/shops/[a-z0-9][a-z0-9_-]*$", re.IGNORECASE)

# 巡回上限（暴走防止・想定 79 ページ）
_MAX_PAGES = 120

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def _strip_pref(pref: str, addr: str) -> str:
    """住所先頭の都道府県名を取り除いた残りを返す。"""
    if pref and addr.startswith(pref):
        return addr[len(pref):].strip()
    return addr.strip()


class YorushokuScraper(StaticCrawler):
    """夜職倶楽部 (yorushoku.jp) 店舗スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["業種"]

    def parse(self, url: str):
        # ルート URL の origin から店舗一覧 (/shops) を導出する
        shops_url = urljoin(url, "/shops")

        detail_urls: list[str] = []
        seen: set[str] = set()

        # Step 1: /shops?page=N を順に辿り、店舗詳細 URL を全件収集
        for page in range(1, _MAX_PAGES + 1):
            paged_url = shops_url if page == 1 else f"{shops_url}?page={page}"
            soup = self.get_soup(paged_url)
            if soup is None:
                break

            page_new = 0
            for a in soup.select("a[href^='/shops/']"):
                href = a.get("href", "").split("?")[0].split("#")[0]
                if not _DETAIL_RE.match(href):
                    continue
                detail_url = urljoin(shops_url, href)
                if detail_url not in seen:
                    seen.add(detail_url)
                    detail_urls.append(detail_url)
                    page_new += 1

            # このページから 1 件も新規リンクが無ければ末尾とみなす
            if page_new == 0:
                break

        self.total_items = len(detail_urls)
        self.logger.info("収集した店舗数: %d", self.total_items)

        # Step 2: 詳細ページスクレイピング
        for detail_url in detail_urls:
            item = self._scrape_detail(detail_url)
            if item:
                yield item

    def _extract_localbusiness(self, soup) -> dict:
        """JSON-LD の LocalBusiness ノードを辞書で返す（無ければ空辞書）。"""
        for script in soup.find_all("script", type="application/ld+json"):
            raw = script.string or script.get_text()
            if not raw or "LocalBusiness" not in raw:
                continue
            try:
                data = json.loads(raw)
            except (ValueError, TypeError):
                continue
            nodes = data.get("@graph", data) if isinstance(data, dict) else data
            if isinstance(nodes, dict):
                nodes = [nodes]
            for node in nodes:
                if isinstance(node, dict) and node.get("@type") == "LocalBusiness":
                    return node
        return {}

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        try:
            lb = self._extract_localbusiness(soup)
            address = lb.get("address", {}) if isinstance(lb.get("address"), dict) else {}

            # 名称: JSON-LD → h1 フォールバック
            name = _clean(lb.get("name", ""))
            if not name:
                h1 = soup.select_one("h1")
                name = _clean(h1.get_text(strip=True)) if h1 else ""

            # 都道府県・住所: JSON-LD → 「住所」ラベルフォールバック
            pref = _clean(address.get("addressRegion", ""))
            raw_addr = _clean(address.get("streetAddress", ""))
            if not raw_addr:
                for lbl in soup.find_all("span", class_="font-bold"):
                    if lbl.get_text(strip=True).startswith("住所"):
                        block = _clean(lbl.parent.get_text(" ", strip=True))
                        raw_addr = block.split(":", 1)[-1].strip() if ":" in block else ""
                        break
            if not pref:
                m = _PREF_PATTERN.match(raw_addr)
                if m:
                    pref = m.group(1)
            addr = _strip_pref(pref, raw_addr)

            # 電話番号
            tel = _clean(lb.get("telephone", ""))
            if not tel:
                tel_a = soup.select_one("a[href^='tel:']")
                if tel_a:
                    tel = tel_a.get("href", "").replace("tel:", "").strip()

            # 業種: 見出し直下のカテゴリ表示 (div.mt-2 内の最初の font-medium span)
            cat_site = ""
            for span in soup.find_all("span", class_="font-medium"):
                if span.find_parent("div", class_="mt-2") is not None:
                    cat_site = _clean(span.get_text(strip=True))
                    break

            return {
                Schema.URL:      url,
                Schema.NAME:     name,
                Schema.PREF:     pref,
                Schema.ADDR:     addr,
                Schema.TEL:      tel,
                Schema.CAT_SITE: cat_site,
                "業種":          cat_site,
            }
        except Exception as e:
            self.logger.error("詳細取得失敗 %s: %s", url, e)
            return None


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = YorushokuScraper()
    scraper.execute(ROOT_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
