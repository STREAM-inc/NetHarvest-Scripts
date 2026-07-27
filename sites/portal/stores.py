"""
STORES 導入事例 (stores.fun/cases) — 導入事例(お客さま事例)一覧スクレイパー

取得対象:
    - STORES を導入した店舗・事業者の事例記事。店舗名・業種・運営会社・
      取材対象者(代表者)・導入サービス・所在地(都道府県) を取得する。

取得フロー:
    1. /sitemap.xml を取得し、/cases/posts/{slug} の事例URLを列挙 (約92件)。
    2. 各事例詳細ページを1件ずつ取得し、ヘッダーブロックの構造化情報を
       抽出して即 yield する (list→detail / 取得即 yield)。

備考:
    - 詳細ページのインタビュー本文・導入の背景/効果などの自由記述プロースは
      著作権リスクのため取得しない (構造化された短い値のみ取得)。
    - フィルター指示は無いため全件取得する。

実行方法:
    # ローカルテスト
    python scripts/sites/portal/stores.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id stores
"""

import re
import sys
import warnings
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import XMLParsedAsHTMLWarning

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# get_soup() は html.parser で sitemap.xml をパースするため警告が出る。動作に支障はないので抑制する。
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# 都道府県 (リード文から所在地の都道府県のみを抽出する)
_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


class Stores(StaticCrawler):
    """STORES 導入事例 スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["運営会社", "取材対象者", "導入サービス"]

    def parse(self, url: str):
        # url = sites.yml の url (https://stores.fun/cases) を唯一のルートとする。
        sitemap_url = urljoin(url, "/sitemap.xml")
        sitemap = self.get_soup(sitemap_url)
        if sitemap is None:
            return

        post_urls = []
        seen = set()
        for loc in sitemap.select("loc"):
            u = loc.get_text(strip=True)
            if "/cases/posts/" in u and u not in seen:
                seen.add(u)
                post_urls.append(u)

        self.total_items = len(post_urls)

        for detail_url in post_urls:
            try:
                item = self._scrape_detail(detail_url)
            except Exception as e:  # noqa: BLE001 — 個別記事の失敗は記録して継続
                self.logger.warning("詳細ページ取得失敗 (スキップ): %s — %s", detail_url, e)
                continue
            if item:
                yield item

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        name_el = soup.select_one("h1")
        if name_el is None:
            return None
        name = name_el.get_text(strip=True)

        # 業種 (CAT_SITE): ヘッダーのバッジのうち role="listitem" でない最初のもの。
        industry = ""
        for badge in soup.select("div.rounded-l.font-semibold"):
            if badge.get("role") != "listitem":
                industry = badge.get_text(strip=True)
                break

        # 導入サービス: ヘッダー先頭の role="list" 内の listitem。
        services = []
        lst = soup.select_one('div[role="list"]')
        if lst is not None:
            services = [d.get_text(strip=True) for d in lst.select('div[role="listitem"]')]

        # 運営会社 + 取材対象者: ヘッダーの会社名 + 取材対象者ブロック。
        company = ""
        interviewee = ""
        interv_el = soup.select_one("div.text-body-m.text-txt-secondary")
        if interv_el is not None:
            interviewee = interv_el.get_text(strip=True)
            prev = interv_el.find_previous_sibling("div")
            if prev is not None:
                company = prev.get_text(strip=True)

        # 取材対象者から代表者名 (REP_NM) と役職 (POS_NM) を分離する。
        rep_nm = ""
        pos_nm = ""
        if interviewee:
            m = re.match(r"(.+?)（(.+?)）", interviewee)
            base = m.group(1) if m else interviewee
            pos_nm = m.group(2).strip() if m else ""
            rep_nm = re.sub(r"\s*(さま|様|さん)\s*$", "", base).strip()

        # 都道府県: リード文 (自由記述) から都道府県表記のみを抽出する。
        pref = ""
        lead = soup.select_one("div.rounded-2xl.bg-bg-primary")
        if lead is not None:
            pm = _PREF_RE.search(lead.get_text(" ", strip=True))
            if pm:
                pref = pm.group(1)

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.REP_NM: rep_nm,
            Schema.POS_NM: pos_nm,
            Schema.CAT_SITE: industry,
            Schema.URL: url,
            "運営会社": company,
            "取材対象者": interviewee,
            "導入サービス": " / ".join(services),
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Stores()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://stores.fun/cases")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
