"""
リクナビ新卒 (job.rikunabi.com) — 掲載企業 会社概要スクレイパー

取得対象 (サイトマップ列挙 → 企業詳細ページで完結):
    - 会社名 / TEL / 都道府県 / 住所 / 代表者名 / 企業HP / 業種 / 掲載URL

取得フロー:
    企業一覧・検索結果ページは巡回しない。robots.txt が公開している
    サイトマップインデックス `/SitemapIndex_companyjobs.xml`
    → 子サイトマップ (約 7,019 件の /company_jobs/<id>/ URL) を唯一の
    列挙ソースとして使用する。

    各企業詳細ページは Next.js の SSR で、初期 HTML 内の
    <script id="__NEXT_DATA__"> に会社概要データ (companyOverview) が
    JSON で埋め込まれている (Static 取得可)。1 件取得するたびに即 yield
    する (Pattern B: 途中 break しても無駄な通信が起きない)。

    会社概要に存在するのは
        companyNameDetail / address / ceoName / phoneNumber / corporateWebsite
    と、トップレベルの industry (業種) のみ。備考で挙がっていた
    代表者役職 / 資本金 / 従業員数 / 設立年月 はサイトが一切露出していない
    ため取得しない (データ欠落)。

    /applicate /my_page /favorite_jobs 等の会員専用パスにはアクセスしない
    (サイトマップ列挙のみを使うため構造上到達しない)。サーバー負荷に配慮し
    DELAY=2.5 秒。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/job_6.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id job_6
"""

import json
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


# 企業詳細 URL の列挙ソース。ルート URL から派生させる (別ルートのハードコード禁止)。
_SITEMAP_INDEX_PATH = "/SitemapIndex_companyjobs.xml"

# 有効な企業詳細 URL パターン (会員専用パス等を誤って拾わないため)
_COMPANY_URL_RE = re.compile(r"/company_jobs/\d+/?$")

# 都道府県 (住所の先頭から都道府県を分割するため)
_PREF = (
    r"北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県"
    r"|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県"
    r"|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県"
    r"|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県"
    r"|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
_PREF_RE = re.compile(_PREF)


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"[ \t　]+", " ", str(s).replace("\r", "")).strip()


class RikunabiShinsotsuScraper(StaticCrawler):
    """リクナビ新卒 掲載企業 会社概要スクレイパー"""

    DELAY = 2.5
    # 業種は Schema.CAT_SITE にマッピングするため EXTRA は無し。
    EXTRA_COLUMNS = []

    def parse(self, url: str) -> Generator[dict, None, None]:
        # ルート URL (= sites.yml の url) からサイトマップインデックスを派生。
        index_url = urljoin(url, _SITEMAP_INDEX_PATH)
        index_soup = self.get_soup(index_url)
        if index_soup is None:
            self.logger.error("サイトマップインデックス取得失敗: %s", index_url)
            return

        child_urls = [
            _clean(loc.get_text())
            for loc in index_soup.find_all("loc")
            if _clean(loc.get_text())
        ]
        self.logger.info("子サイトマップ %d 件", len(child_urls))

        # 子サイトマップから企業詳細 URL を列挙 (重複排除・順序維持)
        detail_urls: list[str] = []
        seen: set[str] = set()
        for child_url in child_urls:
            child_soup = self.get_soup(child_url)
            if child_soup is None:
                self.logger.warning("子サイトマップ取得失敗: %s", child_url)
                continue
            for loc in child_soup.find_all("loc"):
                u = _clean(loc.get_text())
                if u and _COMPANY_URL_RE.search(u) and u not in seen:
                    seen.add(u)
                    detail_urls.append(u)

        self.total_items = len(detail_urls)
        self.logger.info("企業詳細 URL %d 件を列挙", self.total_items)

        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
            except Exception as e:  # 個別エラーはスキップして継続
                self.logger.warning("詳細取得失敗: %s — %s", detail_url, e)
                continue
            if item:
                yield item

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        script = soup.find("script", id="__NEXT_DATA__")
        if script is None or not script.string:
            return None
        try:
            data = json.loads(script.string)
        except (ValueError, TypeError):
            return None

        d = (
            data.get("props", {})
            .get("pageProps", {})
            .get("data")
        )
        if not isinstance(d, dict):
            return None

        overview = d.get("companyOverview") or {}
        name = _clean(overview.get("companyNameDetail") or d.get("employerName"))
        if not name:
            return None

        item = {
            Schema.NAME: name,
            Schema.URL: _clean(d.get("canonical")) or detail_url,
            Schema.TEL: _clean(overview.get("phoneNumber")),
            Schema.REP_NM: _clean(overview.get("ceoName")),
            Schema.HP: _clean(overview.get("corporateWebsite")),
            Schema.CAT_SITE: _clean(d.get("industry")),
            Schema.PREF: "",
            Schema.ADDR: "",
        }

        addr = _clean(overview.get("address"))
        if addr:
            pm = _PREF_RE.search(addr)
            if pm:
                item[Schema.PREF] = pm.group(0)
                item[Schema.ADDR] = addr[pm.start():].strip()
            else:
                item[Schema.ADDR] = addr

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = RikunabiShinsotsuScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://job.rikunabi.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
