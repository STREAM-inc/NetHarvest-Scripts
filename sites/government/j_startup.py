"""
J-Startup (経済産業省 J-Startup 選定スタートアップ一覧) — 選定企業クローラー

取得対象:
    経済産業省が推進するスタートアップ育成支援プログラム「J-Startup」の
    選定企業一覧 (https://www.j-startup.go.jp/startups/) に掲載された全企業。
    2026-08 時点で 267 社。一覧はカテゴリ / A~Z のフィルタが付くが、
    フィルタは JavaScript によるクライアント側の絞り込みで、HTML には
    全件が最初から含まれている (= 1 ページで全件取得できる)。

取得フロー:
    1. 起点 URL (sites.yml の url = 選定企業一覧) を GET し、
       `#startupslist .item > a` から企業カード 267 件を抽出する。
       カード自体に企業名 / カテゴリ (data-cats) / J-Startup Impact 選定フラグ
       (data-impact) / ロゴ画像が入っている。
    2. 各カードの href (例 /startups/001-architek.html) を url からの相対で解決し、
       詳細ページを 1 件取得するごとに即 yield する (全件バッファしない)。
       詳細ページからは 法人番号 / 公式サイト URL / カテゴリ / 英語ページ URL を取得。
    3. 詳細ページの取得に失敗した場合も、一覧カードで得られた情報だけで yield し、
       企業を欠損させない。

備考 (呼び出し指示への対応):
    - 「10 分野フィルタは使わず全件」の指示どおり、data-cats による絞り込みは行わず
      一覧の全カードを対象にする (フィルタ処理は一切実装しない)。
    - 認定区分: 一覧カードの data-impact="impact" が「J-Startup Impact」選定企業
      (2026-08 時点 30 社) を表す。それ以外は「J-Startup」とする。
      詳細ページ側には認定区分の表記が無いため、一覧の属性のみが判別材料。
    - カテゴリ表記の「製造/素材･マテリアル」「製造/素材・マテリアル」は同一分野の
      半角/全角中黒ゆれなので、全角「・」に正規化する。1 社 (KAPOK JAPAN) は
      一覧・詳細ともカテゴリ未設定のため空文字になる。
    - 企業紹介文 (詳細ページの <p class="m-bottom0">) と関連ニュース記事は
      長文の自由記述のため、著作権リスクを避けて取得しない
      (備考に取得許可が無いため Schema.LOB / DESCRIPTION も空)。
    - 利用規約: 当サイトには利用規約・サイトポリシーページが存在せず
      (/terms /privacy /sitepolicy いずれも 404、robots.txt も 404)、
      About ページを含めスクレイピング・自動取得を禁止する記載は無い。
      よって取得を継続する。

実行方法:
    # ローカルテスト
    python scripts/sites/government/j_startup.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id j_startup
"""

import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 詳細ページ URL 末尾から掲載番号を取得: /startups/001-architek.html → 001, /startups/026a-innoqua.html → 026a
_CODE_RE = re.compile(r"/(\d+[a-z]?)-[^/]*\.html$", re.IGNORECASE)

# 法人番号: 「法人番号｜9120001166373」形式 (Luup 等は 0110-01-123515 のようにハイフン付きで掲載)
_CO_NUM_RE = re.compile(r"法人番号\s*[｜|:：]\s*([0-9\-]+)")


class JStartup(StaticCrawler):
    """J-Startup 選定スタートアップ一覧 スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "認定区分",       # J-Startup / J-Startup Impact
        "掲載番号",       # 一覧ページ内の企業掲載番号 (001, 026a 等)
        "ロゴ画像URL",
        "英語ページURL",
    ]

    def parse(self, url: str):
        soup = self.get_soup(url)
        if soup is None:
            logger.error("一覧ページを取得できませんでした: %s", url)
            return

        cards = soup.select("#startupslist .item > a[href]")
        self.total_items = len(cards)
        logger.info("一覧から %d 件の企業カードを検出しました", len(cards))

        seen: set[str] = set()
        for card in cards:
            detail_url = urljoin(url, card.get("href", "").strip())
            if not detail_url or detail_url in seen:
                continue
            seen.add(detail_url)
            yield self._scrape_detail(detail_url, card)

    # ------------------------------------------------------------------ 詳細
    def _scrape_detail(self, detail_url: str, card) -> dict:
        """詳細ページ + 一覧カードから 1 社分の項目を組み立てる。

        詳細ページが取れなかった場合も、一覧カードの情報だけで返す
        (企業を落とさないため)。
        """
        name_el = card.select_one(".company-name")
        item = {
            Schema.URL: detail_url,
            Schema.NAME: name_el.get_text(strip=True) if name_el else "",
            Schema.CO_NUM: "",
            Schema.CAT_SITE: self._normalize_cats(
                [c for c in (card.get("data-cats") or "").split(",") if c.strip()]
            ),
            Schema.HP: "",
            "認定区分": "J-Startup Impact" if (card.get("data-impact") or "").strip() else "J-Startup",
            "掲載番号": self._extract_code(detail_url),
            "ロゴ画像URL": self._logo_url(detail_url, card),
            "英語ページURL": "",
        }

        soup = self.get_soup(detail_url)
        if soup is None:
            logger.warning("詳細ページを取得できませんでした (一覧情報のみ): %s", detail_url)
            return item

        main = soup.select_one("#contentArea") or soup

        # 企業名 (詳細ページの h2 を優先。一覧カードと同一表記)
        h2 = main.select_one("h2")
        if h2 and h2.get_text(strip=True):
            item[Schema.NAME] = h2.get_text(strip=True)

        # カテゴリ (J-Startup 10 分野)
        cats = [li.get_text(strip=True) for li in main.select("ul.cats li") if li.get_text(strip=True)]
        if cats:
            item[Schema.CAT_SITE] = self._normalize_cats(cats)

        # 法人番号
        nums = main.select_one("p.nums")
        if nums:
            m = _CO_NUM_RE.search(nums.get_text(" ", strip=True))
            if m:
                item[Schema.CO_NUM] = m.group(1)

        # 公式サイト URL
        hp = main.select_one(".btm-arrow-blank a[href]")
        if hp:
            href = hp.get("href", "").strip()
            if href.startswith(("http://", "https://")):
                item[Schema.HP] = href

        # 英語ページ URL
        en = soup.select_one('link[rel="alternate"][hreflang="en"][href]')
        if en:
            item["英語ページURL"] = urljoin(detail_url, en.get("href", "").strip())

        return item

    # ------------------------------------------------------------------ helper
    @staticmethod
    def _normalize_cats(cats: list[str]) -> str:
        """カテゴリ配列を正規化して連結する (半角中黒 ･ → 全角 ・ の表記ゆれを吸収)。"""
        out: list[str] = []
        for c in cats:
            c = c.strip().replace("･", "・")
            if c and c not in out:
                out.append(c)
        return "、".join(out)

    @staticmethod
    def _extract_code(detail_url: str) -> str:
        m = _CODE_RE.search(detail_url)
        return m.group(1) if m else ""

    @staticmethod
    def _logo_url(detail_url: str, card) -> str:
        img = card.select_one("img[src]")
        if not img:
            return ""
        return urljoin(detail_url, img.get("src", "").strip())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = JStartup()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.j-startup.go.jp/startups/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
