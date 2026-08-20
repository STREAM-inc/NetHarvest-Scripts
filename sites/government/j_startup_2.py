"""
J-Startup (経済産業省認定スタートアップ一覧) — 選定企業クローラー

取得対象:
    経済産業省が推進するスタートアップ支援プログラム「J-Startup」の選定企業一覧
    (https://www.j-startup.go.jp/startups/)。2026-08 時点で 267 社。
    カテゴリ (10 分野) / A~Z のフィルタは JavaScript によるクライアント側の絞り込みで、
    HTML には最初から全件が含まれている (= ページネーション無し・1 ページで全件列挙)。

取得フロー:
    1. 起点 URL (= sites.yml の url) を GET し、`#startupslist .item > a` から企業カードを抽出。
       カードには 企業名 (p.company-name) / 分野 (data-cats) / 頭文字 (data-atoz) /
       J-Startup Impact 選定フラグ (data-impact) / ロゴ画像 が入っている。
    2. 各カードの href (例 /startups/001-architek.html) を起点 URL からの相対で解決し、
       日本語詳細ページ → 英語詳細ページ の順に取得して 1 社ごとに即 yield する
       (全件バッファしない)。
       - 日本語詳細: h2=社名 / ul.cats li=分野 / p.nums「法人番号｜…」/
         .btm-arrow-blank a=公式サイト URL / #newslist a.item=関連ニュース
       - 英語詳細 (link[hreflang="en"] から解決): h2=英語表記の法人名
    3. 詳細ページの取得に失敗した場合も、一覧カードで得た情報だけで yield して企業を落とさない。

備考 (呼び出し指示への対応):
    - 認定区分: 一覧カードの data-impact="impact" が「J-Startup Impact」選定企業
      (2026-08 時点 30 社) を示す。詳細ページ側には認定区分の表記が無いため、
      一覧カードの属性が唯一の判別材料。
    - 分野表記の「製造/素材･マテリアル」(半角中黒) と「製造/素材・マテリアル」は
      同一分野の表記ゆれなので全角「・」に正規化する。
      1 社 (KAPOK JAPAN) は一覧・詳細とも分野未設定のため空文字が正しい。
    - 法人番号は 12 桁掲載 (例 400001013081 / Homura) やハイフン付き掲載もあるため、
      掲載されている文字列をそのまま出力する。
    - 企業紹介文 (詳細ページの p.m-bottom0) と関連ニュース記事タイトルは長文/見出しの
      自由記述のため、著作権リスクを避けて取得しない (Schema.LOB / DESCRIPTION も空)。
      関連ニュースは「件数」と「最新日付」という構造化された値のみ取得する。
    - 住所 / 電話番号 / 代表者名 / 設立日 はサイト上に一切掲載が無いため取得不可 (空欄)。
    - 利用規約: 当サイトに利用規約・サイトポリシーページは存在せず (/terms /sitemap.xml
      /robots.txt いずれも 404)、About ページを含めスクレイピング・自動取得を禁止する
      記載は無い。よって取得を継続する。

実行方法:
    # ローカルテスト
    python scripts/sites/government/j_startup_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id j_startup_2
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

# 詳細 URL 末尾から掲載番号を取得: /startups/001-architek.html → 001, /startups/026a-innoqua.html → 026a
_CODE_RE = re.compile(r"/(\d+[a-z]?)-[^/]*\.html$", re.IGNORECASE)

# 法人番号: 「法人番号｜9120001166373」形式 (ハイフン付き掲載もあるため - も許容)
_CO_NUM_RE = re.compile(r"法人番号\s*[｜|:：]\s*([0-9\-]+)")


class JStartup2(StaticCrawler):
    """J-Startup 認定スタートアップ一覧 スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "認定区分",         # J-Startup / J-Startup Impact
        "掲載番号",         # 一覧内の企業掲載番号 (001, 026a 等)
        "頭文字",           # A~Z 検索用の頭文字 (data-atoz)
        "英語表記名称",     # 英語ページの法人名 (例 ArchiTek Corporation)
        "ロゴ画像URL",
        "英語ページURL",
        "関連ニュース件数",
        "最新関連ニュース日",  # YYYY-MM-DD
    ]

    # ------------------------------------------------------------------ 一覧
    def parse(self, url: str):
        soup = self.get_soup(url)
        if soup is None:
            logger.error("一覧ページを取得できませんでした: %s", url)
            return

        cards = soup.select("#startupslist .item > a[href]")
        if not cards:
            logger.error("企業カードを検出できませんでした (セレクタ要確認): %s", url)
            return

        self.total_items = len(cards)
        logger.info("一覧から %d 件の企業カードを検出しました", len(cards))

        seen: set[str] = set()
        for card in cards:
            href = (card.get("href") or "").strip()
            if not href:
                continue
            detail_url = urljoin(url, href)
            if detail_url in seen:
                continue
            seen.add(detail_url)
            # 1 社取得するごとに即 yield (全件バッファしない)
            yield self._build_item(detail_url, card)

    # ------------------------------------------------------------------ 詳細
    def _build_item(self, detail_url: str, card) -> dict:
        """一覧カード + 日本語詳細 + 英語詳細をマージして 1 社分の項目を組み立てる。"""
        name_el = card.select_one("p.company-name")
        item = {
            Schema.URL: detail_url,
            Schema.NAME: name_el.get_text(strip=True) if name_el else "",
            Schema.CO_NUM: "",
            Schema.CAT_SITE: self._normalize_cats(
                (card.get("data-cats") or "").split(",")
            ),
            Schema.HP: "",
            "認定区分": "J-Startup Impact" if (card.get("data-impact") or "").strip() else "J-Startup",
            "掲載番号": self._extract_code(detail_url),
            "頭文字": (card.get("data-atoz") or "").strip(),
            "英語表記名称": "",
            "ロゴ画像URL": self._img_url(detail_url, card),
            "英語ページURL": "",
            "関連ニュース件数": "",
            "最新関連ニュース日": "",
        }

        soup = self.get_soup(detail_url)
        if soup is None:
            logger.warning("詳細ページを取得できませんでした (一覧情報のみ): %s", detail_url)
            return item

        main = soup.select_one("#contentArea") or soup

        # 企業名 (詳細ページの h2 を優先)
        h2 = main.select_one("h2")
        if h2 and h2.get_text(strip=True):
            item[Schema.NAME] = h2.get_text(strip=True)

        # 分野 (J-Startup 10 分野)
        cats = [li.get_text(strip=True) for li in main.select("ul.cats li")]
        if any(cats):
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
            href = (hp.get("href") or "").strip()
            if href.startswith(("http://", "https://")):
                item[Schema.HP] = href

        # 関連ニュース (件数と最新日付のみ。記事タイトルは取得しない)
        news = main.select("#newslist a.item")
        item["関連ニュース件数"] = str(len(news))
        dates = sorted(
            (t.get("datetime", "").strip() for a in news for t in a.select("time[datetime]")),
            reverse=True,
        )
        if dates:
            item["最新関連ニュース日"] = dates[0]

        # 英語ページ (link[hreflang="en"]) から英語表記名称を取得
        en_link = soup.select_one('link[rel="alternate"][hreflang="en"][href]')
        if en_link:
            en_url = urljoin(detail_url, (en_link.get("href") or "").strip())
            item["英語ページURL"] = en_url
            item["英語表記名称"] = self._fetch_en_name(en_url)

        return item

    def _fetch_en_name(self, en_url: str) -> str:
        """英語ページの h2 から英語表記の法人名を取得する。取得失敗時は空文字。"""
        en_soup = self.get_soup(en_url)
        if en_soup is None:
            logger.warning("英語ページを取得できませんでした: %s", en_url)
            return ""
        en_main = en_soup.select_one("#contentArea") or en_soup
        en_h2 = en_main.select_one("h2")
        return en_h2.get_text(strip=True) if en_h2 else ""

    # ------------------------------------------------------------------ helper
    @staticmethod
    def _normalize_cats(cats) -> str:
        """分野を正規化して連結する (半角中黒 ･ → 全角 ・ の表記ゆれを吸収し重複を除去)。"""
        out: list[str] = []
        for c in cats:
            c = (c or "").strip().replace("･", "・")
            if c and c not in out:
                out.append(c)
        return "、".join(out)

    @staticmethod
    def _extract_code(detail_url: str) -> str:
        m = _CODE_RE.search(detail_url)
        return m.group(1) if m else ""

    @staticmethod
    def _img_url(detail_url: str, card) -> str:
        img = card.select_one("img[src]")
        if not img:
            return ""
        return urljoin(detail_url, (img.get("src") or "").strip())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = JStartup2()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.j-startup.go.jp/startups/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
