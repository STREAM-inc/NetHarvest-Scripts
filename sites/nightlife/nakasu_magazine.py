"""
中洲マガジン (nakasu-magazine.com) — 中洲「大人の遊び」ブログ掲載店舗スクレイパー

取得対象:
    - カテゴリ「大人の遊び」配下のランキング記事 (例: "中洲の遊び場！大人も楽しめるTop20")
      に掲載された各店舗の店舗情報テーブル
    - 店舗名 / 都道府県 / 郵便番号 / 住所 / TEL / 営業時間 / 定休日 / 店舗HP
    - サイト固有 (EXTRA): 評価(★+口コミ数) / アクセス / 料金 / 掲載記事タイトル

取得フロー (一覧 → 詳細記事 → 店舗テーブル):
    1. カテゴリ一覧ページ (引数 url) から記事詳細リンク (.post-title a) を収集
    2. ページネーション ({url}page/{n}/) を末尾まで巡回
    3. 各記事詳細で「第N位　{店舗名}」h2 → 直後の情報テーブルを解析
    4. 店舗テーブル 1 件を解析するごとに即 yield (Pattern B)

    ※ 記事本文(店舗紹介の自由記述プロース)は著作権配慮のため取得しない。
      テーブル内の構造化された事実情報 (住所/電話/営業時間/定休日/料金/評価) のみを対象とする。
    ※ 利用規約 (privacy-policy) は「全文転載」「RSS盗用」を禁止するが、
      スクレイピング自体の明示的禁止は無く、引用元明示での引用は許諾されている。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/nakasu_magazine.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id nakasu_magazine
"""

from __future__ import annotations

import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import requests

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

# "第1位　GALA RESORT NAKASU" の順位プレフィックスを除去する
_RANK_RE = re.compile(r"^第\s*\d+\s*位[　\s:：.．]*")
# 郵便番号 (〒810-0801 / 810-0801)
_POSTCODE_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")
# 都道府県
_PREF_RE = re.compile(
    r"(北海道|東京都|京都府|大阪府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# テーブル行ラベル → EXTRA カラム名 (Schema に該当しない構造化情報)
#   ※ 料金/評価/アクセスは短い構造化情報 (自由記述プロースではない) であり、
#     呼び出し備考でも明示的に取得対象として指定されている。
_MAX_PAGES = 30  # ページネーションの安全上限


class NakasuMagazine(StaticCrawler):
    """中洲マガジン スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["評価", "アクセス", "料金", "掲載記事"]

    def parse(self, url: str):
        seen_articles: set[str] = set()
        page = 1
        while page <= _MAX_PAGES:
            list_url = url if page == 1 else urljoin(url if url.endswith("/") else url + "/", f"page/{page}/")
            try:
                soup = self.get_soup(list_url)
            except requests.exceptions.RequestException:
                # ページネーション末尾 (404 等)
                break
            if soup is None:
                break

            article_urls = []
            for a in soup.select("article .post-title a[href]"):
                href = a.get("href")
                if href and href not in seen_articles:
                    seen_articles.add(href)
                    article_urls.append(urljoin(list_url, href))

            if not article_urls:
                break

            for article_url in article_urls:
                try:
                    yield from self._scrape_article(article_url)
                except Exception:
                    logger.exception("記事の解析に失敗: %s", article_url)
                    continue

            page += 1

    def _scrape_article(self, article_url: str):
        """ランキング記事 1 本から、掲載された各店舗テーブルを yield する。"""
        soup = self.get_soup(article_url)
        if soup is None:
            return

        title_el = soup.select_one("h1")
        article_title = title_el.get_text(" ", strip=True) if title_el else ""

        for table in soup.select("table"):
            # このテーブル直前の h2 が店舗の見出し。「第N位　店舗名」形式のみ対象。
            h2 = table.find_previous("h2")
            if h2 is None:
                continue
            heading = h2.get_text(" ", strip=True)
            if not _RANK_RE.match(heading):
                continue
            name = _RANK_RE.sub("", heading).strip()
            if not name:
                continue

            rows = self._table_rows(table)
            if not rows:
                continue

            item = {
                Schema.URL: article_url,
                Schema.NAME: name,
                Schema.TEL: rows.get("電話", ""),
                Schema.TIME: rows.get("営業時間", ""),
                Schema.HOLIDAY: rows.get("定休日", ""),
                Schema.HP: rows.get("URL", ""),
                "評価": rows.get("評価", ""),
                "アクセス": rows.get("アクセス", ""),
                "料金": rows.get("料金", ""),
                "掲載記事": article_title,
            }

            # 住所 → 郵便番号 / 都道府県 / 住所 に分解
            raw_addr = rows.get("住所", "")
            item[Schema.POST_CODE] = self._extract_postcode(raw_addr)
            pref, addr = self._split_pref(raw_addr)
            item[Schema.PREF] = pref
            item[Schema.ADDR] = addr

            yield item

    @staticmethod
    def _table_rows(table) -> dict[str, str]:
        """2 列 (ラベル td / 値 td) テーブルを {ラベル: 値} 辞書に変換する。"""
        rows: dict[str, str] = {}
        for tr in table.select("tr"):
            tds = tr.find_all("td", recursive=False)
            if len(tds) < 2:
                continue  # 地図 iframe 等の colspan 行はスキップ
            label = tds[0].get_text(" ", strip=True)
            value = tds[1].get_text(" ", strip=True)
            if label and label not in rows:
                rows[label] = value
        return rows

    @staticmethod
    def _extract_postcode(text: str) -> str:
        m = _POSTCODE_RE.search(text)
        if not m:
            return ""
        code = m.group(1)
        return code if "-" in code else f"{code[:3]}-{code[3:]}"

    @staticmethod
    def _split_pref(text: str) -> tuple[str, str]:
        """住所文字列から都道府県と残りの住所を分離する (郵便番号は除去)。"""
        cleaned = _POSTCODE_RE.sub("", text).strip()
        m = _PREF_RE.search(cleaned)
        if not m:
            return "", cleaned
        pref = m.group(1)
        addr = cleaned[m.end():].strip()
        return pref, addr


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = NakasuMagazine()
    # 🔒 sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://nakasu-magazine.com/category/nakasu-adult/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
