"""
日本仕事百貨 (shigoto100) — 求人一覧クローラー

取得対象:
    - https://shigoto100.com/job の求人一覧 (ページネーション: /job/page/{n})
    - 各求人の詳細ページ (募集要項) から会社情報・勤務条件を取得

取得フロー:
    一覧ページ (box-post-card-01) から詳細URLを取得 → 詳細ページの
    募集要項 (div.list-info の data-title / data-detail ペア) を解析。
    詳細を1件取得するごとに即 yield する (Pattern B)。

著作権配慮:
    日本仕事百貨は長文の編集記事 (仕事内容・求める人物像・選考プロセス・その他・
    本文ストーリー等) が中心のため、これら自由記述プロースは取得しない。
    取得するのは募集要項の構造化された事実情報 (会社名/勤務地/勤務時間/休日/
    募集職種/雇用形態/給与/募集期間/採用予定人数) に限定する。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/shigoto100.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id shigoto100
"""

import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# 都道府県抽出用 (勤務地テキスト内に現れる最初の都道府県を拾う)
_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


class Shigoto100(StaticCrawler):
    """日本仕事百貨 スクレイパー"""

    DELAY = 1.5

    # Schema に無いサイト固有の構造化フィールド (自由記述プロースは含めない)
    EXTRA_COLUMNS = [
        "募集職種",
        "雇用形態",
        "給与",
        "募集期間",
        "採用予定人数",
    ]

    # 詳細ページ募集要項のラベル → EXTRA カラム名 の対応
    _EXTRA_LABELS = {
        "募集職種": "募集職種",
        "雇用形態": "雇用形態",
        "給与": "給与",
        "募集期間": "募集期間",
        "採用予定人数": "採用予定人数",
    }

    @staticmethod
    def _text(node) -> str:
        """<br> を含むノードを 1 行の文字列に正規化して返す。"""
        if node is None:
            return ""
        return " ".join(node.get_text(" ", strip=True).split())

    def parse(self, url: str):
        page = 1
        while True:
            page_url = url if page == 1 else f"{url}/page/{page}"
            soup = self.get_soup(page_url)
            if soup is None:
                break

            cards = soup.select("div.box-post-card-01")
            if not cards:
                break

            # 初回ページでページャ最終番号から総件数を概算 (進捗表示用)
            if page == 1:
                self.total_items = self._estimate_total(soup, len(cards))

            for card in cards:
                link = card.select_one("a.link-group[href]")
                if not link:
                    continue
                detail_url = link.get("href")
                try:
                    item = self._scrape_detail(detail_url)
                except Exception as e:  # noqa: BLE001 — 個別失敗は握りつぶして継続
                    self.logger.warning("詳細取得失敗: %s — %s", detail_url, e)
                    continue
                if item:
                    yield item

            page += 1

    def _estimate_total(self, soup, per_page: int) -> int:
        """ページャの最終ページ番号 × 1ページあたり件数 で総件数を概算。"""
        max_page = 1
        pager = soup.select_one('[class*="pagin"]')
        if pager:
            for a in pager.select("a[href]"):
                m = re.search(r"/page/(\d+)", a.get("href") or "")
                if m:
                    max_page = max(max_page, int(m.group(1)))
        return max_page * per_page

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        item = {Schema.URL: detail_url}

        info = soup.select_one("div.list-info")

        # 会社名 + HP (募集要項先頭の data-title h2)
        if info is not None:
            h2 = info.select_one("div.data-title h2")
            if h2:
                item[Schema.NAME] = self._text(h2)
                a = h2.find("a", href=True)
                if a:
                    item[Schema.HP] = a.get("href").strip()

            # data-title (h3) → 直後の data-detail を値とする
            for dt in info.select("div.data-title"):
                h3 = dt.find("h3")
                if not h3:
                    continue
                label = self._text(h3)
                dd = dt.find_next_sibling("div", class_="data-detail")
                value = self._text(dd)
                if not value:
                    continue

                if label == "勤務地":
                    item[Schema.ADDR] = value
                    m = _PREF_PATTERN.search(value)
                    if m:
                        item[Schema.PREF] = m.group(1)
                elif label == "勤務時間":
                    item[Schema.TIME] = value
                elif label == "休日休暇":
                    item[Schema.HOLIDAY] = value
                elif label in self._EXTRA_LABELS:
                    item[self._EXTRA_LABELS[label]] = value
                # 上記以外 (仕事内容・待遇・応募資格・求める人物像・選考プロセス・
                # その他 等) は自由記述プロースのため取得しない (著作権配慮)

        # NAME が取れない場合は title タグから会社名を補完
        if not item.get(Schema.NAME) and soup.title:
            # "求人タイトル / 会社名 / 日本仕事百貨" の 2 番目が会社名
            parts = [p.strip() for p in soup.title.get_text().split("/")]
            if len(parts) >= 3:
                item[Schema.NAME] = parts[-2]

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Shigoto100()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://shigoto100.com/job")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
