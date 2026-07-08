"""
一般社団法人 栃木県警備業協会（AJSSA 会員名簿・栃木県）— 会員企業一覧

取得対象:
    - 栃木県警備業協会の会員企業（全社・約81社）
    - 会社名 / 所在地(市) / TEL / HP / 警備種別（施設/交通/貴重品/身辺/機械/保安）

取得フロー:
    会員名簿ページ (memberslist) に、会員企業が WordPress(Gutenberg/VK Blocks)の
    カラムブロックとして全件静的に掲載されている。ページネーション無し・詳細ページ無し。
    各会員カード (div.wp-block-columns.are-vertically-aligned-center) を 1 件ずつ即 yield する。

    カード構造 (3 カラム):
      col1: 会社名 (vk_dynamicText[0]) + HP(<a href>)
      col2: 所在地の市 (vk_dynamicText) + 電話番号 (vk_dynamicText)
      col3: 警備種別ボタン (span.vk_button_link_txt を複数)

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_8.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_8
"""

import logging
import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

_PHONE_RE = re.compile(r"0\d{1,4}-\d{1,4}-\d{4}")


class Ajssa8(StaticCrawler):
    """一般社団法人 栃木県警備業協会 会員名簿 スクレイパー"""

    DELAY = 1.5
    # 警備種別（施設/交通 等の短い構造化ラベル）は Schema.CAT_SITE。
    # サイト固有の追加カラムは無し。
    EXTRA_COLUMNS: list[str] = []

    def parse(self, url: str):
        # 引数 url (= sites.yml の url / 会員名簿ページ) を唯一のルートとして使う
        soup = self.get_soup(url)
        if soup is None:
            logger.warning("会員名簿の取得に失敗: %s", url)
            return

        cards = soup.select("div.wp-block-columns.are-vertically-aligned-center")
        self.total_items = len(cards)

        for card in cards:
            try:
                item = self._parse_card(card, url)
                if item:
                    yield item
            except Exception as e:  # 個別カードのエラーはスキップして継続
                logger.warning("カードの解析に失敗しskip: %s", e)
                continue

    def _parse_card(self, card, source_url: str) -> dict | None:
        cols = card.find_all("div", class_="wp-block-column", recursive=False)
        if len(cols) < 3:
            return None
        col1, col2, col3 = cols[0], cols[1], cols[2]

        # col1: 会社名（最初の dynamicText） + HP リンク
        dts1 = col1.select(".vk_dynamicText")
        name = dts1[0].get_text(strip=True) if dts1 else ""
        if not name:
            return None
        # HP は協会外部ドメインへの絶対 URL
        a = card.find(
            "a",
            href=lambda h: h and h.startswith("http") and "tochikeikyo" not in h,
        )
        hp = a["href"].strip() if a else ""

        # col2: 所在地(市) と 電話番号（dynamicText 2 つ。電話は正規表現で判定）
        texts2 = [d.get_text(strip=True) for d in col2.select(".vk_dynamicText")]
        tel = next((t for t in texts2 if _PHONE_RE.search(t)), "")
        addr = next((t for t in texts2 if t and not _PHONE_RE.search(t)), "")

        # col3: 警備種別ボタン（施設/交通/貴重品/身辺/機械/保安）
        gyoushu = [
            s.get_text(strip=True)
            for s in col3.select(".vk_button_link_txt")
            if s.get_text(strip=True)
        ]

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: "栃木県",  # 栃木県警備業協会の会員 = 全て栃木県
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: hp,
            Schema.CAT_SITE: "/".join(gyoushu),
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa8()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://tochikeikyo.or.jp/memberslist/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
