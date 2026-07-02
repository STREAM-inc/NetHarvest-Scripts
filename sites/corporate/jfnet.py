"""
日本フードサービス協会【会員名簿】 — jfnet (https://www.jfnet.or.jp/)

取得対象:
    - 一般社団法人日本フードサービス協会の会員名簿
      (正会員 / 賛助会員) に掲載された会員企業一覧

取得フロー:
    ルート URL から会員名簿ページ (/about-jf/membership-list/) を導出し、
    静的 HTML 内に埋め込まれた全会員 (Alpine.js で頭文字フィルタ表示) を走査する。
    1 ページに 正会員・賛助会員 の全件が含まれるためページネーションは無い。
    詳細ページは存在せず (会員名は各社の外部サイトへのリンク)、一覧のみで完結する。

    データ構造:
        div.membership__list[x-show="page == 1"]  → 正会員
        div.membership__list[x-show="page == 2"]  → 賛助会員
          └ div[x-show="furigana == 'X'"]         → 会員 1 件
               ├ <a href>会社名</a>               → 名称 + HP
               └ div.--text                        → 主なブランド・店舗名 (正会員のみ)

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/jfnet.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id jfnet
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

# 会員名簿ページ (ルート URL からの相対パス)
_MEMBERSHIP_PATH = "about-jf/membership-list/"

# 会員区分の判定 (membership__list コンテナの x-show 値 → ラベル)
_MEMBER_TYPE = {"page == 1": "正会員", "page == 2": "賛助会員"}

# 会員 1 件を示す div の x-show 値: furigana == 'あ' など
_ITEM_XSHOW = re.compile(r"furigana ==")

# 末尾の「　他」「 他」等を除去するための正規表現
_TRAILING_HOKA = re.compile(r"[\s　]*他$")


class Jfnet(StaticCrawler):
    """日本フードサービス協会【会員名簿】 スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["会員区分", "主なブランド・店舗名"]

    def parse(self, url: str):
        # ルート URL を唯一の起点として会員名簿ページを導出する
        membership_url = urljoin(url, _MEMBERSHIP_PATH)
        soup = self.get_soup(membership_url)
        if soup is None:
            return

        containers = soup.select("div.membership__list")

        # 進捗表示用に総件数を先に確定する
        total = 0
        for c in containers:
            total += sum(
                1 for d in c.find_all("div", attrs={"x-show": True})
                if _ITEM_XSHOW.search(d.get("x-show") or "")
            )
        self.total_items = total

        for container in containers:
            member_type = _MEMBER_TYPE.get(container.get("x-show", ""), "")
            items = [
                d for d in container.find_all("div", attrs={"x-show": True})
                if _ITEM_XSHOW.search(d.get("x-show") or "")
            ]
            for item in items:
                try:
                    record = self._parse_item(item, member_type, membership_url)
                    if record:
                        yield record
                except Exception as e:  # noqa: BLE001 個別要素のエラーはスキップして継続
                    self.logger.warning("会員要素の解析に失敗 (スキップ): %s", e)
                    continue

    def _parse_item(self, item, member_type: str, page_url: str) -> dict | None:
        anchor = item.find("a")
        name = anchor.get_text(strip=True) if anchor else ""
        if not name:
            return None
        hp = anchor.get("href", "").strip() if anchor else ""
        if hp and not hp.startswith("http"):
            hp = urljoin(page_url, hp)

        # 主なブランド・店舗名 (「、」区切りの店舗/ブランド名。末尾の「他」は除去)
        brand_el = item.select_one(".--text")
        brands = ""
        if brand_el:
            brands = brand_el.get_text(" ", strip=True).replace("　", " ").strip()
            brands = _TRAILING_HOKA.sub("", brands).strip()

        return {
            Schema.NAME: name,
            Schema.HP: hp,
            Schema.URL: page_url,
            "会員区分": member_type,
            "主なブランド・店舗名": brands,
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = Jfnet()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://www.jfnet.or.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
