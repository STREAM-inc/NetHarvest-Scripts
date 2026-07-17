"""
すすきの観光協会リスト (www.susukino-ta.jp) — 加盟店 (業態=バー) スクレイパー

取得対象:
    - 検索結果一覧 (?kensakuword=&gyotai=2) に列挙された加盟店 (約 114 店)
    - 各店舗詳細ページ (/index.php?shop={slug}) の構造化情報:
      名称 / カナ / ジャンル / 住所 / 電話 / 営業時間 / 定休日 / 席数 / 平均予算 /
      支払い方法 / ホームページ

取得フロー:
    1. 引数 url (一覧ページ) を取得し、店舗詳細リンク (/index.php?shop=...) を列挙
    2. 各詳細ページを取得して構造化情報を抽出し、1 店舗ごとに即 yield (Pattern B)

備考 (依頼):
    - 文章 (自由記述プロース) は著作権のため取得しない:
      キャッチコピー (.leadF) / おすすめメニュー本文 (.shopMenu おすすめ) は除外。
    - 一覧 URL 自体が業態 (gyotai=2) で絞り込まれているため、追加のフィルタは不要。

実行方法:
    python scripts/sites/nightlife/susukino_ta.py
    python bin/run_flow.py --site-id susukino_ta
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


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"[ \t　]+", " ", text.replace("\xa0", " ")).strip()


class SusukinoTa(StaticCrawler):
    """すすきの観光協会リスト スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["席数", "平均予算"]

    # 詳細ページ table の th ラベル → Schema 定数
    _TABLE_MAP = {
        "住所": Schema.ADDR,
        "営業時間": Schema.TIME,
        "定休日": Schema.HOLIDAY,
        "ホームページ": Schema.HP,
    }

    def parse(self, url: str):
        soup = self.get_soup(url)
        links = soup.select('a[href*="index.php?shop="]')

        # 重複除去しつつ順序保持
        seen = set()
        detail_urls = []
        for a in links:
            href = a.get("href")
            if not href:
                continue
            detail_url = urljoin(url, href)
            if detail_url in seen:
                continue
            seen.add(detail_url)
            detail_urls.append(detail_url)

        self.total_items = len(detail_urls)

        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as e:  # 個別店舗の失敗はスキップして継続
                self.logger.warning("詳細ページ取得失敗 %s: %s", detail_url, e)
                continue

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        box = soup.select_one("#detailBox")
        if box is None:
            return None

        item = {
            Schema.URL: url,
            Schema.PREF: "北海道",
        }

        # 名称 (subName span を除いた h3 テキスト)
        h3 = box.select_one("h3")
        if h3 is not None:
            for span in h3.select(".subName"):
                span.extract()
            item[Schema.NAME] = _clean(h3.get_text())

        # カナ
        yomi = box.select_one(".yomiganaF")
        if yomi is not None:
            item[Schema.NAME_KANA] = _clean(yomi.get_text())

        # ジャンル (業態)
        ctgr = box.select_one(".shopctgr")
        if ctgr is not None:
            item[Schema.CAT_SITE] = _clean(ctgr.get_text())

        # 平均予算 (.shopMenu 内の h4「平均予算」に続く p) — カード可なら PAY に転記
        for h4 in box.select(".shopMenu h4"):
            if "平均予算" in h4.get_text():
                p = h4.find_next_sibling("p")
                if p is not None:
                    budget = _clean(p.get_text())
                    item["平均予算"] = budget
                    if "カード可" in budget:
                        item[Schema.PAYMENTS] = "カード可"
                break

        # 詳細テーブル (th ラベル駆動)
        for tr in box.select("#shoptable tr"):
            th = tr.select_one("th")
            td = tr.select_one("td")
            if th is None or td is None:
                continue
            label = _clean(th.get_text())
            if label == "電話":
                strong = td.select_one("strong")
                tel = strong.get_text(strip=True) if strong else td.get_text(strip=True)
                item[Schema.TEL] = _clean(re.sub(r"電話をかける", "", tel))
            elif label == "席数":
                item["席数"] = _clean(td.get_text(" ", strip=True))
            elif label == "ホームページ":
                a = td.select_one("a[href]")
                item[Schema.HP] = a.get("href").strip() if a else _clean(td.get_text())
            elif label in self._TABLE_MAP:
                item[self._TABLE_MAP[label]] = _clean(td.get_text(" ", strip=True))

        return item


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = SusukinoTa()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("http://www.susukino-ta.jp/?kensakuword=&gyotai=2")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
