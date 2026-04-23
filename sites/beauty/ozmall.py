"""
OZmall【ヘアサロン】 — 美容室・美容院・ヘアサロンの検索＆予約ポータル

取得対象:
    - 首都圏(東京/神奈川/千葉/埼玉) + 名古屋(愛知) のヘアサロン全件
    - サロン名, 住所(都道府県), TEL, 代表者名, 営業時間, 定休日, 支払方法,
      最寄駅, アクセス, 座席数, スタッフ数, 設備, 口コミ採点/件数 等

取得フロー:
    1. 5 都道府県の一覧ページから ?pageNo=N で全ページを巡回
    2. 各 .resultlist-box から shop_id / shop_url / 口コミ採点 / 口コミ件数を収集
    3. 各サロンの map ページ (/hairsalon/{id}/map/) から詳細情報を取得

実行方法:
    python scripts/sites/beauty/ozmall.py
    python bin/run_flow.py --site-id ozmall
"""

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


_PREF_PATTERN = re.compile(
    r"^(北海道|東京都|大阪府|京都府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_SHOP_ID_RE = re.compile(r"/hairsalon/(\d+)/?")


class OzmallScraper(StaticCrawler):
    """OZmall ヘアサロンスクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "サロンURL",
        "最寄駅",
        "アクセス",
        "座席数",
        "スタッフ数",
        "駐車場",
        "個室",
        "ロッカー",
        "キッズスペース",
        "フルフラットシャンプー台",
        "こだわり条件",
        "ドリンク",
        "備品",
    ]

    PREF_PATHS = ["tokyo", "kanagawa", "chiba", "saitama", "aichi"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        base = url.rstrip("/")
        # Step 1: 全都道府県をページ送りして shop メタ情報を収集
        shops: dict[str, dict] = {}
        for pref_path in self.PREF_PATHS:
            pref_top = f"{base}/{pref_path}/"
            self.logger.info("一覧クロール開始: %s", pref_top)
            for meta in self._iter_list(pref_top):
                shops.setdefault(meta["shop_id"], meta)
            self.logger.info("収集累計: %d 件", len(shops))

        self.total_items = len(shops)
        self.logger.info("詳細ページ取得開始: %d 件", len(shops))

        # Step 2: 各 shop の map ページから詳細を取得
        for meta in shops.values():
            try:
                item = self._scrape_detail(meta)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("詳細取得エラー shop_id=%s: %s", meta["shop_id"], e)
                continue

    def _iter_list(self, pref_top_url: str) -> Generator[dict, None, None]:
        """指定都道府県の一覧ページを全ページ走査して shop メタを yield する"""
        page = 1
        while True:
            list_url = pref_top_url if page == 1 else f"{pref_top_url}?pageNo={page}"
            soup = self.get_soup(list_url)
            if soup is None:
                break
            boxes = soup.select(".resultlist-box")
            if not boxes:
                break

            for box in boxes:
                shop_link = box.select_one("a.shop-link")
                if not shop_link:
                    continue
                href = shop_link.get("href", "").strip()
                if not href:
                    continue
                m = _SHOP_ID_RE.search(href)
                if not m:
                    continue
                shop_id = m.group(1)
                shop_url = urljoin(pref_top_url, f"/hairsalon/{shop_id}/")

                # 一覧ページ側で取得できる口コミ採点/件数
                score_el = box.select_one(".rate__score")
                count_el = box.select_one(".rate__count .pink")
                score = score_el.get_text(strip=True) if score_el else ""
                review_count = count_el.get_text(strip=True) if count_el else ""

                yield {
                    "shop_id": shop_id,
                    "shop_url": shop_url,
                    "score": score,
                    "review_count": review_count,
                }

            # 次ページの有無を判定
            next_link = soup.select_one(".pager__next a")
            if not next_link:
                break
            page += 1

    def _scrape_detail(self, meta: dict) -> dict | None:
        map_url = urljoin(meta["shop_url"], "map/")
        soup = self.get_soup(map_url)
        if soup is None:
            return None

        item: dict = {
            Schema.URL: map_url,
            Schema.CAT_SITE: "ヘアサロン",
            "サロンURL": meta["shop_url"],
        }
        if meta.get("score"):
            item[Schema.SCORES] = meta["score"]
        if meta.get("review_count"):
            item[Schema.REV_SCR] = meta["review_count"]

        # 共通テーブル(複数)から th-td ペアを横断的に抽出
        for table in soup.select("table.common-table"):
            for tr in table.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                label = th.get_text(strip=True)
                value = self._normalize_text(td.get_text("\n", strip=True))
                if not value:
                    continue
                self._assign(item, label, value)

        # 住所が取れていれば都道府県を分離
        addr = item.get(Schema.ADDR, "")
        if addr:
            pref_m = _PREF_PATTERN.match(addr)
            if pref_m:
                item[Schema.PREF] = pref_m.group(1)
                item[Schema.ADDR] = addr[pref_m.end():].strip()

        if not item.get(Schema.NAME):
            return None
        return item

    @staticmethod
    def _normalize_text(text: str) -> str:
        # 連続する改行/タブ/空白を圧縮して扱いやすくする
        lines = [re.sub(r"\s+", " ", ln).strip() for ln in text.splitlines()]
        lines = [ln for ln in lines if ln]
        return "\n".join(lines)

    def _assign(self, item: dict, label: str, value: str) -> None:
        if label == "サロン名":
            # "MINX RICCa（ミンクス リッカ）" を分離
            m = re.match(r"^(.*?)（(.+?)）\s*$", value)
            if m:
                item[Schema.NAME] = m.group(1).strip()
                item[Schema.NAME_KANA] = m.group(2).strip()
            else:
                item[Schema.NAME] = value
        elif label == "住所":
            item[Schema.ADDR] = value
        elif label == "電話番号":
            # "03-6433-5556※掲載中の…" のような注釈を除去
            tel = re.split(r"[※\s]", value, maxsplit=1)[0].strip()
            item[Schema.TEL] = tel
        elif label == "営業時間":
            item[Schema.TIME] = value
        elif label == "定休日":
            item[Schema.HOLIDAY] = value
        elif label == "支払方法":
            item[Schema.PAYMENTS] = value
        elif label == "代表者名":
            item[Schema.REP_NM] = value
        elif label == "付近の駅":
            item["最寄駅"] = re.sub(r"\s+", " / ", value)
        elif label == "アクセス":
            item["アクセス"] = value
        elif label == "座席":
            item["座席数"] = value
        elif label == "スタッフ数":
            item["スタッフ数"] = value
        elif label == "駐車場":
            item["駐車場"] = value
        elif label == "個室":
            item["個室"] = value
        elif label == "ロッカー":
            item["ロッカー"] = value
        elif label == "キッズスペース":
            item["キッズスペース"] = value
        elif label == "フルフラットシャンプー台":
            item["フルフラットシャンプー台"] = value
        elif label == "こだわり条件":
            item["こだわり条件"] = value
        elif label == "ドリンク":
            item["ドリンク"] = value
        elif label == "備品":
            item["備品"] = value


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = OzmallScraper()
    scraper.execute("https://www.ozmall.co.jp/hairsalon")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
