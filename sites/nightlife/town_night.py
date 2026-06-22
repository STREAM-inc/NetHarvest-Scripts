"""
夜遊びショコラ（東北） — 宮城県のナイト系店舗情報

取得対象:
    - 宮城県のキャバクラ・ガールズバー・スナック等ナイト系店舗 (全 106件)

取得フロー:
    一覧ページ (/miyagi/ → /miyagi/page2/ → /miyagi/page3/) →
    各店舗詳細ページ (dl.shopData-dl を解析)

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/town_night.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id town_night
"""

import re
import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県"
    r"|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県"
    r"|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県"
    r"|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


class TownNightScraper(StaticCrawler):
    """夜遊びショコラ（東北）宮城県ナイト系店舗スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["最寄駅", "料金目安", "タグ"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)
        if soup is None:
            return

        count_el = soup.select_one("p.castAndReviewCount")
        if count_el:
            m = re.search(r"店舗：(\d+)件", count_el.get_text())
            if m:
                self.total_items = int(m.group(1))

        seen: set[str] = set()
        page = 1
        while True:
            for link_tag in soup.select("li.rankingShop span.shop-name a[href]"):
                detail_url = link_tag.get("href", "")
                if not detail_url or detail_url in seen:
                    continue
                seen.add(detail_url)
                try:
                    item = self._scrape_detail(detail_url)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.warning("詳細取得失敗 %s: %s", detail_url, e)

            next_tag = soup.find("link", rel="next")
            if not next_tag:
                break
            page += 1
            soup = self.get_soup(f"{url}page{page}/")
            if soup is None:
                break

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}

        dl = soup.select_one("dl.shopData-dl")
        if not dl:
            return None

        for div in dl.select("div.shopData-item"):
            dt_el = div.select_one("dt.shopData-dt")
            dd_el = div.select_one("dd.shopData-dd")
            if not dt_el or not dd_el:
                continue

            key = re.sub(r"\s+", "", dt_el.get_text())
            val = _clean(dd_el.get_text(" "))

            if key == "店名":
                data[Schema.NAME] = val
            elif key == "ジャンル":
                data[Schema.CAT_SITE] = val
            elif key == "営業時間":
                data[Schema.TIME] = val
            elif key == "定休日":
                data[Schema.HOLIDAY] = val
            elif key == "電話番号":
                data[Schema.TEL] = val
            elif key == "住所":
                p = dd_el.select_one("p.shopData-add-txt")
                addr_raw = _clean(p.get_text(" ")) if p else val
                pref_m = _PREF_RE.search(addr_raw)
                if pref_m:
                    data[Schema.PREF] = pref_m.group(0)
                    data[Schema.ADDR] = addr_raw[pref_m.end():].strip()
                else:
                    data[Schema.ADDR] = addr_raw
            elif key == "アクセス":
                data["最寄駅"] = val
            elif key == "料金目安":
                data["料金目安"] = val
            elif key == "タグ":
                tags = [a.get_text(strip=True) for a in dd_el.select("a")]
                data["タグ"] = " ".join(tags)

        line_a = soup.find("a", href=re.compile(r"https?://lin\.ee/"))
        if line_a:
            data[Schema.LINE] = line_a.get("href", "")

        return data if data.get(Schema.NAME) else None


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = TownNightScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://town-night.jp/miyagi/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
