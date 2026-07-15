"""
すすきの通信 (susukino.tv) — 札幌・すすきの エリアのグルメ / 飲食店情報サイト

取得対象:
    - 掲載店舗の店名・住所・電話番号・営業時間・定休日・支払い方法・
      公式サイト・ジャンル、および席数・煙草(喫煙可否)・料金目安

取得フロー:
    1. sitemap.xml から shop_detail.php?shop_id=... の全店舗を列挙 (約418件)
    2. 各店舗詳細ページ (table.data__table の th/td) をパースし、1件ずつ即 yield

利用規約 (page_service.php) の確認:
    第5条にスクレイピング/クローリング/bot を明示的に禁止する条項は無い。
    robots.txt も User-agent:* Allow:/ で許可。DELAY を確保して運営妨害を避ける。

実行方法:
    # ローカルテスト
    python scripts/sites/food/susukino.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id susukino
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

# ジャンルはページタイトル "{店名} | {ジャンル} | すすきの通信" の中央要素から取得
_SHOP_ID_RE = re.compile(r"shop_detail\.php\?shop_id=([A-Za-z0-9_-]+)")


def _clean(text: str) -> str:
    """全角スペース/連続空白を正規化する。"""
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


class Susukino(StaticCrawler):
    """すすきの通信 スクレイパー (一覧=sitemap → 詳細)"""

    DELAY = 1.5

    # サイト固有カラム (いずれも構造化された短い値。自由記述プロースは含めない)
    EXTRA_COLUMNS = ["席数", "煙草", "料金目安"]

    # 詳細テーブルの th ラベル → Schema 定数 / EXTRA カラム名 のマッピング
    _LABEL_MAP = {
        "店名": Schema.NAME,
        "住所": Schema.ADDR,
        "電話番号": Schema.TEL,
        "営業時間": Schema.TIME,
        "定休日": Schema.HOLIDAY,
        "可能決済": Schema.PAYMENTS,
        "オフィシャル": Schema.HP,
        "席数": "席数",
        "煙草": "煙草",
        "料金目安": "料金目安",
    }

    def parse(self, url: str):
        # --- 一覧: sitemap.xml から全店舗 shop_id を列挙 ---
        sitemap_soup = self.get_soup(urljoin(url, "/sitemap.xml"))
        shop_ids = []
        seen = set()
        if sitemap_soup:
            for loc in sitemap_soup.find_all("loc"):
                m = _SHOP_ID_RE.search(loc.get_text())
                if m and m.group(1) not in seen:
                    seen.add(m.group(1))
                    shop_ids.append(m.group(1))
        self.total_items = len(shop_ids)

        # --- 詳細: 1件取得ごとに即 yield (全件バッファしない) ---
        for shop_id in shop_ids:
            detail_url = urljoin(url, f"shop_detail.php?shop_id={shop_id}")
            try:
                item = self._scrape_detail(detail_url)
            except Exception as exc:  # noqa: BLE001 個別ページ失敗は握って続行
                self.logger.warning("詳細ページ取得失敗 %s: %s", detail_url, exc)
                continue
            if item:
                yield item

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        item = {col: "" for col in self.EXTRA_COLUMNS}
        item[Schema.URL] = detail_url

        table = soup.select_one("table.data__table")
        if table:
            for tr in table.find_all("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                label = _clean(th.get_text())
                target = self._LABEL_MAP.get(label)
                if not target:
                    continue
                value = _clean(td.get_text(" "))
                if target == Schema.TEL:
                    # "011-551-7220 電話をかける" → 番号のみ (全角/正規化は Pipeline 側)
                    value = _clean(value.replace("電話をかける", ""))
                item[target] = value

        # NAME が取れなければ店舗ページとして無効
        name = item.get(Schema.NAME, "")
        if not name:
            return None

        # 住所から都道府県を分離 (すすきの=札幌市=北海道)
        addr = item.get(Schema.ADDR, "")
        if addr.startswith("北海道"):
            item[Schema.PREF] = "北海道"
            item[Schema.ADDR] = addr[len("北海道"):].strip()
        elif "札幌市" in addr or "北海道" in addr:
            item[Schema.PREF] = "北海道"

        # ジャンル: タイトル "{店名} | {ジャンル} | すすきの通信" の中央要素
        if soup.title:
            parts = [p.strip() for p in soup.title.get_text().split("|")]
            if len(parts) >= 3:
                item[Schema.CAT_SITE] = parts[1]

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Susukino()
    # 🔒 sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://susukino.tv/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
