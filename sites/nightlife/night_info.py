"""
みんなの案内所 (night-info.okinawa) — 沖縄・那覇の夜のお店 店舗紹介ポータル

取得対象:
    - 店舗一覧ページ (キャバクラ / スナック・ラウンジ / ガールズバー・ショーパブ・バーの
      3タブすべて) にある全店舗の店舗詳細情報

取得フロー:
    1. 一覧ページ (/shop-list/) を1回取得。3タブ分の店舗リンクはすべて同一HTML内の
       .shop__wrap ブロックに含まれるため、1リクエストで全リンクを収集する。
    2. 各店舗詳細ページ (/shop-list/shop-detail/shop-detailNN.html) を1件ずつ取得し、
       dt.detail__h2-lead / dd.detail__h2-txt の定義リストをラベル駆動でマッピングして
       即 yield する (Pattern B)。

備考:
    - フィルター指示は無し (全店舗取得)。
    - 店舗紹介文 (detail__h1-body) は自由記述プロースのため著作権リスクで除外。
    - Instagram / TikTok リンクはサイト運営者 (案内所) 共通アカウントで店舗固有ではない
      ため除外。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/night_info.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id night_info
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

# 都道府県抽出 (住所先頭)
_PREF_PATTERN = re.compile(
    r"^\s*(北海道|東京都|(?:京都|大阪)府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|新潟|富山|"
    r"石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|和歌山|鳥取|島根|岡山|"
    r"広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県)"
)

# 詳細ページ dt ラベル -> Schema 定数 / EXTRA カラム名
_LABEL_MAP = {
    "店名": Schema.NAME,
    "住所": Schema.ADDR,
    "システム": "システム",
    "クレジットカード": Schema.PAYMENTS,
    "飲み放題": "飲み放題",
    "VIP席": "VIP席",
    "在籍人数": "在籍人数",
    "指名料": "指名料",
    "定休日": Schema.HOLIDAY,
    "営業時間": Schema.TIME,
    "店舗公式URL": Schema.HP,
}


class NightInfo(StaticCrawler):
    """みんなの案内所 スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["システム", "飲み放題", "VIP席", "在籍人数", "指名料"]

    def parse(self, url: str):
        soup = self.get_soup(url)

        # 全タブ (.shop__wrap) の店舗リンクを収集 (重複除去、順序維持)
        detail_urls = []
        seen = set()
        for a in soup.select(".shop__wrap .shop__item a[href*='shop-detail']"):
            href = a.get("href")
            if not href:
                continue
            full = urljoin(url, href)
            if full not in seen:
                seen.add(full)
                detail_urls.append(full)

        self.total_items = len(detail_urls)

        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
            except Exception as e:  # noqa: BLE001
                self.logger.warning("detail 取得失敗 %s: %s", detail_url, e)
                continue
            if item:
                yield item

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)

        item = {Schema.URL: url}

        # サイト定義ジャンル (キャバクラ / ラウンジ / ガールズバー 等)
        cat = soup.select_one(".detail__info-category")
        if cat:
            item[Schema.CAT_SITE] = cat.get_text(strip=True)

        # dt.detail__h2-lead / dd.detail__h2-txt の定義リストをラベル駆動でマッピング
        for dt in soup.select("dt.detail__h2-lead"):
            label = re.sub(r"\s+", "", dt.get_text(strip=True))
            dd = dt.find_next_sibling("dd", class_="detail__h2-txt")
            if dd is None:
                continue
            field = _LABEL_MAP.get(label)
            if field is None:
                continue

            if field == Schema.HP:
                a = dd.find("a", href=True)
                value = a["href"].strip() if a else dd.get_text(" ", strip=True)
            else:
                value = dd.get_text("\n", strip=True)
                value = re.sub(r"\n{2,}", "\n", value).strip()

            if value:
                item[field] = value

        # NAME が取れなければ h1 タイトルで補完
        if not item.get(Schema.NAME):
            h1 = soup.select_one(".detail__h1-ttl")
            if h1:
                item[Schema.NAME] = h1.get_text(strip=True)

        # 名称カナ: 全角括弧内の読みを抽出 (例: "Club Star （クラブ スター）")
        name = item.get(Schema.NAME, "")
        m = re.search(r"[（(]\s*([^（）()]+?)\s*[）)]", name)
        if m:
            item[Schema.NAME_KANA] = m.group(1).strip()

        # 都道府県抽出
        addr = item.get(Schema.ADDR, "")
        pm = _PREF_PATTERN.match(addr)
        if pm:
            item[Schema.PREF] = pm.group(1)
            item[Schema.ADDR] = addr[pm.end():].strip()

        if not item.get(Schema.NAME):
            return None
        return item


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = NightInfo()
    # 🔒 sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://night-info.okinawa/shop-list/#tab1")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
