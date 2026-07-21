"""
ナビスタ（中洲無料案内所） — jobhunter-fukuoka.com

取得対象:
    - 中洲の無料案内所「ナビスタ」が紹介する店舗一覧 (約69店)
    - 各店舗の SHOP DATA (住所/電話/営業時間/定休日/料金 等)

取得フロー:
    1. 案内所ページ (引数 url) から店舗詳細 (/multigallery/detail/{id}/) リンクを収集
    2. 各詳細ページの SHOP DATA テーブル (th/td) をパースし、1件ごとに即 yield

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/jobhunter_fukuoka.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id jobhunter_fukuoka
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import Tag

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# 都道府県抽出 (このサイトは福岡・中洲限定。住所に都道府県が無い場合は福岡県を既定値とする)
_PREF_PATTERN = re.compile(
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
_DEFAULT_PREF = "福岡県"

# SHOP DATA テーブルの th ラベル → Schema 定数のマッピング
_SCHEMA_LABELS = {
    "住所": Schema.ADDR,
    "電話番号": Schema.TEL,
    "営業時間": Schema.TIME,
    "定休日": Schema.HOLIDAY,
}

# EXTRA_COLUMNS へ格納する th ラベル (いずれも短い構造化データ)
_EXTRA_LABELS = [
    "料金システム",
    "出勤人数",
    "VIP ROOM",
    "カラオケ",
    "クレジットカード利用可否",
    "TAX",
    "サービス料",
]


class JobhunterFukuoka(StaticCrawler):
    """ナビスタ（中洲無料案内所） スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = list(_EXTRA_LABELS)

    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)

        # 詳細リンクを重複排除しつつ出現順に収集
        detail_urls: list[str] = []
        seen: set[str] = set()
        for a in soup.select('a[href*="multigallery/detail"]'):
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
                record = self._scrape_detail(detail_url)
                if record:
                    yield record
            except Exception as exc:  # 個別ページの失敗は握りつぶして継続
                self.logger.warning("詳細ページのパースに失敗 %s: %s", detail_url, exc)
                continue

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)

        name_el = soup.select_one("h1")
        name = name_el.get_text(strip=True) if name_el else ""
        if not name:
            return None

        record = {
            Schema.NAME: name,
            Schema.URL: url,
            Schema.PREF: "",
            Schema.ADDR: "",
            Schema.TEL: "",
            Schema.TIME: "",
            Schema.HOLIDAY: "",
        }
        for col in _EXTRA_LABELS:
            record[col] = ""

        # SHOP DATA テーブル (th ラベル / td 値) を解析
        for tr in soup.select("table tr"):
            th = tr.select_one("th")
            td = tr.select_one("td")
            if not th or not td:
                continue
            label = th.get_text(strip=True)
            value = td.get_text(" ", strip=True)
            if label in _SCHEMA_LABELS:
                record[_SCHEMA_LABELS[label]] = value
            elif label in self.EXTRA_COLUMNS:
                record[label] = value

        # 都道府県の導出 (住所に無ければ福岡県を既定値)
        addr = record[Schema.ADDR]
        if addr:
            pref_m = _PREF_PATTERN.search(addr)
            record[Schema.PREF] = pref_m.group(1) if pref_m else _DEFAULT_PREF

        return record


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = JobhunterFukuoka()
    # 🔒 sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://jobhunter-fukuoka.com/navista-nakasu/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
