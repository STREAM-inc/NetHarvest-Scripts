"""
NTT西日本 光コラボレーションモデル 事業者一覧 — フレッツ光コラボ事業者スクレイパー

取得対象:
    - 光コラボレーション事業者（NTT西日本の回線を使って独自サービスを提供する事業者）405件
    - 企業情報（社名・業種・TEL・HP）＋各種オプション取扱可否フラグ

取得フロー:
    1. 一覧ページ（全件1ページ）からbpid・コラボサービス名・サービス可否列を取得
    2. 各社の詳細ページ（detail.php?bpid=N）から企業情報を取得
    3. 1件取得するごとに即 yield

実行方法:
    # ローカルテスト
    python scripts/sites/agency_franchise/ntt_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ntt_2
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

_SERVICE_COLS = [
    "光アクセスサービス",
    "ひかり電話ネクスト",
    "ひかり電話",
    "ひかり電話オフィスタイプ",
    "ひかり電話オフィスエース",
    "リモートサポートサービス",
    "フレッツテレビ",
    "24時間出張修理オプション",
    "7_22時出張修理オプション",
    "ホームゲートウェイ",
    "ホームゲートウェイ無線LAN",
    "転用セキュリティ対策ツール",
    "事業者変更セキュリティ対策ツール",
]


class Ntt2Scraper(StaticCrawler):
    """NTT西日本 光コラボレーションモデル 事業者一覧 スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "コラボサービス名",
        "受付時間",
        *_SERVICE_COLS,
        "ISPサービス名",
        "ISPサービス種類",
        "契約料金",
    ]

    def parse(self, url: str):
        soup = self.get_soup(url)
        rows = [
            r for r in soup.select("table tbody tr")
            if r.select_one('th a[href*="detail"]')
        ]
        self.total_items = len(rows)
        self.logger.info("一覧件数: %d 件", len(rows))

        for row in rows:
            try:
                name_el = row.select_one("th a")
                detail_url = urljoin(url, name_el["href"])

                service_strong = row.select_one("strong")
                collabo_service = (
                    service_strong.get_text(strip=True).strip("【】")
                    if service_strong else ""
                )

                tds = [td.get_text(strip=True) for td in row.select("td")]

                item = self._scrape_detail(detail_url)
                if not item:
                    continue

                item["コラボサービス名"] = collabo_service
                item.setdefault("受付時間", "")
                item.setdefault("ISPサービス名", "")
                item.setdefault("ISPサービス種類", "")
                item.setdefault("契約料金", "")

                for i, col in enumerate(_SERVICE_COLS):
                    item[col] = tds[i] if i < len(tds) else ""

                yield item
            except Exception as e:
                self.logger.error("行スキップ: %s", e)
                continue

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}

        tables = soup.select("table.c-table")
        if not tables:
            return None

        # 企業情報テーブル
        for row in tables[0].select("tr"):
            th = row.select_one("th")
            td = row.select_one("td")
            if not th or not td:
                continue
            key = th.get_text(strip=True)
            raw = td.get_text(separator="\n", strip=True)

            if "企業名" in key:
                data[Schema.NAME] = raw
            elif "ホームページ" in key:
                hp_input = td.select_one("input[name=bpurl]")
                data[Schema.HP] = hp_input["value"].strip() if hp_input else ""
            elif "主な業種" in key:
                data[Schema.CAT_SITE] = raw
            elif "お問い合わせ先" in key and "事業者変更" not in key:
                tel_m = re.search(r"(\d[\d-]{7,})", raw)
                data[Schema.TEL] = tel_m.group(1) if tel_m else ""
                hours_m = re.search(r"受付時間[:：](.+)", raw)
                data["受付時間"] = hours_m.group(1).strip() if hours_m else ""

        if not data.get(Schema.NAME):
            return None

        # サービス情報テーブル（任意）
        if len(tables) > 1:
            for row in tables[1].select("tr"):
                th = row.select_one("th")
                td = row.select_one("td")
                if not th or not td:
                    continue
                key = th.get_text(strip=True)
                val = td.get_text(strip=True)

                if "サービス／商材名" in key:
                    data["ISPサービス名"] = val
                elif "サービス／商材の種類" in key:
                    data["ISPサービス種類"] = val
                elif "契約・料金" in key:
                    data["契約料金"] = val

        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ntt2Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://flets-w.com/collabo/list/index.php")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
