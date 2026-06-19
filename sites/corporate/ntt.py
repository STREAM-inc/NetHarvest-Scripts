"""
NTT東日本 光コラボレーションモデル 事業者一覧

取得対象:
    - 事業者名 / 詳細URL / ホームページURL / お問い合わせ電話番号・受付時間
    - サービス申込・事業者変更の各連絡先
    - 各サービス取扱フラグ (●/―)

取得フロー:
    1. 一覧ページ (https://flets.com/collabo/list/) から全521社を取得
    2. 詳細リンクがある事業者は info.html?id=... からHP・TEL等を補完

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/ntt.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ntt
"""

import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class NttCrawler(StaticCrawler):
    """NTT東日本 光コラボレーションモデル 事業者一覧 スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "申込_組織名",
        "申込_受付時間",
        "申込_備考",
        "変更_組織名",
        "変更_電話番号",
        "変更_受付時間",
        "変更_備考",
        "光アクセスサービス",
        "ひかり電話ネクスト",
        "ひかり電話_基本プラン_エース",
        "ひかり電話_オフィスタイプ",
        "ひかり電話_オフィスA_エース",
        "リモートサポートサービス",
        "フレッツテレビ",
        "24時間出張修理オプション",
        "7_22時出張修理オプション",
    ]

    def parse(self, url: str):
        soup = self.get_soup(url)

        # 一覧データテーブル: table.list の最後の要素が521件データテーブル
        list_tables = soup.select("table.list")
        if not list_tables:
            self.logger.warning("list table not found")
            return
        data_table = list_tables[-1]

        rows = data_table.find_all("tr")
        self.total_items = len(rows)

        for row in rows:
            name_td = row.find("td", class_="c-name")
            if not name_td:
                continue

            name = name_td.get_text(strip=True)
            if not name:
                continue

            # サービス取扱フラグ (td[1]〜td[9])
            tds = row.find_all("td")
            def flag(i):
                return tds[i].get_text(strip=True) if len(tds) > i else ""

            # 詳細ページURL
            link = name_td.find("a")
            detail_href = link.get("href") if link else None
            detail_url = urljoin(url, detail_href) if detail_href else ""

            item = {
                Schema.NAME: name,
                Schema.URL: detail_url or url,
                Schema.HP: "",
                Schema.TEL: "",
                "申込_組織名": "",
                "申込_受付時間": "",
                "申込_備考": "",
                "変更_組織名": "",
                "変更_電話番号": "",
                "変更_受付時間": "",
                "変更_備考": "",
                "光アクセスサービス": flag(1),
                "ひかり電話ネクスト": flag(2),
                "ひかり電話_基本プラン_エース": flag(3),
                "ひかり電話_オフィスタイプ": flag(4),
                "ひかり電話_オフィスA_エース": flag(5),
                "リモートサポートサービス": flag(6),
                "フレッツテレビ": flag(7),
                "24時間出張修理オプション": flag(8),
                "7_22時出張修理オプション": flag(9),
            }

            if detail_url:
                item = self._scrape_detail(detail_url, item)

            yield item

    def _scrape_detail(self, url: str, item: dict) -> dict:
        try:
            soup = self.get_soup(url)
            tables = soup.find_all("table", class_="tbl_collabo_info")

            def get_field(table, field_name: str) -> str:
                for tr in table.find_all("tr"):
                    th = tr.find("th")
                    td = tr.find("td")
                    if th and td and th.get_text(strip=True) == field_name:
                        return td.get_text(strip=True)
                return ""

            if len(tables) > 0:
                item[Schema.HP] = get_field(tables[0], "ホームページURL")

            if len(tables) > 1:
                item[Schema.TEL] = get_field(tables[1], "電話番号")
                item["申込_組織名"] = get_field(tables[1], "組織名")
                item["申込_受付時間"] = get_field(tables[1], "受付時間")
                item["申込_備考"] = get_field(tables[1], "備考")

            if len(tables) > 2:
                item["変更_組織名"] = get_field(tables[2], "組織名")
                item["変更_電話番号"] = get_field(tables[2], "電話番号")
                item["変更_受付時間"] = get_field(tables[2], "受付時間")
                item["変更_備考"] = get_field(tables[2], "備考")

        except Exception as e:
            self.logger.warning("Detail fetch failed %s: %s", url, e)

        return item


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = NttCrawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://flets.com/collabo/list/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
