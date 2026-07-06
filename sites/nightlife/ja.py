"""
しものせきJaナイトリスト — 下関の夜のお店(ラウンジ・バー・スナック等)店舗情報

取得対象:
    - 店舗一覧 (/store/) から各店舗詳細ページを辿り、店舗情報テーブルを取得する

取得フロー:
    一覧ページ (WordPress, wp-pagenavi) を /store/page/{n}/ で全ページ巡回し、
    各 li の詳細リンク (/store/{id}/) へ遷移して table.mb30 の th/td を取得する。
    detail は 1 件取得ごとに即 yield する (Pattern B)。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/ja.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ja
"""

import re
import sys
import urllib.parse
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 住所先頭の郵便番号 (例: 〒7500016 / 〒750-0016)
_POST_PATTERN = re.compile(r"〒?\s*(\d{3})[\-‐ー]?(\d{4})\s*")
# 都道府県抽出
_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
# TEL 欄の定型文 (「しものせきJaナイトを見た」…) を除いた電話番号本体
_TEL_PATTERN = re.compile(r"0[\d\-()\s]{8,}\d")


class ShimonosekiJaNight(StaticCrawler):
    """しものせきJaナイトリスト スクレイパー"""

    DELAY = 1.5
    # 店舗固有の SNS リンクは無し (サイト共通フッターの公式 SNS のみ) のため EXTRA からは除外。
    EXTRA_COLUMNS = ["エリア", "席数", "サービスポイント"]

    def parse(self, url: str):
        # url = sites.yml の正規 URL (https://shimonoseki-ja-night.com/store/) を唯一の起点とする
        page = 1
        while True:
            list_url = url if page == 1 else urllib.parse.urljoin(url, f"page/{page}/")
            soup = self.get_soup(list_url)
            if soup is None:
                break

            items = soup.select("ul.mb30 > li")
            if not items:
                break

            # 詳細リンクを収集。店舗情報テーブルは ?type=store-info&id={id} を
            # 付与したときのみサーバー描画されるため、店舗IDから正規化した URL を組み立てる。
            detail_urls = []
            for li in items:
                a = li.select_one('a[href*="/store/"]')
                if not a:
                    continue
                mid = re.search(r"/store/(\d+)", a.get("href", ""))
                if not mid:
                    continue
                sid = mid.group(1)
                base = urllib.parse.urljoin(url, f"{sid}/")
                detail_urls.append(f"{base}?type=store-info&id={sid}")

            for detail_url in detail_urls:
                try:
                    item = self._scrape_detail(detail_url)
                except Exception as e:  # noqa: BLE001
                    self.logger.warning("詳細ページ取得失敗: %s — %s", detail_url, e)
                    continue
                if item:
                    yield item

            # 次ページの存在確認 (wp-pagenavi の次リンク)。無ければ終了。
            nav = soup.select_one(".wp-pagenavi")
            has_next = bool(
                nav
                and nav.select_one(f'a[href*="page/{page + 1}/"]')
            )
            if not has_next:
                break
            page += 1

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        table = soup.select_one("table.mb30")
        if not table:
            return None

        # th ラベル -> td テキスト の辞書化
        rows = {}
        for tr in table.select("tr"):
            th = tr.select_one("th")
            td = tr.select_one("td")
            if th and td:
                rows[th.get_text(strip=True)] = td.get_text(" ", strip=True)

        name = rows.get("店名", "").strip()
        if not name:
            return None

        item = {
            Schema.NAME: name,
            Schema.URL: detail_url,
            Schema.TIME: rows.get("営業時間", "").strip(),
            Schema.HOLIDAY: rows.get("定休日", "").strip(),
            Schema.CAT_SITE: rows.get("ジャンル", "").strip(),
            Schema.POST_CODE: "",
            Schema.PREF: "",
            Schema.ADDR: "",
            Schema.TEL: "",
            "エリア": rows.get("エリア", "").strip(),
            "席数": rows.get("席数", "").strip(),
            "サービスポイント": rows.get("サービスポイント", "").strip(),
        }

        # 住所: 〒郵便番号 + 都道府県 + 住所
        addr_raw = rows.get("住所", "").strip()
        if addr_raw:
            m = _POST_PATTERN.match(addr_raw)
            if m:
                item[Schema.POST_CODE] = f"{m.group(1)}-{m.group(2)}"
                addr_raw = addr_raw[m.end():].strip()
            pm = _PREF_PATTERN.match(addr_raw)
            if pm:
                item[Schema.PREF] = pm.group(1)
                addr_raw = addr_raw[pm.end():].strip()
            item[Schema.ADDR] = addr_raw

        # TEL: 定型文を除いた電話番号本体のみ (全角→半角は Pipeline が正規化)
        tel_raw = rows.get("TEL", "")
        tm = _TEL_PATTERN.search(tel_raw)
        if tm:
            item[Schema.TEL] = tm.group(0).strip()

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = ShimonosekiJaNight()
    # 🔒 sites.yml に登録する url と完全一致 (SSOT = sites.yml)
    scraper.execute("https://shimonoseki-ja-night.com/store/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
