"""
Offer The Prefer Work — クラブ・キャバクラ・ラウンジ求人紹介サイト クローラー

取得対象:
    - 掲載店舗 (store) の求人情報 (銀座・六本木・新宿等のクラブ/キャバクラ/ラウンジ等)
    - 店名 / 住所 / 都道府県 / 営業時間 / 定休日 / 給料支給方法 / 業種(ジャンル) /
      時給・日給 / エリア / アクセス / 座席数 / 在籍キャスト / 職種 / 勤務時間 / 資格 / 待遇

取得フロー:
    1. WordPress REST API (/wp-json/wp/v2/store) で全 store の詳細URLを列挙
       (per_page=50 で分割取得し read timeout を回避)
    2. 各 store 詳細ページ (/store/{slug}/) を取得し、3つの情報テーブルから
       ラベル→値を抽出 → 1件ずつ即 yield
    ※ TEL は全ページ共通の紹介会社代表番号のみ掲載のため、店舗固有値ではなく取得しない。
    ※ 詳細ページに自由記述の店舗紹介プロースは無く、全項目が構造化テーブルで提供される。

実行方法:
    python scripts/sites/nightlife/prefer_work.py
    docker compose exec worker python /app/bin/run_flow.py --site-id prefer_work
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

_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_PER_PAGE = 50  # REST API のページサイズ (read timeout 回避のため小さめ)


class PreferWork(StaticCrawler):
    """Offer The Prefer Work スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "時給・日給",
        "給料支給方法",
        "エリア",
        "アクセス",
        "座席数",
        "在籍キャスト",
        "職種",
        "勤務時間",
        "資格",
        "待遇",
    ]

    def parse(self, url: str):
        api_base = urljoin(url.rstrip("/") + "/", "wp-json/wp/v2/store")

        page = 1
        while True:
            api_url = f"{api_base}?per_page={_PER_PAGE}&page={page}&_fields=link,title"
            try:
                resp = self.session.get(api_url, timeout=self.TIMEOUT)
                resp.raise_for_status()
                stores = resp.json()
            except Exception as e:  # noqa: BLE001
                self.logger.warning("store一覧の取得に失敗 (page=%s): %s", page, e)
                break

            if not stores:
                break

            # 初回ページで総件数を進捗表示用に設定
            if page == 1:
                total = resp.headers.get("X-WP-Total")
                if total and total.isdigit():
                    self.total_items = int(total)

            for store in stores:
                detail_url = store.get("link")
                if not detail_url:
                    continue
                fallback_name = (store.get("title") or {}).get("rendered", "")
                try:
                    item = self._scrape_detail(detail_url, fallback_name)
                except Exception as e:  # noqa: BLE001
                    self.logger.warning("詳細ページの解析に失敗: %s — %s", detail_url, e)
                    continue
                if item:
                    yield item

            if len(stores) < _PER_PAGE:
                break
            page += 1

    def _scrape_detail(self, url: str, fallback_name: str = "") -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        fields = self._extract_fields(soup)

        # 店名: H1 を優先、無ければテーブルの「店名」、最後に REST のタイトル
        h1 = soup.select_one("h1")
        name = (h1.get_text(strip=True) if h1 else "") or fields.get("店名", "") or fallback_name

        item = {
            Schema.NAME: name,
            Schema.URL: url,
            Schema.TIME: fields.get("営業時間", ""),
            Schema.HOLIDAY: fields.get("定休日", "") or fields.get("休日", ""),
            Schema.CAT_SITE: fields.get("業種", ""),
            "時給・日給": fields.get("時給・日給", ""),
            "給料支給方法": fields.get("給料支給方法", ""),
            "エリア": fields.get("エリア", ""),
            "アクセス": fields.get("アクセス", ""),
            "座席数": fields.get("座席数", ""),
            "在籍キャスト": fields.get("在籍キャスト", ""),
            "職種": fields.get("職種", ""),
            "勤務時間": fields.get("時間", ""),
            "資格": fields.get("資格", ""),
            "待遇": fields.get("待遇", ""),
        }

        # 住所 → 都道府県を分離 (都道府県プレフィックスが無い場合は住所のみ)
        addr = fields.get("住所", "")
        m = _PREF_RE.match(addr)
        if m:
            item[Schema.PREF] = m.group(1)
            item[Schema.ADDR] = addr[m.end():].strip()
        else:
            item[Schema.PREF] = ""
            item[Schema.ADDR] = addr

        return item

    @staticmethod
    def _extract_fields(soup) -> dict:
        """詳細ページの全テーブルからラベル→値を抽出する。

        - 横型テーブル (1行目=ラベル, 2行目=値) は列を zip で対応付ける
        - 縦型テーブル (各行が ラベル/値 の2セル) はそのまま対応付ける
        同名ラベルは最初に出現した値を優先する。
        """
        fields: dict = {}
        for table in soup.select("table"):
            rows = table.select("tr")
            if not rows:
                continue

            head_cells = rows[0].find_all(["th", "td"])
            # 横型: 1行目が全てヘッダ(th)で2行目に値が並ぶ
            if (
                len(rows) == 2
                and len(head_cells) >= 2
                and all(c.name == "th" for c in head_cells)
            ):
                heads = [c.get_text(" ", strip=True) for c in head_cells]
                vals = [c.get_text(" ", strip=True) for c in rows[1].find_all(["th", "td"])]
                for h, v in zip(heads, vals):
                    if h:
                        fields.setdefault(h, v)
                continue

            # 縦型: 各行 [ラベル, 値]
            for tr in rows:
                cells = tr.find_all(["th", "td"])
                if len(cells) >= 2:
                    label = cells[0].get_text(" ", strip=True)
                    value = cells[1].get_text(" ", strip=True)
                    if label:
                        fields.setdefault(label, value)
        return fields


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = PreferWork()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://offer-prefer.site/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
