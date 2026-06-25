"""
便利屋パラダイス — 全国の便利屋検索ポータルサイト

取得対象:
    - 掲載されている便利屋業者 (custom post type: vendor, 約152件) の店舗情報

取得フロー:
    1. WordPress REST API (/wp-json/wp/v2/vendor) で業者一覧をページング取得 (per_page=50)
    2. 各業者の詳細ページ (/vendor/{slug}/) を1件ずつ取得し、その場で yield
       (一覧→詳細パターン B / 取得即 yield)

取得カラム:
    Schema   : 名称 / 住所 / 都道府県 / TEL / メールアドレス / 営業時間 /
               サイト定義業種・ジャンル / 取得URL
    EXTRA    : 対応エリア / 目安料金

実行方法:
    # ローカルテスト
    python scripts/sites/service/benriya_paradise_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id benriya_paradise_2
"""

import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 都道府県の先頭一致用パターン (住所先頭から都道府県を切り出す)
_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# サイト共通の問い合わせ用フリーダイヤル (各業者の番号ではないため TEL から除外)
_PORTAL_FREEDIAL = "0120-480-056"

_PER_PAGE = 50  # REST API のページサイズ (read timeout 回避のため小さめ)


class BenriyaParadise2(StaticCrawler):
    """便利屋パラダイス スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["対応エリア", "目安料金"]

    def parse(self, url: str):
        api_url = urljoin(url, "wp-json/wp/v2/vendor")
        page = 1
        while True:
            try:
                resp = self.session.get(
                    api_url,
                    params={"per_page": _PER_PAGE, "page": page},
                    timeout=self.TIMEOUT,
                )
                resp.raise_for_status()
            except Exception as e:  # noqa: BLE001
                logger.warning("API取得失敗 page=%s: %s", page, e)
                break

            vendors = resp.json()
            if not vendors:
                break

            # 初回ページで総件数を設定して進捗表示を有効化
            if page == 1:
                total = resp.headers.get("X-WP-Total")
                if total and total.isdigit():
                    self.total_items = int(total)

            for v in vendors:
                detail_url = v.get("link")
                name = (v.get("title") or {}).get("rendered", "").strip()
                if not detail_url:
                    continue
                try:
                    item = self._scrape_detail(detail_url, name)
                except Exception as e:  # noqa: BLE001
                    logger.warning("詳細取得失敗 %s: %s", detail_url, e)
                    continue
                if item:
                    yield item

            page += 1

    def _scrape_detail(self, url: str, name: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        # 名称: API の title を優先し、無ければ h1 から取得
        if not name:
            h1 = soup.select_one("h1")
            name = h1.get_text(strip=True) if h1 else ""

        item = {
            Schema.NAME: name,
            Schema.URL: url,
            Schema.ADDR: "",
            Schema.PREF: "",
            Schema.TEL: "",
            Schema.EMAIL: "",
            Schema.TIME: "",
            Schema.CAT_SITE: "",
            "対応エリア": "",
            "目安料金": "",
        }

        # --- 情報テーブル (.dl-row .dl-flex > .dt / .dd) ---
        for row in soup.select(".dl-row .dl-flex"):
            dt = row.select_one(".dt")
            dd = row.select_one(".dd")
            if not dt or not dd:
                continue
            label = dt.get_text(strip=True)
            value = dd.get_text(" ", strip=True)
            if not value:
                continue
            if label == "TEL":
                if value != _PORTAL_FREEDIAL:
                    item[Schema.TEL] = value
            elif label == "メールアドレス":
                item[Schema.EMAIL] = value
            elif label == "営業時間":
                item[Schema.TIME] = value
            elif label.startswith("対応エリア"):
                item["対応エリア"] = value

        # --- 住所: 対応カテゴリ (.cat) の直前の .text が業者所在地 ---
        cat = soup.select_one(".cat")
        if cat:
            addr_el = cat.find_previous("div", class_="text")
            if addr_el:
                addr = addr_el.get_text(strip=True)
                m = _PREF_PATTERN.match(addr)
                if m:
                    item[Schema.PREF] = m.group(1)
                    item[Schema.ADDR] = addr[m.end():].strip()
                else:
                    item[Schema.ADDR] = addr
            # --- 対応カテゴリ (チップ): .cat span.span (見出し span0 は除外) ---
            cats = [s.get_text(strip=True) for s in cat.select("span.span")]
            cats = [c for c in cats if c]
            if cats:
                item[Schema.CAT_SITE] = " / ".join(cats)

        # --- 目安料金: .row2 (.dt2 ラベル / .dd2 値) を「ラベル:値」で結合 ---
        prices = []
        for row in soup.select(".row2"):
            dt2 = row.select_one(".dt2")
            dd2 = row.select_one(".dd2")
            if dt2 and dd2:
                lbl = dt2.get_text(strip=True)
                val = dd2.get_text(" ", strip=True)
                if lbl and val:
                    prices.append(f"{lbl}:{val}")
        if prices:
            item["目安料金"] = " / ".join(prices)

        return item


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BenriyaParadise2()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://benriya-paradise.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
