"""
いい葬儀 (e_sogi) — 全国の葬儀場・斎場ポータル 斎場情報スクレイパー

取得対象:
    - いい葬儀 (e-sogi.com) に掲載されている全国の葬儀場・斎場 (詳細ページ
      /detail_hall/id...html) の構造化情報
    - 斎場名 / 読みカナ / 郵便番号 / 都道府県 / 住所 /
      アクセス・最寄駅 / 斎場の特徴 / 斎場の設備 / 駐車場 / 運営元

取得フロー:
    1. ルート URL から sitemap_detail_hall.xml を導出して取得し、全斎場の
       詳細ページ URL (約 7,100 件) を列挙する。
       ※ サイト上に全斎場の一括リストページが無いため、サイトマップを起点とする。
    2. 列挙した詳細ページを 1 件取得するたびに即 yield する
       (途中中断に強い Pattern B / 早期 yield)。

注意:
    - ルート URL は引数 `url` を唯一の起点 (SSOT) とし、配下 URL はすべて
      urljoin(url, ...) で派生させる。別 URL はハードコードしない。
    - 電話番号は全斎場共通の「いい葬儀」相談ダイヤル (0120-...) のみが掲載され、
      斎場固有の番号は存在しないため Schema.TEL には載せない (共通番号は EXTRA 等にも入れない)。
    - 斎場の特徴・設備・駐車場・アクセスは短い構造化ラベル/タグであり、
      自由記述 PR 文は取得しない (著作権リスク回避)。

実行方法:
    # ローカルテスト
    python scripts/sites/service/e_sogi.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id e_sogi
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


# 住所先頭から都道府県を切り出すためのパターン
_PREF_PATTERN = re.compile(
    r"(北海道|東京都|大阪府|京都府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 〒123-4567 形式の郵便番号
_POST_PATTERN = re.compile(r"〒?\s*(\d{3}-?\d{4})")

# Schema に無いサイト固有の構造化項目 (いずれも短いラベル/タグ。自由記述は含めない)
_COL_ACCESS = "アクセス・最寄駅"
_COL_FEATURE = "斎場の特徴"
_COL_FACILITY = "斎場の設備"
_COL_PARKING = "駐車場"
_COL_OPERATOR = "運営元"

# 詳細テーブルのラベル → EXTRA カラム名 の対応 (完全一致)
_EXTRA_LABELS = {
    "アクセス・最寄駅": _COL_ACCESS,
    "斎場の特徴": _COL_FEATURE,
    "斎場の設備": _COL_FACILITY,
    "駐車場": _COL_PARKING,
    "運営元": _COL_OPERATOR,
}


class ESogi(StaticCrawler):
    """いい葬儀 全国の葬儀場・斎場 情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [_COL_ACCESS, _COL_FEATURE, _COL_FACILITY, _COL_PARKING, _COL_OPERATOR]

    def parse(self, url: str):
        # 1. サイトマップから全斎場の詳細ページ URL を列挙 (一括リストページが無いため)
        sitemap_url = urljoin(url, "/sitemap_detail_hall.xml")
        sitemap = self.get_soup(sitemap_url)
        if sitemap is None:
            return

        detail_urls = []
        seen = set()
        for loc in sitemap.find_all("loc"):
            href = loc.get_text(strip=True)
            if "/detail_hall/" not in href:
                continue
            du = urljoin(url, href)
            if du not in seen:
                seen.add(du)
                detail_urls.append(du)

        self.total_items = len(detail_urls)

        # 2. 詳細ページを 1 件取得するたびに即 yield (早期 yield / 途中中断に強い)
        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as e:  # 個別斎場の失敗は握りつぶして継続
                self.logger.warning("詳細取得失敗 %s — %s", detail_url, e)
                continue

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        item = {Schema.URL: detail_url}

        # 斎場名 (H1)
        h1 = soup.select_one("h1")
        if h1:
            name = h1.get_text(strip=True)
            if name:
                item[Schema.NAME] = name

        # 読みカナ
        kana = soup.select_one("[class*=kana]")
        if kana:
            kana_txt = kana.get_text(strip=True)
            if kana_txt:
                item[Schema.NAME_KANA] = kana_txt

        # 詳細テーブル (th/td) を辞書化
        rows: dict[str, str] = {}
        table = soup.select_one("table.table--access")
        if table:
            for tr in table.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not (th and td):
                    continue
                label = th.get_text(strip=True)
                value = td.get_text(" ", strip=True)
                if label and label not in rows:
                    rows[label] = value

        # 所在地 → 郵便番号 / 都道府県 / 住所
        addr_raw = rows.get("所在地", "")
        if addr_raw:
            mp = _POST_PATTERN.search(addr_raw)
            if mp:
                item[Schema.POST_CODE] = mp.group(1)
                addr = addr_raw[mp.end():].strip()
            else:
                addr = addr_raw.strip()
            if addr:
                item[Schema.ADDR] = addr
            mpref = _PREF_PATTERN.search(addr_raw)
            if mpref:
                item[Schema.PREF] = mpref.group(1)

        # EXTRA カラム (短い構造化ラベル/タグのみ)
        for label, col in _EXTRA_LABELS.items():
            value = rows.get(label, "")
            if value:
                item[col] = value

        # NAME が取れなければ無効なページとして捨てる
        if not item.get(Schema.NAME):
            return None

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = ESogi()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    parse() は引数 url を唯一の起点とし、配下 URL は urljoin で派生させる。
    scraper.execute("https://www.e-sogi.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
