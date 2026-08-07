"""
アイブリー電話番号検索 (ivry.jp/telsearch) — 電話番号から事業者情報を検索できるディレクトリ

取得対象:
    - 全国47都道府県の事業者(法人・屋号)の 名称 / 都道府県 / 住所 / TEL
    - 補助情報として 回線種別 / 業界 / FAX番号 / 電話番号提供事業者 / 関連キーワード / HP

取得フロー:
    1. 起点 URL (都道府県別一覧: /telsearch/area/{slug}/) を取得し、ページ内の
       都道府県リンクから 47 都道府県の slug を動的に収集する。
    2. 各都道府県について ?page=1..20 を巡回 (page=21 以降は 0 件になり break)。
       一覧行は個別詳細ページ /telsearch/{番号}/ へのリンクのみで、住所は
       都道府県までしか出ないため、詳細ページ遷移が必須。
    3. 各詳細ページの定義リスト (dl) を dt/dd で辞書化し、1 件取得ごとに即 yield。

実行方法:
    python scripts/sites/corporate/ivry_2.py
    docker compose exec worker python /app/bin/run_flow.py --site-id ivry_2
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

# 詳細ページ URL (末尾が数字のみ) を判定
_DETAIL_RE = re.compile(r"/telsearch/\d+/?$")
# 都道府県別一覧ページの slug を抽出
_AREA_RE = re.compile(r"/telsearch/area/([a-z]+)/?$")
# 住所先頭の都道府県を抽出
_PREF_RE = re.compile(
    r"^(北海道|東京都|京都府|大阪府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|"
    r"新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|"
    r"和歌山|鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|"
    r"熊本|大分|宮崎|鹿児島|沖縄)県)"
)

_MAX_PAGE = 20  # ?page=21 以降は 404 相当 (0 件)


class Ivry2(StaticCrawler):
    """アイブリー電話番号検索 スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["回線種別", "FAX番号", "電話番号提供事業者", "関連キーワード"]

    def parse(self, url: str):
        # 起点 URL を唯一のルートとして扱う (SSOT)
        m = re.search(r"^(https?://[^/]+/telsearch/area/)([a-z]+)/?", url)
        if not m:
            return
        area_base = m.group(1)          # 例: https://ivry.jp/telsearch/area/
        start_slug = m.group(2)         # 例: tokyo

        # 起点ページから 47 都道府県 slug を動的に収集 (start_slug を先頭に)
        start_soup = self.get_soup(url)
        slugs = [start_slug]
        for a in start_soup.select("a[href]"):
            am = _AREA_RE.search(a.get("href", ""))
            if am and am.group(1) not in slugs:
                slugs.append(am.group(1))

        for slug in slugs:
            for page in range(1, _MAX_PAGE + 1):
                if slug == start_slug and page == 1:
                    list_url = url
                    soup = start_soup
                else:
                    list_url = f"{area_base}{slug}/" if page == 1 else f"{area_base}{slug}/?page={page}"
                    soup = self.get_soup(list_url)

                detail_paths = []
                seen = set()
                for a in soup.select("a[href]"):
                    href = a.get("href", "")
                    if _DETAIL_RE.search(href) and href not in seen:
                        seen.add(href)
                        detail_paths.append(href)

                if not detail_paths:
                    break  # そのページ以降は無い

                for href in detail_paths:
                    detail_url = urljoin(url, href)
                    item = self._scrape_detail(detail_url)
                    if item:
                        yield item

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)

        # 詳細ページの dl を dt/dd で辞書化
        info = {}
        for dl in soup.select("dl"):
            dts = dl.find_all("dt")
            dds = dl.find_all("dd")
            for dt, dd in zip(dts, dds):
                label = dt.get_text(strip=True)
                value = dd.get_text(" ", strip=True)
                if label:
                    info[label] = value

        name = info.get("事業者名", "")
        if not name:
            return None  # 事業者名が取れない = 有効な詳細ページでない

        address = info.get("事業者の住所", "")
        pref = ""
        addr_rest = address
        pm = _PREF_RE.match(address)
        if pm:
            pref = pm.group(1)
            addr_rest = address[pm.end():].strip()

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr_rest,
            Schema.TEL: info.get("電話番号", ""),
            Schema.CAT_SITE: info.get("業界", ""),
            Schema.HP: info.get("URL", ""),
            Schema.URL: detail_url,
            "回線種別": info.get("回線種別", ""),
            "FAX番号": info.get("FAX番号", ""),
            "電話番号提供事業者": info.get("電話番号提供事業者", ""),
            "関連キーワード": info.get("関連するキーワード", ""),
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = Ivry2()
    # 🔒 sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://ivry.jp/telsearch/area/tokyo/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
