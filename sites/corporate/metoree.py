"""
Metoree（メトリー） — 掲載企業 会社概要スクレイパー

取得対象 (一覧=都道府県エリア → 詳細ページで完結):
    - 企業名 / 詳細URL / HP
    - 郵便番号 / 本社住所 (都道府県分割) / 創業年 / 従業員数 / 資本金 / 法人番号

取得フロー:
    企業全体の一覧は存在しない。トップ (/companies/) に 47 都道府県の
    エリアリンク (/companies/area/{slug}/) が並ぶので、これを唯一の起点とする。
    各エリアページを ?p=N でページ送りしながら巡回し、企業詳細リンク
    (/companies/{id}/) を集める。詳細ページ /companies/{id}/ を 1 件取得する
    たびに即 yield する (Pattern B: 取得即 yield なので途中 break しても
    無駄な通信が起きない / テスト実行が早期に最初の1件を返せる)。

    会社概要テーブル (table.table-bordered) の th ラベル → 値 から
    構造化フィールドのみを抽出する。製品紹介・会社紹介などの自由記述プロースは
    著作権リスク回避のため取得しない。

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/metoree.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id metoree
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


# 企業詳細ページのパス (例: /companies/50387/)。distributors 等の派生は除外する。
_DETAIL_RE = re.compile(r"/companies/(\d+)/$")

# 都道府県エリアリンク (例: /companies/area/hokkaido/)
_AREA_RE = re.compile(r"/companies/area/[a-z]+/$")

# 都道府県 (本社住所の先頭から分割するため)
_PREF = (
    r"北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県"
    r"|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県"
    r"|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県"
    r"|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県"
    r"|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
_PREF_RE = re.compile(_PREF)

# 会社概要テーブルの th ラベル → Schema 定数のマッピング
_LABEL_MAP = {
    "郵便番号": Schema.POST_CODE,
    "創業年": Schema.OPEN_DATE,
    "従業員数": Schema.EMP_NUM,
    "資本金": Schema.CAP,
    "法人番号": Schema.CO_NUM,
}

# 1 エリアあたりの巡回上限ページ数 (暴走防止のセーフガード)
_MAX_PAGE = 200


class Metoree(StaticCrawler):
    """Metoree（メトリー） スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = []  # 取得フィールドは全て Schema に対応するため固有カラムは無し

    def parse(self, url: str) -> Generator[dict, None, None]:
        # ルート (= sites.yml の url) から 47 都道府県エリアリンクを取得する。
        index_soup = self.get_soup(url)
        if index_soup is None:
            return

        area_urls = []
        for a in index_soup.find_all("a", href=True):
            href = urljoin(url, a["href"])
            if _AREA_RE.search(href) and href not in area_urls:
                area_urls.append(href)

        seen_detail = set()
        for area_url in area_urls:
            page = 1
            while page <= _MAX_PAGE:
                page_url = f"{area_url}?p={page}"
                soup = self.get_soup(page_url)
                if soup is None:
                    break

                detail_urls = []
                for a in soup.find_all("a", href=True):
                    href = urljoin(area_url, a["href"])
                    if _DETAIL_RE.search(href) and href not in seen_detail:
                        seen_detail.add(href)
                        detail_urls.append(href)

                # このページに新規企業リンクが無ければ、このエリアは打ち止め。
                if not detail_urls:
                    break

                for detail_url in detail_urls:
                    try:
                        item = self._scrape_detail(detail_url)
                    except Exception as e:  # noqa: BLE001
                        self.logger.warning("詳細取得失敗 %s — %s", detail_url, e)
                        continue
                    if item:
                        yield item

                page += 1

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        h1 = soup.find("h1")
        name = h1.get_text(strip=True) if h1 else ""
        if not name:
            return None

        item = {
            Schema.NAME: name,
            Schema.URL: url,
        }

        table = soup.find("table", class_="table-bordered")
        if table:
            for tr in table.find_all("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                label = th.get_text(strip=True)

                if label == "本社住所":
                    addr = td.get_text(" ", strip=True)
                    m = _PREF_RE.match(addr)
                    if m:
                        item[Schema.PREF] = m.group(0)
                        item[Schema.ADDR] = addr[m.end():].strip()
                    else:
                        item[Schema.ADDR] = addr
                elif label == "リンク":
                    a = td.find("a", href=True)
                    if a:
                        item[Schema.HP] = a["href"]
                elif label in _LABEL_MAP:
                    value = td.get_text(" ", strip=True)
                    if label == "郵便番号":
                        value = value.lstrip("〒").strip()
                    item[_LABEL_MAP[label]] = value

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Metoree()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://metoree.com/companies/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
