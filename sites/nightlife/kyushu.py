"""
体入がるる｜九州版 — ガールズバー・コンカフェ求人ポータル (kyushu.garuru.work)

取得対象:
    - お仕事一覧 (joblist.html) に掲載された全求人 (店舗) の情報

取得フロー:
    1. 一覧ページ (引数 url) を取得し、各求人カード (.joblist-wrapper) の
       詳細ページリンク (/jobN/) を抽出する
    2. 詳細ページを1件取得するごとに即 yield する (Pattern B)
       ※ 一覧は「検索結果30件」で全件1ページに収まりページネーションは無い

備考 / 方針:
    - 年齢層・給与・勤務時間・休日・待遇・求人キャッチ・注目ポイント等の
      自由記述プロースは著作権リスク回避のため取得しない
    - お店の規模/雰囲気/料金帯/客層/常連割合はスライダーの端点ラベルのみで
      実値を持たないため取得しない
    - 電話番号は当サイトに掲載が無い

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/kyushu.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id kyushu
"""

import re
import sys
import logging
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 九州(+沖縄)の都道府県。住所先頭から都道府県を切り出す。
_PREF_PATTERN = re.compile(r"^(福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)")
_POST_PATTERN = re.compile(r"(\d{3}-\d{4})")


class Kyushu(StaticCrawler):
    """体入がるる｜九州版 スクレイパー (一覧 → 詳細)"""

    DELAY = 1.5

    # 詳細ページの th ラベル → EXTRA カラム名 の対応 (構造化された短い値のみ)
    EXTRA_COLUMNS = [
        "エリア",
        "最寄駅",
        "職種",
        "雇用形態",
        "加入保険",
    ]

    def parse(self, url: str):
        soup = self.get_soup(url)
        if soup is None:
            return

        detail_urls = []
        seen = set()
        for wrapper in soup.select(".joblist-wrapper"):
            a = wrapper.select_one(".joblist_cts > a[href]")
            if not a:
                continue
            detail_url = urljoin(url, a["href"])
            if detail_url in seen:
                continue
            seen.add(detail_url)
            detail_urls.append(detail_url)

        self.total_items = len(detail_urls)

        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
            except Exception:
                logger.exception("詳細ページの解析に失敗: %s", detail_url)
                continue
            if item:
                yield item

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        # 詳細ページの全テーブル (table.over / table.under) から th→td のラベルマップを構築。
        # 同一ラベルが複数テーブルに出るため「最初の非空値」を採用する。
        labels: dict[str, str] = {}
        for table in soup.select("table.over, table.under"):
            for tr in table.select("tr"):
                th = tr.select_one("th")
                td = tr.select_one("td")
                if not th or not td:
                    continue
                key = th.get_text(strip=True)
                val = td.get_text(" ", strip=True)
                if key and (not labels.get(key)) and val:
                    labels[key] = val

        name = labels.get("店舗名", "")
        item = {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.CAT_SITE: labels.get("業種", ""),
            Schema.HP: self._extract_hp(soup, labels),
        }

        # 住所: "810-0801 福岡県福岡市博多区中洲3丁目5-6　三信ビル1F 地図を見る"
        raw_addr = labels.get("住所", "")
        post_code = ""
        m = _POST_PATTERN.search(raw_addr)
        if m:
            post_code = m.group(1)
            raw_addr = raw_addr.replace(m.group(1), "", 1).strip()
        raw_addr = re.sub(r"\s*地図を見る\s*$", "", raw_addr).strip()
        pref = ""
        pm = _PREF_PATTERN.match(raw_addr)
        if pm:
            pref = pm.group(1)
            raw_addr = raw_addr[pm.end():].strip()
        item[Schema.POST_CODE] = post_code
        item[Schema.PREF] = pref

        # addrにprefを追加する．
        if not raw_addr.startswith(pref):
            raw_addr = pref + raw_addr

        item[Schema.ADDR] = raw_addr

        # EXTRA カラム (構造化された短い値)
        for col in self.EXTRA_COLUMNS:
            item[col] = labels.get(col, "")

        return item

    @staticmethod
    def _extract_hp(soup, labels: dict) -> str:
        """公式HP の th を持つ行から <a href> を優先して取得する。"""
        for th in soup.select("table.under th"):
            if th.get_text(strip=True) == "公式HP":
                td = th.find_next("td")
                if td:
                    a = td.select_one("a[href]")
                    if a and a.get("href"):
                        return a["href"].strip()
                    return td.get_text(" ", strip=True)
        # フォールバック: ラベルマップのテキスト値
        return labels.get("公式HP", "")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Kyushu()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://kyushu.garuru.work/joblist.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
