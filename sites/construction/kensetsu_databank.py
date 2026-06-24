"""
建築計画のお知らせ看板情報【首都圏】 — 株式会社建設データバンク

取得対象:
    - 首都圏(東京・神奈川・千葉・埼玉)の「建築計画のお知らせ看板」標識設置届情報
    - 月別アーカイブ (osirase/lastyear.php) から各月の一覧 (month.php) を辿り、
      各物件の詳細 (detail.php?id=N) を取得する

取得フロー:
    1. {url}osirase/lastyear.php から月別リンク (month.php?target=YYYYMM) を新しい順に収集
    2. 各月の一覧ページから物件詳細リンク (detail.php?id=N) を収集
    3. 詳細ページの th/td テーブルを 1 件ずつパースして即 yield (Pattern B)

実行方法:
    # ローカルテスト
    python scripts/sites/construction/kensetsu_databank.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id kensetsu_databank
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

# 住居表示・地名地番から都道府県を抜き出す
_PREF_RE = re.compile(r"(北海道|東京都|大阪府|京都府|.{2,3}県)")


class KensetsuDatabankScraper(StaticCrawler):
    """建築計画のお知らせ看板情報【首都圏】スクレイパー"""

    DELAY = 1.0

    # Schema に該当しないサイト固有の構造化カラム (長文の自由記述は除外)
    EXTRA_COLUMNS = [
        "物件番号",
        "届出日付",
        "地名地番",
        "主要用途",
        "工事種別",
        "構造",
        "基礎",
        "階数_地上",
        "階数_地下",
        "延床面積",
        "建築面積",
        "敷地面積",
        "建築主",
        "建築主住所",
        "設計者",
        "設計者住所",
        "施工者",
        "施工者住所",
        "着工日",
        "完成日",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url を唯一のルート(SSOT)として全 URL を派生させる
        lastyear_url = urljoin(url, "osirase/lastyear.php")
        index_soup = self.get_soup(lastyear_url)
        if index_soup is None:
            self.logger.warning("月別インデックス取得失敗: %s", lastyear_url)
            return

        month_urls: list[str] = []
        seen_month: set[str] = set()
        for a in index_soup.select('a[href*="month.php?target="]'):
            href = a.get("href", "").strip()
            if not href:
                continue
            full = urljoin(lastyear_url, href)
            if full not in seen_month:
                seen_month.add(full)
                month_urls.append(full)
        self.logger.info("月別ページ収集完了: %d 件", len(month_urls))

        total_set = False
        for month_url in month_urls:
            month_soup = self.get_soup(month_url)
            if month_soup is None:
                continue

            detail_urls: list[str] = []
            seen_detail: set[str] = set()
            for a in month_soup.select('a[href*="detail.php?id="]'):
                href = a.get("href", "").strip()
                if not href:
                    continue
                full = urljoin(month_url, href)
                if full not in seen_detail:
                    seen_detail.add(full)
                    detail_urls.append(full)

            # 進捗表示用: 最初の月の件数を目安として設定
            if not total_set and detail_urls:
                self.total_items = len(detail_urls)
                total_set = True

            self.logger.info("%s: 詳細 %d 件", month_url, len(detail_urls))
            for detail_url in detail_urls:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        kv: dict[str, str] = {}
        for tr in soup.select("table tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            k = re.sub(r"\s+", " ", th.get_text(strip=True)).replace("　", "").strip()
            v = re.sub(r"\s+", " ", td.get_text(" ", strip=True)).replace("　", " ").strip()
            if k:
                kv[k] = v

        name = kv.get("件名", "")
        if not name:
            return None

        # 住所: 住居表示を優先、無ければ地名地番
        addr = kv.get("住居表示", "") or kv.get("地名地番", "")

        data: dict = {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.ADDR: addr,
        }

        pref_m = _PREF_RE.search(addr)
        if pref_m:
            data[Schema.PREF] = pref_m.group(1)

        data["物件番号"] = kv.get("物件番号", "")
        data["届出日付"] = kv.get("日付", "")
        data["地名地番"] = kv.get("地名地番", "")
        data["主要用途"] = kv.get("主要用途", "")
        data["工事種別"] = kv.get("工事種別", "")
        data["構造"] = kv.get("構造", "")
        data["基礎"] = kv.get("基礎", "")
        data["階数_地上"] = kv.get("階数（地上）", "")
        data["階数_地下"] = kv.get("階数（地下）", "")
        data["延床面積"] = kv.get("延床面積", "")
        data["建築面積"] = kv.get("建築面積", "")
        data["敷地面積"] = kv.get("敷地面積", "")
        data["建築主"] = kv.get("建築主", "")
        data["建築主住所"] = kv.get("建築主住所", "")
        data["設計者"] = kv.get("設計者", "")
        data["設計者住所"] = kv.get("設計者住所", "")
        data["施工者"] = kv.get("施工者", "")
        data["施工者住所"] = kv.get("施工者住所", "")
        data["着工日"] = kv.get("着工", "")
        data["完成日"] = kv.get("完成", "")

        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = KensetsuDatabankScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://www.kensetsu-databank.co.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
