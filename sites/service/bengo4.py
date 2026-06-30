"""
弁護士ドットコム — 弁護士検索 プロフィール情報スクレイパー

取得対象 (検索結果一覧ページのみで完結):
    - 弁護士名 / 都道府県 / 住所(市区町村) / TEL / 詳細URL
    - 所属事務所 / 最寄駅 / 注力分野 / 解決事例数 (EXTRA, いずれも短い構造化値)

設計方針 (重要 — なぜ詳細ページを取得しないか):
    旧実装は一覧 (約 410 ページ・総 8,200 名超) を巡回しつつ 1 件ごとに詳細ページを
    追加取得していた。これだと総リクエスト数が約 16,400 件・推定所要 6.5 時間となり、
    最終 CSV は close() 時にのみ生成される本フレームワークではコンテナ実行が時間切れ
    kill され、CSV が 1 件も書き出されず「0 件」になっていた (実環境での 0 件は
    セレクタのバグではなく所要時間の問題)。

    一覧カセット (div.p-lawyer-cassette) には 弁護士名 / 都道府県 / 市区町村 / TEL /
    所属事務所 / 最寄駅 / 注力分野 / 解決事例数 が揃っているため、詳細ページを取得せず
    一覧のみで完結させる。これでリクエスト数は約 410 件に減り、現実的な時間で完走できる。
    名称カナ・住所の番地・所属弁護士会は詳細ページにしか無いため取得対象から外す
    (完走して 0 件を回避することを優先)。

    料金表・解決事例本文・自己紹介 (PR文) などの自由記述プロースは著作権リスク回避のため
    取得しない (Schema.LOB / DESCRIPTION も同様に除外)。

取得フロー:
    一覧 (検索結果) ページ /search/result/?page=N を 1 ページずつ取得し、各ページの
    弁護士カセットから構造化フィールドを抜き出して 1 件ずつ即 yield する
    (Pattern B: 取得即 yield なので途中 break しても無駄な通信が起きない)。
    カセットが 0 件になったページで末尾と判断して停止する。

実行方法:
    # ローカルテスト
    python scripts/sites/service/bengo4.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id bengo4
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


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"[ \t　]+", " ", str(s).replace("\r", "")).strip()


class Bengo4Scraper(StaticCrawler):
    """弁護士ドットコム 弁護士検索 スクレイパー (一覧ページのみで完結)"""

    # DELAY はフレームワーク上「yield した 1 件ごと」に sleep される。一覧のみ取得の本実装は
    # HTTP リクエストが 1 ページ (≈20 件) につき 1 回しか発生しないため、0.1 秒/件としても
    # ページ取得間隔は実質 ≈2 秒/ページに保たれ、かつ全 8,200 件を現実的な時間で完走できる
    # (1.5 秒/件だと約 3.4 時間かかり、close() 時にしか CSV が出ない本基盤では時間切れ=0 件)。
    DELAY = 0.1
    EXTRA_COLUMNS = ["所属事務所", "最寄駅", "注力分野", "解決事例数"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        page = 1
        while True:
            page_url = url if page == 1 else f"{url}?page={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                break

            cassettes = soup.select("div.p-lawyer-cassette")
            if not cassettes:
                break

            # 1 ページ目で総件数 (≈8,200 名) を ETA 用に設定
            if page == 1 and self.total_items is None:
                total_el = soup.select_one('[class*="total"]')
                if total_el:
                    m = re.search(r"([\d,]+)", total_el.get_text())
                    if m:
                        self.total_items = int(m.group(1).replace(",", ""))

            for cas in cassettes:
                item = self._extract_cassette(cas, url)
                if item:
                    yield item

            page += 1

    def _extract_cassette(self, cas, root_url: str) -> dict | None:
        """一覧カセット 1 件から構造化フィールドを取り出す。"""
        # 弁護士名リンク (詳細 URL の起点)
        name_a = cas.select_one("a.p-lawyer-cassette-profile__name[href]")
        if not name_a:
            return None

        # 弁護士名 (タイトル "弁護士" の span を除いた最初の span)
        name = ""
        name_span = name_a.select_one("span")
        if name_span and "title" not in " ".join(name_span.get("class", [])):
            name = _clean(name_span.get_text(" ", strip=True))
        if not name:
            # フォールバック: リンク全体から接尾辞 "弁護士" を除去
            name = _clean(re.sub(r"弁護士\s*$", "", name_a.get_text(" ", strip=True)))
        if not name:
            return None

        item = {
            Schema.NAME: name,
            Schema.URL: urljoin(root_url, name_a.get("href", "")),
        }

        # 都道府県
        pref = cas.select_one(".p-lawyer-cassette-location__prefecture")
        if pref:
            item[Schema.PREF] = _clean(pref.get_text(" ", strip=True))

        # 住所 (市区町村まで。番地は詳細ページにしか無いため省略)
        city = cas.select_one(".p-lawyer-cassette-location__autonomy")
        if city:
            item[Schema.ADDR] = _clean(city.get_text(" ", strip=True))

        # TEL (tel: リンク)。全角→半角は Pipeline が正規化する
        tel_a = cas.select_one('a[href^="tel:"]')
        if tel_a:
            item[Schema.TEL] = _clean(tel_a.get("href", "").replace("tel:", ""))

        # 所属事務所
        firm = cas.select_one(".p-lawyer-cassette-profile__law-firm")
        if firm:
            firm_txt = _clean(firm.get_text(" ", strip=True))
            if firm_txt:
                item["所属事務所"] = firm_txt

        # 最寄駅 + 徒歩分 (例: "仙台駅 徒歩2分")
        station = cas.select_one(".p-lawyer-cassette-transportation__station")
        on_foot = cas.select_one(".p-lawyer-cassette-transportation__on-foot")
        station_parts = [
            _clean(el.get_text(" ", strip=True))
            for el in (station, on_foot)
            if el is not None
        ]
        station_parts = [p for p in station_parts if p]
        if station_parts:
            item["最寄駅"] = " ".join(station_parts)

        # 注力分野タグ (例: 借金・債務整理 / 交通事故 …) — 短い構造化ラベルのみ
        tags = [
            _clean(t.get_text(strip=True))
            for t in cas.select('[class*="scroll-snap-tab--field"]')
        ]
        tags = [t for t in tags if t]
        if tags:
            # 重複除去しつつ順序維持
            item["注力分野"] = "/".join(dict.fromkeys(tags))

        # 解決事例数 (例: "解決事例 10" → 10)
        rec = cas.select_one('[class*="track-record"]')
        if rec:
            m = re.search(r"(\d+)", rec.get_text())
            if m:
                item["解決事例数"] = m.group(1)

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Bengo4Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.bengo4.com/search/result/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
