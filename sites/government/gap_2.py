"""
日本GAP協会 認証農場検索 (gap_2) — JGAP/ASIAGAP 認証農場の全国一覧

取得対象:
    - JGAP/ASIAGAP 認証農場 (全国)。所在地セレクト pref=01〜47 を順に検索し、
      各都道府県の認証農場一覧を全ページ巡回して取得する。

取得フロー:
    検索フォーム (class=farm_searchBox) は素の GET では結果を描画しないが、
    フォーム送信時に `?...&search=1` 付きの GET で同一 URL にナビゲートすると
    サーバ側で結果テーブル (table.farm_result) が SSR される (Static 取得可)。
    - 一覧 URL: {base}?q=&pref={PP}&ver=&item1=..item5=&search=1&page={N}
    - 1 ページ 15 件。ページングは div.pagination の「次へ ≫」リンクで判定。
    - 農場ごとの詳細ページは無い。農場名セルに farm 自社サイトへの <a> が付く
      ことがあり、それを HP として取得する。
    - 所在地列は都道府県のみ (市区町村以下の住所・認証機関・認証日は本サイトの
      公開検索には存在しない)。

実行方法:
    python scripts/sites/government/gap_2.py
    docker compose exec worker python /app/bin/run_flow.py --site-id gap_2
"""

import re
import sys
from pathlib import Path
from urllib.parse import urlencode

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 所在地セレクト: 47 都道府県コード (01〜47)。海外 (50〜55) は対象外。
_PREF_CODES = [f"{i:02d}" for i in range(1, 48)]

# 農場名セル末尾のかっこ内表記 (経営者名 or 屋号。並び順はサイト内で不定のため
# 分離せず補足として保持する)。
_PAREN_RE = re.compile(r"[（(]([^（）()]+)[)）]\s*$")


class GapFarmSearch(StaticCrawler):
    """日本GAP協会 認証農場検索 スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "登録番号",
        "認証区分",
        "認証品目",
        "認証有効期限",
        "所属農場数",
        "農場名かっこ内表記",
    ]

    @staticmethod
    def _classify(ver: str) -> str:
        """版文字列 (例: JGAP青果物2022) から認証区分の大分類を導出する。"""
        if "ASIAGAP" in ver:
            return "ASIAGAP"
        if "家畜" in ver or "畜産" in ver:
            return "JGAP畜産"
        if ver:
            return "JGAP農産"
        return ""

    def parse(self, url: str):
        base = url.split("?")[0]

        for code in _PREF_CODES:
            page = 1
            while True:
                query = urlencode({
                    "q": "",
                    "pref": code,
                    "ver": "",
                    "item1": "",
                    "item2": "",
                    "item3": "",
                    "item4": "",
                    "item5": "",
                    "search": "1",
                    "page": page,
                })
                page_url = f"{base}?{query}"

                soup = self.get_soup(page_url)
                if soup is None:
                    break

                table = soup.select_one("table.farm_result")
                rows = table.select("tbody tr") if table else []
                if not rows:
                    break

                for tr in rows:
                    try:
                        item = self._parse_row(tr, page_url)
                        if item:
                            yield item
                    except Exception as e:  # noqa: BLE001 — 個別行の失敗はスキップ
                        self.logger.warning("行の解析に失敗: %s — %s", page_url, e)
                        continue

                # ページング: 「次へ」リンクが無ければ当該県は終了
                pager = soup.select_one("div.pagination")
                has_next = bool(pager) and any(
                    "次へ" in a.get_text() for a in pager.select("a")
                )
                if not has_next:
                    break
                page += 1

    def _parse_row(self, tr, page_url: str) -> dict | None:
        tds = tr.find_all("td", recursive=False)
        if len(tds) < 7:
            return None

        name_td = tds[0]
        anchor = name_td.find("a", href=True)
        hp = anchor["href"].strip() if anchor else ""
        name = name_td.get_text(" ", strip=True)
        if not name:
            return None

        paren = ""
        m = _PAREN_RE.search(name)
        if m:
            paren = m.group(1).strip()

        reg_no = tds[1].get_text(" ", strip=True)
        pref = tds[2].get_text(" ", strip=True)
        ver = tds[3].get_text(" ", strip=True)
        items = tds[4].get_text(" ", strip=True)
        valid_until = tds[5].get_text(" ", strip=True)
        farm_count = tds[6].get_text(" ", strip=True)

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.CAT_SITE: ver,
            Schema.HP: hp,
            Schema.URL: page_url,
            "登録番号": reg_no,
            "認証区分": self._classify(ver),
            "認証品目": items,
            "認証有効期限": valid_until,
            "所属農場数": farm_count,
            "農場名かっこ内表記": paren,
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = GapFarmSearch()
    # 🔒 sites.yml の url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://jgap.jp/certification/farm-search/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
