# -*- coding: utf-8 -*-
"""
公益社団法人全日本不動産協会（全日） — 会員検索結果一覧

対象サイト:
    https://www.zennichi.or.jp/member_search/list/

取得対象:
    会員検索結果一覧テーブル (table.member-result-table) の各行。
    詳細ページは存在せず、全データが一覧行に含まれる (list-only)。
    1 行 = 3 セル:
        セル1 (免許番号)      : 免許権者 / (免許更新回数) / 会員番号
        セル2 (商号/代表者)   : 名称 / 代表者: 代表者名
        セル3 (所在地/HP/メール): 〒郵便番号 / 住所 / TEL: / HP: / Mail:

取得フロー:
    parse(url) が引数 url を起点に &pages=N でページ送り (15件/ページ)。
    各行を都度 yield する (Pattern B, 早期 yield)。

フィルター (備考: 東京都知事):
    免許権者が「東京都知事」の会員のみを yield する。

実行方法:
    # ローカルテスト / スモークテスト
    python scripts/sites/realestate/zennichi.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id zennichi
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Dict, Generator, List, Optional

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 備考で指定されたフィルター: 免許権者がこの値の会員のみ取得する
LICENSE_HOLDER_FILTER = "東京都知事"

# 1 ページあたりの件数 (サイト実測)
PER_PAGE = 15


def _clean(text: Optional[str]) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.replace("　", " ")).strip()


def _pref_from_holder(holder: str) -> str:
    """免許権者 (例: 東京都知事 / 鹿児島県知事) から都道府県を導出する。

    知事免許は事務所の所在する都道府県が発行するため、都道府県が確定できる。
    国土交通大臣免許 (全国) は導出不能なので空文字を返す。
    """
    m = re.match(r"(北海道|東京都|(?:京都|大阪)府|.+?県)知事", holder)
    return m.group(1) if m else ""


class ZennichiScraper(StaticCrawler):
    """全日本不動産協会 会員検索 — 東京都知事免許の会員を取得"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "免許権者",
        "免許更新回数",
        "会員番号",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 総件数 → 総ページ数を初回ページで把握する
        soup = self.get_soup(url)
        total_pages = self._total_pages(soup)

        page = 1
        while page <= total_pages:
            if page > 1:
                soup = self.get_soup(f"{url}&pages={page}")

            rows = self._data_rows(soup)
            if not rows:
                break

            for row in rows:
                item = self._parse_row(row, url)
                if item is None:
                    continue
                yield item

            page += 1

    # ------------------------------------------------------------------ #
    # helpers
    # ------------------------------------------------------------------ #
    def _total_pages(self, soup) -> int:
        body = soup.get_text(" ", strip=True)
        m = re.search(r"([0-9,]+)\s*件", body)
        if m:
            total = int(m.group(1).replace(",", ""))
            self.total_items = total
            return (total + PER_PAGE - 1) // PER_PAGE
        return 1

    def _data_rows(self, soup) -> List:
        table = soup.select_one("table.member-result-table")
        if table is None:
            return []
        # 先頭行はヘッダー (免許番号 / 商号 / 所在地)
        return table.select("tr")[1:]

    def _parse_row(self, row, url: str) -> Optional[Dict[str, str]]:
        tds = row.select("td")
        if len(tds) < 3:
            return None

        # セル1: スマホ用ラベル span を除去してから行を取得
        c1 = tds[0]
        for sp in c1.select("span.display-sp"):
            sp.decompose()
        lines1 = [_clean(x) for x in c1.stripped_strings if _clean(x)]
        holder = lines1[0] if len(lines1) >= 1 else ""

        # フィルター: 免許権者が指定値でなければスキップ
        if LICENSE_HOLDER_FILTER not in holder:
            return None

        renewal = ""
        member_no = ""
        if len(lines1) >= 2:
            renewal = lines1[1].strip("（）()")
        if len(lines1) >= 3:
            member_no = lines1[2]

        # セル2: 名称 / 代表者
        lines2 = [_clean(x) for x in tds[1].stripped_strings if _clean(x)]
        name = ""
        rep = ""
        for ln in lines2:
            if ln.startswith("代表者"):
                rep = re.sub(r"^代表者[:：]\s*", "", ln).strip()
            elif not name:
                name = ln
        if not name:
            return None  # 名称が無い行は不完全なのでスキップ

        # セル3: 郵便番号 / 住所 / TEL / HP / Mail
        post_code = addr = tel = hp = email = ""
        for ln in [_clean(x) for x in tds[2].stripped_strings if _clean(x)]:
            if ln.startswith("〒"):
                post_code = ln[1:].strip()
            elif ln.startswith("TEL"):
                tel = re.sub(r"^TEL[:：]\s*", "", ln).strip()
            elif ln.startswith("HP"):
                hp = re.sub(r"^HP[:：]\s*", "", ln).strip()
            elif ln.startswith("Mail") or ln.startswith("mail"):
                email = re.sub(r"^[Mm]ail[:：]\s*", "", ln).strip()
            elif ln.startswith("http"):
                hp = ln
            elif not addr:
                addr = ln

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: _pref_from_holder(holder),
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.REP_NM: rep,
            Schema.HP: hp,
            Schema.EMAIL: email,
            "免許権者": holder,
            "免許更新回数": renewal,
            "会員番号": member_no,
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = ZennichiScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute(
        "https://www.zennichi.or.jp/member_search/list/?prefecture=&branch=&address=&representative=&shogo=&shogo_kana=&license_holder=&number=&region="
    )

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
