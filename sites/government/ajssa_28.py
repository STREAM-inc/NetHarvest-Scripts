"""
一般社団法人 和歌山県警備業協会（AJSSA 会員名簿・和歌山県）— 会員名簿

取得対象:
    - 和歌山県警備業協会の会員（加盟員 約50社 + 賛助会員 約4社）
    - 会社名 / 会員区分 / 郵便番号 / 都道府県 / 住所 / TEL / FAX / HP / 業務種別

取得フロー:
    引数 url (= sites.yml の url = /member-list/) が唯一のルート。会員名簿ページには
    「加盟員名簿」「賛助会員名簿」の 2 つの <table> があり、いずれも
    会社名 / 所在地 / TEL/FAX / 業務種別 の 4 列で同一構造。
    会社名 th を先頭に持つヘッダ行を確認できたテーブルだけを対象とし、
    各データ行 (td 行) を 1 件ずつ即 yield する (Pattern B)。詳細ページ・
    ページネーションは無く、全件が 1 ページに収まる。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_28.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_28
"""

import logging
import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 〒NNN-NNNN もしくは 〒NNNNNNN の郵便番号
_POST_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")
# 住所先頭の都道府県 (賛助会員に大阪府・東京都あり。加盟員は県名省略が多く 和歌山県 を既定)
_PREF_RE = re.compile(r"(北海道|東京都|(?:京都|大阪)府|..県)")
# TEL / FAX を "TEL：xxx FAX：yyy" 形式から抽出
_TEL_RE = re.compile(r"TEL[：:]\s*([0-9\-]+)")
_FAX_RE = re.compile(r"FAX[：:]\s*([0-9\-]+)")


class Ajssa28(StaticCrawler):
    """一般社団法人 和歌山県警備業協会 会員名簿 スクレイパー"""

    DELAY = 1.5
    # 業務種別 = 警備種別の短い構造化ラベル(交/雑/施 等 or 制服販売 等) → Schema.CAT_SITE。
    # FAX(電話番号)・会員区分(加盟員/賛助会員) はサイト固有の構造化情報として EXTRA。
    EXTRA_COLUMNS = ["FAX", "会員区分"]

    def parse(self, url: str):
        soup = self.get_soup(url)
        if soup is None:
            logger.warning("会員名簿ページの取得に失敗: %s", url)
            return

        # 会社名 th を持つヘッダ行のテーブルだけを対象にする
        tables = [t for t in soup.find_all("table") if self._is_member_table(t)]
        if not tables:
            logger.warning("会員テーブルが見つかりません: %s", url)
            return

        rows = []
        for table in tables:
            # 区分ラベルは直前の見出し「加盟員名簿 / 賛助会員名簿」から (末尾の名簿を除去)
            heading = table.find_previous(["h1", "h2", "h3", "h4"])
            kubun = heading.get_text(strip=True).replace("名簿", "").strip() if heading else ""
            for tr in table.find_all("tr"):
                if tr.find("td"):
                    rows.append((tr, kubun))
        self.total_items = len(rows)

        for tr, kubun in rows:
            try:
                item = self._parse_row(tr, kubun, url)
                if item:
                    yield item
            except Exception as e:  # 個別会員のエラーはスキップして継続
                logger.warning("会員の解析に失敗しskip: %s", e)
                continue

    @staticmethod
    def _is_member_table(table) -> bool:
        header = table.find("tr")
        if header is None:
            return False
        ths = header.find_all("th")
        return any("会社名" in th.get_text(strip=True) for th in ths)

    def _parse_row(self, tr, kubun: str, source_url: str) -> dict | None:
        tds = tr.find_all("td")
        if len(tds) < 4:
            return None

        # 会社名: \xa0(NBSP) を通常空白へ、BOM を除去
        name = tds[0].get_text(" ", strip=True).replace("\xa0", " ").replace("﻿", "").strip()
        if not name:
            return None

        # 会社名セル内の外部リンク = HP
        a = tds[0].find("a", href=True)
        hp = a["href"].strip() if a else ""

        # 所在地: 〒郵便番号 + 都道府県 + 住所
        loc = tds[1].get_text(" ", strip=True)
        pm = _POST_RE.search(loc)
        post = pm.group(1) if pm else ""
        rest = _POST_RE.sub("", loc).strip()
        prm = _PREF_RE.search(rest)
        if prm:
            pref = prm.group(1)
            addr = rest[prm.end():].strip()
        else:
            # 県名省略 (有田郡/和歌山市 等) は和歌山県
            pref = "和歌山県"
            addr = rest

        # TEL / FAX
        telfax = tds[2].get_text(" ", strip=True)
        tm = _TEL_RE.search(telfax)
        tel = tm.group(1) if tm else ""
        fm = _FAX_RE.search(telfax)
        fax = fm.group(1) if fm else ""

        gyoumu = tds[3].get_text(" ", strip=True)

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.POST_CODE: post,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: hp,
            Schema.CAT_SITE: gyoumu,
            "FAX": fax,
            "会員区分": kubun,
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa28()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://w-keibi.or.jp/member-list/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
