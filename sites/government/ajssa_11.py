"""
一般社団法人 千葉県警備業協会（千警協 / AJSSA 会員名簿・千葉県）— 加盟会社名簿

取得対象:
    - 千葉県警備業協会の加盟会社（7支部 + 賛助会員）
    - 会社名 / 支部 / 都道府県 / TEL / HP / 警備種別（区分）

取得フロー:
    n_membership.html は索引ページで、7支部 + 賛助会員の各名簿サブページ
    (/n_membership_{slug}.html) へのリンクを列挙するだけ。実データは各サブページの
    <table> にある。データ行は 3 セル構成:
        <th class="col-title">会社名(HPリンク内包の場合あり)</th>
        <td>区分(施設/交通/貴重品/機械/身辺/ホーム/保安 …)</td>
        <td>電話番号</td>
    ヘッダ行は全て <th>。賛助会員テーブルの 2 列目は「区分」ではなく「業務内容」で、
    自由記述の文章（会社の事業説明）が入るため、著作権リスクを避けて CAT_SITE には
    格納しない（ヘッダの列名で構造化ラベルか判定する）。
    索引 → 各サブページの順に巡回し、会員 1 件ごとに即 yield する (Pattern B)。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_11.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_11
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

# 支部名簿サブページ (n_membership_chuoh.html 等)。索引自身 (n_membership.html) は除外
_SUBPAGE_RE = re.compile(r"n_membership_[a-z0-9]+\.html$", re.I)


class Ajssa11(StaticCrawler):
    """一般社団法人 千葉県警備業協会 加盟会社名簿 スクレイパー"""

    DELAY = 1.5
    # 区分(施設/交通 等の短い警備種別ラベル) → Schema.CAT_SITE。支部は EXTRA。
    # 賛助会員の「業務内容」列は自由記述の文章のため取得しない（著作権リスク）。
    EXTRA_COLUMNS = ["支部"]

    def parse(self, url: str):
        # 引数 url (= sites.yml の url / 索引ページ) を唯一のルートとして派生させる
        index_soup = self.get_soup(url)
        if index_soup is None:
            logger.warning("索引ページの取得に失敗: %s", url)
            return

        content = index_soup.select_one("div.page-company") or index_soup
        # 各支部・賛助会員のサブページリンク (テキスト=支部名, href=名簿ページ)
        subpages = []
        seen = set()
        for a in content.find_all("a", href=True):
            href = a["href"]
            if not _SUBPAGE_RE.search(href):
                continue
            full = urljoin(url, href)
            if full in seen:
                continue
            seen.add(full)
            branch = a.get_text(strip=True)
            subpages.append((branch, full))

        if not subpages:
            logger.warning("サブページリンクが見つかりません: %s", url)
            return

        for branch, page_url in subpages:
            soup = self.get_soup(page_url)
            if soup is None:
                logger.warning("名簿ページの取得に失敗しskip: %s", page_url)
                continue
            # 賛助会員ページ(その他加盟会社を含む)は千葉県外の企業も含むため PREF は空にする
            pref = "" if "賛助" in branch else "千葉県"
            for table in soup.select("div.page-company table, div.site-content table"):
                yield from self._parse_table(table, branch, pref, page_url)

    def _parse_table(self, table, branch: str, pref: str, source_url: str):
        rows = table.find_all("tr")
        if not rows:
            return

        # ヘッダ行(全て <th>)の 2 列目が「区分」なら構造化ラベル、「業務内容」等は文章列。
        # 見出しは「区　分」のように全角スペースを含むため空白を除去して判定する
        header_cells = rows[0].find_all(["th", "td"])
        second_header = header_cells[1].get_text(strip=True) if len(header_cells) >= 2 else ""
        second_header = re.sub(r"[\s　]", "", second_header)
        is_structured = "区分" in second_header

        for row in rows:
            tds = row.find_all("td", recursive=False)
            if not tds:  # ヘッダ行(td 無し)はスキップ
                continue
            try:
                item = self._parse_row(row, tds, branch, pref, is_structured, source_url)
                if item:
                    yield item
            except Exception as e:  # 個別会員のエラーはスキップして継続
                logger.warning("会員の解析に失敗しskip: %s", e)
                continue

    def _parse_row(self, row, tds, branch, pref, is_structured, source_url) -> dict | None:
        name_cell = row.find("th")
        if name_cell is None:
            return None
        name = name_cell.get_text(" ", strip=True)
        name = re.sub(r"\s+", " ", name).strip()
        if not name:
            return None

        # HP: 会社名セル内の外部リンク (協会内リンクは除外)
        a = name_cell.find(
            "a",
            href=lambda h: h and h.startswith("http") and "chikeikyo" not in h,
        )
        hp = a["href"].strip() if a else ""

        # 区分(警備種別) は構造化ラベルのときのみ CAT_SITE に格納。業務内容(文章)は除外
        cat_site = ""
        if is_structured and len(tds) >= 2:
            cat_site = re.sub(r"\s+", " ", tds[0].get_text(" ", strip=True)).strip()

        # 電話番号は最終セル
        tel = tds[-1].get_text(strip=True) if tds else ""

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.TEL: tel,
            Schema.HP: hp,
            Schema.CAT_SITE: cat_site,
            "支部": branch,
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa11()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://www.chikeikyo.or.jp/n_membership.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
