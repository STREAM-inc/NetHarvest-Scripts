"""
全国警備業協会（AJSSA）会員名簿(沖縄県) — 一般社団法人沖縄県警備業協会 会員企業一覧

取得対象:
    - 沖縄県警備業協会の会員企業（正会員 約87社 + 賛助会員 約7社 = 約94社）
    - 掲載番号・会社名・郵便番号・所在地・代表者・電話番号・業種・会員区分

取得フロー:
    members.php の単一ページ内にある会員テーブルを解析する (ページネーション無し)。
    ページ内には 3 つの <table class="tbl"> があり、
      - 正会員テーブル (6列: No / 会社名 / 所在地 / 代表者 / 電話番号 / 業種)
      - 賛助会員テーブル (4列: 会社名 / 所在地 / 代表者 / 電話番号, 業種・番号無し)
      - 会費テーブル (会員データではないため除外)
    ヘッダ行に「会社名」を含むテーブルのみ会員テーブルとして処理する。
    列数 (6 / 4) で正会員・賛助会員を判定する。
    所在地は「〒xxx-xxxx 住所」形式のため郵便番号と住所を分離し、
    住所に都道府県が含まれなければ「沖縄県」を補完する (賛助会員に県外あり)。
    業種列は 1 文字略号 (施/交/貴/…) のため凡例に基づき正式名称へ展開する。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_43.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_43
"""

import logging
import re
import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)


# 郵便番号 (〒付き/半角・全角ハイフン許容)
_POST_RE = re.compile(r"〒?\s*(\d{3}[-−‐－]\d{4})")
# 都道府県 (住所先頭に付いている場合のみ抽出)
_PREF_RE = re.compile(r"^\s*(北海道|東京都|(?:京都|大阪)府|..県|...県)")

# 業種略号 → 正式名称 (ページ内 .table-top の凡例より)
_GYOSHU = {
    "施": "施設警備",
    "交": "交通誘導警備",
    "貴": "貴重品運搬警備",
    "身": "身辺警備",
    "機": "機械警備",
    "空": "空港警備",
    "ホ": "ホームセキュリティ",
    "保": "保安警備",
    "雑": "雑踏警備",
}


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"[\s　\xa0]+", " ", text).strip()


def _split_location(loc: str) -> tuple[str, str, str]:
    """所在地文字列を (郵便番号, 都道府県, 住所) に分解する。"""
    loc = _clean(loc)
    m = _POST_RE.search(loc)
    post = m.group(1) if m else ""
    rest = _POST_RE.sub("", loc).strip()
    pm = _PREF_RE.match(rest)
    pref = pm.group(1) if pm else "沖縄県"
    return post, pref, rest


def _expand_gyoshu(raw: str) -> str:
    """業種略号列 (例: '施 交 貴 機') を正式名称へ展開する。"""
    names = [_GYOSHU.get(ch) for ch in raw if ch.strip()]
    return " / ".join(n for n in names if n)


class Ajssa43(StaticCrawler):
    """全国警備業協会（AJSSA）会員名簿(沖縄県) スクレイパー"""

    DELAY = 1.5
    # 掲載番号 / 会員区分 はサイト固有の短い構造化ラベル → EXTRA。
    EXTRA_COLUMNS = ["掲載番号", "会員区分"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)
        if not soup:
            logger.warning("ページの取得に失敗: %s", url)
            return

        # class に "tbl" を含むテーブルのうち、ヘッダに「会社名」を含むものだけが会員テーブル。
        member_tables = []
        for table in soup.find_all("table"):
            classes = table.get("class") or []
            if "tbl" not in classes:
                continue
            header = table.find("tr")
            if header is None:
                continue
            header_txt = _clean(header.get_text("", strip=True))
            if "会社名" in header_txt.replace(" ", ""):
                member_tables.append(table)

        # 先に全会員行数を数えて進捗表示を有効化する (DOM 走査のみ・通信は発生しない)。
        data_rows = []  # [(table_kind, tds)]
        for table in member_tables:
            for tr in table.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) == 6:
                    data_rows.append(("正会員", tds))
                elif len(tds) == 4:
                    data_rows.append(("賛助会員", tds))
        self.total_items = len(data_rows)

        for kind, tds in data_rows:
            try:
                if kind == "正会員":
                    no = _clean(tds[0].get_text(strip=True))
                    name = _clean(tds[1].get_text(strip=True))
                    loc = tds[2].get_text(" ", strip=True)
                    rep = _clean(tds[3].get_text(strip=True))
                    tel = _clean(tds[4].get_text(strip=True))
                    gyoshu = _expand_gyoshu(_clean(tds[5].get_text("", strip=True)))
                else:  # 賛助会員 (番号・業種列なし)
                    no = ""
                    name = _clean(tds[0].get_text(strip=True))
                    loc = tds[1].get_text(" ", strip=True)
                    rep = _clean(tds[2].get_text(strip=True))
                    tel = _clean(tds[3].get_text(strip=True))
                    gyoshu = ""

                if not name:
                    continue

                post, pref, addr = _split_location(loc)

                yield {
                    Schema.URL: url,
                    Schema.NAME: name,
                    Schema.PREF: pref,
                    Schema.POST_CODE: post,
                    Schema.ADDR: addr,
                    Schema.REP_NM: rep,
                    Schema.TEL: tel,
                    Schema.CAT_SITE: gyoshu,
                    "掲載番号": no,
                    "会員区分": kind,
                }
            except Exception as e:  # noqa: BLE001
                logger.warning("行の解析に失敗: %s", e)
                continue


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa43()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.okikei.com/members.php#a01")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
