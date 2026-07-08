"""
一般社団法人 神奈川県警備業協会（神警協 / AJSSA 会員名簿・神奈川県）— 会員会社名簿

取得対象:
    - 神奈川県警備業協会の会員会社（10 支部）+ 賛助会員会社
    - 会社名 / 支部 / 都道府県 / HP / 警備種別（業種）/ 業種コード

取得フロー:
    kaiinn.html 1 ページに全データがある静的ページ。ページネーション無し。
    - 支部別の会員テーブルは <table class="tableizer-table">（10 個）。各テーブルの
      直前の <h4> が支部名（横浜中央支部 …）。データ行は 2 セル:
          <td>会社名(HP リンク内包の場合あり)</td>
          <td>業種コード(1･2･＊ 等)</td>
      先頭行は <th>（会社名 / 業種）のヘッダ。
      業種コードは 1=施設 2=交通 3=貴重品 4=身辺警護 5=機械警備 6=空港保安警備
      ホ=ホームセキュリティ 保=保安警備 ＊=ビルメンテナンス業等との兼業。
      → CAT_SITE には展開したラベル、EXTRA「業種コード」には生コードを格納。
    - 賛助会員テーブルは class 無しでヘッダに「会社名」を含む 3 列
      (項 / 会社名 / 業種=業務内容)。業務内容は「制服販売」「保険」等の短い構造化
      ラベルなので CAT_SITE に格納する（賛助は県外企業も含むため PREF は空、支部=賛助会員）。
    - class 無しの「全会員」マスタ表（4 列, 項番/会社名/支店/業種）は支部別と重複し
      HP リンクを持たないため取得しない（重複回避）。
    会員 1 件ごとに即 yield する (Pattern B)。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_12.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_12
"""

import logging
import re
import sys
import unicodedata
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 業種コード → 警備種別ラベル（NFKC 正規化後のキーで照合）
_CODE_LABELS = {
    "1": "施設",
    "2": "交通",
    "3": "貴重品",
    "4": "身辺警護",
    "5": "機械警備",
    "6": "空港保安警備",
    "ホ": "ホームセキュリティ",
    "保": "保安警備",
    "*": "兼業(ビルメンテナンス業等)",
}
# 業種コードの区切り（NFKC 後は中黒 ・ に統一される）
_CODE_SEP_RE = re.compile(r"[・,、/／\s]+")
# 会員会社名の飾りスペース除去（CJK 文字間の空白のみ）
_CJK_SPACE_RE = re.compile(r"(?<=[^\x00-\x7F])\s+(?=[^\x00-\x7F])")


class Ajssa12(StaticCrawler):
    """一般社団法人 神奈川県警備業協会 会員会社名簿 スクレイパー"""

    DELAY = 1.5
    # 業種(警備種別)は CAT_SITE。支部・生の業種コードは EXTRA。
    EXTRA_COLUMNS = ["支部", "業種コード"]

    def parse(self, url: str):
        # 引数 url (= sites.yml の url) を唯一のルートとする
        soup = self.get_soup(url)
        if soup is None:
            logger.warning("ページの取得に失敗: %s", url)
            return

        # 1) 支部別の会員テーブル（class="tableizer-table"）
        for table in soup.select("table.tableizer-table"):
            h4 = table.find_previous("h4")
            branch = h4.get_text(strip=True) if h4 else ""
            for row in table.find_all("tr"):
                tds = row.find_all("td", recursive=False)
                if not tds:  # ヘッダ行(<th>)はスキップ
                    continue
                try:
                    item = self._parse_member_row(tds, branch, url)
                    if item:
                        yield item
                except Exception as e:  # 個別会員のエラーはスキップして継続
                    logger.warning("会員の解析に失敗しskip: %s", e)
                    continue

        # 2) 賛助会員テーブル（class 無し・ヘッダに「会社名」を含む 3 列）
        for table in soup.find_all("table"):
            if table.get("class"):  # 支部別テーブルは 1) で処理済み
                continue
            if not re.search(r"会.{0,3}社.{0,3}名", table.get_text()):
                continue  # 全会員マスタ表(ヘッダ無し)はここで除外
            for row in table.find_all("tr"):
                tds = row.find_all("td", recursive=False)
                if len(tds) < 3:
                    continue
                try:
                    item = self._parse_support_row(tds, url)
                    if item:
                        yield item
                except Exception as e:
                    logger.warning("賛助会員の解析に失敗しskip: %s", e)
                    continue

    def _parse_member_row(self, tds, branch: str, source_url: str) -> dict | None:
        name = self._clean_name(tds[0].get_text(" ", strip=True))
        if not name or name == "会社名":
            return None

        # HP: 会社名セル内の外部リンク（協会内リンクは除外）
        a = tds[0].find(
            "a",
            href=lambda h: h and h.startswith("http") and "shinkeikyo" not in h,
        )
        hp = a["href"].strip() if a else ""

        raw_code = tds[1].get_text(strip=True) if len(tds) >= 2 else ""
        cat_site = self._expand_code(raw_code)

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: "神奈川県",
            Schema.HP: hp,
            Schema.CAT_SITE: cat_site,
            "支部": branch,
            "業種コード": raw_code,
        }

    def _parse_support_row(self, tds, source_url: str) -> dict | None:
        # 賛助会員: 項 / 会社名 / 業務内容
        name = self._clean_name(tds[1].get_text(" ", strip=True))
        if not name or re.search(r"会.{0,3}社.{0,3}名", name):  # ヘッダ行を除外
            return None

        a = tds[1].find(
            "a",
            href=lambda h: h and h.startswith("http") and "shinkeikyo" not in h,
        )
        hp = a["href"].strip() if a else ""
        biz = re.sub(r"\s+", " ", tds[2].get_text(" ", strip=True)).strip()

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: "",  # 賛助会員は県外企業も含むため空
            Schema.HP: hp,
            Schema.CAT_SITE: biz,  # 業務内容(制服販売/保険 等の短い構造化ラベル)
            "支部": "賛助会員",
            "業種コード": "",
        }

    @staticmethod
    def _clean_name(text: str) -> str:
        # CJK 文字間の飾りスペースを除去してから空白を正規化
        text = _CJK_SPACE_RE.sub("", text)
        return re.sub(r"\s+", " ", text).strip()

    @staticmethod
    def _expand_code(raw_code: str) -> str:
        if not raw_code:
            return ""
        nfkc = unicodedata.normalize("NFKC", raw_code)
        labels = []
        for tok in _CODE_SEP_RE.split(nfkc):
            if not tok:
                continue
            label = _CODE_LABELS.get(tok)
            if label and label not in labels:
                labels.append(label)
        return "・".join(labels)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa12()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://www.shinkeikyo.or.jp/kaiinn.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
