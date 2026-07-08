"""
一般社団法人 群馬県警備業協会（AJSSA 会員名簿・群馬県）— 会員企業一覧

取得対象:
    - 群馬県警備業協会の会員企業（正会員 約84社 + 賛助会員 約7社）
    - 会社名 / 郵便番号 / 都道府県 / 住所 / 電話番号 / HP / 主たる業務(警備種別) / 会員区分

取得フロー:
    会員名簿ページ (recruit.html) に、会員企業が 2 つの <table> として全件静的に
    掲載されている（Homepage Builder 製・Shift_JIS）。ページネーション無し・詳細ページ無し。
      table1: 会員企業(正会員) — 会社名 / 所在地 / 電話番号 / 主たる業務 の 4 列
      table2: 賛助会員        — 会社名 / 所在地 / 電話番号 の 3 列（主たる業務なし）
    先頭行はヘッダ (会社名 …) なのでスキップし、各データ行を 1 件ずつ即 yield する。

    列の中身:
      会社名: <a href> があれば HP。テキストが社名。
      所在地: 「〒370-0841　高崎市栄町26-25 …」→ 郵便番号 / 都道府県 / 住所 に分解。
              群馬県内は都道府県表記が省略され市名始まりなので PREF=群馬県 を補完する。
              賛助会員は東京都等の県外を含むため、都道府県表記があればそれを採用。
      電話番号: そのまま TEL。
      主たる業務: 施/身/交/機/雑/ホ/貴/保 の略号を「・」区切り → 正式名称に展開し「/」連結。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_9.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_9
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

# 郵便番号 〒NNN-NNNN
_POST_RE = re.compile(r"〒?\s*(\d{3}-\d{4})")

# 都道府県（住所先頭判定用）
_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 主たる業務: 略号 → 正式名称
_GYOUSHU_MAP = {
    "施": "施設警備業務",
    "身": "身辺警備業務",
    "交": "交通誘導警備業務",
    "機": "機械警備業務",
    "雑": "雑踏警備業務",
    "ホ": "ホームセキュリティ",
    "貴": "貴重品運搬警備業務",
    "保": "保安警備業務",
}


def _clean(text: str) -> str:
    """全角空白・連続空白・nbsp を正規化して trim する。"""
    return re.sub(r"[\s　\xa0]+", " ", text).strip()


class Ajssa9(StaticCrawler):
    """一般社団法人 群馬県警備業協会 会員名簿 スクレイパー"""

    DELAY = 1.5
    # 会員区分（正会員/賛助会員）は Schema に無い構造化ラベルのため EXTRA。
    # 主たる業務(警備種別) は Schema.CAT_SITE に格納する。
    EXTRA_COLUMNS = ["会員区分"]

    def parse(self, url: str):
        # 引数 url (= sites.yml の url) を唯一のルートとして使う
        soup = self.get_soup(url)
        if soup is None:
            logger.warning("会員名簿の取得に失敗: %s", url)
            return

        tables = soup.find_all("table")

        # 総件数（進捗表示用）: 各テーブルの会社名を持つデータ行を数える
        total = 0
        for tb in tables:
            for tr in tb.find_all("tr"):
                tds = tr.find_all("td")
                if not tds:
                    continue
                name = _clean(tds[0].get_text(" ", strip=True))
                if name and name != "会社名":
                    total += 1
        self.total_items = total

        for tb in tables:
            rows = tb.find_all("tr")
            if not rows:
                continue
            # ヘッダ行に「主たる業務」列があれば正会員テーブル、無ければ賛助会員
            header_text = rows[0].get_text(" ", strip=True)
            membership = "正会員" if "主たる業務" in header_text else "賛助会員"

            for tr in rows:
                tds = tr.find_all("td")
                if len(tds) < 3:
                    continue
                if _clean(tds[0].get_text(" ", strip=True)) in ("", "会社名"):
                    continue  # ヘッダ / 空行
                try:
                    item = self._parse_row(tds, url, membership)
                    if item:
                        yield item
                except Exception as e:  # 個別行のエラーはスキップして継続
                    logger.warning("行の解析に失敗しskip: %s", e)
                    continue

    def _parse_row(self, tds, source_url: str, membership: str) -> dict | None:
        # td0: 会社名 + HP リンク
        name = _clean(tds[0].get_text(" ", strip=True))
        if not name:
            return None
        a = tds[0].find("a", href=True)
        hp = a["href"].strip() if a and a["href"].strip().startswith("http") else ""

        # td1: 所在地（郵便番号 + 住所） → 分解
        loc = _clean(tds[1].get_text(" ", strip=True))
        post_code = ""
        m = _POST_RE.search(loc)
        if m:
            post_code = m.group(1)
            loc = _clean(loc[m.end():])
        pref = ""
        pm = _PREF_RE.match(loc)
        if pm:
            pref = pm.group(1)
            addr = _clean(loc[pm.end():])
        else:
            # 群馬県内は都道府県表記が省略されている（市名始まり）
            pref = "群馬県"
            addr = loc

        # td2: 電話番号
        tel = _clean(tds[2].get_text(" ", strip=True))

        # td3(あれば): 主たる業務 略号 → 正式名称
        cat_site = ""
        if len(tds) >= 4:
            raw = _clean(tds[3].get_text(" ", strip=True))
            codes = [c for c in re.split(r"[・,、/\s]+", raw) if c]
            cat_site = "/".join(_GYOUSHU_MAP.get(c, c) for c in codes)

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: hp,
            Schema.CAT_SITE: cat_site,
            "会員区分": membership,
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa9()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("http://www.gunkeikyo.or.jp/recruit.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
