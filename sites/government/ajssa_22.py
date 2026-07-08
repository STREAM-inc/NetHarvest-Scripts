"""
一般社団法人 三重県警備業協会（AJSSA 会員名簿・三重県）— 会員企業一覧

取得対象:
    - 三重県警備業協会の会員企業（警備会社・約95社）
    - 会社名 / 業種(警備種別コード) / 郵便番号 / 住所 / 都道府県 / TEL

取得フロー:
    引数 url (= sites.yml の url, https://mie.r.ajssa.or.jp/menber.html) は
    静的な単一ページ (Homepage Builder 生成 / Shift_JIS)。会員一覧は複数の
    <table> に分かれて全件掲載されている。ページネーション無し。

    テーブル構造:
      - 正会員テーブル: 見出し (th) = 名称 / 業種 / 郵便番号・所在地 / 電話番号
        業種は "施交貴身機ホ保" のような警備種別の 1 文字コード連結
        (施:施設警備 交:交通誘導警備 貴:貴重品運搬警備 身:身辺警備
         機:機械警備 ホ:ホームセキュリティ 保:保安警備)
      - 県外賛助会員テーブル: 見出し = 名称 / 郵便番号・所在地 / 電話番号 (業種無し)
      - 入会金・会費テーブル: 見出しに「名称」を含まない → 会員データではないため除外

    見出し行 (th) に「名称」を含むテーブルのみを会員一覧として扱い、見出し順から
    列インデックスを決定して各データ行 (td) を 1 件ずつ即 yield する (Pattern B)。

    郵便番号・所在地は 1 セル内に <br> 区切りで格納される。先頭が郵便番号、
    残りが住所。住所が都道府県名で始まる場合 (県外賛助会員) はその都道府県を
    PREF に、残りを ADDR に格納する。都道府県で始まらない場合 (三重県内会員は
    市区町村から始まる) は PREF="三重県" を補完する。

実行方法:
    # ローカルテスト
    python scripts/sites/government/ajssa_22.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ajssa_22
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

_DEFAULT_PREF = "三重県"

# 郵便番号 (123-4567 / 1234567) 判定
_POSTAL_RE = re.compile(r"^〒?\s*\d{3}-?\d{4}$")

# 住所先頭の都道府県抽出用 (県外賛助会員の住所から PREF を切り出す)
_PREFECTURES = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]


class Ajssa22(StaticCrawler):
    """一般社団法人 三重県警備業協会 会員名簿 スクレイパー"""

    DELAY = 1.5
    # 業種 (警備種別の短い構造化コード) は Schema.CAT_SITE に格納。
    # サイト固有の追加カラムは無し。
    EXTRA_COLUMNS: list[str] = []

    def parse(self, url: str):
        # 引数 url (= sites.yml の url) を唯一のルートとして取得する
        soup = self.get_soup(url)
        if soup is None:
            logger.warning("ページ取得に失敗しました: %s", url)
            return

        # 見出し (th) に「名称」を含むテーブルのみが会員一覧
        # (入会金・会費テーブルは th に「名称」を持たないため除外)
        member_tables = []
        for table in soup.find_all("table"):
            headers = [th.get_text(strip=True) for th in table.find_all("th")]
            if any("名称" in h for h in headers):
                member_tables.append(table)

        # 進捗表示のため総件数を事前集計 (単一ページ内・ネットワーク不要)
        total = 0
        for table in member_tables:
            total += sum(1 for tr in table.find_all("tr") if tr.find("td"))
        self.total_items = total

        for table in member_tables:
            col_index = self._header_index(table)
            for tr in table.find_all("tr"):
                tds = tr.find_all("td")
                if not tds:
                    continue  # 見出し行
                try:
                    item = self._parse_row(tds, col_index, url)
                    if item:
                        yield item
                except Exception as e:  # 個別行のエラーはスキップして継続
                    logger.warning("行の解析に失敗しskip: %s", e)
                    continue

    @staticmethod
    def _header_index(table) -> dict:
        """テーブル見出し (th) の種別 → 列インデックスの対応を返す。

        テーブルにより列構成が異なる (業種の有無) ため、見出しテキストの
        キーワードで論理列を判定する。
        """
        head_row = table.find("tr")
        cells = head_row.find_all("th") if head_row else []
        idx = {}
        for i, th in enumerate(cells):
            t = th.get_text(strip=True)
            if "名称" in t and "name" not in idx:
                idx["name"] = i
            elif "業種" in t and "gyoushu" not in idx:
                idx["gyoushu"] = i
            elif ("所在地" in t or "住所" in t or "郵便番号" in t) and "addr" not in idx:
                idx["addr"] = i
            elif "電話" in t and "tel" not in idx:
                idx["tel"] = i
        return idx

    def _parse_row(self, tds, col_index: dict, source_url: str) -> dict | None:
        def cell(key: str):
            i = col_index.get(key)
            if i is None or i >= len(tds):
                return None
            return tds[i]

        name_cell = cell("name")
        if name_cell is None:
            return None
        name = self._norm(name_cell.get_text(" ", strip=True))
        if not name:
            return None

        # 業種: 警備種別の 1 文字コード連結 (県外賛助会員テーブルには列が無い)
        gyoushu_cell = cell("gyoushu")
        gyoushu = self._norm(gyoushu_cell.get_text("", strip=True)) if gyoushu_cell else ""

        # TEL
        tel_cell = cell("tel")
        tel = self._norm(tel_cell.get_text(" ", strip=True)) if tel_cell else ""

        # 郵便番号・所在地: 1 セル内に <br> 区切り (先頭=〒, 残り=住所)
        post_code, pref, addr = "", _DEFAULT_PREF, ""
        addr_cell = cell("addr")
        if addr_cell is not None:
            parts = [self._norm(s) for s in addr_cell.stripped_strings]
            parts = [p for p in parts if p]
            if parts and _POSTAL_RE.match(parts[0]):
                post_code = parts[0].lstrip("〒").strip()
                addr_parts = parts[1:]
            else:
                addr_parts = parts
            full_addr = "".join(addr_parts)
            # 住所が都道府県名で始まる場合 (県外賛助会員) は PREF を切り出す
            for p in _PREFECTURES:
                if full_addr.startswith(p):
                    pref = p
                    full_addr = full_addr[len(p):]
                    break
            addr = full_addr

        return {
            Schema.URL: source_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.CAT_SITE: gyoushu,
        }

    @staticmethod
    def _norm(text: str) -> str:
        """全角/半角スペース・改行を単一スペースに整形する。"""
        return re.sub(r"[\s　]+", " ", text).strip()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Ajssa22()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://mie.r.ajssa.or.jp/menber.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
