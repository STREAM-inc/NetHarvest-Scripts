"""
Ferðamálastofa (Icelandic Tourist Board / アイスランド観光局)
「Ferðaskrifstofur – Gild leyfi (旅行会社の有効免許一覧)」 — クローラー (STREAMREQ-18921)

取得対象:
    アイスランド観光局が公表している ferðaskrifstofa (パッケージ旅行を販売できる
    旅行会社) の有効免許一覧。2026-09-30 時点で 454 行 / 固有免許 428 件。
    1 社が複数拠点 (starfsstöð) を持つ場合は拠点ごとに 1 行になるため、
    行数 > 免許数 となる。拠点情報を落とさないよう **行単位でそのまま出力** し、
    同一社の統合は後工程 (⑪) で Kennitala (法人番号) をキーに行う。

    ⚠ 日帰りツアー業者 (ferðasali dagsferða,
      https://www.ferdamalastofa.is/is/leyfi/utgefin-leyfi/utgefin-leyfi) は
      別一覧であり本クローラーの対象外 (起点 URL が異なるだけなので追加の除外処理は不要)。

取得フロー:
    1. 起点 URL (sites.yml の url = アイスランド語版の一覧ページ) を GET する。
       1 ページ完結でページングは無い (DataTables のクライアントサイド描画だが、
       サーバーが返す HTML に全 454 行がベタ書きされている)。
    2. table.dataTable.permitTable の tbody の行を 1 行ずつ即 yield する。
    ※ 通信は 1 リクエストのみ。詳細ページは存在しない
      (会社名セルのリンク先は各社の公式サイトであり、本サイト内の詳細ページではない)。

元データの列 (アイスランド語版 / 英語版はヘッダ文言だけが異なり中身は同一):
    Leyfisnr. (Licence no.)      … 免許番号 "2011-012"
    Kennitala (Id)               … アイスランドの法人番号 "470809-0190"
    Heiti (Name)                 … 会社名。約 93% に公式サイトへの <a href> が付く
    Hjáheiti (Alternate name)    … 別名・ブランド名 (151/454 行)
    Starfsstöð (Home)            … 拠点の住所 (番地まで。稀に空 / "-")
    Póstnr.-Staður (Postcode)    … "101 Reykjavík" 形式。稀に "999" のみ / 空
    Leyfi útgefið (Date)         … 免許発行日 "29.06.2011" (DD.MM.YYYY)

カラム設計 (呼び出しの【参考スクレイピングカラム】への対応):
    名称        ← Heiti (アイスランド文字 ð/þ/æ/ö/á 等は UTF-8 のまま保持)
    法人番号    ← Kennitala。依頼で「作業用カラムとして保持」とあるため
                  EXTRA「Kennitala(作業用)」にも同値を出す
    住所        ← Starfsstöð (番地まで。"-" のみのプレースホルダは空にする)
    HP          ← 会社名セルのリンク先 (各社の公式サイト)
    サイト定義業種 ← "Ferðaskrifstofa (旅行会社)" 固定。この一覧が
                  ferðaskrifstofa 免許の名簿であることを示す区分ラベル
    取得URL     ← 引数で渡された一覧ページ URL
    EXTRA       ← 免許番号 / Kennitala(作業用) / 別名 / 郵便番号(Póstnúmer) /
                  都市 / 国 / 免許発行日

    ※ 都道府県 (Schema.PREF) は日本の行政区分カラムのため空欄
      (アイスランドに対応する列も無く、推測で埋めない)。
    ※ 郵便番号は Schema.POST_CODE を使わない。アイスランドの Póstnúmer は 3 桁で、
      基盤の正規化 (7 桁必須) が空文字に落としてしまうため EXTRA に出す。
    ※ TEL・メール・代表者名・従業員数・資本金は一覧に列自体が存在しない
      (依頼どおり後工程⑬の公式 HP 巡回で補完する想定)。空欄のままとする。
    ※ 国カラムは依頼指示どおり「アイスランド」で固定する。
      都市は Póstnr.-Staður の 3 桁の後ろの地名 (Reykjavík 等)。

利用規約:
    https://www.ferdamalastofa.is/robots.txt は User-agent: * に対して
    /*/search? ・ /*/leit? ・ /_w/ ・ /inc/ ・ /lang/ ・ /lib/ ・ /local/ ・
    /modules/ ・ /sql/ ・ /static/header/ を Disallow、Crawl-delay: 5。
    本一覧のパス (/is/leyfi/utgefin-leyfi/…) は Disallow 対象外。
    サイトに利用規約ページは無く (クッキーポリシーのみ)、スクレイピングを
    禁止する記載は確認できなかった。取得は 1 リクエストのみで Crawl-delay も満たす。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/streamreq_18921feramalastofa.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id streamreq_18921feramalastofa
"""

import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

# 免許一覧表 (1 ページに 1 個だけ存在する)
_TABLE_SELECTOR = "table.dataTable.permitTable"

# "101 Reykjavík" → ("101", "Reykjavík")。地名の無い "999" だけの行もある。
_POSTCODE_RE = re.compile(r"^(\d{3})\s*(.*)$")

# 免許発行日 "29.06.2011"
_DATE_RE = re.compile(r"^(\d{2})\.(\d{2})\.(\d{4})$")

# 住所・別名セルの「値なし」プレースホルダ
_PLACEHOLDERS = {"-", "–", "—", "‐"}

# 依頼指示により固定する国名
_COUNTRY = "アイスランド"

# この一覧が対象とする免許区分 (日帰りツアー業者の一覧とは別)
_LICENCE_CATEGORY = "Ferðaskrifstofa (旅行会社)"

# 元データの列順 (Leyfisnr. / Kennitala / Heiti / Hjáheiti / Starfsstöð /
# Póstnr.-Staður / Leyfi útgefið)
_COL_COUNT = 7


class StreamReq18921Ferdamalastofa(StaticCrawler):
    """Ferðamálastofa 旅行会社有効免許一覧 スクレイパー"""

    # 通信は一覧ページ 1 回のみ。robots.txt の Crawl-delay: 5 を DELAY で表現しつつ、
    # 1 ページから 454 件を取り出すためアイテム間の待機は 0 にする
    # (ITEM_DELAY=0 にしないと 454 × 5 秒 = 約 38 分かかり完走できない)。
    DELAY = 5.0
    ITEM_DELAY = 0.0
    CONTINUE_ON_ERROR = True
    TIMEOUT = 60

    EXTRA_COLUMNS = [
        "免許番号",
        "Kennitala(作業用)",
        "別名",
        "郵便番号(Póstnúmer)",
        "都市",
        "国",
        "免許発行日",
    ]

    # ------------------------------------------------------------------ utils

    @staticmethod
    def _txt(node) -> str:
        """セルのテキストを取り出し、NBSP・改行・連続空白を半角スペース 1 個に整理する。"""
        if node is None:
            return ""
        s = node.get_text(" ", strip=True) if hasattr(node, "get_text") else str(node)
        s = s.replace(" ", " ").replace("\r", " ").replace("\n", " ")
        s = re.sub(r"\s+", " ", s).strip()
        return "" if s in _PLACEHOLDERS else s

    @staticmethod
    def _split_postcode(value: str) -> tuple[str, str]:
        """"101 Reykjavík" から (郵便番号, 都市) を取り出す。

        3 桁の郵便番号が無い場合 (空欄など) は郵便番号を空にし、
        値全体を都市として返す (推測で補完しない)。
        """
        if not value:
            return "", ""
        m = _POSTCODE_RE.match(value)
        if not m:
            return "", value
        return m.group(1), m.group(2).strip()

    @staticmethod
    def _to_iso_date(value: str) -> str:
        """免許発行日 "29.06.2011" を "2011-06-29" に整形する。想定外の形式は原文のまま。"""
        m = _DATE_RE.match(value or "")
        if not m:
            return value or ""
        day, month, year = m.groups()
        return f"{year}-{month}-{day}"

    # ------------------------------------------------------------------ parse

    def parse(self, url: str):
        """一覧ページを 1 回だけ取得し、免許を 1 行ずつ yield する。"""
        soup = self.get_soup(url)
        if soup is None:
            raise RuntimeError(f"一覧ページを取得できませんでした: {url}")

        table = soup.select_one(_TABLE_SELECTOR)
        if table is None:
            raise RuntimeError(f"免許一覧表 ({_TABLE_SELECTOR}) が見つかりません: {url}")

        body = table.find("tbody") or table
        rows = body.find_all("tr", recursive=False)
        self.total_items = len(rows)

        count = 0
        for tr in rows:
            tds = tr.find_all("td", recursive=False)
            if len(tds) < _COL_COUNT:
                # ヘッダ行や DataTables の "データなし" 行
                continue

            name = self._txt(tds[2])
            if not name:
                continue

            kennitala = self._txt(tds[1])
            postcode, city = self._split_postcode(self._txt(tds[5]))

            # 会社名セルのリンク先が各社の公式サイト (サイト内詳細ページではない)
            link = tds[2].find("a", href=True)
            homepage = urljoin(url, link["href"].strip()) if link else ""

            yield {
                Schema.NAME: name,
                Schema.CO_NUM: kennitala,
                Schema.ADDR: self._txt(tds[4]),
                Schema.HP: homepage,
                Schema.CAT_SITE: _LICENCE_CATEGORY,
                Schema.URL: url,
                "免許番号": self._txt(tds[0]),
                "Kennitala(作業用)": kennitala,
                "別名": self._txt(tds[3]),
                "郵便番号(Póstnúmer)": postcode,
                "都市": city,
                "国": _COUNTRY,
                "免許発行日": self._to_iso_date(self._txt(tds[6])),
            }
            count += 1

        logger.info("有効免許として抽出した行数: %d", count)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    scraper = StreamReq18921Ferdamalastofa()
    scraper.execute("https://www.ferdamalastofa.is/is/leyfi/utgefin-leyfi/ferdaskrifstofur-utgefin-leyfi")
