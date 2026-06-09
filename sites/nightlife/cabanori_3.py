"""
cabanori.com 求人 — 全エリアのナイトワーク求人情報

cabanori.com は複数の地域ブランドを同一ドメインでホストしている。
従来は「ハマのり(神奈川)」だけを取得していたが、東京エリアの「キャバのり」など
他ブランドも存在するため、全ブランドを巡回して「全エリア」を取得する。

    - ""          … キャバのり (東京: 立川/八王子/吉祥寺/国分寺/府中/調布/町田 ほか)
                     → /recruits/page:N         / 詳細: /shops/{id}/recruit
    - "yokohama"  … ハマのり   (神奈川: 横浜/川崎/湘南/相模原 ほか)
                     → /yokohama/recruits/page:N / 詳細: /yokohama/shops/{id}/recruit

  ※ 求人詳細リンクは各ブランドの絶対パス (/shops/... または /yokohama/shops/...) で
    出力されるため、詳細ページの解析処理 (_scrape_recruit) は両ブランド共通で利用できる。

取得対象:
    - 各ブランドのキャバクラ/ガールズバー/セクキャバ等の
      「店舗別 求人(女性求人)」情報
    - 1店舗に複数の募集職種がある場合は職種ごとに1レコード

取得フロー (一覧 → 詳細 / Pattern B):
    1. ブランドごとに /{prefix}/recruits/page:N を page:1 から順に巡回
       (CakePHP 形式のページネーション。NEXT ボタン(a.c-button--next)が無くなるまでループ)
    2. 各ページの li.p-recruit-list から求人詳細ページ URL
       (a.p-recruit-list__head-item → /shops/{id}/recruit 等) を取得
    3. 詳細ページを 1 件ずつ取得して即 yield (途中 break しても無駄通信を抑える)
         - 店舗情報テーブル (table.p-content--shop-information__table):
             店名 / 所在地 / 業種 / 営業時間 / 定休日
         - 募集要項テーブル (table.c-table--no-border) を募集職種ごとに:
             募集職種 / 給与 / 勤務日 / 勤務時間(時間)
         - エリア (h2 の "○○エリア｜業種　店名" の左側)
         - TEL (a[href^="tel:"])
    4. 同一詳細 URL は重複除去

著作権配慮で除外したフィールド (長文の自由記述プロースのため):
    - 資格 (応募資格の自由記述) … 文章形式の自由記述のため
    - ルート/アクセス (例 "横浜駅西口から徒歩3分 …です") … 文章形式の自由記述のため
    - キャッチコピー / こんな人にオススメ (一覧・詳細の宣伝文) … 自由記述プロースのため
    - 店舗紹介文 … 長文の自由記述のため
    - 待遇 / その他待遇 … (除外指示により取得しない)
    ※ 給与 / 勤務日 / 勤務時間 / 募集職種 は求人の事実情報(構造化データ)として取得する。

著作権配慮 (勤務日 / 勤務時間 の単語化):
    - 勤務日 / 勤務時間 はスクレイピングした文章をそのまま保存すると著作権上の懸念があるため、
      保存前に接続詞・助詞・助動詞・記号を除去し「単語のみ」を空白区切りで保存する
      (_words_only)。例: "週1日からOK!自由出勤です" → "週1日 OK 自由出勤"

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/cabanori_3.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id cabanori_3
"""

import re
import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


_BASE = "https://cabanori.com"

# cabanori.com 上の地域ブランド一覧 (prefix が空文字 = キャバのり)。
# 全エリアを取得するため、ここに列挙した全ブランドを巡回する。
_BRANDS = [
    {"prefix": "", "label": "キャバのり"},        # 東京エリア
    {"prefix": "yokohama", "label": "ハマのり"},   # 神奈川エリア
]


def _list_url(prefix: str, page: int) -> str:
    """ブランド prefix とページ番号から求人一覧ページの URL を組み立てる。"""
    seg = f"/{prefix}" if prefix else ""
    return f"{_BASE}{seg}/recruits/page:{page}"

_POSTAL_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


def _norm_label(text: str) -> str:
    """テーブル見出しから空白(全角含む)を除去して正規化する。"""
    return re.sub(r"\s+", "", (text or "").replace("　", "")).strip()


def _clean(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip()


# 著作権配慮: 勤務日/勤務時間 は文章(自由記述プロース)をそのまま保存せず、
# 接続詞・助詞・助動詞・記号を除いた「単語のみ」を空白区切りで保存する。
_STOPWORDS = [
    # 接続詞
    "そして", "それから", "さらに", "また", "および", "ならびに", "並びに",
    "かつ", "だから", "ですので", "しかし", "ですが", "ただし", "なお",
    "または", "もしくは", "若しくは",
    # 助動詞・語尾
    "ました", "ません", "である", "です", "ます", "だっ", "でし",
    # 助詞 (長いものから先に並べて部分一致の取りこぼしを防ぐ)
    "から", "まで", "より", "など", "ので", "けど", "って",
    "は", "が", "を", "に", "へ", "と", "で", "や", "の", "も", "ね", "よ", "か",
]
_STOP_RE = re.compile("|".join(re.escape(w) for w in _STOPWORDS))
_WORD_SEP_RE = re.compile(r"[\s、。，．,.・…「」『』（）()\[\]【】〜~/／｜|！？!?:：]+")


def _words_only(text: str | None) -> str:
    """文章から接続詞・助詞などを除き、単語のみを空白区切りで返す(著作権配慮)。"""
    if not text:
        return ""
    # 区切り記号を空白に置換
    text = _WORD_SEP_RE.sub(" ", str(text))
    # 接続詞・助詞・助動詞を空白に置換
    text = _STOP_RE.sub(" ", text)
    # 単語化 (出現順を保ちつつ重複除去)
    seen: set[str] = set()
    words: list[str] = []
    for w in text.split():
        if w and w not in seen:
            seen.add(w)
            words.append(w)
    return " ".join(words)


class Cabanori3Crawler(StaticCrawler):
    """ハマのり (神奈川) ナイトワーク求人スクレイパー"""

    DELAY = 1.5
    # Schema に該当しないサイト固有カラム (求人の構造化情報のみ。自由記述プロースは除外)
    EXTRA_COLUMNS = ["エリア", "募集職種", "給与", "勤務日", "勤務時間"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 全ブランド(全エリア)を巡回。詳細 URL の重複除去はブランド横断で共有する。
        seen: set[str] = set()
        for brand in _BRANDS:
            self.logger.info("ブランド [%s] の求人取得を開始します。", brand["label"])
            yield from self._crawl_brand(brand["prefix"], brand["label"], seen)

    def _crawl_brand(
        self, prefix: str, label: str, seen: set[str]
    ) -> Generator[dict, None, None]:
        """1ブランド分の求人一覧をページ送りしながら巡回する。"""
        page = 1
        while True:
            list_url = _list_url(prefix, page)
            soup = self.get_soup(list_url)
            if soup is None:
                self.logger.warning("求人一覧ページ取得失敗: %s", list_url)
                break

            cards = soup.select("li.p-recruit-list")
            if not cards:
                self.logger.info("[%s] ページ %d に求人なし。終了します。", label, page)
                break

            for card in cards:
                head = card.select_one("a.p-recruit-list__head-item[href]")
                if not head:
                    continue
                href = (head.get("href") or "").strip()
                if not href:
                    continue
                detail_url = href if href.startswith("http") else _BASE + href
                if detail_url in seen:
                    continue
                seen.add(detail_url)

                try:
                    yield from self._scrape_recruit(detail_url)
                except Exception as e:
                    self.logger.warning("求人詳細ページ解析失敗 (%s): %s", detail_url, e)
                    continue

            # NEXT ボタンが無ければ最終ページ
            if not soup.select_one("a.c-button--next"):
                self.logger.info("[%s] 最終ページ (%d) を処理しました。", label, page)
                break
            page += 1

    def _scrape_recruit(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)
        if soup is None:
            return

        # --- 店舗情報テーブル (店名/所在地/業種/営業時間/定休日) ---
        info: dict[str, str] = {}
        info_table = soup.select_one("table.p-content--shop-information__table")
        if info_table:
            for row in info_table.select("tr"):
                th = row.select_one("th")
                td = row.select_one("td")
                if not th or not td:
                    continue
                info[_norm_label(th.get_text())] = _clean(td.get_text(" ", strip=True))

        name = info.get("店名", "")
        if not name:
            self.logger.warning("店舗名が空です: %s", url)
            return

        post_code, pref, addr = self._split_address(info.get("所在地", ""))

        # エリア (h2: "○○エリア｜業種　店名")
        area = ""
        h2 = soup.select_one("h2")
        if h2 and "｜" in h2.get_text():
            area = h2.get_text(strip=True).split("｜")[0].strip()

        # TEL
        tel = ""
        tel_a = soup.select_one('a[href^="tel:"]')
        if tel_a:
            tel = (tel_a.get("href") or "").replace("tel:", "").strip()

        base = {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.POST_CODE: post_code,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.CAT_SITE: info.get("業種", ""),
            Schema.TIME: info.get("営業時間", ""),
            Schema.HOLIDAY: info.get("定休日", ""),
            "エリア": area,
        }

        # --- 募集要項テーブル (募集職種ごとに1レコード) ---
        emitted = False
        for job_table in soup.select("table.c-table--no-border"):
            job = self._parse_job_table(job_table)
            # 募集職種/給与/勤務日/勤務時間 が全て空のテーブル(プレースホルダ)はスキップ
            if not any(job.values()):
                continue
            record = dict(base)
            record.update(job)
            yield record
            emitted = True

        # 募集要項が1件も取れなくても店舗情報レコードは残す
        if not emitted:
            yield dict(base)

    @staticmethod
    def _parse_job_table(table) -> dict[str, str]:
        """募集要項テーブルから構造化された求人情報のみ抽出する (資格=自由記述は除外)。"""
        raw: dict[str, str] = {}
        for row in table.select("tr"):
            th = row.select_one("th")
            td = row.select_one("td")
            if not th or not td:
                continue
            raw[_norm_label(th.get_text())] = _clean(td.get_text(" ", strip=True))

        return {
            "募集職種": raw.get("募集職種", ""),
            "給与": raw.get("給与", ""),
            # 著作権配慮: 文章をそのまま保存せず単語のみ抽出する
            "勤務日": _words_only(raw.get("勤務日", "")),
            "勤務時間": _words_only(raw.get("時間", "")),
        }

    @staticmethod
    def _split_address(raw: str) -> tuple[str, str, str]:
        """所在地文字列から 郵便番号 / 都道府県 / 住所(市区町村以降) を分離する。"""
        post_code = ""
        raw = raw or ""
        pm = _POSTAL_RE.search(raw)
        if pm:
            post_code = pm.group(1)
            raw = raw[pm.end():].strip()
        raw = raw.lstrip("〒 ").strip()

        pref = ""
        addr = raw
        prm = _PREF_RE.search(raw)
        if prm:
            pref = prm.group(1)
            addr = raw[prm.end():].strip()
        return post_code, pref, addr


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # parse() は内部で全ブランド(全エリア)を巡回するため、シード URL は起点の目印。
    scraper = Cabanori3Crawler()
    scraper.execute("https://cabanori.com/recruits/page:1")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
