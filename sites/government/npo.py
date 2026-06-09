"""
内閣府NPO法人情報ポータルサイト — 全国の特定非営利活動法人(NPO法人)の行政入力情報

運営: 内閣府
ポータルURL: https://www.npo-homepage.go.jp/npoportal/

取得対象:
    - 全国の NPO法人 (約 6 万件。活動中・解散済みを含む行政入力情報の全件)
    - 法人名称 / カナ / 所轄庁 / 所在地 / 代表者 / 法人番号 / 設立年月日 /
      活動分野 (定款の20分野) / 認定区分 / PST基準 / 監督・解散情報 等の構造化情報

取得フロー:
    1. ポータルの一括ダウンロードページ (/download/all) を取得
    2. ページ内の「行政入力情報データ」ZIP リンクから全国版 (gyousei_000.zip) を特定
       (gyousei_000 = 全国全件。001〜047=都道府県, 101〜120=政令市 はその部分集合のため使わない)
    3. ZIP を取得し、内包の CSV (CP932) を 1 行ずつストリーム解析して即 yield
       (一覧→詳細パターンではなく、CSV に全構造化情報が揃っている)

設計メモ:
    - /list (検索UI) は AWS WAF の JS チャレンジ (HTTP 202) で requests からは取得不可。
      一方で一括ダウンロード ZIP は静的に取得でき、検索UIより網羅的な構造化データを含む。
    - ZIP 内 CSV のファイル名は日付入り (000_AdministrativeInputData_YYYYMMDD.csv) で変動するため、
      拡張子 .csv のメンバを動的に選択する。
    - 「定款に記載された目的」「特定非営利活動に係る事業」「その他の事業」は自由記述の長文
      (著作権リスク) のため取得しない。活動分野・認定区分等の構造化ラベルのみ取得する。
    - 所在地に都道府県が含まれない行が多いため、都道府県は所轄庁から導出する
      (所轄庁が政令市の場合は所属都道府県へマップ)。

実行方法:
    # ローカルテスト
    python scripts/sites/government/npo.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id npo
"""

import csv
import io
import re
import sys
import zipfile
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 全国全件 ZIP のファイル名 (内閣府=000)。001〜047/101〜120 は部分集合なので使わない。
_NATIONAL_ZIP = "gyousei_000.zip"

# ダウンロードページからの ZIP リンク抽出 (絶対/相対どちらにもマッチ)
_ZIP_LINK_RE = re.compile(r'href="([^"]*gyousei_000\.zip)"', re.IGNORECASE)

# 活動分野 1〜20 の表記 (CSV 列12〜31 の ○ フラグに対応。同梱 readme より)
_KATSUDO_FIELDS = [
    "保健、医療又は福祉の増進を図る活動",
    "社会教育の推進を図る活動",
    "まちづくりの推進を図る活動",
    "観光の振興を図る活動",
    "農山漁村又は中山間地域の振興を図る活動",
    "学術、文化、芸術又はスポーツの振興を図る活動",
    "環境の保全を図る活動",
    "災害救援活動",
    "地域安全活動",
    "人権の擁護又は平和の推進を図る活動",
    "国際協力の活動",
    "男女共同参画社会の形成の促進を図る活動",
    "子どもの健全育成を図る活動",
    "情報化社会の発展を図る活動",
    "科学技術の振興を図る活動",
    "経済活動の活性化を図る活動",
    "職業能力の開発又は雇用機会の拡充を支援する活動",
    "消費者の保護を図る活動",
    "連絡、助言又は援助の活動",
    "都道府県又は指定都市の条例で定める活動",
]

# 認定 (認定・特例認定 1〜4) の表記 (CSV 列32〜35 の ○ フラグに対応)
_NINTEI_KUBUN = [
    "認定NPO法人",
    "特例認定NPO法人",
    "国税庁による旧認定",
    "認定の更新中",
]

# PST基準 1〜3 の表記 (CSV 列36〜38 の ○ フラグに対応)
_PST_KIJUN = [
    "相対値基準",
    "絶対値基準",
    "条例個別指定",
]

# 政令指定都市 (所轄庁) → 所属都道府県
_DESIGNATED_CITY_PREF = {
    "札幌市": "北海道", "仙台市": "宮城県", "さいたま市": "埼玉県", "千葉市": "千葉県",
    "横浜市": "神奈川県", "川崎市": "神奈川県", "相模原市": "神奈川県", "新潟市": "新潟県",
    "静岡市": "静岡県", "浜松市": "静岡県", "名古屋市": "愛知県", "京都市": "京都府",
    "大阪市": "大阪府", "堺市": "大阪府", "神戸市": "兵庫県", "岡山市": "岡山県",
    "広島市": "広島県", "北九州市": "福岡県", "福岡市": "福岡県", "熊本市": "熊本県",
}

_PREF_SUFFIX_RE = re.compile(r"(都|道|府|県)$")


def _clean(s) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


class NpoPortalScraper(StaticCrawler):
    """内閣府NPO法人情報ポータルサイト スクレイパー (全国 NPO法人 行政入力情報)"""

    # 全データは ZIP を 1 回ダウンロードした後にローカル CSV をストリーム解析するだけで、
    # 1 行ごとのサーバーリクエストは発生しない。よって行間ウェイトは不要 (= 0)。
    # ここを正の値にすると base ループが yield ごとに time.sleep してしまい、
    # 約 6 万行 × 待機秒数 で処理が間に合わず、時間切れ kill 時に
    # CSV 先頭 (= 所轄庁コード順で先頭に並ぶ北海道) しか出力されない。
    # これが「住所が北海道しか取れない / 全体が取得できていない」の真因。
    DELAY = 0.0
    # ZIP は約 10MB あるためダウンロードタイムアウトを延長
    TIMEOUT = 90

    EXTRA_COLUMNS = [
        "所轄庁",                # 例: 東京都 / 横浜市 / 内閣府 (規制当局。構造化ラベル)
        "権限移譲先市町村",       # 例: 町田市 (短いラベル)
        "代表者名カナ",          # 例: ハラ マサアキ
        "従たる事務所の所在地",   # 例: 札幌市中央区... (構造化住所)
        "法人設立認証年月日",     # 例: 1999/02/23
        "活動分野",              # 例: まちづくりの推進を図る活動 / 環境の保全を図る活動 (構造化ラベルの結合)
        "認定区分",              # 例: 認定NPO法人 (構造化ラベル)
        "PST基準",               # 例: 絶対値基準 (構造化ラベル)
        "認定開始日",            # 例: 2014/09/02
        "認定満了日",            # 例: 2029/09/01
        "事業年度開始日",        # 例: 04/01
        "事業年度終了日",        # 例: 03/31
        "監督情報",              # 例: 2020年02月19日 認証取消し(...) (日付+事由の定型短文)
        "解散情報",              # 例: 2004年05月18日 社員総会の決議(...) (日付+事由の定型短文)
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        zip_url = self._resolve_zip_url(url)
        self.logger.info("全国版 ZIP を取得します: %s", zip_url)

        raw = self._download(zip_url)
        if raw is None:
            self.logger.error("ZIP のダウンロードに失敗しました。中断します。")
            return

        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            csv_members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_members:
                self.logger.error("ZIP 内に CSV が見つかりません: %s", zf.namelist())
                return
            csv_name = csv_members[0]
            self.logger.info("CSV を解析します: %s", csv_name)

            with zf.open(csv_name) as fp:
                text = io.TextIOWrapper(fp, encoding="cp932", newline="")
                reader = csv.reader(text)
                header = next(reader, None)  # ヘッダ行を読み飛ばす
                if header is None:
                    self.logger.error("CSV が空です。")
                    return

                count = 0
                for row in reader:
                    try:
                        item = self._build_item(row, source_url=url)
                    except Exception as e:  # 1行の不整合で全体を止めない
                        self.logger.warning("行のパースに失敗 (スキップ): %s", e)
                        continue
                    if item is None:
                        continue
                    count += 1
                    yield item  # 取得即 yield (全件バッファしない)
                self.logger.info("CSV 解析完了: %d 件", count)

    # ------------------------------------------------------------------
    # ZIP URL の解決 (引数 url を唯一のルートとする)
    # ------------------------------------------------------------------

    def _resolve_zip_url(self, url: str) -> str:
        """一括ダウンロードページから全国版 ZIP の URL を導出する。

        ページ取得に失敗した場合は url からの相対パスでフォールバックする。
        いずれも引数 url を起点とし、別ルートをハードコードしない。
        """
        download_page = urljoin(url, "download/all")
        soup = self.get_soup(download_page)
        if soup is not None:
            m = _ZIP_LINK_RE.search(str(soup))
            if m:
                return urljoin(download_page, m.group(1))
            a = soup.find("a", href=re.compile(r"gyousei_000\.zip", re.IGNORECASE))
            if a and a.get("href"):
                return urljoin(download_page, a["href"])
        # フォールバック: 既知の相対パス構造から組み立てる
        self.logger.warning("DLページから ZIP リンクを取得できず。相対パスで構築します。")
        return urljoin(url, f"download/zip/{_NATIONAL_ZIP}")

    def _download(self, zip_url: str) -> bytes | None:
        try:
            # session.get はテストランナーのソフトタイムアウト対象 (中断可能)
            resp = self.session.get(zip_url, timeout=self.TIMEOUT)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            if self.CONTINUE_ON_ERROR:
                self.error_count += 1
                self.logger.warning("ZIP 取得エラー (継続): %s — %s", zip_url, e)
                return None
            raise

    # ------------------------------------------------------------------
    # 1 行 → レコード
    # ------------------------------------------------------------------

    def _build_item(self, row: list, source_url: str) -> dict | None:
        # 列数が不足する行はスキップ (CSV の体裁崩れ対策)
        if len(row) < 50:
            return None

        name = _clean(row[0])
        if not name:
            return None  # 名称は必須

        shokatsu = _clean(row[2])             # 所轄庁
        addr = _clean(row[4])                 # 主たる事務所の所在地
        pref = self._derive_pref(shokatsu)
        # 所在地に都道府県が含まれない行が多いため、判明していれば先頭に補う。
        # 解散法人 (清算人) 等は所在地そのものが空のため、所轄庁由来の都道府県だけでも住所として補う。
        # 実データ検証 (全国版 60,172 件) より:
        #   - 所在地が空の行は 7,319 件 (12.2%)。うち 99.3% (7,268 件) は監督/解散情報を持つ
        #     解散・監督対象法人で、現在の事務所所在地が存在しない出典データ欠落である
        #     (= セレクタ不具合ではなく仕様)。活動中法人はほぼ全件で住所が取得できる。
        #   - 空行は「先頭 20 件だけ」ではない。CSV は所轄庁順で、各所轄庁ブロックの
        #     先頭に解散法人が固まるため、空行はファイル全域に再出現する
        #     (例: 末尾 1 万件中にも約 1,100 件)。よってこの fallback は全域で効く。
        if not addr:
            full_addr = pref
        elif pref and not addr.startswith(pref):
            full_addr = pref + addr
        else:
            full_addr = addr

        katsudo = self._collect_flags(row, 12, _KATSUDO_FIELDS)
        nintei = self._collect_flags(row, 32, _NINTEI_KUBUN)
        pst = self._collect_flags(row, 36, _PST_KIJUN)

        detail_url = _clean(row[48])

        return {
            Schema.NAME: name,
            Schema.NAME_KANA: _clean(row[1]),
            Schema.PREF: pref,
            Schema.POST_CODE: _clean(row[5]),
            Schema.ADDR: full_addr,
            Schema.REP_NM: _clean(row[7]),
            Schema.CO_NUM: _clean(row[49]),
            Schema.OPEN_DATE: _clean(row[10]),       # 設立年月日
            Schema.CAT_SITE: katsudo,                # サイト定義業種=活動分野
            Schema.URL: detail_url or source_url,
            # --- EXTRA ---
            "所轄庁": shokatsu,
            "権限移譲先市町村": _clean(row[3]),
            "代表者名カナ": _clean(row[8]),
            "従たる事務所の所在地": _clean(row[6]),
            "法人設立認証年月日": _clean(row[9]),
            "活動分野": katsudo,
            "認定区分": nintei,
            "PST基準": pst,
            "認定開始日": _clean(row[40]),
            "認定満了日": _clean(row[41]),
            "事業年度開始日": _clean(row[53]),
            "事業年度終了日": _clean(row[54]),
            "監督情報": _clean(row[46]),
            "解散情報": _clean(row[47]),
        }

    @staticmethod
    def _derive_pref(shokatsu: str) -> str:
        """所轄庁から都道府県を導出する。

        - 所轄庁が都道府県名 (末尾が都道府県) ならそのまま
        - 政令指定都市なら所属都道府県へマップ
        - 内閣府 (複数都道府県にまたがる法人) 等は空文字
        """
        if not shokatsu or shokatsu == "内閣府":
            # 内閣府所轄は複数都道府県にまたがるため特定の都道府県を持たない
            return ""
        if _PREF_SUFFIX_RE.search(shokatsu):
            return shokatsu
        return _DESIGNATED_CITY_PREF.get(shokatsu, "")

    @staticmethod
    def _collect_flags(row: list, start: int, labels: list) -> str:
        """row[start:start+len(labels)] の ○ フラグを対応ラベルに変換し ' / ' で結合。"""
        out = []
        for offset, label in enumerate(labels):
            idx = start + offset
            if idx < len(row) and _clean(row[idx]):
                out.append(label)
        return " / ".join(out)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = NpoPortalScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.npo-homepage.go.jp/npoportal/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
