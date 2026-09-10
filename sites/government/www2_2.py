"""
産廃情報ネット 行政情報検索システム — 東北6県 + 北陸4県の産業廃棄物収集運搬業許可

取得対象:
    - 「産業廃棄物処理業許可 行政情報検索システム」(自治体から提供された許可情報) のうち、
      事業者の所在地が 東北6県 (青森/岩手/宮城/秋田/山形/福島) + 新潟/富山/石川/福井 の
      計10県にあり、かつ **収集運搬業** (産廃収運業 / 特管収運業) の許可を持つもの
    - 1レコード = 1許可 (許可番号単位)。同一事業者が複数自治体の許可を持つ場合は
      許可ごとに行が出る (許可番号・許可主体・許可年月日が許可単位の情報のため)

取得フロー:
    1. 対象都道府県ごとに検索条件を組み立て、CSV 出力エンドポイントを GET
       (`csv_download.php?address_pref=...&unpan=all&tokubetu_unpan=all&search=3&nopaging=1`)
       ※ unpan / tokubetu_unpan は OR 条件 (富山県: 1,836 + 247 = 2,083 件で検証済み)
    2. 返る CSV (CP932。Content-Type は UTF-8 と申告するが実体は CP932) を1行ずつ yield
    3. HTML 一覧 (search_list.php, 100件/ページ) ではなく CSV を使うことで、
       1県あたり 20〜50 リクエスト → 1 リクエストに削減している (robots の Crawl-delay 対策)

取得できない項目 (robots.txt 遵守のため):
    - 郵便番号 / 電話番号 / 取り扱う産業廃棄物の種類
      これらは詳細ページ `https://www2.sanpainet.or.jp/status.php` にのみ存在するが、
      robots.txt が `Disallow: /status.php` で明示的に拒否しているためアクセスしない。
      該当カラムは空文字で出力する。

利用規約:
    https://www.sanpainet.or.jp/content.php?id=2 (著作権・リンクについて)
    スクレイピング/クローリングを禁止する条項は無い。
    ただし「商用目的で複製する場合は事前に財団総務部へ連絡」との記載があるため、
    商用利用時は事前連絡が必要。

実行方法:
    # ローカルテスト (robots の Crawl-delay 120 秒を守るため県間で待機する)
    python scripts/sites/government/www2_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id www2_2
"""

import csv
import io
import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urlencode, urljoin

import requests

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# --- 検索条件 -----------------------------------------------------------
# 所在地 (address_pref) で絞り込む対象都道府県。検索フォームの option value そのまま。
TARGET_PREFS: list[str] = [
    "青森県",
    "岩手県",
    "宮城県",
    "秋田県",
    "山形県",
    "福島県",
    "新潟県",
    "富山県",
    "石川県",
    "福井県",
]

# 業の区分: 産業廃棄物収集運搬業 + 特別管理産業廃棄物収集運搬業 (両者は OR 条件)
SEARCH_PARAMS: dict[str, str] = {
    "address": "",
    "name": "",
    "unpan": "all",           # 産業廃棄物収集運搬業 (積替え有無を問わない)
    "tokubetu_unpan": "all",  # 特別管理産業廃棄物収集運搬業
    "search": "3",            # 「業者情報内容で検索」
    "_perPage": "100",
}

FETCH_RETRY = 3          # CSV 取得の再試行回数 (上限あり。無限リトライは行わない)
CSV_HEADER_FIRST = "業者名"  # 文字コード判定に使うヘッダ先頭カラム名

_PREF_RE = re.compile(r"^(東京都|北海道|(?:京都|大阪)府|.{2,3}県)")


def _clean(value) -> str:
    """全角スペース・連続空白をならして前後を trim する。"""
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).replace("　", " ")).strip()


def _decode_csv(raw: bytes) -> str:
    """CSV のバイト列をデコードする。

    サーバは `Content-Type: text/csv;charset=UTF-8` を返すが、実体は CP932。
    ヘッダ行が「業者名」で始まるかで正しい文字コードを判定する。
    """
    for enc in ("cp932", "utf-8-sig", "utf-8"):
        try:
            text = raw.decode(enc)
        except UnicodeDecodeError:
            continue
        if text.lstrip().startswith(CSV_HEADER_FIRST):
            return text
    # どれでも判定できなければ CP932 で強制デコード (1文字の化けで全件落とさない)
    return raw.decode("cp932", errors="replace")


def _permit_kind(gyou_kubun: str) -> str:
    """業の区分から許可の種類 (収集運搬 / 処分) を判定する。"""
    if "収運" in gyou_kubun or "収集運搬" in gyou_kubun:
        return "収集運搬"
    if "処分" in gyou_kubun:
        return "処分"
    return ""


def _waste_category(gyou_kubun: str) -> str:
    """業の区分から廃棄物の区分 (産業廃棄物 / 特別管理産業廃棄物) を判定する。"""
    if gyou_kubun.startswith("特管") or "特別管理" in gyou_kubun:
        return "特別管理産業廃棄物"
    if gyou_kubun.startswith("産廃") or "産業廃棄物" in gyou_kubun:
        return "産業廃棄物"
    return ""


class SanpaiGyouseiTohokuHokurikuScraper(StaticCrawler):
    """行政情報検索システム — 東北6県 + 北陸4県の収集運搬業許可スクレイパー"""

    # robots.txt: Crawl-delay 120 → 都道府県ごとの CSV 取得の間は 120 秒空ける
    DELAY = 120
    # 1回の CSV に数千件入るため、アイテム単位の待機は行わない (待機は取得側で行う)
    ITEM_DELAY = 0
    TIMEOUT = 120

    EXTRA_COLUMNS = [
        "固有番号",
        "許可番号",
        "許可の種類",
        "許可主体",
        "業の区分",
        "廃棄物区分",
        "許可年月日",
        "許可期限年月日",
        "優良認定の有無",
        # 詳細ページ (/status.php) が robots.txt で Disallow のため取得不可。常に空文字。
        "取り扱う産業廃棄物の種類",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # url = https://www2.sanpainet.or.jp/sanpai/ (sites.yml の url が唯一の正)
        csv_endpoint = urljoin(url, "csv_download.php")
        list_endpoint = urljoin(url, "search_list.php")
        search_page = urljoin(url, "search.php")
        self.session.headers.update({"Referer": search_page})

        yielded = 0
        rows_by_pref: list[int] = []

        for index, pref in enumerate(TARGET_PREFS):
            # robots.txt Crawl-delay 遵守: 2県目以降は取得前に待機する
            if index > 0:
                time.sleep(self.DELAY)

            params = {"address_pref": pref, **SEARCH_PARAMS}
            csv_url = f"{csv_endpoint}?{urlencode({**params, 'nopaging': '1', 'submit': 'CSV出力'})}"
            # Schema.URL には人が開ける検索結果一覧の URL を入れる
            source_url = f"{list_endpoint}?{urlencode(params)}"

            self.logger.info("取得中: %s (所在地=%s)", csv_endpoint, pref)
            text = self._fetch_csv(csv_url, pref)
            if text is None:
                self.logger.warning("%s の CSV を取得できませんでした。次の県へ進みます。", pref)
                continue

            reader = csv.DictReader(io.StringIO(text))
            pref_rows = 0
            for row in reader:
                try:
                    item = self._build_item(row, pref, source_url)
                except Exception as e:  # 1行の崩れで全体を止めない
                    self.error_count += 1
                    self.logger.warning("行のパースに失敗しました (%s): %s", pref, e)
                    continue
                if item is None:
                    continue
                pref_rows += 1
                yielded += 1
                yield item

            rows_by_pref.append(pref_rows)
            self.logger.info("%s: %d 件", pref, pref_rows)
            self._update_total_estimate(rows_by_pref)

        if yielded == 0:
            raise RuntimeError(
                "1件も取得できませんでした (検索条件または CSV 出力エンドポイントの変更を確認してください)"
            )

    # ------------------------------------------------------------------
    # CSV 取得 (リトライ上限あり)
    # ------------------------------------------------------------------

    def _fetch_csv(self, csv_url: str, pref: str) -> str | None:
        """CSV 出力エンドポイントを GET してテキストを返す。

        再試行は FETCH_RETRY 回まで。全滅したら None を返して呼び出し元で次の県へ進む
        (自己再帰による無限リトライはしない)。
        """
        for attempt in range(FETCH_RETRY):
            if attempt > 0:
                time.sleep(min(2 ** attempt, 30))
            try:
                response = self.session.get(csv_url, timeout=self.TIMEOUT)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                self.error_count += 1
                self.logger.warning(
                    "CSV 取得に失敗 (所在地=%s 試行=%d/%d): %s", pref, attempt + 1, FETCH_RETRY, e
                )
                continue

            text = _decode_csv(response.content)
            if CSV_HEADER_FIRST not in text[:200]:
                self.error_count += 1
                self.logger.warning(
                    "CSV ヘッダを確認できません (所在地=%s 試行=%d/%d)", pref, attempt + 1, FETCH_RETRY
                )
                continue
            return text

        return None

    # ------------------------------------------------------------------
    # 1行パース
    # ------------------------------------------------------------------

    def _build_item(self, row: dict, searched_pref: str, source_url: str) -> dict | None:
        name = _clean(row.get("業者名"))
        if not name:
            return None

        gyou_kubun = _clean(row.get("業の区分"))
        # 念のためのガード: 収集運搬業以外が混ざっていたら出力しない
        if _permit_kind(gyou_kubun) != "収集運搬":
            return None

        address = _clean(row.get("所在地"))
        m = _PREF_RE.match(address)
        pref = m.group(1) if m else searched_pref

        return {
            Schema.NAME: name,
            Schema.URL: source_url,
            Schema.PREF: pref,
            # 所在地に郵便番号は含まれないため空欄 (詳細ページは robots で Disallow)
            Schema.POST_CODE: "",
            Schema.ADDR: address,
            Schema.TEL: "",
            Schema.CAT_SITE: gyou_kubun,
            "固有番号": _clean(row.get("固有番号")),
            "許可番号": _clean(row.get("許可番号")),
            "許可の種類": _permit_kind(gyou_kubun),
            "許可主体": _clean(row.get("許可主体")),
            "業の区分": gyou_kubun,
            "廃棄物区分": _waste_category(gyou_kubun),
            "許可年月日": _clean(row.get("許可年月日")),
            "許可期限年月日": _clean(row.get("許可期限年月日")),
            "優良認定の有無": _clean(row.get("優良認定の有無")),
            # /status.php (robots Disallow) にしか無いため取得不可
            "取り扱う産業廃棄物の種類": "",
        }

    # ------------------------------------------------------------------
    # 進捗表示用の総件数推定
    # ------------------------------------------------------------------

    def _update_total_estimate(self, rows_by_pref: list[int]) -> None:
        """取得済み県の平均件数から全体件数を推定して ETA 表示に反映する。"""
        if not rows_by_pref:
            return
        average = sum(rows_by_pref) / len(rows_by_pref)
        remaining = len(TARGET_PREFS) - len(rows_by_pref)
        self.total_items = int(sum(rows_by_pref) + average * remaining)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = SanpaiGyouseiTohokuHokurikuScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www2.sanpainet.or.jp/sanpai/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
