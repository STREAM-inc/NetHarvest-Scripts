"""
さんぱいくん (産廃情報ネット) — 全国版 産業廃棄物処理業者検索【index_u3.php】

取得対象:
    - 全国の産業廃棄物処理業者 (約20万件超) の公表情報 (無条件検索)
    - 運営: 公益財団法人産業廃棄物処理事業振興財団

⚠ 重複に関する注意:
    `index_u3.php` はフレームで `searchprm.php?Param1=01` を読み込み、検索フォームを
    無条件で送信すると `searchprm_rsl.php` へ POST する。これは既存の site_id
    `sanpai_kun` (scripts/sites/government/sanpai_kun.py, sites.yml 登録済み,
    url=https://www2.sanpainet.or.jp/zyohou) が取得している全国無条件検索と
    **同一のバックエンド・同一のデータ** である (URL 冒頭が `/zyohou/` か
    ルート直下かの違いのみで、どちらも同じ `searchprm_rsl.php` に着地する)。
    そのため本スクレイパーは `sanpai_kun` と実質的な重複であることをオーケストレーターに
    報告する (JSON サマリの notes 参照)。sites.yml への登録要否はオーケストレーター側で判断。

取得フロー:
    1. GET index_u3.php でセッション (Cookie) を確立する
    2. searchprm_rsl.php へ無条件 (自治体・業区分等を指定しない) 検索を POST (200件/ページ)
    3. iIDspPage を 1 から最終ページまでインクリメント
    4. 各行から固有番号・事業者名・優良認定・許可自治体・廃棄物種類・業区分・
       二次委託先開示可否/開示状況を抽出

重要な制約 (robots.txt):
    - `Crawl-delay: 120` → 検索 POST の間隔は 120 秒空ける (DELAY=120)
      ※ ITEM_DELAY=0: 1ページに最大200件入るため、アイテム単位で待つと完走できない
    - `Disallow: /zyohou/status.php` (詳細ページ相当) → 一切アクセスしない
      そのため 都道府県・郵便番号・住所・TEL・HP・法人番号・代表者名・SNS 等は
      一覧ページに存在せず取得不可 (空欄で出力する)

実行方法:
    # ローカルテスト (1リクエスト120秒待つため非常に時間がかかる)
    python scripts/sites/government/www2_3.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id www2_3
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

import bs4
import requests

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

DSP_COUNT = 200          # 1ページあたり件数 (フォームの最大値)
MAX_PAGES = 2000         # 安全上限 (現在の全件数で約1,100ページ想定)
POST_RETRY = 3           # POST の再試行回数 (上限あり: 無限リトライ禁止)

_ALLCNT_RE = re.compile(r"allCnt\s*=\s*Number\('([\d,]+)'")
_KYOKA_RE = re.compile(r"許可自治体：(.+?)<br\s*/?>")
_SANGYOU_RE = re.compile(
    r"【産業廃棄物】<br\s*/?>(.+?)(?:<br\s*/?>【特別管理産業廃棄物】|<br\s*/?>業区分：)"
)
_TOKUBETSU_RE = re.compile(r"【特別管理産業廃棄物】<br\s*/?>(.+?)<br\s*/?>業区分：")
_GYOUKUBUN_RE = re.compile(r"業区分：(.+?)<br\s*/?>")
_COLOR1_RE = re.compile(r"<span class=['\"]color1['\"]>(.+?)</span>")
_COLOR0_RE = re.compile(r"<span class=['\"]color0['\"]>[^<]*</span>\s*[，,]?\s*")


def _clean(value) -> str:
    """全角スペース・連続空白をならして前後を trim する。"""
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).replace("　", " ")).strip()


def _strip_tags(fragment: str) -> str:
    """HTML タグを剥がしてテキストだけ返す (許可自治体は color2 等が混ざる)。"""
    return _clean(re.sub(r"<[^>]+>", "", fragment))


def _color1_items(fragment: str) -> list[str]:
    """廃棄物種類のうち <span class='color1'> (=許可あり) だけを取り出す。

    color0 は「許可を持たない種類」なので除外する。
    """
    return [_clean(m.group(1)) for m in _COLOR1_RE.finditer(fragment) if _clean(m.group(1))]


def _active_gyoukubun(fragment: str) -> list[str]:
    """業区分から color0 (=未許可) を除いた、実際に許可を持つ区分だけ返す。

    業区分は許可あり=素のテキスト / 許可なし=<span class='color0'> で描画されるため、
    color0 の span を丸ごと落としてからカンマ (，) で分割する。
    """
    plain = _strip_tags(_COLOR0_RE.sub("", fragment))
    return [x for x in (_clean(v) for v in re.split(r"[，,]", plain)) if x]


class SanpaiNetNationalScraper(StaticCrawler):
    """産廃情報ネット さんぱいくん — 全国無条件検索スクレイパー (index_u3.php)"""

    # robots.txt: Crawl-delay 120 → 検索 POST 間は 120 秒空ける
    DELAY = 120
    # 1ページ最大200件のため、アイテム単位の待機は行わない (待機はページ取得側で行う)
    ITEM_DELAY = 0
    TIMEOUT = 60

    EXTRA_COLUMNS = [
        "固有番号",
        "優良認定",
        "許可自治体",
        "産業廃棄物種類",
        "特別管理産業廃棄物種類",
        "業区分",
        "二次委託先開示可否",
        "二次委託先開示状況",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # url = https://www2.sanpainet.or.jp/index_u3.php (sites.yml の正)
        self._search_url = urljoin(url, "searchprm_rsl.php")

        self._prime_session(url)

        seen_ids: set[str] = set()
        yielded = 0

        for page in range(1, MAX_PAGES + 1):
            # robots.txt Crawl-delay 遵守: 2回目以降の検索 POST 前に待機する
            if page > 1:
                time.sleep(self.DELAY)

            self.logger.info("全国無条件検索 page=%d を取得中", page)
            soup = self._post_search(page)
            if soup is None:
                self.logger.warning("page=%d を取得できませんでした。終了します。", page)
                break

            html_text = str(soup)

            if page == 1:
                m = _ALLCNT_RE.search(html_text)
                if m:
                    self.logger.info("該当件数: %s 件", m.group(1))

            page_item_count = 0
            for tr in soup.select("table.content tr"):
                tds = tr.find_all("td", recursive=False)
                if len(tds) < 2:
                    continue
                # 実データ行は先頭 td が 6 桁の固有番号
                if not re.fullmatch(r"\d{6}", _clean(tds[0].get_text())):
                    continue

                page_item_count += 1
                try:
                    item = self._parse_row(tds, url)
                except Exception as e:  # 1行の崩れで全体を止めない
                    self.logger.warning("行のパースに失敗しました: %s", e)
                    continue
                if item is None:
                    continue

                cid = item["固有番号"]
                if cid in seen_ids:
                    continue
                seen_ids.add(cid)
                yielded += 1
                yield item

            if page_item_count == 0:
                self.logger.info("これ以上レコードがありません。終了します。")
                break
            if not self._has_next_page(html_text, page):
                self.logger.info("最終ページ (page=%d) に到達しました。", page)
                break

        if yielded == 0:
            raise RuntimeError("1件も取得できませんでした (検索条件またはサイト構造の変更を確認してください)")

    # ------------------------------------------------------------------
    # セッション確立
    # ------------------------------------------------------------------

    def _prime_session(self, url: str) -> None:
        """検索 POST 前に index_u3.php を GET してセッション (Cookie) を確立する。

        この GET は本文取得ではなく前置き扱いのため Crawl-delay の対象外とし、
        POST の待機 (DELAY) とは別に扱う。
        """
        soup = self.get_soup(url)
        if soup is None:
            self.logger.warning("セッション確立用ページを取得できませんでした: %s", url)
        self.session.headers.update({"Referer": url})

    # ------------------------------------------------------------------
    # 検索 POST (リトライ上限あり)
    # ------------------------------------------------------------------

    def _post_search(self, page: int) -> bs4.BeautifulSoup | None:
        data = {
            "Param1": "01",
            "iIDspCount": str(DSP_COUNT),
            "iIDspPage": str(page),
            "orderType": "0",
        }

        for attempt in range(POST_RETRY):
            if attempt > 0:
                time.sleep(min(2 ** attempt, 30))
            try:
                resp = self.session.post(self._search_url, data=data, timeout=self.TIMEOUT)
                resp.raise_for_status()
            except requests.exceptions.RequestException as e:
                self.error_count += 1
                self.logger.warning(
                    "検索 POST に失敗 (page=%d 試行=%d/%d): %s",
                    page, attempt + 1, POST_RETRY, e,
                )
                continue
            # サーバは UTF-8 を返す
            resp.encoding = "utf-8"
            return bs4.BeautifulSoup(resp.text, "html.parser")

        return None

    # ------------------------------------------------------------------
    # 1行パース (一覧ページに表示されている情報のみ)
    # ------------------------------------------------------------------

    def _parse_row(self, tds: list, url: str) -> dict | None:
        cid = _clean(tds[0].get_text())
        if not re.fullmatch(r"\d{6}", cid):
            return None

        body = tds[1]
        body_html = body.decode_contents()

        # 事業者名
        anchor = body.select_one("a.company")
        name = _clean(anchor.get_text()) if anchor else ""
        if not name:
            return None

        # 優良認定: span.komejirushi に認定マーク画像が入る
        kome = body.select_one("span.komejirushi")
        yuryo = "優良認定" if (kome is not None and kome.find("img") is not None) else ""

        # 許可自治体 (検索対象の自治体は color2 で強調される → タグを剥がす)
        m = _KYOKA_RE.search(body_html)
        kyoka = _strip_tags(m.group(1)) if m else ""

        # 産業廃棄物の種類 (許可あり = color1 のみ)
        m = _SANGYOU_RE.search(body_html)
        sangyou = " / ".join(_color1_items(m.group(1))) if m else ""

        # 特別管理産業廃棄物の種類 (許可あり = color1 のみ)
        m = _TOKUBETSU_RE.search(body_html)
        tokubetsu = " / ".join(_color1_items(m.group(1))) if m else ""

        # 業区分 (未許可 = color0 を除外)
        m = _GYOUKUBUN_RE.search(body_html)
        gyoukubun = " / ".join(_active_gyoukubun(m.group(1))) if m else ""

        # 二次委託先の開示可否 / 開示状況
        disclose_kahi = _clean(tds[2].get_text()) if len(tds) > 2 else ""
        disclose_status = _clean(tds[3].get_text()) if len(tds) > 3 else ""

        return {
            Schema.NAME: name,
            Schema.URL: url,
            # 以下は詳細ページ相当 (robots で Disallow) にしか無いため取得不可 → 空欄
            Schema.PREF: "",
            Schema.POST_CODE: "",
            Schema.ADDR: "",
            Schema.TEL: "",
            Schema.HP: "",
            Schema.CO_NUM: "",
            Schema.REP_NM: "",
            Schema.CAT_SITE: gyoukubun,
            "固有番号": cid,
            "優良認定": yuryo,
            "許可自治体": kyoka,
            "産業廃棄物種類": sangyou,
            "特別管理産業廃棄物種類": tokubetsu,
            "業区分": gyoukubun,
            "二次委託先開示可否": disclose_kahi,
            "二次委託先開示状況": disclose_status,
        }

    # ------------------------------------------------------------------
    # 次ページ判定
    # ------------------------------------------------------------------

    def _has_next_page(self, html_text: str, current: int) -> bool:
        """ページャの DoSubmit(200, 次ページ番号, ...) が存在するかで判定する。"""
        return bool(re.search(rf"DoSubmit\({DSP_COUNT},{current + 1},", html_text))


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = SanpaiNetNationalScraper()
    scraper.execute("https://www2.sanpainet.or.jp/index_u3.php")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
