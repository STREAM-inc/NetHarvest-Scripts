"""
さんぱいくん【処理業者検索】 (産廃情報ネット / www2.sanpainet.or.jp)

取得対象:
    - 産業廃棄物処理業者検索 (https://www2.sanpainet.or.jp/searchprm.php?Param1=01) の
      検索結果一覧に表示される全業者 (業区分・廃棄物種類の絞り込みなし = 全件)
    - 運営: 公益財団法人産業廃棄物処理事業振興財団

取得フロー:
    1. GET index.php → GET {url} (検索フォーム) でセッションを確立し、
       同時にフォームから「許可自治体」の一覧 (エリア / 都道府県47 / 政令市83 = 計130) を採取する
    2. 自治体ごとに searchprm_rsl.php へ検索条件を POST (200件/ページ)
       ※ iIGvrnID[] に複数自治体を同時指定すると 0 件になるため 1自治体ずつ投げる
       ※ 業区分を指定しない場合も「許可自治体」だけで検索条件成立と判定される (= 全業区分)
       ※ 都道府県 (コード1-47) を先に巡回し、その後に政令市を巡回する。
          大半の業者は都道府県許可を持つため、先に広く網羅できる
    3. 一覧の各行から 固有番号・事業者名・許可自治体・廃棄物の種類・業区分・
       二次委託先個社名の開示可否/状況 を抽出
    4. 自治体をまたぐ重複 (複数自治体の許可を持つ同一業者) は 固有番号 で 1 回だけ出力する

重要な制約 (robots.txt: https://www2.sanpainet.or.jp/robots.txt):
    - `Crawl-delay: 120` → 検索 POST の間隔は 120 秒空ける (DELAY=120)
      ※ ITEM_DELAY=0 は必須。1ページに最大200件入るため、アイテム単位で待つと完走できない
    - `Disallow: /status.php` (業者詳細) → 一切アクセスしない。
      そのため 住所・郵便番号・TEL・FAX・代表者・法人番号・資本金・従業員数・
      設立日・メール・HP・SNS は取得不可 (空欄で出力する)。
      一覧に出ている詳細ページ URL のみ参考値として列に残す (取得はしない)
    - 都道府県 (Schema.PREF) も空欄。検索した「許可自治体」は業者の所在地ではなく
      許可を出した自治体なので、所在地として出力すると誤りになる
      (例: 琉球海運株式会社は東京都・大阪府・福岡県・鹿児島県・那覇市の許可を持つ)。
      許可自治体側の情報は EXTRA カラム (検索対象自治体 / 同都道府県 / エリア) に入れる

一覧から取得できない項目 (フォームの絞り込み条件としてのみ存在):
    優良認定 / 電子マニフェスト対応 / 中間処理の処理方法 / 再生利用 / 環境配慮の取組。
    行単位のマーク (span.komejirushi) はこの画面では描画されず、判定するには自治体ごとに
    絞り込み検索を追加で投げる必要がある (130リクエスト × Crawl-delay 120秒)。
    「最初の1件を数秒以内に yield する」要件と両立しないため取得しない。

利用規約 (https://www.sanpainet.or.jp/content.php?id=2):
    スクレイピング・クローリングの明示的な禁止条項はない。
    ただし「商用目的で複製する場合は事前に財団総務部へ連絡」との記載がある。

実行方法:
    # ローカルテスト (1リクエスト120秒待つため非常に時間がかかる)
    python scripts/sites/government/www2_4.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id www2_4
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import parse_qs, urljoin, urlparse

import bs4
import requests

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

DSP_COUNT = 200          # 1ページあたり件数 (フォームの最大値)
MAX_PAGES = 400          # 1自治体あたりの安全上限 (最大 80,000 件相当)
POST_RETRY = 3           # POST の再試行回数 (上限あり: 無限リトライ禁止)
PRIME_DELAY = 2.0        # セッション確立用 GET 間の待機秒数

_ALLCNT_RE = re.compile(r"allCnt\s*=\s*Number\('([\d,]+)'")
_KYOKA_RE = re.compile(r"許可自治体：(.+?)<br\s*/?>")
_SANGYOU_RE = re.compile(
    r"【産業廃棄物】<br\s*/?>(.+?)(?:<br\s*/?>【特別管理産業廃棄物】|<br\s*/?>業区分：)"
)
_TOKUBETSU_RE = re.compile(r"【特別管理産業廃棄物】<br\s*/?>(.+?)<br\s*/?>業区分：")
_GYOUKUBUN_RE = re.compile(r"業区分：(.+?)<br\s*/?>")
_COLOR1_RE = re.compile(r"<span class=['\"]color1['\"]>(.+?)</span>")
_COLOR0_RE = re.compile(r"<span class=['\"]color0['\"]>[^<]*</span>\s*[，,]?\s*")
_CONTENTS_ID_RE = re.compile(r"contents(\d+)")


def _clean(value) -> str:
    """全角スペース・連続空白をならして前後を trim する。"""
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).replace("　", " ")).strip()


def _strip_tags(fragment: str) -> str:
    """HTML タグを剥がしてテキストだけ返す。

    許可自治体の欄には検索対象を強調する color2 span が入り、
    しかも閉じ忘れの </span> が混ざるためタグごと落とす。
    """
    return _clean(re.sub(r"<[^>]+>", "", fragment))


def _color1_items(fragment: str) -> list[str]:
    """廃棄物の種類のうち <span class='color1'> (=許可あり) だけを取り出す。

    color0 は「許可を持たない種類」なので除外する。
    """
    return [_clean(m.group(1)) for m in _COLOR1_RE.finditer(fragment) if _clean(m.group(1))]


def _active_gyoukubun(fragment: str) -> list[str]:
    """業区分から color0 (=未許可) を除いた、実際に許可を持つ区分だけ返す。

    業区分は許可あり=素のテキスト / 許可なし=<span class='color0'> で描画されるため、
    color0 の span を丸ごと落としてから全角カンマ (，) で分割する。
    """
    plain = _strip_tags(_COLOR0_RE.sub("", fragment))
    return [x for x in (_clean(v) for v in re.split(r"[，,]", plain)) if x]


class SanpaiKunOperatorSearchScraper(StaticCrawler):
    """さんぱいくん【処理業者検索】— 全国130自治体 × 全業区分の処理業者スクレイパー"""

    # robots.txt: Crawl-delay 120 → 検索 POST 間は 120 秒空ける
    DELAY = 120
    # 1ページ最大200件のため、アイテム単位の待機は行わない (待機はページ取得側で行う)
    ITEM_DELAY = 0
    TIMEOUT = 60

    EXTRA_COLUMNS = [
        "固有番号",
        "検索対象自治体",
        "検索対象自治体の都道府県",
        "エリア",
        "許可自治体",
        "産業廃棄物種類",
        "特別管理産業廃棄物種類",
        "業区分",
        "二次委託先個社名の開示の可否",
        "二次委託先個社名の開示の状況",
        "詳細ページURL",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # url = https://www2.sanpainet.or.jp/searchprm.php?Param1=01 (sites.yml の正)
        self._search_url = urljoin(url, "searchprm_rsl.php")
        self._param1 = (parse_qs(urlparse(url).query).get("Param1") or ["01"])[0]

        form_soup = self._prime_session(url)
        if form_soup is None:
            raise RuntimeError(f"検索フォームを取得できませんでした: {url}")

        governments = self._extract_governments(form_soup)
        if not governments:
            raise RuntimeError(
                "検索フォームから許可自治体の一覧を抽出できませんでした "
                "(iIGvrnID[] のマークアップ変更を確認してください)"
            )
        self.logger.info("許可自治体を %d 件抽出しました", len(governments))

        seen_ids: set[str] = set()
        yielded = 0
        request_count = 0

        for gov_code, gov_name, pref_name, area_name in governments:
            for page in range(1, MAX_PAGES + 1):
                # robots.txt Crawl-delay 遵守: 2回目以降の検索 POST 前に待機する
                if request_count > 0:
                    time.sleep(self.DELAY)
                request_count += 1

                self.logger.info(
                    "検索中: %s (コード=%s) page=%d", gov_name, gov_code, page
                )
                soup = self._post_search(gov_code, page)
                if soup is None:
                    self.logger.warning(
                        "%s page=%d を取得できませんでした。次の自治体へ進みます。",
                        gov_name, page,
                    )
                    break

                html_text = str(soup)

                if page == 1:
                    m = _ALLCNT_RE.search(html_text)
                    if m:
                        self.logger.info("%s の該当件数: %s 件", gov_name, m.group(1))

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
                        item = self._parse_row(
                            tds, url, gov_name, pref_name, area_name
                        )
                    except Exception as e:  # 1行の崩れで全体を止めない
                        self.logger.warning("行のパースに失敗しました: %s", e)
                        continue
                    if item is None:
                        continue

                    cid = item["固有番号"]
                    # 自治体をまたぐ重複 (複数自治体の許可を持つ業者) は 1 回だけ出力
                    if cid in seen_ids:
                        continue
                    seen_ids.add(cid)
                    yielded += 1
                    yield item

                if page_item_count == 0:
                    self.logger.info("%s: これ以上レコードがありません。", gov_name)
                    break
                if not self._has_next_page(html_text, page):
                    self.logger.info(
                        "%s: 最終ページ (page=%d) に到達しました。", gov_name, page
                    )
                    break

        if yielded == 0:
            raise RuntimeError(
                "1件も取得できませんでした (検索条件またはサイト構造の変更を確認してください)"
            )

    # ------------------------------------------------------------------
    # セッション確立
    # ------------------------------------------------------------------

    def _prime_session(self, url: str) -> bs4.BeautifulSoup | None:
        """index.php → 検索フォームを GET してセッションを確立し、フォームを返す。

        POST に必要な Cookie / Referer を用意する目的。この2本は本文取得ではなく
        前置き扱いのため、Crawl-delay ではなく短い PRIME_DELAY を挟む。
        """
        index_url = urljoin(url, "index.php")
        if self.get_soup(index_url) is None:
            self.logger.warning("セッション確立用ページを取得できませんでした: %s", index_url)
        time.sleep(PRIME_DELAY)

        form_soup = self.get_soup(url)
        # 以降の POST は検索フォームからの遷移として扱われる
        self.session.headers.update({"Referer": url})
        return form_soup

    # ------------------------------------------------------------------
    # 許可自治体一覧 (エリア / 都道府県 / 政令市) の抽出
    # ------------------------------------------------------------------

    def _extract_governments(
        self, form_soup: bs4.BeautifulSoup
    ) -> list[tuple[str, str, str, str]]:
        """検索フォームから (自治体コード, 自治体名, 都道府県名, エリア名) を組み立てる。

        - 都道府県: <ul> 内の <a href='#contents{都道府県コード}'> にチェックボックスが入る。
          エリア名は同じ <ul> の <strong> (例: 「北海道・東北エリア」)。
        - 政令市: <div id='contents{都道府県コード}' class='content_block'> の中に並ぶ。
          親の都道府県コードから都道府県名・エリア名を引き継ぐ。

        都道府県を先に、その後で政令市を返す (広い母集団から先に網羅するため)。
        """
        prefs: dict[str, tuple[str, str]] = {}   # code -> (都道府県名, エリア名)
        for ul in form_soup.find_all("ul"):
            strong = ul.find("strong")
            if strong is None:
                continue
            area = _clean(strong.get_text()).replace(" ", "")
            area = re.sub(r"エリア$", "", area)
            for anchor in ul.find_all("a"):
                checkbox = anchor.find("input", attrs={"name": "iIGvrnID[]"})
                if checkbox is None or not checkbox.get("value"):
                    continue
                name = _clean(anchor.get_text())
                if name:
                    prefs[checkbox["value"]] = (name, area)

        cities: list[tuple[str, str, str, str]] = []
        for block in form_soup.select("div.content_block"):
            m = _CONTENTS_ID_RE.fullmatch(block.get("id") or "")
            if m is None:
                continue
            parent = prefs.get(m.group(1))
            if parent is None:
                continue
            pref_name, area = parent
            for checkbox in block.find_all("input", attrs={"name": "iIGvrnID[]"}):
                if not checkbox.get("value"):
                    continue
                # 政令市名はチェックボックスを包む <td> のテキスト
                cell = checkbox.find_parent("td") or checkbox.parent
                name = _clean(cell.get_text())
                if name:
                    cities.append((checkbox["value"], name, pref_name, area))

        ordered = [
            (code, name, name, area)
            for code, (name, area) in sorted(prefs.items(), key=lambda kv: int(kv[0]))
        ]
        ordered.extend(cities)
        return ordered

    # ------------------------------------------------------------------
    # 検索 POST (リトライ上限あり)
    # ------------------------------------------------------------------

    def _post_search(self, gov_code: str, page: int) -> bs4.BeautifulSoup | None:
        # 業区分・廃棄物種類は未指定 = 全件。許可自治体だけで検索条件は成立する
        data = {
            "Param1": self._param1,
            "iIGvrnID[]": gov_code,
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
                    "検索 POST に失敗 (自治体=%s page=%d 試行=%d/%d): %s",
                    gov_code, page, attempt + 1, POST_RETRY, e,
                )
                continue
            # サーバは UTF-8 を返す
            resp.encoding = "utf-8"
            # 条件不正・セッション切れは err_message.php への meta refresh で返る
            if "err_message.php" in resp.text[:300]:
                self.logger.warning(
                    "エラー画面が返りました (自治体=%s page=%d 試行=%d/%d)",
                    gov_code, page, attempt + 1, POST_RETRY,
                )
                continue
            return bs4.BeautifulSoup(resp.text, "html.parser")

        return None

    # ------------------------------------------------------------------
    # 1行パース (一覧ページに表示されている情報のみ)
    # ------------------------------------------------------------------

    def _parse_row(
        self,
        tds: list,
        url: str,
        searched_gov: str,
        searched_pref: str,
        area_name: str,
    ) -> dict | None:
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

        # 詳細ページ URL (robots で Disallow のため取得はせず、参考値として列に残す)
        detail_url = ""
        if anchor is not None and anchor.get("href"):
            detail_url = urljoin(url, anchor["href"])

        # 許可自治体 (検索した自治体は color2 で強調される → タグを剥がす)
        # 10件を超えると末尾が " ..." で省略されるため、そのまま省略形で出力する
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

        # 二次委託先個社名の開示の可否 / 状況
        disclose_kahi = _clean(tds[2].get_text()) if len(tds) > 2 else ""
        disclose_status = _clean(tds[3].get_text()) if len(tds) > 3 else ""

        return {
            Schema.NAME: name,
            Schema.URL: url,
            # 以下は詳細ページ (robots で Disallow) にしか無いため取得不可 → 空欄
            Schema.PREF: "",
            Schema.POST_CODE: "",
            Schema.ADDR: "",
            Schema.TEL: "",
            Schema.CO_NUM: "",
            Schema.REP_NM: "",
            Schema.POS_NM: "",
            Schema.CAP: "",
            Schema.SALES: "",
            Schema.EMP_NUM: "",
            Schema.OPEN_DATE: "",
            Schema.EMAIL: "",
            Schema.HP: "",
            Schema.INSTA: "",
            Schema.FB: "",
            Schema.X: "",
            Schema.LINE: "",
            Schema.CAT_SITE: gyoukubun,
            "固有番号": cid,
            "検索対象自治体": searched_gov,
            "検索対象自治体の都道府県": searched_pref,
            "エリア": area_name,
            "許可自治体": kyoka,
            "産業廃棄物種類": sangyou,
            "特別管理産業廃棄物種類": tokubetsu,
            "業区分": gyoukubun,
            "二次委託先個社名の開示の可否": disclose_kahi,
            "二次委託先個社名の開示の状況": disclose_status,
            "詳細ページURL": detail_url,
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

    scraper = SanpaiKunOperatorSearchScraper()
    scraper.execute("https://www2.sanpainet.or.jp/searchprm.php?Param1=01")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
