"""
マイナビ転職 — 求人掲載企業情報スクレイパー

取得対象:
    - 求人掲載中の企業情報（会社概要・採用条件）

取得フロー:
    1. https://tenshoku.mynavi.jp/list/pg{N}/ を巡回（最大1036ページ、50件/ページ）
    2. 各ページの求人カード（cassetteRecruit / cassetteRecruitRecommend）から詳細URLを収集
    3. 詳細ページで会社情報テーブル（table.jobOfferTable.thL）と
       募集要項テーブル（table.jobOfferTable）を解析して企業データを取得

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/mynavi_tenshoku.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id mynavi_tenshoku
"""

import re
import sys
import time
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://tenshoku.mynavi.jp"
LIST_URL = "https://tenshoku.mynavi.jp/list/pg{}/"
MAX_PAGES = 1036

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class MynaviTenshokuScraper(StaticCrawler):
    """マイナビ転職 求人企業情報スクレイパー（tenshoku.mynavi.jp）"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "求人タイトル",
        "雇用形態",
        "初年度年収",
        "給与",
        "勤務地",
        "勤務時間",
        "昇給賞与",
        "諸手当",
        "休日休暇",
        "福利厚生",
        "売上高",
        "事業所",
    ]

    # 一覧ページ取得失敗時の挙動チューニング用パラメータ
    PAGE_FETCH_RETRIES = 4      # 1ページあたりの取得リトライ回数
    PAGE_RETRY_WAIT = 5.0       # リトライ時の待機秒数（指数バックオフのベース）
    MAX_EMPTY_PAGES = 3         # 連続でカード無し/取得失敗が続いたら終了とみなす閾値

    def _setup(self):
        """通信セッションをマイナビ向けに強化する。

        マイナビ転職は Bot 対策により、ブラウザ相当の HTTP ヘッダや Cookie が
        揃っていないリクエストへ 403 / 429 を返すことがある。基底クラスの
        Retry は 5xx しか対象にしないため、403/429 が返ると get_soup() が即座に
        None を返し「1ページめからページ取得失敗」でクロールが止まってしまう。

        そこで本メソッドで以下を上書きする:
            1. ブラウザ相当の追加ヘッダ（Accept / Accept-Language / Referer 等）
            2. 403 / 408 / 429 / 5xx を対象に含む Retry（Retry-After 尊重）
            3. 初回アクセスでトップ/一覧ルートを踏んで Cookie をウォームアップ
        """
        super()._setup()

        # 1. ブラウザ相当の追加ヘッダ（Bot 判定の主要因を潰す）
        self.session.headers.update({
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,*/*;q=0.8"
            ),
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            "Upgrade-Insecure-Requests": "1",
            "Referer": BASE_URL + "/",
        })

        # 2. 403/408/429/5xx も対象に含む Retry を再マウントする
        retries = Retry(
            total=4,
            backoff_factor=2,
            status_forcelist=[403, 408, 429, 500, 502, 503, 504],
            allowed_methods=frozenset(["GET", "HEAD"]),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        # 3. Cookie ウォームアップ（トップページを踏んでセッション Cookie を取得）
        try:
            self.session.get(BASE_URL + "/", timeout=self.TIMEOUT)
        except Exception as e:  # noqa: BLE001 — ウォームアップ失敗は致命的ではない
            self.logger.debug("Cookie ウォームアップ失敗（続行）: %s", e)

    def _fetch_list_soup(self, list_url: str) -> bs4.BeautifulSoup | None:
        """一覧ページを取得する。一時的な失敗はその場でリトライする。

        基底クラスの get_soup() は 1 回失敗すると None を返すだけなので、
        ここで明示的にリトライ＋バックオフを挟み、単発の失敗で
        クロール全体が止まらないようにする。
        """
        for attempt in range(1, self.PAGE_FETCH_RETRIES + 1):
            soup = self.get_soup(list_url)
            if soup is not None:
                return soup
            if attempt < self.PAGE_FETCH_RETRIES:
                wait = self.PAGE_RETRY_WAIT * attempt
                self.logger.warning(
                    "一覧ページ取得失敗 (%d/%d) %s — %.1f秒後に再試行",
                    attempt, self.PAGE_FETCH_RETRIES, list_url, wait,
                )
                time.sleep(wait)
        return None

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        empty_streak = 0  # 連続で「カード無し/取得失敗」が続いた回数

        for page in range(1, MAX_PAGES + 1):
            list_url = LIST_URL.format(page)
            soup = self._fetch_list_soup(list_url)
            if soup is None:
                # 単発失敗ではクロールを止めず、連続失敗が閾値に達したら終了する
                empty_streak += 1
                self.logger.warning(
                    "ページ取得失敗: %s (連続失敗 %d/%d)",
                    list_url, empty_streak, self.MAX_EMPTY_PAGES,
                )
                if empty_streak >= self.MAX_EMPTY_PAGES:
                    self.logger.error("連続でページ取得に失敗したため終了します")
                    break
                continue

            # 初回ページで総件数を設定
            if page == 1:
                count_m = re.search(r"([\d,]+)件", soup.get_text())
                if count_m:
                    self.total_items = int(count_m.group(1).replace(",", ""))
                else:
                    self.total_items = MAX_PAGES * 50

            # 求人カードから詳細URLを収集
            cards = soup.select("div.cassetteRecruit, div.cassetteRecruitRecommend")
            if not cards:
                empty_streak += 1
                self.logger.info(
                    "pg%d: カードなし (連続 %d/%d)",
                    page, empty_streak, self.MAX_EMPTY_PAGES,
                )
                if empty_streak >= self.MAX_EMPTY_PAGES:
                    self.logger.info("カード無しが続いたため終了します")
                    break
                continue

            # 取得・カード抽出に成功したので連続失敗カウンタをリセット
            empty_streak = 0

            page_urls: list[str] = []
            for card in cards:
                a = card.select_one("a[href*='jobinfo-']")
                if not a:
                    continue
                href = a.get("href", "")
                if href.startswith("//"):
                    href = "https:" + href
                elif href.startswith("/"):
                    href = BASE_URL + href
                if href and href not in seen:
                    seen.add(href)
                    page_urls.append(href)

            self.logger.info("pg%d: %d件の詳細URLを収集", page, len(page_urls))

            for detail_url in page_urls:
                try:
                    item = self._scrape_detail(detail_url)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.warning("詳細取得失敗: %s / %s", detail_url, e)
                    continue

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}

        # 企業名
        cn = soup.select_one(".companyName")
        if cn:
            data[Schema.NAME] = _clean(cn.get_text())

        # 求人タイトル (h1)
        h1 = soup.select_one("h1")
        if h1:
            data["求人タイトル"] = _clean(h1.get_text(" "))

        # DLサマリー（初年度年収・雇用形態）
        for dl in soup.select("dl"):
            dts = dl.select("dt")
            dds = dl.select("dd")
            for dt, dd in zip(dts, dds):
                key = _clean(dt.get_text())
                val = _clean(dd.get_text(" "))
                if key == "初年度年収":
                    data["初年度年収"] = val
                elif key == "雇用形態" and "雇用形態" not in data:
                    data["雇用形態"] = val

        # 募集要項テーブル（table.jobOfferTable で thL クラスを持たない最初のもの）
        for tbl in soup.select("table.jobOfferTable"):
            if "thL" not in tbl.get("class", []):
                for row in tbl.select("tr"):
                    th = row.select_one("th")
                    td = row.select_one("td")
                    if not th or not td:
                        continue
                    key = _clean(th.get_text())
                    val = _clean(td.get_text(" "))
                    if "雇用形態" in key:
                        data["雇用形態"] = val
                    elif "勤務時間" in key:
                        data["勤務時間"] = val
                    elif key == "勤務地":
                        data["勤務地"] = val
                    elif key == "給与":
                        data["給与"] = val
                    elif "昇給" in key or "賞与" in key:
                        data["昇給賞与"] = val
                    elif "諸手当" in key:
                        data["諸手当"] = val
                    elif "休日" in key or "休暇" in key:
                        data["休日休暇"] = val
                    elif "福利厚生" in key:
                        data["福利厚生"] = val
                break  # 最初の非thLテーブルのみ対象

        # 会社情報テーブル（table.jobOfferTable.thL）
        company_tbl = soup.select_one("table.jobOfferTable.thL")
        if company_tbl:
            for row in company_tbl.select("tr"):
                th = row.select_one("th")
                td = row.select_one("td")
                if not th or not td:
                    continue
                key = _clean(th.get_text())
                val = _clean(td.get_text(" "))
                if key == "設立":
                    data[Schema.OPEN_DATE] = val
                elif key == "代表者":
                    data[Schema.REP_NM] = val
                elif key == "従業員数":
                    data[Schema.EMP_NUM] = val
                elif key == "資本金":
                    data[Schema.CAP] = val
                elif key == "売上高":
                    data["売上高"] = val
                elif key == "事業内容":
                    data[Schema.LOB] = val
                elif key == "本社所在地":
                    # 住所が複数の子要素（<span>/<p>/<div>、または <br> 区切り）に
                    # 分割されているケースがあるため、td 配下の全テキストノードを
                    # ドキュメント順に取得して結合する。
                    # これにより「建物名だけ」「階数だけ」しか取れない取得漏れを防ぐ。
                    full = _clean(" ".join(td.stripped_strings))
                    # 郵便番号を抽出し、本文からは除去（前後どちらに在っても対応）
                    m_post = re.search(r"〒\s*(\d{3})-?(\d{4})", full)
                    if m_post:
                        data[Schema.POST_CODE] = f"{m_post.group(1)}-{m_post.group(2)}"
                        full = _clean(full[:m_post.start()] + " " + full[m_post.end():])
                    # 都道府県を抽出。都道府県名が見つかった場合は、その位置以降を
                    # 住所として採用することで、必ず「都道府県・市区町村」から始まり
                    # 建物名・階数まで含んだ完全な住所にする。
                    m_pref = _PREF_PATTERN.search(full)
                    if m_pref:
                        data[Schema.PREF] = m_pref.group(1)
                        data[Schema.ADDR] = _clean(full[m_pref.start():])
                    else:
                        # 政令指定都市表記のみ（例: 京都市…）で都道府県名が無い場合は
                        # 結合・整形済みの全文をそのまま住所として採用する。
                        data[Schema.ADDR] = full
                elif key == "事業所":
                    data["事業所"] = val
                elif "企業ホームページ" in key or "ホームページ" in key:
                    a_tag = td.select_one("a[href]")
                    if a_tag:
                        href = a_tag.get("href", "")
                        # mynavi 転送URLは除外
                        if "url-forwarder" not in href and "mynavi.jp" not in href:
                            data[Schema.HP] = href
                        else:
                            data[Schema.HP] = val
                    else:
                        data[Schema.HP] = val

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = MynaviTenshokuScraper()
    scraper.execute(LIST_URL.format(1))

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
