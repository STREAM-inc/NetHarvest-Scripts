"""
FABEX 出展社検索 (日本食糧新聞社 / Salesforce Sites) スクレイパー

取得対象:
    中食・外食業界の業務用専門展「ファベックス」出展社検索システムに
    掲載されている全出展社の情報。

サイト構造 (2026-09 調査):
    - 一覧 (検索) ページ  : /fabex/searchExhibitor?ev=fbx
      Visualforce ページ。GET だけで検索が実行され、結果行 (出展社名/展示会名/
      区分1/小間番号/主な出展予定品) が描画される。
      `year=YYYY` クエリで開催年 (2018〜当年) を切り替えられる。
    - 詳細 (出展社一覧) : 検索結果の全チェックボックスを ON にして
      「詳細を見る」(toDetailList) を POST すると /fabex/exhibitorlist が返る。
      1 出展社 = 1 つの table.table-01 で、th/td ペアに
      出展社名・展示会名・小間番号・出展区分1・出展区分2・Webページ・
      主な出展予定品・共同出展社名 などが入る。50 件ごとのページ送り
      (goNext を POST)。
    - ViewState (com.salesforce.visualforce.ViewState) を含む hidden を
      そのまま詰め直して POST する必要がある。アクション名 (j_id59 等) は
      自動採番で変動するため、ページ内 JS の jsfcljs(...) 引数から解決する。

備考:
    - 所在地・電話番号はこのシステムには掲載されていない (ラベル自体が存在しない)。
      将来掲載された場合に拾えるよう、ラベル→Schema のマッピングだけ実装してある。
    - 当年開催分は出展社データが未公開の期間があるため (調査時点の 2026 年版は 0 件)、
      正規 URL から派生した `year=YYYY` を新しい年から順に辿り、
      掲載のある開催年の出展社をすべて取得する。

実行方法:
    python scripts/sites/corporate/fabex.py
    python bin/run_flow.py --site-id fabex
"""

import datetime
import html as html_lib
import logging
import re
import sys
import time
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import bs4

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

SITE_NAME = "FABEX出展社検索"

# searchExhibitor?year=YYYY が有効な最も古い開催年 (2017 以前はエラーページ)
OLDEST_YEAR = 2018
# 1 開催年あたりの詳細ページ (50 件/ページ) の巡回上限。無限ループ防止。
MAX_DETAIL_PAGES = 200
# POST のリトライ上限 (Visualforce のシステムエラー画面/一時エラー対策)
MAX_POST_ATTEMPTS = 3

# 詳細ページの th ラベル → Schema カラムの対応
_LABEL_TO_SCHEMA = {
    "所在地": Schema.ADDR,
    "住所": Schema.ADDR,
    "郵便番号": Schema.POST_CODE,
    "TEL": Schema.TEL,
    "電話番号": Schema.TEL,
    "代表者名": Schema.REP_NM,
}

# 長文の自由記述 (出展PR / 商品情報) は著作権リスクのため取得しない
EXTRA_YEAR = "開催年"
EXTRA_EVENT = "展示会名"
EXTRA_BOOTH = "小間番号"
EXTRA_KUBUN2 = "出展区分2"
EXTRA_PRODUCTS = "主な出展予定品"
EXTRA_COEXHIBITOR = "共同出展社名"


class FabexScraper(StaticCrawler):
    """FABEX 出展社検索 (Salesforce Sites / Visualforce) のスクレイパー。"""

    DELAY = 0.0          # ページ取得側 (parse 内) でウェイトを入れる
    ITEM_DELAY = 0.0     # 1 ページから 50 件 yield するためアイテム単位の待機は無し
    TIMEOUT = 60         # Visualforce の応答が遅いため長めに取る
    PAGE_DELAY = 1.0     # 1 リクエストごとのウェイト秒数

    EXTRA_COLUMNS = [
        EXTRA_YEAR,
        EXTRA_EVENT,
        EXTRA_BOOTH,
        EXTRA_KUBUN2,
        EXTRA_PRODUCTS,
        EXTRA_COEXHIBITOR,
    ]

    def prepare(self):
        """重複排除用の集合を初期化する。"""
        super().prepare()
        self._seen: set[tuple[str, str, str]] = set()

    # ─────────────────────────────────────────────
    # メイン処理
    # ─────────────────────────────────────────────
    def parse(self, url: str):
        """正規 URL を起点に、掲載のある開催年の出展社を 1 件ずつ yield する。

        Args:
            url (str): sites.yml に登録された正規 URL (出展社検索ページ)

        Yields:
            dict: Schema / EXTRA_COLUMNS をキーに持つ出展社 1 件分のデータ
        """
        for edition_url in self._edition_urls(url):
            try:
                yield from self._crawl_edition(edition_url)
            except Exception as e:  # 1 開催年の失敗で全体を止めない
                self.error_count += 1
                logger.warning("開催年ページの処理に失敗 (スキップ): %s — %s", edition_url, e)

    def _edition_urls(self, url: str):
        """正規 URL から開催年別 URL を派生させて返す (新しい年が先)。

        Args:
            url (str): 正規 URL

        Returns:
            list[str]: 巡回対象 URL のリスト
        """
        urls = [url]  # まず正規 URL (= 現行開催回) をそのまま辿る
        this_year = datetime.date.today().year
        for year in range(this_year, OLDEST_YEAR - 1, -1):
            urls.append(self._with_query(url, "year", str(year)))
        return urls

    @staticmethod
    def _with_query(url: str, key: str, value: str) -> str:
        """URL のクエリパラメータを 1 つ差し替えた URL を組み立てる。"""
        parts = urlparse(url)
        query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k != key]
        query.append((key, value))
        return urlunparse(parts._replace(query=urlencode(query)))

    def _crawl_edition(self, edition_url: str):
        """1 開催年分の出展社を取得して yield する。

        Args:
            edition_url (str): 開催年別の検索ページ URL

        Yields:
            dict: 出展社 1 件分のデータ
        """
        soup = self.get_soup(edition_url)          # ← 中断可能な session.get
        if soup is None:
            return
        time.sleep(self.PAGE_DELAY)

        total = self._published_count(soup)
        if total == 0:
            logger.info("掲載件数 0 件のためスキップ: %s", edition_url)
            return
        year_label = self._year_label(soup)
        logger.info("%s 年版: 掲載件数 %d 件", year_label or "?", total)

        # 検索結果の全チェックボックスを ON にして「詳細を見る」を POST する
        command = self._command_param(str(soup), "toDetailList")
        if not command:
            logger.warning("詳細表示アクションを解決できません: %s", edition_url)
            return
        data = self._form_data(soup, check_all=True)
        data[command] = command
        response_text = self._post_form(edition_url, soup, data)

        for page_no in range(1, MAX_DETAIL_PAGES + 1):
            detail = bs4.BeautifulSoup(response_text, "html.parser")
            count = 0
            for table in self._exhibitor_tables(detail):
                item = self._build_item(table, edition_url, year_label)
                if item is None:
                    continue
                count += 1
                yield item                        # ← 1 件ごとに即 yield
            logger.info("詳細ページ %d: %d 件", page_no, count)

            current, last = self._pager(response_text)
            if current is None or current >= last:
                break
            next_command = self._command_param(response_text, "goNext")
            if not next_command:
                logger.warning("次ページのアクションを解決できません (打ち切り)")
                break
            next_data = self._form_data(detail)
            next_data[next_command] = next_command
            response_text = self._post_form(edition_url, detail, next_data)

    # ─────────────────────────────────────────────
    # 1 出展社 → dict
    # ─────────────────────────────────────────────
    def _build_item(self, table: bs4.Tag, page_url: str, year_label: str) -> dict | None:
        """出展社 1 社分の table から dict を組み立てる。

        Args:
            table (bs4.Tag): table.table-01 (出展社 1 社分)
            page_url (str): 取得元ページの URL
            year_label (str): 開催年 (例 "2021")

        Returns:
            dict | None: データ。会社名が取れない/重複の場合は None
        """
        fields = self._label_cells(table)

        name_box = table.select_one("div.name1")
        if name_box is not None:
            name = self._first_text(name_box)
            kana_tag = name_box.select_one("span.name2")
            kana = kana_tag.get_text(" ", strip=True) if kana_tag else ""
        else:
            name, kana = self._cell_text(fields.get("出展社名")), ""
        if not name:
            return None

        booth = self._cell_text(fields.get("小間番号"))
        key = (year_label, name, booth)
        if key in self._seen:
            return None
        self._seen.add(key)

        item = {
            Schema.URL: page_url,
            Schema.NAME: name,
            Schema.NAME_KANA: kana,
            Schema.PREF: "",
            Schema.ADDR: "",
            Schema.TEL: "",
            Schema.POST_CODE: "",
            Schema.REP_NM: "",
            Schema.CAT_SITE: self._cell_text(fields.get("出展区分1")),
            Schema.HP: self._link(fields.get("Webページ")),
            EXTRA_YEAR: year_label,
            EXTRA_EVENT: self._cell_text(fields.get("展示会名")),
            EXTRA_BOOTH: booth,
            EXTRA_KUBUN2: self._cell_text(fields.get("出展区分2")),
            EXTRA_PRODUCTS: self._cell_text(fields.get("主な出展予定品")),
            EXTRA_COEXHIBITOR: self._cell_text(fields.get("共同出展社名")),
        }
        # 将来 所在地/TEL 等のラベルが追加された場合に拾う
        for label, column in _LABEL_TO_SCHEMA.items():
            if label in fields and not item.get(column):
                item[column] = self._cell_text(fields[label])
        if item[Schema.ADDR] and not item[Schema.PREF]:
            item[Schema.PREF] = self._pref_of(item[Schema.ADDR])
        return item

    # ─────────────────────────────────────────────
    # HTML 解析ヘルパー
    # ─────────────────────────────────────────────
    @staticmethod
    def _own_tags(table: bs4.Tag, names) -> list:
        """入れ子の table (商品情報の table-02) を除いた自前のタグだけを返す。"""
        return [t for t in table.find_all(names) if t.find_parent("table") is table]

    def _exhibitor_tables(self, soup: bs4.BeautifulSoup) -> list:
        """詳細ページから出展社 1 社分の table を取り出す。"""
        tables = []
        for table in soup.find_all("table", class_="table-01"):
            labels = [th.get_text(strip=True) for th in self._own_tags(table, "th")]
            if "出展社名" in labels:
                tables.append(table)
        return tables

    def _label_cells(self, table: bs4.Tag) -> dict:
        """table 内の th/td ペアを {ラベル: td タグ} の dict にする。"""
        fields: dict[str, bs4.Tag] = {}
        for tr in self._own_tags(table, "tr"):
            cells = [c for c in tr.find_all(["th", "td"]) if c.find_parent("table") is table]
            i = 0
            while i < len(cells) - 1:
                if cells[i].name == "th" and cells[i + 1].name == "td":
                    fields.setdefault(cells[i].get_text(" ", strip=True), cells[i + 1])
                    i += 2
                else:
                    i += 1
        return fields

    @staticmethod
    def _first_text(tag: bs4.Tag) -> str:
        """タグ直下の最初のテキストノード (社名本体) を返す。"""
        for node in tag.children:
            if isinstance(node, bs4.NavigableString):
                text = node.strip()
                if text:
                    return text
        return tag.get_text(" ", strip=True)

    @staticmethod
    def _cell_text(cell) -> str:
        """td の表示テキストを 1 行に整形して返す。"""
        if cell is None:
            return ""
        text = cell.get_text(" ", strip=True)
        return re.sub(r"\s+", " ", text).strip()

    @staticmethod
    def _link(cell) -> str:
        """td 内のリンク URL を返す (無ければテキストが URL ならそれを返す)。"""
        if cell is None:
            return ""
        a = cell.find("a", href=True)
        if a:
            href = a["href"].strip()
            if href.lower().startswith("http"):
                return href
        text = cell.get_text(" ", strip=True)
        return text if text.lower().startswith("http") else ""

    @staticmethod
    def _pref_of(address: str) -> str:
        """住所文字列の先頭から都道府県を切り出す。"""
        m = re.match(r"\s*(.{2,3}?[都道府県])", address)
        return m.group(1) if m else ""

    @staticmethod
    def _published_count(soup: bs4.BeautifulSoup) -> int:
        """「出展社データ掲載件数 N 件」から N を取り出す。"""
        for div in soup.select("div.f14"):
            m = re.search(r"掲載件数\s*([0-9,]+)\s*件", div.get_text(" ", strip=True))
            if m:
                return int(m.group(1).replace(",", ""))
        return 0

    @staticmethod
    def _year_label(soup: bs4.BeautifulSoup) -> str:
        """<title>ファベックス 2021</title> から開催年を取り出す。"""
        title = soup.find("title")
        if not title:
            return ""
        m = re.search(r"(20\d{2})", title.get_text(strip=True))
        return m.group(1) if m else ""

    @staticmethod
    def _pager(response_text: str) -> tuple[int | None, int]:
        """詳細ページの「N / M」表記から現在ページと総ページ数を返す。"""
        m = re.search(r"(\d+)\s*/\s*(\d+)", html_lib.unescape(response_text))
        if not m:
            return None, 0
        return int(m.group(1)), int(m.group(2))

    @staticmethod
    def _command_param(page_html: str, func_name: str) -> str | None:
        """Visualforce の JS 関数名から actionFunction のパラメータ名を解決する。

        `j_id59` 等の ID は自動採番で変動するため、ページ内の
        `function toDetailList() { ... jsfcljs(document.forms[...],'NAME,NAME','') }`
        から NAME を取り出す。
        """
        text = html_lib.unescape(page_html)
        m = re.search(
            r"function\s+" + re.escape(func_name) + r"\s*\(\s*\)\s*\{.*?"
            r"jsfcljs\(\s*document\.forms\[[^\]]+\]\s*,\s*'([^',]+),",
            text,
            re.S,
        )
        return m.group(1) if m else None

    @staticmethod
    def _form_data(soup: bs4.BeautifulSoup, check_all: bool = False) -> dict:
        """フォームの hidden / input / select を POST 用の dict に詰め直す。

        ViewState は <form> の外 (ajax-view-state) にあるため、
        ページ全体から input を拾う (ブラウザの vfPrepareForms と同じ挙動)。
        """
        data: dict[str, str] = {}
        for inp in soup.find_all("input"):
            name = inp.get("name")
            if not name:
                continue
            input_type = (inp.get("type") or "text").lower()
            if input_type == "checkbox":
                if check_all and "myCheck" in (inp.get("class") or []):
                    data[name] = "on"
                continue
            if input_type in ("radio", "submit", "image", "button", "file"):
                continue
            data[name] = inp.get("value", "")
        for select in soup.find_all("select"):
            name = select.get("name")
            if not name:
                continue
            option = select.find("option", selected=True)
            data[name] = option.get("value", "") if option else ""
        return data

    # ─────────────────────────────────────────────
    # 通信
    # ─────────────────────────────────────────────
    def _post_form(self, base_url: str, soup: bs4.BeautifulSoup, data: dict) -> str:
        """Visualforce のフォームを POST し、レスポンス HTML を返す。

        Args:
            base_url (str): 相対 action を解決するための基準 URL
            soup (bs4.BeautifulSoup): action を持つフォームを含むページ
            data (dict): POST するフォームデータ

        Returns:
            str: レスポンスの HTML

        Raises:
            RuntimeError: MAX_POST_ATTEMPTS 回すべて失敗した場合
        """
        form = soup.find("form")
        action = urljoin(base_url, form.get("action") if form else "")
        last_error: Exception | None = None
        for attempt in range(MAX_POST_ATTEMPTS):
            try:
                response = self.session.post(action, data=data, timeout=self.TIMEOUT)
                response.raise_for_status()
                time.sleep(self.PAGE_DELAY)
                return response.text
            except Exception as e:  # requests の通信/HTTP エラー
                last_error = e
                logger.warning("POST 失敗 (%d/%d): %s — %s",
                               attempt + 1, MAX_POST_ATTEMPTS, action, e)
                time.sleep(min(2 ** attempt, 8))
        raise RuntimeError(f"POST に {MAX_POST_ATTEMPTS} 回失敗しました: {action} — {last_error}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = FabexScraper()
    scraper.site_name = SITE_NAME
    scraper.execute("https://nissyoku.my.salesforce-sites.com/fabex/searchExhibitor?ev=fbx")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
