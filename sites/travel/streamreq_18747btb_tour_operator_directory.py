"""
【STREAMREQ-18747】BTB Tour Operator Directory（ベリーズ） — streamreq_18747btb_tour_operator_directory

取得対象:
    ベリーズ観光局 (Belize Tourism Board / BTB) が公開する
    「Licensed Tour Operators」(認可ツアーオペレーター) 名簿の全件。
    2026-09 時点で 549 件 (40 件 / ページ × 13 ページ + 29 件)。

取得フロー:
    1. ルート (引数 url = https://www.belizetourismboard.org/tour-operator/) を取得し、
       ページ内の iframe から実データ (ASP.NET WebForms レポート) の URL を導出する。
       ルート側は Cloudflare の managed challenge (403 "Just a moment...") で
       requests からは取得できないため、その場合はルート URL のホスト名から組み立てる
       (www.{domain} -> datalink.{domain}:8443/BtbWebReports/TourOperatorListing.aspx)。
       ※ どちらの経路でも起点は常に引数 url であり、別の起点 URL はハードコードしない。
    2. レポートページを GET し、hidden フィールド
       (__VIEWSTATE / __VIEWSTATEGENERATOR / __EVENTVALIDATION) を収集する。
    3. 初期表示は 0 件。非表示の submit ボタン ctl00$Body$btnLoadPage を POST すると
       テーブル #Body_gvTourOperator にデータが描画される
       (ブラウザでは $("#btnLoadPage").click() が onload で実行される)。
       UpdatePanel の非同期 postback 用ヘッダを付けない通常の POST なので、
       レスポンスは次ページ分の hidden フィールドを含む完全な HTML が返る
       (= requests だけで巡回でき、データ側ホストに Cloudflare は無い)。
    4. 1 行 = 1 事業者。行を 1 件読むたびに即 yield する (全件バッファしない)。
    5. ページ送りは #Body_Paging_upnlPaging 内のリンク。href は
       javascript:__doPostBack('ctl00$Body$Paging$rptPaging$ctlXX$lbtnShowPage','')
       形式なので、イベントターゲットを取り出して __EVENTTARGET に載せて POST する。
       次ページ番号のリンクが可視ウィンドウ外 (11 ページ目以降) の場合は "Next" を使う。
       リンクが無くなった時点 (最終ページ) で終了する。

一覧テーブル (#Body_gvTourOperator) の列構成 (549 件中の充足数):
    1列目 Tour Operator Name … 事業者名        (549/549)
    2列目 Phone              … 電話番号        (548/549)
    3列目 Address            … 住所 ," 都市 ," 地区 (549/549、うち 35 件は実質空)
    4列目 Email              … メールアドレス  (548/549)
    5列目 Website            … Web サイト      (286/549 = 52%)
    ※ 行に詳細ページへのリンクは無く、一覧のみで全項目が揃う (詳細ページ自体が存在しない)。

備考:
    - 依頼指示どおり 国カラムは全件 "ベリーズ" 固定、TEL は +501 を付与した国際表記に
      正規化し、Schema.URL (取得元URL) には引数 url をそのまま入れる。
    - Address 列は " ," 区切りで「住所 ," 都市 ," 地区 (District)」の 3 フィールド。
      Schema.ADDR には 3 つを連結した完全な住所を入れ、EXTRA の "都市" / "地区" に
      分解した値も入れる。表記ゆれ (Cayo / Cayo District / STANN CREEK 等) は
      サイト側の入力ゆれだが、勝手に丸めると原データが壊れるため素の表記を保つ。
    - ベリーズの事業者のため Schema.PREF (日本の都道府県) は使用しない。
    - 元データ側で Email 列が 50 文字で切られている行がある (例: "...@alimermaidadventure")。
      サイトの掲載内容そのものなので補完はしない。
    - Website 列にはメールアドレスや 2 サイト併記など不正値が混ざるため、
      URL として妥当なものだけを Schema.HP に入れる (_normalize_website 参照)。
    - 同名の事業者が数組あるが、住所 (支店) が異なるサイト側の別レコードのため
      名寄せ・重複排除はしない (掲載総件数をそのまま反映する)。
    - 自由記述の長文フィールドはサイト上に存在しない (掲載は構造化項目のみ)。
    - 海外事業者のため STX 名寄せは行わない (依頼指示)。
    - robots.txt (https://www.belizetourismboard.org/robots.txt) は /wp-admin/ のみ
      Disallow で本ページは許可。データ側ホスト (datalink) に robots.txt は無い。
      スクレイピングを禁止する文言は無い。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/streamreq_18747btb_tour_operator_directory.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id streamreq_18747btb_tour_operator_directory
"""

import logging
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4
import requests

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# EXTRA カラム名
COL_COUNTRY = "国"
COL_CITY = "都市"
COL_DISTRICT = "地区"
COL_UPDATED = "名簿更新月"

# href="javascript:__doPostBack('<target>','<arg>')" からイベントターゲットを取り出す
_DOPOSTBACK_PATTERN = re.compile(r"__doPostBack\(\s*'([^']*)'\s*,\s*'([^']*)'\s*\)")

# 見出し "Licensed Tour Operators updated September, 2026" の更新月
_UPDATED_PATTERN = re.compile(r"updated\s+(.+)$", re.IGNORECASE | re.DOTALL)

# 電話番号末尾の内線表記 ("ext403" / "ext. 403" / "x403")
_EXT_PATTERN = re.compile(r"(?:ext\.?|x)\s*(\d+)\s*$", re.IGNORECASE)

# Website 列の値が URL として妥当か (ドメイン + TLD を含むか)
_DOMAIN_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.\-]*\.[A-Za-z]{2,}(?:[:/?#].*)?$")


class Streamreq18747BtbTourOperatorDirectory(StaticCrawler):
    """BTB (ベリーズ観光局) Tour Operator Directory スクレイパー"""

    # 1 ページ (postback) で 40 件まとめて取れるため、待機はアイテム単位ではなく
    # ページ単位でかける (通信は全 14 ページ分 = 15 リクエストのみ)。
    DELAY = 1.0
    ITEM_DELAY = 0.0
    TIMEOUT = 60

    EXTRA_COLUMNS = [COL_COUNTRY, COL_CITY, COL_DISTRICT, COL_UPDATED]

    # 国・業種は名簿の性質上すべて固定値
    COUNTRY = "ベリーズ"
    CATEGORY = "Licensed Tour Operator"

    # ベリーズの国番号 (国内番号は 7 桁)
    COUNTRY_CODE = "501"
    LOCAL_TEL_DIGITS = 7

    # 実データ (ASP.NET レポート) の場所。ルート URL のホストから導出する際に使う
    DATA_HOST_PREFIX = "datalink"
    DATA_PORT = 8443
    DATA_PATH = "/BtbWebReports/TourOperatorListing.aspx"

    # ASP.NET コントロール名
    GRID_SELECTOR = "#Body_gvTourOperator"
    PAGING_SELECTOR = "#Body_Paging_upnlPaging"
    LOAD_BUTTON_NAME = "ctl00$Body$btnLoadPage"
    SEARCH_BOX_NAME = "ctl00$Body$txtSearchText"

    # ページ巡回の安全上限 (2026-09 時点で 14 ページ) と postback のリトライ上限
    MAX_PAGES = 60
    MAX_ATTEMPTS = 3

    # ------------------------------------------------------------------
    # メイン
    # ------------------------------------------------------------------
    def parse(self, url: str):
        listing_url = self._resolve_listing_url(url)
        logger.info("実データ (ASP.NET レポート) URL: %s", listing_url)

        soup = self._load_first_page(listing_url)
        self.total_items = self._read_total(soup)
        updated = self._read_updated_label(soup)

        for page in range(1, self.MAX_PAGES + 1):
            rows = self._data_rows(soup)
            if not rows:
                logger.info("%d ページ目でデータ行が 0 件になったため終了します", page)
                break
            logger.info("%d ページ目: %d 件", page, len(rows))

            for row in rows:
                try:
                    item = self._build_item(row, url, updated)
                except Exception as e:  # 1 行の不正で全体を止めない
                    self.error_count += 1
                    logger.warning("行の解析に失敗 (スキップ): %s", e)
                    continue
                if item:
                    yield item

            target = self._next_page_target(soup, page + 1)
            if not target:
                logger.info("次ページのリンクが無いため終了します (最終ページ: %d)", page)
                break

            payload = self._hidden_fields(soup)
            payload["__EVENTTARGET"] = target
            payload["__EVENTARGUMENT"] = ""
            payload[self.SEARCH_BOX_NAME] = ""
            payload.pop(self.LOAD_BUTTON_NAME, None)
            time.sleep(self.DELAY)  # ページ送りの間隔 (アイテム単位ではなくページ単位)
            soup = self._post_with_retry(listing_url, payload)

    # ------------------------------------------------------------------
    # URL 解決
    # ------------------------------------------------------------------
    def _resolve_listing_url(self, url: str) -> str:
        """ルート URL から実データ (iframe 先) の URL を導出する。

        1. ルートページが取得できれば iframe の src を使う。
        2. Cloudflare challenge 等で取得できない場合は同じルート URL のホスト名から
           組み立てる (起点はあくまで引数 url)。
        """
        soup = None
        try:
            soup = self.get_soup(url)
        except requests.exceptions.RequestException as e:
            logger.warning("ルートページを取得できません (ホスト名から導出します): %s", e)

        if soup is not None:
            for iframe in soup.select("iframe[src]"):
                src = iframe.get("src", "")
                if self.DATA_PATH.lower() in src.lower():
                    return urljoin(url, src)
            logger.warning("ルートページに実データの iframe が見つかりません。ホスト名から導出します")
        else:
            logger.warning("ルートページを取得できませんでした。ホスト名から導出します")

        parts = urlsplit(url)
        domain = parts.hostname or ""
        if domain.startswith("www."):
            domain = domain[len("www."):]
        return f"https://{self.DATA_HOST_PREFIX}.{domain}:{self.DATA_PORT}{self.DATA_PATH}"

    # ------------------------------------------------------------------
    # ASP.NET postback
    # ------------------------------------------------------------------
    def _load_first_page(self, listing_url: str) -> bs4.BeautifulSoup:
        """レポートページを開き、btnLoadPage の postback で 1 ページ目を描画させる。"""
        last_error: Exception | None = None

        for attempt in range(self.MAX_ATTEMPTS):
            try:
                # 初回はキャッシュ・自動リトライ付きの get_soup を使う。
                # 2 回目以降は ViewState が古い可能性があるのでキャッシュを介さず取り直す。
                soup = self.get_soup(listing_url) if attempt == 0 else self._get_fresh(listing_url)
                if soup is None:
                    raise RuntimeError(f"レポートページを取得できません: {listing_url}")

                payload = self._hidden_fields(soup)
                payload[self.LOAD_BUTTON_NAME] = ""
                payload[self.SEARCH_BOX_NAME] = ""
                loaded = self._post(listing_url, payload)
                if loaded.select_one(self.GRID_SELECTOR):
                    return loaded
                last_error = RuntimeError("btnLoadPage の postback でテーブルが描画されませんでした")
            except (requests.exceptions.RequestException, RuntimeError) as e:
                last_error = e
            logger.warning("1 ページ目の取得に失敗 (%d/%d): %s", attempt + 1, self.MAX_ATTEMPTS, last_error)
            time.sleep(min(2 ** attempt, 8))

        raise RuntimeError(
            f"1 ページ目を {self.MAX_ATTEMPTS} 回試行しても取得できませんでした: {listing_url}"
        ) from last_error

    def _post_with_retry(self, listing_url: str, payload: dict) -> bs4.BeautifulSoup:
        """ページ送りの postback。上限付きでリトライし、尽きたら例外にする。"""
        last_error: Exception | None = None

        for attempt in range(self.MAX_ATTEMPTS):
            try:
                soup = self._post(listing_url, payload)
                if soup.select_one(self.GRID_SELECTOR):
                    return soup
                last_error = RuntimeError("postback の応答にテーブルが含まれていません")
            except requests.exceptions.RequestException as e:
                last_error = e
            logger.warning("ページ送りに失敗 (%d/%d): %s", attempt + 1, self.MAX_ATTEMPTS, last_error)
            time.sleep(min(2 ** attempt, 8))

        raise RuntimeError(
            f"ページ送りを {self.MAX_ATTEMPTS} 回試行しても成功しませんでした: {listing_url}"
        ) from last_error

    def _post(self, listing_url: str, payload: dict) -> bs4.BeautifulSoup:
        """WebForms への POST。レスポンスは完全な HTML (次の ViewState を含む)。"""
        response = self.session.post(
            listing_url,
            data=payload,
            timeout=self.TIMEOUT,
            headers={"Referer": listing_url},
        )
        response.raise_for_status()
        return bs4.BeautifulSoup(response.text, "html.parser")

    def _get_fresh(self, listing_url: str) -> bs4.BeautifulSoup:
        """キャッシュを介さずレポートページを取り直す (ViewState 再取得用)。"""
        response = self.session.get(listing_url, timeout=self.TIMEOUT)
        response.raise_for_status()
        return bs4.BeautifulSoup(response.text, "html.parser")

    @staticmethod
    def _hidden_fields(soup: bs4.BeautifulSoup) -> dict:
        """フォームの hidden フィールド (ViewState 等) をそのまま引き継ぐ。"""
        fields = {}
        for tag in soup.select("input[type=hidden][name]"):
            fields[tag["name"]] = tag.get("value", "")
        return fields

    def _next_page_target(self, soup: bs4.BeautifulSoup, next_page: int) -> str | None:
        """ページャから次ページの __EVENTTARGET を取り出す。

        次ページ番号のリンクが可視ウィンドウに無い場合 (11 ページ目以降) は
        "Next" リンクにフォールバックする。
        """
        links = soup.select(f"{self.PAGING_SELECTOR} a[href*='__doPostBack']")
        fallback = None

        for link in links:
            text = link.get_text(strip=True)
            match = _DOPOSTBACK_PATTERN.search(link.get("href", ""))
            if not match:
                continue
            if text == str(next_page):
                return match.group(1)
            if text.lower() == "next":
                fallback = match.group(1)

        return fallback

    # ------------------------------------------------------------------
    # 抽出
    # ------------------------------------------------------------------
    def _data_rows(self, soup: bs4.BeautifulSoup) -> list:
        """ヘッダ行を除いたデータ行 (td 5 列) を返す。"""
        grid = soup.select_one(self.GRID_SELECTOR)
        if grid is None:
            return []
        return [tr for tr in grid.select("tr") if len(tr.select("td")) >= 5]

    def _read_total(self, soup: bs4.BeautifulSoup) -> int | None:
        """ページャの総件数表示 (例: "1 - 40 of 549") を読む。"""
        label = soup.select_one("#Body_Paging_lblTotal")
        if label is None:
            return None
        digits = re.sub(r"\D", "", label.get_text())
        return int(digits) if digits else None

    @staticmethod
    def _read_updated_label(soup: bs4.BeautifulSoup) -> str:
        """見出し "Licensed Tour Operators updated September, 2026" の更新月を取り出す。"""
        heading = soup.select_one("h3")
        if heading is None:
            return ""
        match = _UPDATED_PATTERN.search(" ".join(heading.get_text(" ", strip=True).split()))
        return match.group(1).strip() if match else ""

    def _build_item(self, row: bs4.element.Tag, source_url: str, updated: str) -> dict:
        """1 行 (tr) から 1 件分の dict を組み立てる。"""
        cells = [" ".join(td.get_text(" ", strip=True).split()) for td in row.select("td")]
        name = cells[0]
        if not name:
            return {}

        address, city, district = self._split_address(cells[2])

        return {
            Schema.URL: source_url,           # 取得元URL = sites.yml の url (依頼指示)
            Schema.NAME: name,
            Schema.ADDR: address,
            Schema.TEL: self._normalize_tel(cells[1]),
            Schema.EMAIL: self._normalize_email(cells[3]),
            Schema.HP: self._normalize_website(cells[4]),
            Schema.CAT_SITE: self.CATEGORY,
            COL_COUNTRY: self.COUNTRY,
            COL_CITY: city,
            COL_DISTRICT: district,
            COL_UPDATED: updated,
        }

    # ------------------------------------------------------------------
    # 値の正規化
    # ------------------------------------------------------------------
    @staticmethod
    def _split_address(raw: str) -> tuple[str, str, str]:
        """Address 列を「住所 (完全形)」「都市」「地区」に分ける。

        掲載側のフィールド区切りは " ," (半角スペース + カンマ) で、
        「番地・通り ," 都市 ," 地区(District)」の 3 フィールド。
        番地・通りの中には素のカンマが含まれることがあるため、
        素の "," ではなく " ," で分割する。

        例: "Buena Vista New Area, Bullet Tree Road, San Ignacio ,Cayo ,"
            -> ("Buena Vista New Area, Bullet Tree Road, San Ignacio, Cayo", "Cayo", "")
            "49.5 Mls. Phillip Goldson Highway ,Carmelita Village ,Orange Walk"
            -> ("49.5 Mls. ... Highway, Carmelita Village, Orange Walk",
                "Carmelita Village", "Orange Walk")
        カンマだけの行 (", ,") は 3 つとも空になる。
        """
        if not raw:
            return "", "", ""

        segments = [seg.strip().strip(",").strip() for seg in raw.split(" ,")]
        if len(segments) == 1:
            # 想定外の区切り (" ," 無し) は素のカンマで分割する
            segments = [seg.strip() for seg in raw.split(",")]

        street = segments[0] if len(segments) > 0 else ""
        city = segments[1] if len(segments) > 1 else ""
        district = segments[2] if len(segments) > 2 else ""

        # 都市が空で地区だけ埋まっている行は、地区を都市として扱う
        if not city and district:
            city, district = district, ""

        address = ", ".join(seg for seg in (street, city, district) if seg)
        return address, city, district

    def _normalize_tel(self, raw: str) -> str:
        """電話番号をベリーズの国際表記 (+501-XXXXXXX) に正規化する。

        掲載表記のゆれ:
            "6564889" / "522-2044"        … 国内 7 桁            -> +501-6564889
            "501-522-2200"                … 国番号込み 10 桁      -> +501-5222200
            "+502-3100-7968"              … 他国 (グアテマラ等)   -> +502-3100-7968 (国番号を保つ)
            "8802237 ; 8804626"           … 複数番号 (; , /)      -> " / " で連結
            "6201332 620-1332"            … 空白区切りの複数番号  -> 7 桁ずつに分割
            "226-2071 ext403"             … 内線付き              -> +501-2262071 ext403
        """
        if not raw:
            return ""

        results: list[str] = []
        for part in re.split(r"[;,/]", raw):
            part = part.strip()
            if not part:
                continue

            ext_match = _EXT_PATTERN.search(part)
            ext = ext_match.group(1) if ext_match else ""
            if ext_match:
                part = part[: ext_match.start()]

            has_plus = part.lstrip().startswith("+")
            digits = re.sub(r"\D", "", part)
            if not digits:
                continue

            if has_plus:
                # 掲載側が国番号を明示している (ベリーズ以外を含む) ので国番号と区切りを尊重する
                grouped = re.sub(r"[^\d-]", "-", part.strip().lstrip("+"))
                grouped = re.sub(r"-+", "-", grouped).strip("-")
                numbers = [f"+{grouped}"]
            elif (
                digits.startswith(self.COUNTRY_CODE)
                and len(digits) == len(self.COUNTRY_CODE) + self.LOCAL_TEL_DIGITS
            ):
                numbers = [f"+{self.COUNTRY_CODE}-{digits[len(self.COUNTRY_CODE):]}"]
            elif len(digits) > self.LOCAL_TEL_DIGITS and len(digits) % self.LOCAL_TEL_DIGITS == 0:
                # 空白区切りの複数番号が 1 つの文字列に繋がっているケース
                numbers = [
                    f"+{self.COUNTRY_CODE}-{digits[i:i + self.LOCAL_TEL_DIGITS]}"
                    for i in range(0, len(digits), self.LOCAL_TEL_DIGITS)
                ]
            else:
                numbers = [f"+{self.COUNTRY_CODE}-{digits}"]

            if ext:
                numbers[-1] = f"{numbers[-1]} ext{ext}"

            for number in numbers:
                if number not in results:
                    results.append(number)

        return " / ".join(results)

    @staticmethod
    def _normalize_email(raw: str) -> str:
        """メールアドレス。複数併記 (; や ,) は " / " で連結し、末尾の区切り記号を落とす。"""
        if not raw:
            return ""
        parts = [p.strip() for p in re.split(r"[;,]", raw)]
        return " / ".join(p for p in parts if p)

    @staticmethod
    def _normalize_website(raw: str) -> str:
        """Website 列から URL として妥当な値だけを取り出す。

        掲載側の不正値 (メールアドレス、2 サイト併記、"https:/" のスラッシュ抜け) に対応する。
        スキームが無い表記 (www.example.com / example.com) には https:// を補う。
        """
        if not raw:
            return ""

        for token in re.split(r"[\s+]+", raw.strip()):
            if not token or "@" in token:  # メールアドレスは HP ではない
                continue
            token = re.sub(r"^(https?:)/(?!/)", r"\1//", token, flags=re.IGNORECASE)  # "https:/x" -> "https://x"
            bare = re.sub(r"^https?://", "", token, flags=re.IGNORECASE)
            if not _DOMAIN_PATTERN.match(bare):
                continue
            return token if re.match(r"^https?://", token, flags=re.IGNORECASE) else f"https://{token}"

        return ""


if __name__ == "__main__":
    import logging as _logging
    _logging.basicConfig(level=_logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = Streamreq18747BtbTourOperatorDirectory()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.belizetourismboard.org/tour-operator/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
