"""
BAR-NAVI (バーナビ) — サントリー公式バー検索サイト / 携帯番号掲載店フォーカス版

取得カラム (依頼仕様):
    名称 / 住所 / TEL / 掲載URL (店舗詳細ページ) / 都道府県 (住所から抽出)
    + 電話番号種別 (携帯 / 固定 / フリーダイヤル) … 後続クレンジングで
      携帯番号 (090/080/070) の行だけを残すための判定用フラグ
    + 取得ソース (live / commoncrawl)

携帯番号フィルタについて (依頼備考への対応):
    依頼は「090/080/070 で始まる携帯番号の行のみを対象とする。固定電話・
    フリーダイヤル等の行は取得してもよいが後続クレンジング工程で必ず除外する」。
    掲載電話番号は一覧ページに無く店舗詳細ページにしか無いため、事前に
    携帯番号店だけを絞り込むことはできない (下記「店舗IDの規則」参照)。
    そこで既定では全件を取得し、EXTRA カラム「電話番号種別」で
    携帯 / 固定 / フリーダイヤル を機械判定できる形にして出力する。
    クローラー側で携帯番号店だけに絞りたい場合は MOBILE_ONLY = True にする。
    ライブ取得時は一覧カード (div.shop_block の li.btn_tel) に掲載電話番号が
    載っているので、MOBILE_ONLY なら詳細ページを開く前に携帯番号店だけへ
    絞り込める (住所は詳細ページにしか無いため詳細取得自体は必要)。

取得フロー (2 系統。実行元 IP が本番サイトに通るかで自動的に切り替わる):

    [A] ライブ取得 (WAF に通る IP の場合)
        1. {url} を起点に都道府県検索ページ /search/f__{JISコード}/ を生成
           (トップ / sitemap.html にリンクがあればそれを優先)
        2. /search/f__{NN}/page__{N}/ でページ送り (20 件/ページ。パス形式で、
           ?page=N は無効)。総件数は span.search_header_result_count の
           2 つ目 (「全 N 件」) から取得
        3. div.shop_block の a[href*="/shop/{id}/"] から店舗詳細へ入り、
           1 件取得ごとに即 yield する (カードの li.btn_tel に掲載TELがある)
    [B] Common Crawl フォールバック (WAF に 403 拒否される IP の場合)
        1. index.commoncrawl.org/collinfo.json で最新クロールを列挙
        2. 各クロールの CDX に url={host}/shop/* を投げ status 200 を列挙
        3. data.commoncrawl.org へ Range リクエストで WARC レコードを 1 件だけ
           取り出し、gzip 展開した HTML を [A] と同じパーサに流す

サイト構造の注意点 (2026-08 調査):
    - 店舗詳細は JSON-LD (BarOrPub) が主ソース。name / telephone / address /
      url が入る。dl(dt/dd) の「住所 / 電話」と a[href^="tel:"] も同値なので補完に使う。
    - <h1> はサイト名 (「バー検索サイト[BAR-NAVI]」) なので店名に使えない。
      店名は JSON-LD name → <title> ("店名 エリア -BAR-NAVI") → h2 の順で解決する。
    - 都道府県は JSON-LD address の先頭。無ければパンくずの都道府県、
      それも無ければ巡回元の都道府県コードから補完する。
      Schema.ADDR には都道府県を除いた市区町村以降を入れる。
    - 一覧カード div.shop_block は 店名(h3) / キャッチコピー / アクセス /
      予算 / 席数 / 喫煙 / 掲載TEL(li.btn_tel の tel: リンク) を持つが、
      住所は無いので住所が必要な限り詳細ページ取得は省けない。
    - 店舗IDの規則: 多くは掲載電話番号のハイフン抜き (0432013633 = 043-201-3633)。
      ただし携帯番号掲載店・番号非掲載店は 0X00xxxxxx / S000xxxxxx / 末尾英字
      (033667111A) といった別ID空間になる。よって「携帯番号店は ID が 090…」
      とは限らず、ID から携帯番号店を先に見つけることはできない。
      逆に「ID が数字のみで 090/080/070 以外で始まる」店は固定電話確定なので、
      MOBILE_ONLY 時のみ詳細取得をスキップする。
    - /{ローマ字県名}/ (例 /tokyo/) はカテゴリ入口で店舗リンクを含まないため
      一覧には使えない。/search/f__{NN}/ を使う。
    - robots.txt は /search/f__*/cc (市区町村ドリルダウン)、/search/map/、
      /search/refine/ のみ Disallow。本クローラーが使う
      /search/f__{NN}/[page__{N}/] と /shop/{id}/ は許可されている。
    - 利用規約 (/terms.html) 第5条(禁止事項)にスクレイピング/クローリングの
      明示的な禁止条項は無い (過度な負荷・運営妨害の一般条項のみ)。
      第4条で知的財産権を自社帰属としているため、店舗紹介文などの
      自由記述テキストは取得しない (依頼カラムも事実情報のみ)。

アクセス制限 (2026-08 時点、本コンテナからの実測):
    - 本番サイト: Akamai WAF がデータセンター (AWS) の IP を全パスで 403 拒否
      ("Access Denied")。JS チャレンジではなく IP 起因なので Playwright でも
      突破できない。よって基底クラスは StaticCrawler を使う
      (WAF を通る IP なら静的 HTML に全データが入っているため十分)。
    - Wayback Machine: web.archive.org は AWS から 429 で拒否されるため使えない。
      代わりに Common Crawl を使う (index/data.commoncrawl.org は AWS から取得可)。
    - Common Crawl のカバー範囲はクロール済みページのみ (1 クロールあたり
      店舗詳細 100 件前後) なので CC_MAX_COLLECTIONS 本を巡回して重複排除する。

実行方法:
    # ローカルテスト
    python scripts/sites/food/bar_navi_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id bar_navi_2
"""

import gzip
import json
import re
import sys
import urllib.parse
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# 都道府県検索ページ: /search/f__13/ (13 = JIS 都道府県コード)
_PREF_SEARCH_HREF_RE = re.compile(r"/search/f__(\d{2})/?$")
# 店舗詳細ページ (トップのみ)。/shop/{id}/map.html 等のサブページは除外する
_SHOP_URL_RE = re.compile(r"/shop/([0-9A-Za-z_-]+)/?$")
# ページ送り: /search/f__13/page__2/
_PAGE_HREF_RE = re.compile(r"/page__(\d+)/?$")

_TEL_RE = re.compile(r"0\d{1,4}[-(）)－−]?\d{1,4}[-(）)－−]?\d{3,4}")
_WS_RE = re.compile(r"\s+")

# WAF ブロック時に返る Akamai のエラーページ
_DENIED_RE = re.compile(r"Access Denied|errors\.edgesuite\.net", re.I)

_PREF_NAMES = {
    "01": "北海道", "02": "青森県", "03": "岩手県", "04": "宮城県", "05": "秋田県",
    "06": "山形県", "07": "福島県", "08": "茨城県", "09": "栃木県", "10": "群馬県",
    "11": "埼玉県", "12": "千葉県", "13": "東京都", "14": "神奈川県", "15": "新潟県",
    "16": "富山県", "17": "石川県", "18": "福井県", "19": "山梨県", "20": "長野県",
    "21": "岐阜県", "22": "静岡県", "23": "愛知県", "24": "三重県", "25": "滋賀県",
    "26": "京都府", "27": "大阪府", "28": "兵庫県", "29": "奈良県", "30": "和歌山県",
    "31": "鳥取県", "32": "島根県", "33": "岡山県", "34": "広島県", "35": "山口県",
    "36": "徳島県", "37": "香川県", "38": "愛媛県", "39": "高知県", "40": "福岡県",
    "41": "佐賀県", "42": "長崎県", "43": "熊本県", "44": "大分県", "45": "宮崎県",
    "46": "鹿児島県", "47": "沖縄県",
}
_PREF_NAME_SET = set(_PREF_NAMES.values())
_PREF_RE = re.compile("^(" + "|".join(_PREF_NAMES.values()) + ")")

# 電話番号種別
_TEL_KIND_MOBILE = "携帯"
_TEL_KIND_TOLLFREE = "フリーダイヤル"
_TEL_KIND_FIXED = "固定"
_MOBILE_PREFIXES = ("090", "080", "070")
_TOLLFREE_PREFIXES = ("0120", "0800", "0570", "0990")


class BarNavi2Scraper(StaticCrawler):
    """BAR-NAVI (バーナビ) スクレイパー — 携帯番号掲載店フォーカス版"""

    DELAY = 1.0
    TIMEOUT = 25
    EXTRA_COLUMNS = ["電話番号種別", "取得ソース"]

    # True にすると携帯番号 (090/080/070) の行だけを yield する。
    # 既定は False = 全件取得 + 「電話番号種別」で判定可能にする
    # (依頼備考「固定電話等は取得してもよいが後続工程で除外」に対応)。
    MOBILE_ONLY = False

    # 1 都道府県あたりの一覧ページ上限 (東京 1,729 件 = 87 ページ)
    MAX_PAGES_PER_PREF = 300

    # ---- Common Crawl フォールバックの設定 ----
    CC_FALLBACK = True
    CC_COLLINFO_URL = "https://index.commoncrawl.org/collinfo.json"
    CC_INDEX_URL = "https://index.commoncrawl.org/{collection}-index"
    CC_DATA_URL = "https://data.commoncrawl.org/{filename}"
    CC_MAX_COLLECTIONS = 12
    CC_INDEX_LIMIT = 5000

    def prepare(self):
        # ライブ取得が WAF で拒否されたか (最初の 1 回で判定して以降は通信しない)
        self._live_blocked = False
        self._blocked_logged = False
        self._total_estimate = 0

    # ------------------------------------------------------------------ #
    #  parse() — 引数の url を唯一の起点にする                            #
    # ------------------------------------------------------------------ #
    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_shops: set[str] = set()

        top = self._get_live(url)
        if top is not None:
            self.logger.info("本番サイトから取得します: %s", url)
            yield from self._crawl_live(url, top, seen_shops)
            return

        if not self.CC_FALLBACK:
            self.logger.error(
                "本番サイトが WAF に拒否され、Common Crawl フォールバックも無効です: %s", url
            )
            return

        self.logger.warning(
            "本番サイトが Akamai WAF に拒否されました (実行元 IP 起因)。"
            "Common Crawl のアーカイブから取得します。"
        )
        yield from self._crawl_common_crawl(url, seen_shops)

    # ------------------------------------------------------------------ #
    #  [A] ライブ取得                                                     #
    # ------------------------------------------------------------------ #
    def _crawl_live(
        self, base_url: str, top: BeautifulSoup, seen_shops: set[str]
    ) -> Generator[dict, None, None]:
        for code, pref_url in self._collect_pref_search_urls(base_url, top):
            pref_name = _PREF_NAMES[code]
            page_num = 1
            while page_num <= self.MAX_PAGES_PER_PREF:
                list_url = pref_url if page_num == 1 else f"{pref_url}page__{page_num}/"
                soup = self._get_live(list_url)
                if soup is None:
                    break

                if page_num == 1:
                    total = self._extract_total(soup)
                    if total:
                        self._total_estimate += total
                        self.total_items = self._total_estimate
                        self.logger.info("%s: 全 %d 件", pref_name, total)

                cards = self._extract_shop_cards(soup, base_url, seen_shops)
                if not cards:
                    self.logger.info("店舗リンクなし: %s", list_url)
                    break

                for shop_url, list_tel in cards:
                    if self._skip_before_detail(shop_url, list_tel):
                        continue
                    detail = self._get_live(shop_url)
                    if detail is None:
                        continue
                    item = self._parse_detail(
                        detail, shop_url, pref_name, "live", fallback_tel=list_tel
                    )
                    if item:
                        yield item

                if not self._has_next_page(soup, page_num):
                    break
                page_num += 1

    def _get_live(self, url: str) -> BeautifulSoup | None:
        """本番サイトを取得する。WAF 拒否済みなら通信せずに None を返す。"""
        if self._live_blocked:
            return None
        soup = self.get_soup(url)
        if soup is None:
            return None
        if _DENIED_RE.search(soup.get_text(" ", strip=True)[:600]):
            self._live_blocked = True
            if not self._blocked_logged:
                self.logger.warning("Akamai WAF に拒否されました: %s", url)
                self._blocked_logged = True
            return None
        return soup

    def _collect_pref_search_urls(
        self, base_url: str, top: BeautifulSoup
    ) -> list[tuple[str, str]]:
        """/search/f__{JISコード}/ の一覧を (コード, URL) で返す。"""
        found: dict[str, str] = {}
        for soup in (top, self._get_live(urllib.parse.urljoin(base_url, "sitemap.html"))):
            if soup is None:
                continue
            for a in soup.find_all("a", href=True):
                m = _PREF_SEARCH_HREF_RE.search(a["href"].strip())
                if m and m.group(1) in _PREF_NAMES:
                    found.setdefault(
                        m.group(1),
                        urllib.parse.urljoin(base_url, f"/search/f__{m.group(1)}/"),
                    )
            if found:
                break

        if not found:
            # リンクが拾えなくても URL 規則から全国分を生成できる
            self.logger.info("都道府県リンクが無いため 01〜47 を生成します")
            found = {
                code: urllib.parse.urljoin(base_url, f"/search/f__{code}/")
                for code in _PREF_NAMES
            }

        self.logger.info("都道府県検索ページ: %d 件", len(found))
        return sorted(found.items())

    @staticmethod
    def _extract_total(soup: BeautifulSoup) -> int:
        """「1〜20件を表示 ／ 全 140 件」の総件数を返す。"""
        nums = [
            int(t)
            for span in soup.select("span.search_header_result_count")
            for t in [span.get_text(strip=True).replace(",", "")]
            if t.isdigit()
        ]
        return nums[-1] if nums else 0

    def _extract_shop_cards(
        self, soup: BeautifulSoup, base_url: str, seen_shops: set[str]
    ) -> list[tuple[str, str]]:
        """一覧の店舗カードから (店舗詳細URL, カード掲載TEL) を返す。

        一覧カード div.shop_block は li.btn_tel の a[href^="tel:"] に掲載電話番号を
        持つので、MOBILE_ONLY 時は詳細ページを開く前に携帯番号店を選別できる。
        """
        cards: list[tuple[str, str]] = []
        blocks = soup.select("div.shop_block")
        if not blocks:
            # カード構造が変わった場合でも列挙だけは続けられるようにする
            blocks = [soup]
        for block in blocks:
            tel = ""
            tel_a = block.select_one('a[href^="tel:"], a[href^="TEL:"]')
            if tel_a:
                tel = tel_a["href"].split(":", 1)[1].strip()
            for a in block.find_all("a", href=True):
                m = _SHOP_URL_RE.search(a["href"].strip())
                if not m:
                    continue
                shop_url = urllib.parse.urljoin(base_url, f"/shop/{m.group(1)}/")
                if shop_url in seen_shops:
                    continue
                seen_shops.add(shop_url)
                # カード単位なら 1 ブロック 1 店舗。フォールバック時は TEL を紐付けない
                cards.append((shop_url, tel if len(blocks) > 1 else ""))
        return cards

    def _skip_before_detail(self, shop_url: str, list_tel: str) -> bool:
        """MOBILE_ONLY 時、携帯番号でないと確定できる店は詳細を取得しない。"""
        if not self.MOBILE_ONLY:
            return False
        if list_tel:
            # 一覧カードの掲載番号が最も確実な判定材料
            return self._tel_kind(list_tel) != _TEL_KIND_MOBILE
        m = _SHOP_URL_RE.search(shop_url)
        if not m:
            return False
        shop_id = m.group(1)
        # 数字のみの ID は掲載電話番号のハイフン抜き。
        # 英字を含む ID (0X00…/S000…/末尾英字) は番号が別なので必ず詳細を見る。
        if not shop_id.isdigit():
            return False
        return not shop_id.startswith(_MOBILE_PREFIXES)

    @staticmethod
    def _has_next_page(soup: BeautifulSoup, current: int) -> bool:
        for a in soup.find_all("a", href=True):
            m = _PAGE_HREF_RE.search(a["href"].strip())
            if m and int(m.group(1)) > current:
                return True
        return False

    # ------------------------------------------------------------------ #
    #  [B] Common Crawl フォールバック                                    #
    # ------------------------------------------------------------------ #
    def _crawl_common_crawl(
        self, base_url: str, seen_shops: set[str]
    ) -> Generator[dict, None, None]:
        host = urllib.parse.urlsplit(base_url).netloc
        for collection in self._cc_collections():
            for record in self._cc_shop_records(collection, host, seen_shops):
                shop_url = urllib.parse.urljoin(
                    base_url, urllib.parse.urlsplit(record["url"]).path
                )
                if self._skip_before_detail(shop_url, ""):
                    continue
                html = self._cc_fetch_html(record)
                if not html:
                    continue
                item = self._parse_detail(
                    BeautifulSoup(html, "html.parser"), shop_url, "", "commoncrawl"
                )
                if item:
                    yield item

    def _cc_collections(self) -> list[str]:
        """新しい順のクロール ID (CC-MAIN-YYYY-WW) を返す。"""
        data = self._cc_get_json(self.CC_COLLINFO_URL)
        if not isinstance(data, list):
            self.logger.error("Common Crawl のクロール一覧を取得できませんでした")
            return []
        ids = [c["id"] for c in data if isinstance(c, dict) and c.get("id")]
        return ids[: self.CC_MAX_COLLECTIONS]

    def _cc_shop_records(
        self, collection: str, host: str, seen_shops: set[str]
    ) -> list[dict]:
        """1 クロール分の店舗詳細ページ (status 200) のインデックスを返す。"""
        records: list[dict] = []
        # /shop/* だけだと CDX の limit を数字ID店で使い切ることがあるため、
        # 携帯番号店が入る別ID空間 (0X00…/S000…) も個別に問い合わせる。
        for pattern in (f"{host}/shop/*", f"{host}/shop/0X*", f"{host}/shop/S0*"):
            params = {
                "url": pattern,
                "output": "json",
                "filter": "status:200",
                "limit": str(self.CC_INDEX_LIMIT),
            }
            index_url = self.CC_INDEX_URL.format(collection=collection)
            text = self._cc_get_text(f"{index_url}?{urllib.parse.urlencode(params)}")
            for line in text.splitlines():
                line = line.strip()
                if not line.startswith("{"):
                    continue
                try:
                    rec = json.loads(line)
                except ValueError:
                    continue
                url = rec.get("url", "")
                # /shop/{id}/ 直下のみ (map.html / coupon.html 等を除外)
                if not _SHOP_URL_RE.search(urllib.parse.urlsplit(url).path):
                    continue
                if not rec.get("filename"):
                    continue
                if rec.get("mime-detected") not in (None, "text/html"):
                    continue
                key = urllib.parse.urlsplit(url).path.rstrip("/")
                if key in seen_shops:
                    continue
                seen_shops.add(key)
                records.append(rec)

        self.logger.info("%s: 店舗詳細 %d 件", collection, len(records))
        return records

    def _cc_fetch_html(self, record: dict) -> str:
        """WARC の該当レコードだけを Range リクエストで取り出し HTML を返す。"""
        url = self.CC_DATA_URL.format(filename=record["filename"])
        try:
            offset = int(record["offset"])
            length = int(record["length"])
        except (KeyError, TypeError, ValueError):
            return ""

        # Accept-Encoding は identity 必須。gzip を許可すると CloudFront/S3 が
        # SlowDown (503) を返して WARC を取得できない。
        headers = {
            "Range": f"bytes={offset}-{offset + length - 1}",
            "Accept-Encoding": "identity",
        }
        try:
            res = self.session.get(url, headers=headers, timeout=self.TIMEOUT)
            if res.status_code not in (200, 206):
                self.logger.warning(
                    "Common Crawl の WARC 取得に失敗 (%s): %s",
                    res.status_code, record.get("url"),
                )
                return ""
            raw = gzip.decompress(res.content)
        except Exception as exc:  # 通信エラー / gzip 破損はスキップして継続
            self.error_count += 1
            self.logger.warning("Common Crawl 取得エラー %s: %s", record.get("url"), exc)
            return ""

        # WARC ヘッダ → HTTP レスポンスヘッダ → 本文 の順に空行で区切られている
        parts = raw.split(b"\r\n\r\n", 2)
        if len(parts) < 3:
            return ""
        http_header, body = parts[1], parts[2]
        charset = "utf-8"
        m = re.search(rb"charset=([\w-]+)", http_header, re.I)
        if m:
            charset = m.group(1).decode("ascii", "ignore")
        try:
            return body.decode(charset, "replace")
        except LookupError:
            return body.decode("utf-8", "replace")

    def _cc_get_text(self, url: str) -> str:
        try:
            res = self.session.get(url, timeout=self.TIMEOUT)
        except Exception as exc:
            self.error_count += 1
            self.logger.warning("Common Crawl インデックス取得エラー %s: %s", url, exc)
            return ""
        if res.status_code != 200:
            # 該当クロールに 1 件も無い場合は 404、混雑時は 503/504 が返る
            self.logger.info("Common Crawl インデックス応答 %s: %s", res.status_code, url)
            return ""
        return res.text

    def _cc_get_json(self, url: str):
        text = self._cc_get_text(url)
        if not text:
            return None
        try:
            return json.loads(text)
        except ValueError:
            return None

    # ------------------------------------------------------------------ #
    #  店舗詳細ページのパース (ライブ / Common Crawl 共通)                 #
    # ------------------------------------------------------------------ #
    def _parse_detail(
        self,
        soup: BeautifulSoup,
        shop_url: str,
        pref_name: str,
        source: str,
        fallback_tel: str = "",
    ) -> dict | None:
        ld, crumbs = self._parse_jsonld(soup)
        kv = self._extract_dl(soup)

        # ---- 名称 ----
        name = _WS_RE.sub(" ", str(ld.get("name", ""))).strip()
        if not name:
            name = self._name_from_title(soup)
        if not name:
            h2 = soup.find("h2")
            name = h2.get_text(" ", strip=True) if h2 else ""
        if not name:
            self.logger.warning("店名が取得できませんでした: %s", shop_url)
            return None

        # ---- 住所 ----
        addr = _WS_RE.sub(" ", str(ld.get("address", "") or kv.get("住所", ""))).strip()

        # ---- TEL ----
        tel = self._pick_tel(ld, kv, soup) or self._normalize_tel(fallback_tel)
        tel_kind = self._tel_kind(tel)
        if self.MOBILE_ONLY and tel_kind != _TEL_KIND_MOBILE:
            return None

        # ---- 都道府県 (住所先頭から抽出) ----
        pref = ""
        m = _PREF_RE.match(addr) if addr else None
        if m:
            pref = m.group(1)
            addr = addr[m.end():].strip()
        else:
            pref = next((c for c in crumbs if c in _PREF_NAME_SET), "") or pref_name

        return {
            Schema.URL: shop_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            "電話番号種別": tel_kind,
            "取得ソース": source,
        }

    @staticmethod
    def _name_from_title(soup: BeautifulSoup) -> str:
        """<title> "店名(カナ) エリア -BAR-NAVI" から店名を切り出す。"""
        el = soup.find("title")
        if not el:
            return ""
        title = re.sub(r"\s*[-－—]\s*BAR-NAVI\s*$", "", el.get_text(strip=True)).strip()
        m = re.match(r"^(.+?)[（(][^）)]+[）)]", title)
        if m:
            return m.group(1).strip()
        # カナが無い場合は末尾のエリア名しか区切りが無いので全体を返す
        return title.split(" ")[0].strip() if " " in title else title

    def _pick_tel(self, ld: dict, kv: dict, soup: BeautifulSoup) -> str:
        """JSON-LD → dl「電話」→ a[href^=tel:] の順に掲載電話番号を探す。"""
        candidates = [str(ld.get("telephone", "") or ""), kv.get("電話", "")]
        candidates += [
            a["href"].split(":", 1)[1]
            for a in soup.find_all("a", href=True)
            if a["href"].lower().startswith("tel:")
        ]
        for cand in candidates:
            normalized = self._normalize_tel(cand)
            if normalized:
                return normalized
        return ""

    @staticmethod
    def _normalize_tel(raw: str) -> str:
        m = _TEL_RE.search(raw or "")
        return re.sub(r"[^\d-]", "-", m.group(0)).strip("-") if m else ""

    @staticmethod
    def _tel_kind(tel: str) -> str:
        digits = re.sub(r"\D", "", tel)
        if not digits:
            return ""
        if digits.startswith(_MOBILE_PREFIXES):
            return _TEL_KIND_MOBILE
        if digits.startswith(_TOLLFREE_PREFIXES):
            return _TEL_KIND_TOLLFREE
        return _TEL_KIND_FIXED

    # ---- JSON-LD (BarOrPub / BreadcrumbList) ----
    @staticmethod
    def _parse_jsonld(soup: BeautifulSoup) -> tuple[dict, list[str]]:
        """店舗情報 dict とパンくず名リストを返す。"""
        shop: dict = {}
        crumbs: list[str] = []
        for script in soup.find_all("script", type="application/ld+json"):
            raw = script.string or script.get_text()
            if not raw:
                continue
            try:
                parsed = json.loads(raw)
            except ValueError:
                continue
            for node in parsed if isinstance(parsed, list) else [parsed]:
                if not isinstance(node, dict):
                    continue
                if node.get("@type") == "BreadcrumbList":
                    for el in node.get("itemListElement", []) or []:
                        item = el.get("item", {}) if isinstance(el, dict) else {}
                        if isinstance(item, dict) and item.get("name"):
                            crumbs.append(str(item["name"]))
                elif not shop and node.get("name"):
                    shop = node
        return shop, crumbs

    # ---- dl/dt-dd (「住所」「電話」の補完用) ----
    @staticmethod
    def _extract_dl(soup: BeautifulSoup) -> dict[str, str]:
        kv: dict[str, str] = {}
        for dl in soup.find_all("dl"):
            dts, dds = dl.find_all("dt"), dl.find_all("dd")
            for i, dt in enumerate(dts):
                if i >= len(dds):
                    break
                key = _WS_RE.sub("", dt.get_text(strip=True)).rstrip("：:")
                val = _WS_RE.sub(" ", dds[i].get_text(" ", strip=True)).strip()
                if key in ("住所", "所在地", "電話", "電話番号", "TEL") and val:
                    kv.setdefault("住所" if key in ("住所", "所在地") else "電話", val)
        return kv


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BarNavi2Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://bar-navi.suntory.co.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
