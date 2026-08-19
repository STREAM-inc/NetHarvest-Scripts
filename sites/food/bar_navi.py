"""
バーナビ (BAR-NAVI) — サントリー公式バー検索サイト (日本最大級)

取得対象:
    - 全国のバー・酒場情報
    - 店名 / 都道府県 / 住所 / TEL / 営業時間 / 定休日 / 説明 /
      エリア / 席数 / 予算目安 / アクセス / 喫煙 / キャッチコピー

取得フロー (2 系統。実行元 IP が本番サイトに通るかで自動的に切り替わる):

    [A] ライブ取得 (WAF に通る IP の場合)
        1. {url} / {url}sitemap.html から都道府県検索ページ
           /search/f__{JISコード}/ を収集 (取れない場合は 01〜47 を生成)
        2. /search/f__{NN}/page__{N}/ でページ送り (パス形式。?page=N は無効)
        3. 店舗詳細 /shop/{id}/ を取得して即 yield

    [B] Common Crawl フォールバック (WAF に 403 拒否される IP の場合)
        1. https://index.commoncrawl.org/collinfo.json で最新クロールを列挙
        2. 各クロールの CDX インデックスに
           url={host}/shop/* を投げ、status 200 の店舗詳細ページを列挙
        3. data.commoncrawl.org へ Range リクエストを投げて WARC レコードを
           1 件だけ取り出し、gzip 展開した HTML を [A] と同じパーサに流す

サイト構造の注意点 (2026-08 調査):
    - 店舗詳細は JSON-LD (BarOrPub) が主ソース。name / address / telephone /
      openingHours / priceRange / maximumAttendeeCapacity / description が入る。
      dl(dt/dd) にも「住所 / 電話 / 営業時間 / 定休日 / 予算目安 / 客席数 /
      喫煙区分 / アクセス」があるので補完に使う。
    - <h1> はサイト名 (「バー検索サイト[BAR-NAVI]」) なので店名には使えない。
      <title> は「店名 エリア -BAR-NAVI」形式で、店名を除いた残りがエリア。
    - servesCuisine は業種ではなく店のキャッチコピー。よって CAT_SITE には
      入れず EXTRA_COLUMNS「キャッチコピー」に格納する
      (詳細ページに業態・ジャンル項目は存在しない)。
    - 都道府県は住所先頭、無い場合は JSON-LD BreadcrumbList の 2 階層目。
    - /{pref}/ (例 /tokyo/) はカテゴリ入口で店舗リンクを含まないため一覧に使えない。
    - ページ送りはクエリ (?page=N) ではなくパス (page__N/)。

アクセス制限 (2026-08 時点、本コンテナからの実測):
    - 本番サイト: Akamai WAF がデータセンター (AWS) の IP を全パスで 403 拒否。
      requests / Playwright (実 Chromium) いずれも "Access Denied" で
      突破不可 = IP 起因。JS チャレンジではないので Playwright は無意味。
      よって基底クラスは DynamicCrawler ではなく StaticCrawler を使う
      (WAF を通る IP なら静的 HTML に全データが入っているため十分)。
    - Wayback Machine: web.archive.org は AWS からのアクセスを
      429 Too Many Requests で拒否 (x-nid: AMAZON-02 / x-rl: 0) するため
      フォールバック先として使えない。代わりに Common Crawl を使用する
      (index.commoncrawl.org / data.commoncrawl.org は AWS から取得可)。
    - Common Crawl のカバー範囲はサイト全体ではなくクロール済みページのみ。
      1 クロールあたり数十〜百件程度なので、CC_MAX_COLLECTIONS 本を巡回して
      店舗 ID で重複排除する。取得元は EXTRA_COLUMNS「取得ソース」に記録する。

実行方法:
    # ローカルテスト
    python scripts/sites/food/bar_navi.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id bar_navi
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
from src.framework.static import StaticCrawler
from src.const.schema import Schema


# 都道府県検索ページ: /search/f__13/ (13 = JIS 都道府県コード)
_PREF_SEARCH_HREF_RE = re.compile(r"/search/f__(\d{2})/?$")
# 店舗詳細ページ (トップ): /shop/0355685818/, /shop/S000006676/
# ※ /shop/{id}/food.html などのサブページは除外する
_SHOP_URL_RE = re.compile(r"/shop/([0-9A-Za-z_-]+)/?$")
# ページ送り: /search/f__13/page__2/
_PAGE_HREF_RE = re.compile(r"/page__(\d+)/?$")

_TEL_RE = re.compile(r"0\d{1,4}[-(）)−]?\d{1,4}[-(）)−]?\d{3,4}")

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
_WS_RE = re.compile(r"\s+")

# WAF ブロック時に返る Akamai のエラーページ
_DENIED_RE = re.compile(r"Access Denied|errors\.edgesuite\.net", re.I)

# HP として採用しない URL (自サイト / SNS / 電話・メールリンク)
_HP_EXCLUDE_RE = re.compile(
    r"^(tel:|mailto:|javascript:|#|https?://[^/]*suntory\.co\.jp|"
    r"https?://([^/]*\.)?(web\.archive\.org|commoncrawl\.org|line\.me|"
    r"instagram\.com|twitter\.com|x\.com|facebook\.com|tiktok\.com|"
    r"google\.[^/]+|goo\.gl|doubleclick\.net|yahoo\.co\.jp|addthis\.com))"
)


class BarNaviScraper(StaticCrawler):
    """バーナビ (BAR-NAVI) スクレイパー"""

    DELAY = 0.5
    TIMEOUT = 25
    EXTRA_COLUMNS = [
        "エリア", "席数", "予算目安", "アクセス", "喫煙", "キャッチコピー", "取得ソース",
    ]

    # ---- ライブ取得の設定 ----
    # 1 都道府県あたりの一覧ページ上限 (東京で 90 ページ程度)
    MAX_PAGES_PER_PREF = 200

    # ---- Common Crawl フォールバックの設定 ----
    CC_FALLBACK = True
    CC_COLLINFO_URL = "https://index.commoncrawl.org/collinfo.json"
    CC_INDEX_URL = "https://index.commoncrawl.org/{collection}-index"
    CC_DATA_URL = "https://data.commoncrawl.org/{filename}"
    # 巡回する最新クロールの本数 (1 本あたり店舗詳細 80 件前後)
    CC_MAX_COLLECTIONS = 12
    # CDX インデックス 1 回あたりの取得上限 (店舗サブページ込みの生件数)
    CC_INDEX_LIMIT = 2000

    # 詳細ページのラベル → Schema 定数 / EXTRA_COLUMNS 名
    _FIELD_MAP: dict[str, str] = {
        "住所": Schema.ADDR,
        "所在地": Schema.ADDR,
        "TEL": Schema.TEL,
        "電話": Schema.TEL,
        "電話番号": Schema.TEL,
        "営業時間": Schema.TIME,
        "定休日": Schema.HOLIDAY,
        "ジャンル": Schema.CAT_SITE,
        "業態": Schema.CAT_SITE,
        "ホームページ": Schema.HP,
        "HP": Schema.HP,
        "URL": Schema.HP,
        "エリア": "エリア",
        "席数": "席数",
        "客席数": "席数",
        "収容人数": "席数",
        "予算": "予算目安",
        "予算目安": "予算目安",
        "アクセス": "アクセス",
        "交通手段": "アクセス",
        "喫煙": "喫煙",
        "喫煙区分": "喫煙",
    }

    def prepare(self):
        # ライブ取得が WAF で拒否されたかどうか (最初の 1 回で判定)
        self._live_blocked = False
        self._blocked_logged = False

    # ---------------------------------------------------------- #
    #  parse() — 引数の url を唯一の起点にする                     #
    # ---------------------------------------------------------- #
    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_shops: set[str] = set()

        # まず本番サイトを試す。403 (Akamai) なら Common Crawl へ切り替える。
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

    # ---------------------------------------------------------- #
    #  [A] ライブ取得                                             #
    # ---------------------------------------------------------- #
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

                shop_urls = self._extract_shop_urls(soup, base_url, seen_shops)
                if not shop_urls:
                    self.logger.info("店舗リンクなし: %s", list_url)
                    break

                for shop_url in shop_urls:
                    detail = self._get_live(shop_url)
                    if detail is None:
                        continue
                    item = self._parse_detail(detail, shop_url, pref_name, "live")
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

    def _extract_shop_urls(
        self, soup: BeautifulSoup, base_url: str, seen_shops: set[str]
    ) -> list[str]:
        urls: list[str] = []
        for a in soup.find_all("a", href=True):
            m = _SHOP_URL_RE.search(a["href"].strip())
            if not m:
                continue
            shop_url = urllib.parse.urljoin(base_url, f"/shop/{m.group(1)}/")
            if shop_url in seen_shops:
                continue
            seen_shops.add(shop_url)
            urls.append(shop_url)
        return urls

    def _has_next_page(self, soup: BeautifulSoup, current: int) -> bool:
        for a in soup.find_all("a", href=True):
            m = _PAGE_HREF_RE.search(a["href"].strip())
            if m and int(m.group(1)) > current:
                return True
        return False

    # ---------------------------------------------------------- #
    #  [B] Common Crawl フォールバック                            #
    # ---------------------------------------------------------- #
    def _crawl_common_crawl(
        self, base_url: str, seen_shops: set[str]
    ) -> Generator[dict, None, None]:
        host = urllib.parse.urlsplit(base_url).netloc
        for collection in self._cc_collections():
            records = self._cc_shop_records(collection, host, seen_shops)
            for record in records:
                html = self._cc_fetch_html(record)
                if not html:
                    continue
                shop_url = urllib.parse.urljoin(base_url, urllib.parse.urlsplit(record["url"]).path)
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
        params = {
            "url": f"{host}/shop/*",
            "output": "json",
            "filter": "status:200",
            "limit": str(self.CC_INDEX_LIMIT),
        }
        index_url = self.CC_INDEX_URL.format(collection=collection)
        text = self._cc_get_text(f"{index_url}?{urllib.parse.urlencode(params)}")
        if not text:
            return []

        records: list[dict] = []
        for line in text.splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            url = rec.get("url", "")
            # /shop/{id}/ 直下のみ (food.html / coupon.html 等のサブページを除外)
            if not _SHOP_URL_RE.search(urllib.parse.urlsplit(url).path):
                continue
            if not rec.get("filename") or rec.get("mime-detected") not in (
                None, "text/html",
            ):
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
                    "Common Crawl の WARC 取得に失敗 (%s): %s", res.status_code, record["url"]
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
            self.logger.info(
                "Common Crawl インデックス応答 %s: %s", res.status_code, url
            )
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

    # ---------------------------------------------------------- #
    #  店舗詳細ページのパース (ライブ / Common Crawl 共通)         #
    # ---------------------------------------------------------- #
    def _parse_detail(
        self, soup: BeautifulSoup, shop_url: str, pref_name: str, source: str
    ) -> dict | None:
        ld, crumbs = self._parse_jsonld(soup)

        data: dict = {Schema.URL: shop_url}

        # ---- 店名 / エリア ----
        # <h1> はサイト名なので使わない。JSON-LD の name と
        # <title> "店名 エリア -BAR-NAVI" (店名(カナ) 形式の店もある) を併用する。
        title_el = soup.find("title")
        title = title_el.get_text(strip=True) if title_el else ""
        title = re.sub(r"\s*[-－—]\s*BAR-NAVI\s*$", "", title).strip()

        name = _WS_RE.sub(" ", str(ld.get("name", ""))).strip()
        if not name:
            m = re.match(r"^(.+?)[（(]([^）)]+)[）)]\s*(.*)$", title)
            name = (m.group(1) if m else title).strip()
        if not name:
            return None
        data[Schema.NAME] = name

        area = title
        m = re.match(r"^(.+?)[（(]([^）)]+)[）)]\s*(.*)$", title)
        if m and m.group(1).strip() == name:
            data[Schema.NAME_KANA] = m.group(2).strip()
            area = m.group(3)
        elif title.startswith(name):
            area = title[len(name):]
        if area.strip() and area.strip() != title:
            data["エリア"] = area.strip()

        # ---- JSON-LD (BarOrPub) を主ソースにする ----
        for key, col in (
            ("address", Schema.ADDR),
            ("telephone", Schema.TEL),
            ("openingHours", Schema.TIME),
            ("priceRange", "予算目安"),
            ("description", Schema.DESCRIPTION),
            ("servesCuisine", "キャッチコピー"),
        ):
            val = ld.get(key)
            if isinstance(val, list):
                val = " ".join(str(v) for v in val)
            if val:
                data[col] = _WS_RE.sub(" ", str(val)).strip()
        seats = ld.get("maximumAttendeeCapacity")
        if seats:
            data["席数"] = f"{seats}席"

        # ---- dl / table のラベル・値で補完 ----
        kv: dict[str, str] = {}
        self._extract_dl(soup, kv)
        self._extract_table(soup, kv)
        for raw_key, val in kv.items():
            col = self._FIELD_MAP.get(raw_key)
            if col and val:
                data.setdefault(col, val)

        # ---- TEL ----
        tel = data.get(Schema.TEL, "")
        m = _TEL_RE.search(tel)
        if m:
            data[Schema.TEL] = re.sub(r"[^\d-]", "", m.group(0)).strip("-")
        else:
            data.pop(Schema.TEL, None)

        # ---- 住所から都道府県を分離 ----
        addr = data.get(Schema.ADDR, "")
        pm = _PREF_RE.match(addr) if addr else None
        if pm:
            data[Schema.PREF] = pm.group(1)
            data[Schema.ADDR] = addr[pm.end():].strip()
        else:
            # パンくず (2 階層目が都道府県) → 巡回元の都道府県 の順で補完
            crumb_pref = next((c for c in crumbs if c in _PREF_NAME_SET), "")
            if crumb_pref or pref_name:
                data[Schema.PREF] = crumb_pref or pref_name

        # ---- HP (ラベル優先、無ければ外部リンクから補完) ----
        if not data.get(Schema.HP, "").startswith("http"):
            data.pop(Schema.HP, None)
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                if href.startswith("http") and not _HP_EXCLUDE_RE.match(href):
                    data[Schema.HP] = href
                    break

        data["取得ソース"] = source
        return data

    # ---- JSON-LD (BarOrPub / BreadcrumbList) ----
    def _parse_jsonld(self, soup: BeautifulSoup) -> tuple[dict, list[str]]:
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
                    for el in node.get("itemListElement", []):
                        item = el.get("item", {}) if isinstance(el, dict) else {}
                        if isinstance(item, dict) and item.get("name"):
                            crumbs.append(str(item["name"]))
                elif not shop and node.get("name"):
                    shop = node
        return shop, crumbs

    # ---- dl/dt-dd ----
    def _extract_dl(self, soup: BeautifulSoup, kv: dict) -> None:
        for dl in soup.find_all("dl"):
            dts, dds = dl.find_all("dt"), dl.find_all("dd")
            for i, dt in enumerate(dts):
                if i >= len(dds):
                    break
                key = _WS_RE.sub("", dt.get_text(strip=True)).rstrip("：:")
                val = _WS_RE.sub(" ", dds[i].get_text(" ", strip=True)).strip()
                if key and val:
                    kv.setdefault(key, val)

    # ---- table/th-td ----
    def _extract_table(self, soup: BeautifulSoup, kv: dict) -> None:
        for table in soup.find_all("table"):
            for tr in table.find_all("tr"):
                th, td = tr.find("th"), tr.find("td")
                if th and td:
                    key = _WS_RE.sub("", th.get_text(strip=True)).rstrip("：:")
                    val = _WS_RE.sub(" ", td.get_text(" ", strip=True)).strip()
                    if key and val:
                        kv.setdefault(key, val)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = BarNaviScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://bar-navi.suntory.co.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
