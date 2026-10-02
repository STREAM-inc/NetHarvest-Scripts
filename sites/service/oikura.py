"""
おいくら (oikura) — リサイクルショップ検索 (1都3県) スクレイパー

取得対象:
    おいくら (oikura.jp) の都道府県別リサイクルショップ一覧のうち、
    東京都 (pref13) / 神奈川県 (pref14) / 埼玉県 (pref11) / 千葉県 (pref12)
    の 4 都県に掲載された加盟ショップ (調査時点で合計約 480 件)。
    店名 / 電話番号 / 住所 / 都道府県 / 市区町村 / 買取形式 (出張・店頭・宅配) /
    取扱ジャンル / 営業時間 / 定休日 / ホームページ / 担当者名 / 駐車場 /
    古物商許可番号 / 特徴 / 口コミ件数・評価 / 買取実績件数 /
    掲載ページ URL を取得する。

取得フロー:
    1. ルート URL (= sites.yml の url / 東京都 pref13.html) から都道府県コードを
       読み取り、備考で指定された 4 都県 (13/14/11/12) の一覧 URL を
       urljoin で派生させる。ルート URL で指定された都県を必ず先頭に置く。
    2. 各都県の一覧ページ (1ページ20件) の div.shopBox から
       店名 / 掲載ページURL / 取扱ジャンル / 買取形式 (li.on) / 住所 /
       電話番号 / 口コミ件数・評価 / 買取実績件数 を取得する。
       ページ送りは <link rel="next"> を辿る (pref{N}_{P}.html 形式)。
       rel=next が無い場合のみ連番 URL で補完する。
    3. 各ショップの掲載ページ (/shop/{id}/) の table.detailTbl から
       所在地 / 交通アクセス / 駐車場 / 担当者名 / 営業時間 / 定休日 /
       ホームページ / 買取方法 / 古物商許可番号 / 特徴 を取得し、
       パンくず (JSON-LD BreadcrumbList) から都道府県・市区町村を補完する。
    4. 1 件組み立てるたびに即 yield する (Pattern B / 早期 yield)。

備考 (依頼元の指示):
    - 巡回対象は東京都 (pref13) / 神奈川県 (pref14) / 埼玉県 (pref11) /
      千葉県 (pref12) の 4 都県のみ。他県の一覧は巡回しない。
    - 買取形式は一覧 dd 内 li の class="on" の有無で判定する
      (on が付いているものだけを採用)。
    - 1ページ20件、2ページ目以降は pref{N}_{P}.html の連番。

注意:
    - 利用規約 (https://oikura.jp/guide/kiyaku.html) にスクレイピング・クローリングを
      禁止する条項は無い。robots.txt も /shop/ 配下を Allow している。
    - 自由記述の文章 (出張/店頭/宅配コメント、得意アイテム、苦手アイテム、
      キャッチコピー、主な取扱商品) は著作権リスク回避のため取得しない。
    - 郵便番号・法人番号・代表者名はサイト上に掲載が無いため取得しない
      (「担当者名」は代表者ではなく窓口担当のため EXTRA カラムに格納する)。
    - ルート URL は引数 `url` を唯一の起点 (SSOT) とし、配下 URL はすべて
      urljoin(url, ...) で派生させる。

実行方法:
    # ローカルテスト
    python scripts/sites/service/oikura.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id oikura
"""

import json
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


# 備考で指定された巡回対象 (都道府県コード → 名称)
_TARGET_PREFS = {
    "13": "東京都",
    "14": "神奈川県",
    "11": "埼玉県",
    "12": "千葉県",
}

# ルート URL から都道府県コードを読み取る (例: /shop/pref13.html)
_PREF_URL_PATTERN = re.compile(r"pref(\d+)(?:_(\d+))?\.html")

# 住所先頭から都道府県を切り出す
_PREFS = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県",
    "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]
_PREF_PATTERN = re.compile("^(" + "|".join(_PREFS) + ")")

# 一覧見出しの件数 (例: 東京都のリサイクルショップ 171件 (1~20件))
_COUNT_PATTERN = re.compile(r"([\d,]+)\s*件")
# 掲載ページ URL からショップ ID
_SHOP_ID_PATTERN = re.compile(r"/shop/(\d+)/")

# 掲載ページ table.detailTbl のラベル → 内部キー
_DETAIL_LABELS = {
    "所在地": "addr",
    "交通アクセス": "access",
    "駐車場": "parking",
    "担当者名": "staff",
    "営業時間": "time",
    "定休日": "holiday",
    "ホームページ": "hp",
    "買取方法": "method",
    "古物商許可番号": "license",
    "特徴": "feature",
}

# サイト固有の構造化カラム (短いラベル / 数値 / URL のみ。自由記述は含めない)
_COL_SHOP_ID = "ショップID"
_COL_CITY = "市区町村"
_COL_STYLE = "買取形式"
_COL_VISIT = "出張買取"
_COL_STORE = "店頭買取"
_COL_DELIVERY = "宅配買取"
_COL_METHOD = "買取方法"
_COL_FEATURE = "特徴"
_COL_STAFF = "担当者名"
_COL_PARKING = "駐車場"
_COL_ACCESS = "交通アクセス"
_COL_LICENSE = "古物商許可番号"
_COL_BOUGHT = "買取実績件数"

# 安全弁: 1 都県あたりのページ送り上限 (無限ループ防止)
_MAX_PAGES = 100
# 値が未入力のときにサイトが入れるプレースホルダ
_EMPTY_VALUES = {"", "-", "ー", "−", "–"}


class Oikura(StaticCrawler):
    """おいくら リサイクルショップ検索 (1都3県) スクレイパー"""

    DELAY = 1.0

    EXTRA_COLUMNS = [
        _COL_SHOP_ID,
        _COL_CITY,
        _COL_STYLE,
        _COL_VISIT,
        _COL_STORE,
        _COL_DELIVERY,
        _COL_METHOD,
        _COL_FEATURE,
        _COL_STAFF,
        _COL_PARKING,
        _COL_ACCESS,
        _COL_LICENSE,
        _COL_BOUGHT,
    ]

    # ------------------------------------------------------------------ 本体
    def parse(self, url: str):
        seen_shops = set()
        total = 0

        for list_url in self._pref_urls(url):
            page_url = list_url
            page = 1

            while page_url and page <= _MAX_PAGES:
                soup = self.get_soup(page_url)
                if soup is None:
                    break

                if page == 1:
                    count = self._detect_count(soup)
                    if count:
                        total += count
                        self.total_items = total

                boxes = soup.select("div.shopBox")
                if not boxes:
                    break

                for box in boxes:
                    listing = self._parse_box(box, page_url)
                    if not listing:
                        continue
                    shop_url = listing[Schema.URL]
                    if shop_url in seen_shops:
                        continue
                    seen_shops.add(shop_url)

                    try:
                        item = self._build_item(listing)
                    except Exception as e:  # 個別ショップの失敗は継続
                        self.logger.warning("ショップ取得失敗 %s — %s", shop_url, e)
                        continue
                    if item:
                        yield item

                page_url = self._next_page_url(soup, page_url, page)
                page += 1

    # -------------------------------------------------------------- URL 派生
    def _pref_urls(self, url: str) -> list[str]:
        """ルート URL から巡回対象 4 都県の一覧 URL を派生させる。

        ルート URL が指す都県を必ず先頭に置き、残りを備考の指定順で続ける。
        """
        m = _PREF_URL_PATTERN.search(url)
        root_code = m.group(1) if m else None

        codes = []
        if root_code in _TARGET_PREFS:
            codes.append(root_code)
        for code in _TARGET_PREFS:
            if code not in codes:
                codes.append(code)

        urls = []
        for code in codes:
            if code == root_code:
                # ルート URL (sites.yml の url) はそのまま使う
                urls.append(url)
            else:
                urls.append(urljoin(url, f"pref{code}.html"))
        return urls

    def _next_page_url(self, soup, page_url: str, page: int) -> str | None:
        """<link rel="next"> を辿る。無ければ連番 URL で補完する。"""
        link = soup.find("link", rel="next")
        if link and link.get("href"):
            return urljoin(page_url, link["href"])

        # rel=next が無い場合のフォールバック: pref{N}_{P}.html
        m = _PREF_URL_PATTERN.search(page_url)
        if not m:
            return None
        # 1 ページ目で rel=next が無い = 1 ページのみ → 終了
        boxes = soup.select("div.shopBox")
        if len(boxes) < 20:
            return None
        return urljoin(page_url, f"pref{m.group(1)}_{page + 1}.html")

    # ------------------------------------------------------------------ 一覧
    def _detect_count(self, soup) -> int | None:
        """見出し「○○都のリサイクルショップ 171件 (1~20件)」から総件数を取得。"""
        for h2 in soup.find_all("h2"):
            text = h2.get_text(" ", strip=True)
            if "リサイクルショップ" not in text:
                continue
            m = _COUNT_PATTERN.search(text)
            if m:
                return int(m.group(1).replace(",", ""))
        return None

    def _parse_box(self, box, page_url: str) -> dict | None:
        """一覧の div.shopBox から 1 ショップ分の情報を取得する。"""
        link = box.select_one("div.shop_name h3 a[href]")
        if not link:
            return None
        name = self._clean(link.get_text(" ", strip=True))
        if not name:
            return None

        shop_url = urljoin(page_url, link["href"])
        listing = {Schema.URL: shop_url, Schema.NAME: name}

        m = _SHOP_ID_PATTERN.search(shop_url)
        listing[_COL_SHOP_ID] = m.group(1) if m else ""

        # 取扱ジャンル (例: 総合リサイクル / ブランド・ジュエリー・貴金属)
        genre = box.select_one("div.shop_name dl dt")
        listing[Schema.CAT_SITE] = self._clean(genre.get_text(" ", strip=True)) if genre else ""

        # 買取形式: class="on" が付いた li のみ採用 (「出 張」等の空白を除去)
        styles = []
        for li in box.select("div.shop_name dl dd li"):
            if "on" not in (li.get("class") or []):
                continue
            label = re.sub(r"\s+", "", li.get_text())
            if label:
                styles.append(label)
        listing[_COL_STYLE] = "/".join(styles)
        listing[_COL_VISIT] = "○" if "出張" in styles else ""
        listing[_COL_STORE] = "○" if "店頭" in styles else ""
        listing[_COL_DELIVERY] = "○" if "宅配" in styles else ""

        # 住所 (「ショップ住所 ： 東京都…」から値だけを切り出す)
        for p in box.select("div.address p"):
            span = p.find("span")
            if not span:
                continue
            label = span.get_text(strip=True)
            value = self._clean(p.get_text(" ", strip=True).replace(label, "", 1).lstrip("：: "))
            value = "" if value in _EMPTY_VALUES else value
            if label == "ショップ住所":
                listing[Schema.ADDR] = value
            elif label == "交通アクセス":
                listing[_COL_ACCESS] = value

        # 電話番号
        tel = box.select_one("div.number-phone .number-phone__content span")
        listing[Schema.TEL] = self._clean(tel.get_text(" ", strip=True)) if tel else ""

        # 口コミ件数 / 評価 (1つ目の p が件数、2つ目の p の末尾 a が評価)
        paragraphs = box.select("div.kuchikomi p")
        if paragraphs:
            a = paragraphs[0].find("a")
            listing[Schema.REV_SCR] = self._clean(a.get_text(strip=True)) if a else ""
        if len(paragraphs) > 1:
            links = paragraphs[1].find_all("a")
            listing[Schema.SCORES] = self._clean(links[-1].get_text(strip=True)) if links else ""

        # 買取実績件数
        results = box.select_one("div.results a")
        listing[_COL_BOUGHT] = self._clean(results.get_text(strip=True)) if results else ""

        return listing

    # -------------------------------------------------------------- 掲載ページ
    def _build_item(self, listing: dict) -> dict:
        """一覧の情報に掲載ページの詳細情報をマージして 1 レコードを作る。"""
        item = {
            Schema.URL: listing[Schema.URL],
            Schema.NAME: listing[Schema.NAME],
            Schema.PREF: "",
            Schema.ADDR: listing.get(Schema.ADDR, ""),
            Schema.TEL: listing.get(Schema.TEL, ""),
            Schema.CAT_SITE: listing.get(Schema.CAT_SITE, ""),
            Schema.HP: "",
            Schema.TIME: "",
            Schema.HOLIDAY: "",
            Schema.REV_SCR: listing.get(Schema.REV_SCR, ""),
            Schema.SCORES: listing.get(Schema.SCORES, ""),
            _COL_SHOP_ID: listing.get(_COL_SHOP_ID, ""),
            _COL_CITY: "",
            _COL_STYLE: listing.get(_COL_STYLE, ""),
            _COL_VISIT: listing.get(_COL_VISIT, ""),
            _COL_STORE: listing.get(_COL_STORE, ""),
            _COL_DELIVERY: listing.get(_COL_DELIVERY, ""),
            _COL_METHOD: "",
            _COL_FEATURE: "",
            _COL_STAFF: "",
            _COL_PARKING: "",
            _COL_ACCESS: listing.get(_COL_ACCESS, ""),
            _COL_LICENSE: "",
            _COL_BOUGHT: listing.get(_COL_BOUGHT, ""),
        }

        detail = self._scrape_detail(listing[Schema.URL])
        if detail:
            if detail.get("addr"):
                item[Schema.ADDR] = detail["addr"]
            if "access" in detail:
                item[_COL_ACCESS] = detail["access"]
            item[Schema.HP] = detail.get("hp", "")
            item[Schema.TIME] = detail.get("time", "")
            item[Schema.HOLIDAY] = detail.get("holiday", "")
            item[_COL_METHOD] = detail.get("method", "")
            item[_COL_FEATURE] = detail.get("feature", "")
            item[_COL_STAFF] = detail.get("staff", "")
            item[_COL_PARKING] = detail.get("parking", "")
            item[_COL_LICENSE] = detail.get("license", "")
            if detail.get("pref"):
                item[Schema.PREF] = detail["pref"]
            if detail.get("city"):
                item[_COL_CITY] = detail["city"]

        # 都道府県: パンくずで取れなければ住所先頭から導出する
        if not item[Schema.PREF]:
            m = _PREF_PATTERN.match(item[Schema.ADDR])
            item[Schema.PREF] = m.group(1) if m else ""

        return item

    def _scrape_detail(self, shop_url: str) -> dict:
        """掲載ページ (/shop/{id}/) から構造化された店舗情報を取得する。"""
        soup = self.get_soup(shop_url)
        if soup is None:
            return {}

        detail = {}
        table = soup.select_one("table.detailTbl")
        if table:
            for tr in table.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                key = _DETAIL_LABELS.get(self._clean(th.get_text(" ", strip=True)))
                if not key:
                    continue
                if key == "hp":
                    a = td.find("a", href=True)
                    value = self._clean(a["href"] if a else td.get_text(" ", strip=True))
                else:
                    value = self._clean(td.get_text(" ", strip=True))
                detail[key] = "" if value in _EMPTY_VALUES else value

        detail.update(self._parse_breadcrumb(soup))
        return detail

    def _parse_breadcrumb(self, soup) -> dict:
        """JSON-LD BreadcrumbList から都道府県 (position 3) と市区町村 (position 4)。"""
        for script in soup.find_all("script", type="application/ld+json"):
            raw = script.string or script.get_text()
            if not raw or "BreadcrumbList" not in raw:
                continue
            try:
                data = json.loads(raw)
            except ValueError:
                continue
            if data.get("@type") != "BreadcrumbList":
                continue
            names = {
                elem.get("position"): self._clean(elem.get("name", ""))
                for elem in data.get("itemListElement", [])
            }
            result = {}
            pref = names.get(3, "")
            if pref in _PREFS:
                result["pref"] = pref
            city = names.get(4, "")
            if city and city not in _PREFS:
                result["city"] = city
            return result
        return {}

    # ------------------------------------------------------------------ 共通
    @staticmethod
    def _clean(text: str) -> str:
        """NBSP・連続空白を正規化する。"""
        if not text:
            return ""
        return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


if __name__ == "__main__":
    scraper = Oikura()
    scraper.execute("https://oikura.jp/shop/pref13.html")
