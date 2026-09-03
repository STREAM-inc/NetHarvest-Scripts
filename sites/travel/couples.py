"""
カップルズ (couples.jp) — 全国ラブホテル・レジャーホテル検索サイト

取得対象:
    全国 (沖縄県を除く) のラブホテル/レジャーホテルのうち、
    **時間貸し料金表記 (ご休憩 / フリータイム / サービスタイム / 深夜料金) があるもの** のみ。
    宿泊料金しか掲載していないホテル、料金表そのものが無いホテルは parse() 内で除外する。

取得フロー:
    /prefectures/via-hotelareas                都道府県一覧 (47件・エリア検索トップ)
      → /prefectures/{pref_id}/hotelareas      都道府県別のホテルエリア一覧
        → /hotels/search-by/hotelareas/{id}    ホテルエリア別一覧 (30件/ページ・?page=N)
          → /hotel-details/{hotel_id}          施設詳細 (基本情報テーブル + 利用料金テーブル)

    全ページが SSR の静的 HTML なので StaticCrawler (requests) で取得できる。
    pref_id は JIS コードではなくサイト独自採番 (東京=8, 沖縄=47)。

利用規約 (https://couples.jp/user-terms) 確認結果:
    スクレイピング/クローリングを明示的に禁止する条項は無い。
    ただし第4条(禁止事項)に「本サービスのネットワークまたはシステム等に過度な負荷を
    かける行為」があるため、DELAY = 1.2 秒 (1秒以上) を厳守する。

除外方針 (著作権配慮):
    アクセス案内・特典・ホテル紹介文・口コミ本文などの自由記述文は一切保存しない。
    料金は「5000円～11500円」のような **金額表記だけ** を抽出し、注意書き (※〜) や
    説明文は落とす。設備・駐車場も「24室」「3台」のような数値のみを保存する。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/couples.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id couples
"""

import json
import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 正規表現
# ---------------------------------------------------------------------------
# 都道府県ページ  /prefectures/8/hotelareas
_PREF_HREF_RE = re.compile(r"^/prefectures/(\d+)/hotelareas$")
# ホテルエリア一覧  /hotels/search-by/hotelareas/194
_AREA_HREF_RE = re.compile(r"^/hotels/search-by/hotelareas/(\d+)(?:\?|$)")
# 施設詳細  /hotel-details/1009  (…/review, …/coupon は除外するため完全一致)
_DETAIL_HREF_RE = re.compile(r"^/hotel-details/(\d+)(?:\?|$)")

# 「5000円～11500円」「7800～18950円」のような金額レンジ / 単独金額
_PRICE_RE = re.compile(
    r"[\d,]+\s*円?\s*[～~〜ー－-]\s*[\d,]+\s*円"  # レンジ表記
    r"|[\d,]+\s*円"                                # 単独金額
)
# 「24室」「87室」
_ROOM_RE = re.compile(r"(\d+)\s*室")
# 「3台」「20台」
_PARKING_RE = re.compile(r"(\d+)\s*台")
# 24時間営業と読み取れる明示表記のみを拾う (紹介文の「24時間いつでも」等は拾わない)
_ALLDAY_RE = re.compile(r"24\s*時間\s*(?:営業|受付|フロント|オープン|チェックイン可)")
# 料金の時間帯が終日 (0:00〜24:00) を示す表記
_ALLDAY_TIME_RE = re.compile(r"0?0:00\s*[～~〜-]\s*24:00")

# 都道府県名 (住所先頭からのフォールバック抽出用)
_PREFECTURES = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]

# 料金テーブルの th ラベル → 出力先の対応 (部分一致で判定)
_PRICE_REST = "休憩"
_PRICE_FREE = "フリータイム"
_PRICE_STAY = "宿泊"
_PRICE_SERVICE = "サービスタイム"
_PRICE_NIGHT = "深夜"

# EXTRA カラム名
COL_HOTEL_ID = "ホテルID"
COL_CITY = "市区町村"
COL_PRICE_REST = "休憩料金"
COL_PRICE_FREE = "フリータイム料金"
COL_PRICE_STAY = "宿泊料金"
COL_HAS_SERVICE = "サービスタイム有無"
COL_HAS_NIGHT = "深夜料金有無"
COL_ROOMS = "客室数"
COL_PARKING = "駐車場台数"
COL_ALLDAY = "24時間営業有無"


class Couples(StaticCrawler):
    """カップルズ (couples.jp) スクレイパー"""

    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
    # 利用規約 第4条「ネットワーク・システム等に過度な負荷をかける行為」への配慮で 1 秒以上空ける
    DELAY = 1.2
    TIMEOUT = 30

    # 沖縄県は取得対象外 (依頼仕様)
    EXCLUDE_PREF_NAMES = ("沖縄",)

    MAX_PAGES_PER_AREA = 40  # 30件/ページ × 40 = 1,200件。暴走防止の上限

    EXTRA_COLUMNS = [
        COL_HOTEL_ID,
        COL_CITY,
        COL_PRICE_REST,
        COL_PRICE_FREE,
        COL_PRICE_STAY,
        COL_HAS_SERVICE,
        COL_HAS_NIGHT,
        COL_ROOMS,
        COL_PARKING,
        COL_ALLDAY,
    ]

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str):
        """都道府県 → ホテルエリア → 施設詳細 の順に巡回し、1件取得ごとに yield する。

        Args:
            url (str): エリア検索トップ (sites.yml の url = SSOT)

        Yields:
            dict: Schema + EXTRA_COLUMNS のデータ 1 件
        """
        root = self.get_soup(url)
        if root is None:
            logger.error("エリア検索トップを取得できませんでした: %s", url)
            return

        prefectures = self._extract_prefectures(root, url)
        if not prefectures:
            logger.error("都道府県リンクが取得できませんでした: %s", url)
            return
        logger.info("対象都道府県: %d件 (沖縄県は除外)", len(prefectures))

        seen_hotels: set[str] = set()

        for pref_url, pref_name in prefectures:
            pref_soup = self.get_soup(pref_url)
            if pref_soup is None:
                logger.warning("都道府県ページの取得に失敗: %s", pref_url)
                continue

            area_urls = self._extract_area_urls(pref_soup, pref_url)
            logger.info("%s: ホテルエリア %d件", pref_name, len(area_urls))

            for area_url in area_urls:
                yield from self._crawl_area(area_url, seen_hotels)

    # ------------------------------------------------------------------ #
    # 一覧
    # ------------------------------------------------------------------ #
    def _extract_prefectures(self, soup, base_url: str) -> list[tuple[str, str]]:
        """エリア検索トップから (都道府県ページURL, 都道府県名) のリストを作る。"""
        results: list[tuple[str, str]] = []
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            m = _PREF_HREF_RE.match(a["href"])
            if not m:
                continue
            name = a.get_text(strip=True)
            if not name or any(ex in name for ex in self.EXCLUDE_PREF_NAMES):
                continue
            full = urljoin(base_url, a["href"])
            if full in seen:
                continue
            seen.add(full)
            results.append((full, name))
        return results

    def _extract_area_urls(self, soup, base_url: str) -> list[str]:
        """都道府県ページからホテルエリア一覧 URL を抽出する (重複除去・掲載順維持)。"""
        urls: list[str] = []
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            if not _AREA_HREF_RE.match(a["href"]):
                continue
            full = urljoin(base_url, a["href"])
            if full in seen:
                continue
            seen.add(full)
            urls.append(full)
        return urls

    def _crawl_area(self, area_url: str, seen_hotels: set[str]):
        """ホテルエリア一覧を ?page=N で送りつつ、詳細を 1 件取得するごとに yield する。"""
        for page in range(1, self.MAX_PAGES_PER_AREA + 1):
            list_url = area_url if page == 1 else f"{area_url}?page={page}"
            soup = self.get_soup(list_url)
            if soup is None:
                logger.warning("一覧ページの取得に失敗: %s", list_url)
                return

            detail_urls = self._extract_detail_urls(soup, list_url)
            if not detail_urls:
                return

            new_urls = [u for u in detail_urls if u not in seen_hotels]
            seen_hotels.update(new_urls)

            for detail_url in new_urls:
                item = self._parse_detail(detail_url)
                if item:
                    yield item

            # 次ページへのリンクが無ければ終了
            if not self._has_next_page(soup, page):
                return

    @staticmethod
    def _extract_detail_urls(soup, base_url: str) -> list[str]:
        """一覧ページから施設詳細 URL を抽出する (トラッキングクエリは除去)。"""
        urls: list[str] = []
        seen: set[str] = set()
        for a in soup.select('a[href*="/hotel-details/"]'):
            href = a.get("href") or ""
            path = urlparse(href).path if href.startswith("http") else href.split("?")[0]
            m = re.fullmatch(r"/hotel-details/(\d+)", path)
            if not m:
                continue
            full = urljoin(base_url, path)
            if full in seen:
                continue
            seen.add(full)
            urls.append(full)
        return urls

    @staticmethod
    def _has_next_page(soup, current_page: int) -> bool:
        """ページャに ?page={current_page + 1} のリンクがあるか判定する。"""
        needle = f"page={current_page + 1}"
        return any(needle in (a.get("href") or "") for a in soup.find_all("a", href=True))

    # ------------------------------------------------------------------ #
    # 詳細
    # ------------------------------------------------------------------ #
    def _parse_detail(self, detail_url: str) -> dict | None:
        """施設詳細ページを 1 件パースする。時間貸し料金の記載が無い場合は None。"""
        soup = self.get_soup(detail_url)
        if soup is None:
            logger.warning("詳細ページの取得に失敗: %s", detail_url)
            return None

        basic = self._table_map(soup, "table.hd-table", "th", "td")
        prices = self._price_map(soup)

        # --- フィルタ: 時間貸し料金 (休憩/フリータイム/サービスタイム/深夜料金) 必須 ---
        hourly = [
            prices.get(_PRICE_REST),
            prices.get(_PRICE_FREE),
            prices.get(_PRICE_SERVICE),
            prices.get(_PRICE_NIGHT),
        ]
        if not any(hourly):
            logger.debug("時間貸し料金の記載が無いため除外: %s", detail_url)
            return None

        ld = self._hotel_jsonld(soup)

        name = self._hotel_name(soup, basic, ld)
        if not name:
            logger.debug("施設名が取得できないため除外: %s", detail_url)
            return None

        address_full = basic.get("住所", "") or self._jsonld_address(ld)
        pref, addr = self._split_address(address_full, ld)

        item = {
            Schema.NAME: name,
            Schema.NAME_KANA: self._text(soup.select_one(".hd-infoBasic__ruby")),
            Schema.PREF: pref,
            # 郵便番号はサイト上に掲載が無いため空欄
            Schema.POST_CODE: "",
            Schema.ADDR: addr,
            Schema.TEL: basic.get("TEL", "") or (ld.get("telephone") or "").strip(),
            Schema.HP: self._official_site(soup),
            Schema.URL: detail_url,
            Schema.FAC_NAME: name,
            Schema.SCORES: self._star_rating(ld),
            COL_HOTEL_ID: detail_url.rstrip("/").rsplit("/", 1)[-1],
            COL_CITY: (ld.get("address") or {}).get("addressLocality", ""),
            COL_PRICE_REST: prices.get(_PRICE_REST, ""),
            COL_PRICE_FREE: prices.get(_PRICE_FREE, ""),
            COL_PRICE_STAY: prices.get(_PRICE_STAY, ""),
            COL_HAS_SERVICE: "有" if prices.get(_PRICE_SERVICE) else "無",
            COL_HAS_NIGHT: "有" if prices.get(_PRICE_NIGHT) else "無",
            COL_ROOMS: self._first_match(_ROOM_RE, basic.get("部屋数", ""), suffix="室"),
            COL_PARKING: self._first_match(_PARKING_RE, basic.get("駐車場", ""), suffix="台"),
            COL_ALLDAY: self._all_day(soup, basic),
        }
        return item

    # ------------------------------------------------------------------ #
    # 詳細ページのパーツ
    # ------------------------------------------------------------------ #
    @staticmethod
    def _text(node) -> str:
        return node.get_text(" ", strip=True) if node else ""

    @staticmethod
    def _table_map(soup, table_selector: str, th_name: str, td_name: str) -> dict[str, str]:
        """th をラベル、td をテキストとする dict を作る (同一ラベルは先勝ち)。"""
        result: dict[str, str] = {}
        for table in soup.select(table_selector):
            for tr in table.find_all("tr"):
                th = tr.find(th_name)
                td = tr.find(td_name)
                if not th or not td:
                    continue
                label = th.get_text(" ", strip=True)
                if label and label not in result:
                    result[label] = td.get_text(" ", strip=True)
        return result

    def _price_map(self, soup) -> dict[str, str]:
        """利用料金テーブルを {正規化ラベル: 金額表記のみ} の dict にする。

        注意書き (※〜) や説明文は保存せず、金額表記だけを抽出する (著作権配慮)。
        """
        result: dict[str, str] = {}
        for table in soup.select("table.hd-tablePrice"):
            for tr in table.find_all("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                label = th.get_text(" ", strip=True)
                key = self._normalize_price_label(label)
                if not key:
                    continue
                amounts = self._extract_amounts(td)
                if amounts and key not in result:
                    result[key] = amounts
        return result

    @staticmethod
    def _normalize_price_label(label: str) -> str:
        """料金テーブルの th ラベルを既知のキーに正規化する。"""
        for key in (_PRICE_FREE, _PRICE_SERVICE, _PRICE_NIGHT, _PRICE_REST, _PRICE_STAY):
            if key in label:
                return key
        return ""

    @staticmethod
    def _extract_amounts(td) -> str:
        """料金セルから金額表記だけを抽出して " / " 連結で返す。"""
        values: list[str] = []

        def _push(text: str) -> None:
            for m in _PRICE_RE.finditer(text):
                v = re.sub(r"\s+", "", m.group(0))
                if v not in values:
                    values.append(v)

        # 1) 構造化された金額 span があればそれを最優先で使う
        spans = td.select(".hd-priceRange__price")
        if spans:
            for span in spans:
                _push(span.get_text(" ", strip=True))

        # 2) 無い場合はプラン注記から金額行だけを拾う (共通注意書き・※行は除外)
        if not values:
            for note in td.select("ul.hd-pricePlan__note > li"):
                classes = note.get("class") or []
                if "hd-pricePlan__note--common" in classes:
                    continue
                for line in note.get_text("\n", strip=True).split("\n"):
                    line = line.strip()
                    if not line or line.startswith(("※", "*", "＊")):
                        continue
                    _push(line)

        return " / ".join(values[:20])

    @staticmethod
    def _hotel_jsonld(soup) -> dict:
        """@type=Hotel の JSON-LD を返す (無ければ空 dict)。"""
        for script in soup.find_all("script", type="application/ld+json"):
            raw = script.string or script.get_text() or ""
            if not raw.strip():
                continue
            try:
                data = json.loads(raw)
            except (ValueError, TypeError):
                continue
            if isinstance(data, dict) and data.get("@type") == "Hotel":
                return data
        return {}

    def _hotel_name(self, soup, basic: dict, ld: dict) -> str:
        """施設名を取得する (基本情報テーブル → JSON-LD → h1 の順)。"""
        for table in soup.select("table.hd-table"):
            for tr in table.find_all("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td or th.get_text(" ", strip=True) != "ホテル名":
                    continue
                # 1行目はカナ (hd-infoBasic__ruby)、2行目が正式名称
                paragraphs = [
                    p.get_text(" ", strip=True)
                    for p in td.find_all("p")
                    if "hd-infoBasic__ruby" not in (p.get("class") or [])
                ]
                for text in paragraphs:
                    if text:
                        return text
        name = (ld.get("name") or "").strip()
        if name:
            return name
        return self._text(soup.select_one("h1"))

    @staticmethod
    def _jsonld_address(ld: dict) -> str:
        addr = ld.get("address") or {}
        if not isinstance(addr, dict):
            return ""
        return "".join(
            (addr.get(k) or "")
            for k in ("addressRegion", "addressLocality", "streetAddress")
        )

    @staticmethod
    def _split_address(address_full: str, ld: dict) -> tuple[str, str]:
        """住所を (都道府県, 市区町村以降) に分割する。"""
        address_full = (address_full or "").strip()
        addr_ld = ld.get("address") or {}
        pref = ""
        if isinstance(addr_ld, dict):
            pref = (addr_ld.get("addressRegion") or "").strip()
        if not pref:
            for p in _PREFECTURES:
                if address_full.startswith(p):
                    pref = p
                    break
        rest = address_full
        if pref and address_full.startswith(pref):
            rest = address_full[len(pref):].strip()
        return pref, rest

    @staticmethod
    def _official_site(soup) -> str:
        """基本情報テーブルの「ホームページ」行から公式サイト URL を取得する。"""
        link = soup.select_one("#officialsite")
        if link and link.get("href"):
            return link["href"].strip()
        for table in soup.select("table.hd-table"):
            for tr in table.find_all("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td or "ホームページ" not in th.get_text(strip=True):
                    continue
                a = td.find("a", href=True)
                if a:
                    return a["href"].strip()
        return ""

    @staticmethod
    def _star_rating(ld: dict) -> str:
        rating = ld.get("starRating") or {}
        if isinstance(rating, dict):
            value = rating.get("ratingValue")
            if value is not None:
                return str(value)
        return ""

    @staticmethod
    def _first_match(pattern: re.Pattern, text: str, suffix: str = "") -> str:
        """テキストから最初の数値を取り出す (文章は保存しない)。"""
        m = pattern.search(text or "")
        return f"{m.group(1)}{suffix}" if m else ""

    @staticmethod
    def _all_day(soup, basic: dict) -> str:
        """24時間営業か判定する。

        couples.jp に「24時間営業」専用の項目は無いため、
          - 基本情報の受付/営業系の行に「24時間営業/受付/フロント」等の明示表記がある
          - 料金の利用可能時間帯が 0:00〜24:00 (終日) になっている
        のいずれかを満たす場合のみ「有」とする (紹介文中の「24時間いつでも」は拾わない)。
        """
        for label in ("受付方法", "営業時間", "公式予約情報", "外出情報", "支払い方法"):
            if _ALLDAY_RE.search(basic.get(label, "")):
                return "有"
        for node in soup.select(".hd-timezone__time"):
            text = node.get_text(" ", strip=True)
            if _ALLDAY_TIME_RE.search(text) or _ALLDAY_RE.search(text):
                return "有"
        return "無"


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Couples()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://couples.jp/prefectures/via-hotelareas")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
