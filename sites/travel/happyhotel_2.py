"""
ハッピーホテル (happyhotel.jp) — 全国ラブホテル・レジャーホテル情報

取得対象:
    - 全国47都道府県に掲載されているラブホテル/レジャーホテル (約4,900件)
    - 名称・都道府県・住所・TEL・部屋数・駐車台数・最安料金・口コミ評価/件数 等
    - 料金プラン系: フリータイム料金・サービスタイム有無・深夜料金有無・24時間営業有無・
      チェックイン時間 (いずれも詳細ページの料金表 hotelPricesKind から導出)

取得フロー:
    /search/address/pref/{pref_id}                              都道府県別ページ
        └─ __NEXT_DATA__ の pageData.cities から市区町村リストを取得
      → /search/address/pref/{pref_id}/cities/{jis_code}?page=N  市区町村別一覧 (30件/ページ)
        → /hotels/{hotel_id}                                     ホテル詳細

    サイトは Next.js の SSR で、一覧・詳細ともに <script id="__NEXT_DATA__"> の
    props.pageProps.pageData に全データが入っているため StaticCrawler (requests) で取得できる。

非自明な仕様 (実測):
    - 一覧の pager リンクは href="./" (JS 制御) だが、サーバは ?page=N を解釈する。
      ?p= / ?pageNo= / ?offset= は無視され 1 ページ目固定になる。範囲外ページは 0 件。
    - prHotels は「他市区町村のホテルの広告枠」なので使わない。
      その市区町村の全件は basicHotels のみ (総件数は basicHotelQty)。
    - 詳細ページには料金の要約 (roomCharges) が無い。最安料金は一覧の roomCharges を主に使い、
      無ければ詳細の hotelPricesKind に散らばる価格から最小値を導出する。
    - hotelPricesKind の name (料金プラン名) は店舗ごとに完全に自由記述で、実測では
      「休憩 / 宿泊 / サービスタイム（フリータイム） / 深夜休憩 / ショートタイム / Midnight /
      休憩90分 / 宿泊1部 …」など 60 種類以上ある。よってプラン種別は完全一致ではなく
      キーワード包含で判定する (フリータイム / サービスタイム / 深夜 等)。
    - 「24時間」は "24時間制チェックイン"(＝24時間受付) と "24時間ご利用 / 24時間宿泊"(＝滞在時間)
      の 2 用法があり、後者は 24 時間営業の根拠にならない。24時間制・24時間営業・オールタイム
      のみを 24時間営業の判定語とする。
    - 郵便番号はサイト上に掲載が無い (address は「北海道旭川市6条通4-2474」形式で 〒 なし)。
      将来掲載された場合に拾えるよう抽出は実装してあるが、通常は空欄になる。
    - チェックイン時間は独立した項目が無く、料金表の timeZone
      ("4:00～24:00チェックインより 6時間ご利用" / "17:00～翌 12:00の間で最大 19時間ご利用")
      からのみ導出できる。宿泊プランの先頭 (通常は平日) を優先して採用する。
    - parking は null で parking_all にだけ値が入るホテルがある (例: /hotels/25900724)。
    - parking / bldgType / headCount 等は「構造化値 \n 自由記述」の連結。1 行目のみ採用する。
    - credit は行数が可変 (カード / 電子マネー / コード決済)。既知ラベル行のみ採用する。

除外方針 (著作権配慮):
    pr / message / formatedPr (ホテル紹介文)、access (アクセス自由記述)、
    privilege / roomService / otherService (特典文)、hotelEquipByKind (設備長文)、
    caution、口コミ本文 は自由記述プロースのため取得しない。
    運営法人名・代表者名はサイトに掲載が無いため取得しない。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/happyhotel_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id happyhotel_2
"""

import json
import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

# 起点 URL "https://happyhotel.jp/search/address/pref/1" から "/pref/1..." を切り落とす
_PREF_SUFFIX_RE = re.compile(r"/pref/\d+(?:/.*)?$")

# 「有り：43台」「ハイルーフ可：11台」などから台数を取り出す
_PARKING_COUNT_RE = re.compile(r"(\d+)\s*台")

# credit のうち採用する構造化ラベル行 (カード：可 VISA … / 電子マネー（…） 等)
_PAYMENT_LABEL_RE = re.compile(r"^(?:カード|自動精算機|電子マネー|コード決済|QR)")

# 「￥4,520～」「￥3,490～￥6,990」から金額を取り出す
_PRICE_RE = re.compile(r"[\d,]+")

# 料金プラン名 (hotelPricesKind[].name) の種別判定。店舗ごとに自由記述のため包含判定にする
_FREETIME_RE = re.compile(r"フリー\s*タイム|FREE\s*TIME", re.I)
_SERVICETIME_RE = re.compile(r"サービス\s*タイム|SERVICE\s*TIME", re.I)
_MIDNIGHT_RE = re.compile(r"深夜|ミッドナイト|MIDNIGHT", re.I)
_STAY_RE = re.compile(r"宿泊|STAY", re.I)

# 24時間営業の判定語。「24時間ご利用」「24時間宿泊」(滞在時間) は対象外
_ALLTIME_RE = re.compile(r"24\s*時間制|24\s*時間営業|オール\s*タイム|ALL\s*TIME", re.I)

# 郵便番号。電話番号 (03-1234-5678 等) を拾わないよう前後に数字・ハイフンが無いことを要求する
_ZIP_RE = re.compile(r"(?<![\d-])(\d{3}-\d{4})(?![\d-])")
_ZIP_MARKED_RE = re.compile(r"〒\s*(\d{3}-?\d{4})")

# timeZone からチェックイン部分を切り出す (…「チェックイン」より前 / 「の間で」より前)
_CHECKIN_SPLIT_RE = re.compile(r"チェックイン|の間で")
# 「17:00～翌 12:00」から先頭の時刻「17:00」を取り出す
_TIME_HEAD_RE = re.compile(r"\d{1,2}(?::\d{2})?\s*時?")

# SNS 判定 (dataType よりドメイン判定の方が堅牢)
_SNS_PATTERNS = [
    (Schema.X, re.compile(r"(?:twitter\.com|x\.com)/", re.I)),
    (Schema.INSTA, re.compile(r"instagram\.com/", re.I)),
    (Schema.FB, re.compile(r"facebook\.com/", re.I)),
    (Schema.LINE, re.compile(r"line\.me/", re.I)),
    (Schema.TIKTOK, re.compile(r"tiktok\.com/", re.I)),
]

# EXTRA カラム名
COL_HOTEL_ID = "ホテルID"
COL_CITY = "市区町村"
COL_ROOM_COUNT = "部屋数"
COL_PARKING = "駐車場"
COL_PARKING_COUNT = "駐車台数"
COL_PRICE_REST = "休憩最安料金"
COL_PRICE_STAY = "宿泊最安料金"
COL_PRICE_MIN = "最安料金"
COL_PRICE_FREE = "フリータイム料金"
COL_HAS_SERVICE_TIME = "サービスタイム有無"
COL_HAS_MIDNIGHT = "深夜料金有無"
COL_HAS_24H = "24時間営業有無"
COL_CHECKIN = "チェックイン時間"
COL_STATION = "最寄駅"
COL_IC = "最寄IC"
COL_GROUP = "グループ名"
COL_LAT = "緯度"
COL_LON = "経度"


class HappyHotel2(StaticCrawler):
    """ハッピーホテル スクレイパー"""

    # 既定 UA は Chrome/94 と古いため、新しめの UA を使う
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
    DELAY = 0.5
    TIMEOUT = 30

    PREF_IDS = range(1, 48)  # 全国47都道府県 (pref_id = JIS 都道府県コード)
    ITEMS_PER_PAGE = 30
    MAX_PAGES_PER_CITY = 40  # 暴走防止 (1市区町村 最大1,200件想定)

    EXTRA_COLUMNS = [
        COL_HOTEL_ID,
        COL_CITY,
        COL_ROOM_COUNT,
        COL_PARKING,
        COL_PARKING_COUNT,
        COL_PRICE_REST,
        COL_PRICE_STAY,
        COL_PRICE_MIN,
        COL_PRICE_FREE,
        COL_HAS_SERVICE_TIME,
        COL_HAS_MIDNIGHT,
        COL_HAS_24H,
        COL_CHECKIN,
        COL_STATION,
        COL_IC,
        COL_GROUP,
        COL_LAT,
        COL_LON,
    ]

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str):
        """都道府県 → 市区町村 → ホテル詳細 の順に巡回し、1件取得ごとに yield する。

        Args:
            url (str): sites.yml の url (例: https://happyhotel.jp/search/address/pref/1)。
                       この URL のみを起点とし、他の URL はすべてここから派生させる。

        Yields:
            dict: ホテル1件分のレコード。
        """
        # "https://happyhotel.jp/search/address/pref/1" -> "https://happyhotel.jp/search/address"
        base_url = _PREF_SUFFIX_RE.sub("", url.rstrip("/"))

        seen_ids: set[str] = set()
        estimated_total = 0

        for pref_id in self.PREF_IDS:
            pref_url = f"{base_url}/pref/{pref_id}"
            pref_data = self._get_page_data(pref_url)
            if not pref_data:
                logger.warning("都道府県ページを取得できませんでした: %s", pref_url)
                continue

            pref_name = self._clean((pref_data.get("prefNames") or {}).get(str(pref_id)))
            cities = pref_data.get("cities") or []

            # 判明した分だけ総件数の見込みを積み増す (ETA 表示用)
            pref_qty = sum(int(c.get("numberOfHotels") or 0) for c in cities)
            estimated_total += pref_qty
            self.total_items = estimated_total
            logger.info(
                "[pref %s] %s: %d市区町村 / 約%d件",
                pref_id, pref_name or "?", len(cities), pref_qty,
            )

            for city in cities:
                jis_code = city.get("id")
                if not jis_code:
                    continue
                city_url = f"{base_url}/pref/{pref_id}/cities/{jis_code}"

                for hotel in self._iter_city_hotels(city_url):
                    hotel_id = str(hotel.get("id") or "")
                    if not hotel_id or hotel_id in seen_ids:
                        continue  # 同一ホテルが複数市区町村に現れた場合の重複除去
                    seen_ids.add(hotel_id)

                    detail_url = urljoin(url, f"/hotels/{hotel_id}")
                    item = self._scrape_detail(detail_url, hotel, pref_name)
                    if item:
                        yield item  # 1件取得ごとに即 yield する

    # ------------------------------------------------------------------ #
    # 一覧
    # ------------------------------------------------------------------ #
    def _iter_city_hotels(self, city_url: str):
        """市区町村別一覧をページ送りしながら、ホテルの一覧データを1件ずつ返す。

        Args:
            city_url (str): 市区町村別一覧の URL (ページ指定なし)。

        Yields:
            dict: basicHotels の要素 (料金・緯度経度・最寄駅など一覧側の情報)。
        """
        collected = 0
        total = None

        for page in range(1, self.MAX_PAGES_PER_CITY + 1):
            page_url = city_url if page == 1 else f"{city_url}?page={page}"
            data = self._get_page_data(page_url)
            if not data:
                break

            # prHotels は他市区町村の広告枠なので使わない
            hotels = data.get("basicHotels") or []
            if not hotels:
                break
            if total is None:
                total = data.get("basicHotelQty")

            for hotel in hotels:
                collected += 1
                yield hotel

            if len(hotels) < self.ITEMS_PER_PAGE:
                break
            if total is not None and collected >= int(total):
                break

    # ------------------------------------------------------------------ #
    # 詳細
    # ------------------------------------------------------------------ #
    def _scrape_detail(self, detail_url: str, list_hotel: dict, pref_name: str) -> dict | None:
        """ホテル詳細ページを取得し、一覧データとマージして1レコードを組み立てる。

        Args:
            detail_url (str): 詳細ページ URL (/hotels/{id})。
            list_hotel (dict): 一覧側のホテルデータ (料金等のフォールバック元)。
            pref_name (str): 巡回中の都道府県名 (詳細側が空のときのフォールバック)。

        Returns:
            dict | None: レコード。名称が取れない場合は None。
        """
        data = self._get_page_data(detail_url)
        info = (data or {}).get("hotelBasicInfo") or {}

        name = self._clean(info.get("hotelName")) or self._clean(list_hotel.get("name"))
        if not name:
            logger.warning("名称を取得できないためスキップ: %s", detail_url)
            return None

        charges = list_hotel.get("roomCharges") or {}
        nearest = info.get("nearest") or {}
        geo = info.get("map") if isinstance(info.get("map"), dict) else {}

        rest = self._price(charges.get("rest"))
        stay = self._price(charges.get("stay"))
        charge = self._price(charges.get("charge"))
        candidates = [p for p in (rest, stay, charge) if p]
        if not candidates:
            # 詳細ページに roomCharges は無いため、料金表 (hotelPricesKind) から導出する
            candidates = self._prices_from_price_table(info.get("hotelPricesKind"))
        price_min = str(min(int(p) for p in candidates)) if candidates else ""

        # フリータイム料金 / 各種プランの有無 / チェックイン時間は料金表からのみ導出できる
        features = self._price_features(info.get("hotelPricesKind"))

        # parking が null で parking_all にだけ値が入るホテルがある
        parking_raw = self._clean(info.get("parking")) or self._clean(info.get("parking_all"))
        parking_head = self._first_line(parking_raw)
        m = _PARKING_COUNT_RE.search(parking_head)
        if m:
            parking_count = m.group(1)
        elif parking_head.startswith("なし"):
            parking_count = "0"
        else:
            parking_count = ""

        item = {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.NAME_KANA: self._clean(info.get("hotelNameKana")),
            Schema.PREF: self._clean(info.get("prefName")) or pref_name,
            Schema.POST_CODE: self._post_code(info, list_hotel),
            Schema.ADDR: self._clean(info.get("address")) or self._clean(list_hotel.get("address")),
            Schema.TEL: self._clean(info.get("tel")),
            Schema.CAT_SITE: self._first_line(self._clean(info.get("bldgType"))),
            Schema.HP: self._official_site(info.get("site") or list_hotel.get("site")),
            Schema.LINE: "",
            Schema.INSTA: "",
            Schema.X: "",
            Schema.FB: "",
            Schema.TIKTOK: "",
            Schema.PAYMENTS: self._payments(info.get("credit")),
            Schema.SCORES: self._first(info.get("kuchikomiAvgStr"), list_hotel.get("kuchikomiAvgStr")),
            Schema.REV_SCR: self._first(info.get("kuchikomiAllCount"), list_hotel.get("kuchikomiAllCount")),
            COL_HOTEL_ID: self._first(info.get("hotelId"), list_hotel.get("id")),
            COL_CITY: self._clean(info.get("cityName")),
            COL_ROOM_COUNT: self._first(info.get("roomCount"), self._first_line(self._clean(info.get("roomAllCount")))),
            COL_PARKING: parking_head,
            COL_PARKING_COUNT: parking_count,
            COL_PRICE_REST: rest,
            COL_PRICE_STAY: stay,
            COL_PRICE_MIN: price_min,
            COL_PRICE_FREE: features["free_price"],
            COL_HAS_SERVICE_TIME: features["has_service_time"],
            COL_HAS_MIDNIGHT: features["has_midnight"],
            COL_HAS_24H: features["has_24h"],
            COL_CHECKIN: features["checkin"],
            COL_STATION: self._clean(nearest.get("station")) or self._clean(list_hotel.get("rootStationText")),
            COL_IC: self._clean(nearest.get("ic")) or self._clean(list_hotel.get("rootIcText")),
            COL_GROUP: " / ".join(
                self._clean(g.get("group_name")) for g in (info.get("group") or []) if g.get("group_name")
            ),
            COL_LAT: self._first(geo.get("lat"), list_hotel.get("latitude")),
            COL_LON: self._first(geo.get("lon"), list_hotel.get("longitude")),
        }

        for sns in (info.get("sns") or []):
            sns_url = self._clean(sns.get("url"))
            if not sns_url:
                continue
            for column, pattern in _SNS_PATTERNS:
                if pattern.search(sns_url):
                    if not item[column]:
                        item[column] = sns_url
                    break

        return item

    # ------------------------------------------------------------------ #
    # ユーティリティ
    # ------------------------------------------------------------------ #
    def _get_page_data(self, url: str) -> dict | None:
        """__NEXT_DATA__ の props.pageProps.pageData を取り出す。

        Args:
            url (str): 取得対象 URL。

        Returns:
            dict | None: pageData。取得・解析に失敗した場合は None。
        """
        soup = self.get_soup(url)
        if soup is None:
            return None
        script = soup.find("script", id="__NEXT_DATA__")
        if script is None or not script.string:
            logger.warning("__NEXT_DATA__ が見つかりません: %s", url)
            return None
        try:
            payload = json.loads(script.string)
        except json.JSONDecodeError as e:
            logger.warning("__NEXT_DATA__ の解析に失敗: %s — %s", url, e)
            return None
        return (payload.get("props") or {}).get("pageProps", {}).get("pageData") or None

    @staticmethod
    def _clean(value) -> str:
        """前後の空白を落とした文字列を返す (None は空文字)。"""
        if value is None:
            return ""
        return str(value).strip()

    @staticmethod
    def _first_line(value: str) -> str:
        """「構造化値 \\n 自由記述」の連結から1行目だけを返す。"""
        if not value:
            return ""
        return value.split("\n")[0].strip()

    @classmethod
    def _first(cls, *values) -> str:
        """最初に見つかった非空の値を文字列で返す。"""
        for v in values:
            s = cls._clean(v)
            if s:
                return s
        return ""

    @staticmethod
    def _price(value) -> str:
        """「￥4,520～」→「4520」。取得できなければ空文字。"""
        if not value:
            return ""
        m = _PRICE_RE.search(str(value))
        return m.group(0).replace(",", "") if m else ""

    @staticmethod
    def _prices_from_price_table(kinds) -> list[str]:
        """hotelPricesKind (休憩/宿泊などの料金表) に現れる全金額を文字列リストで返す。

        詳細ページには一覧の roomCharges に相当する要約が無いため、
        料金表からの最安値算出に使う。

        Args:
            kinds (list | None): hotelBasicInfo.hotelPricesKind。

        Returns:
            list[str]: 数字のみに正規化した金額のリスト。
        """
        found: list[str] = []
        for kind in (kinds or []):
            for info in (kind.get("priceInfo") or []):
                for tp in (info.get("timePrice") or []):
                    for raw in _PRICE_RE.findall(str(tp.get("price") or "")):
                        digits = raw.replace(",", "")
                        if digits.isdigit():
                            found.append(digits)
        return found

    @classmethod
    def _price_features(cls, kinds) -> dict:
        """料金表 (hotelPricesKind) からプラン種別のフラグとフリータイム料金・チェックイン時間を導出する。

        プラン名は店舗ごとの自由記述 (「サービスタイム（フリータイム）」「深夜休憩」「Midnight」
        「休憩90分」等) なのでキーワード包含で判定する。

        Args:
            kinds (list | None): hotelBasicInfo.hotelPricesKind。

        Returns:
            dict: free_price / has_service_time / has_midnight / has_24h / checkin。
        """
        free_prices: list[int] = []
        has_service_time = False
        has_midnight = False
        has_24h = False
        stay_checkin = ""   # 宿泊プランのチェックイン (優先)
        other_checkin = ""  # 休憩等のチェックイン (フォールバック)

        for kind in (kinds or []):
            name = cls._clean(kind.get("name"))
            if _SERVICETIME_RE.search(name):
                has_service_time = True
            if _MIDNIGHT_RE.search(name):
                has_midnight = True
            if _ALLTIME_RE.search(name) or _ALLTIME_RE.search(cls._clean(kind.get("remarks"))):
                has_24h = True

            is_free = bool(_FREETIME_RE.search(name))
            is_stay = bool(_STAY_RE.search(name))

            for info in (kind.get("priceInfo") or []):
                for tp in (info.get("timePrice") or []):
                    zone = cls._clean(tp.get("timeZone"))
                    # 「24時間制チェックイン」＝24時間受付。「24時間ご利用」(滞在時間) は該当しない
                    if _ALLTIME_RE.search(zone):
                        has_24h = True
                    if is_free:
                        for raw in _PRICE_RE.findall(cls._clean(tp.get("price"))):
                            digits = raw.replace(",", "")
                            if digits.isdigit():
                                free_prices.append(int(digits))
                    checkin = cls._checkin(zone)
                    if checkin:
                        if is_stay:
                            stay_checkin = stay_checkin or checkin
                        else:
                            other_checkin = other_checkin or checkin

        return {
            "free_price": str(min(free_prices)) if free_prices else "",
            "has_service_time": "有り" if has_service_time else "無し",
            "has_midnight": "有り" if has_midnight else "無し",
            "has_24h": "有り" if has_24h else "無し",
            "checkin": stay_checkin or other_checkin,
        }

    @staticmethod
    def _checkin(time_zone: str) -> str:
        """timeZone からチェックイン時間 (帯) を切り出す。

        実測パターン:
            "4:00～24:00チェックインより 6時間ご利用"      -> "4:00～24:00"
            "24時間制チェックインより 1時間30分ご利用"      -> "24時間制"
            "17:00～翌 12:00の間で最大 19時間ご利用"        -> "17:00" (利用可能帯の開始＝入室開始時刻)

        Args:
            time_zone (str): hotelPricesKind[].priceInfo[].timePrice[].timeZone。

        Returns:
            str: チェックイン時間。判定できない場合は空文字。
        """
        if not time_zone:
            return ""
        m = _CHECKIN_SPLIT_RE.search(time_zone)
        if not m:
            return ""
        head = re.sub(r"\s+", " ", time_zone[: m.start()]).strip()
        if not head:
            return ""
        if m.group(0) == "チェックイン":
            return head  # 「〜チェックイン」の直前はチェックイン受付帯そのもの
        # 「A～Bの間で最大N時間」は滞在可能帯なので、開始時刻のみをチェックイン時刻とする
        t = _TIME_HEAD_RE.match(head)
        return t.group(0).strip() if t else head

    @classmethod
    def _post_code(cls, info: dict, list_hotel: dict) -> str:
        """郵便番号を住所・詳細データから拾う。

        現状サイトに郵便番号の掲載は無く (address は「北海道旭川市6条通4-2474」形式)、
        通常は空文字になる。将来掲載された場合に備えた抽出。

        Args:
            info (dict): hotelBasicInfo。
            list_hotel (dict): 一覧側のホテルデータ。

        Returns:
            str: "123-4567" 形式の郵便番号。無ければ空文字。
        """
        for source in (info.get("address"), list_hotel.get("address")):
            text = cls._clean(source)
            if not text:
                continue
            m = _ZIP_MARKED_RE.search(text) or _ZIP_RE.search(text)
            if m:
                zip_code = m.group(1)
                return zip_code if "-" in zip_code else f"{zip_code[:3]}-{zip_code[3:]}"
        # 住所以外 (備考等) に 〒 付きで書かれている場合のみ拾う (電話番号の誤検出を避ける)
        m = _ZIP_MARKED_RE.search(json.dumps(info, ensure_ascii=False))
        if m:
            zip_code = m.group(1)
            return zip_code if "-" in zip_code else f"{zip_code[:3]}-{zip_code[3:]}"
        return ""

    @staticmethod
    def _payments(credit) -> str:
        """credit から「カード：〜」「電子マネー（〜）」等の構造化行のみを抜き出す。

        credit は行数が可変で末尾に店舗の自由記述が付くこともあるため、
        既知のラベルで始まる行だけを採用する。

        Args:
            credit (str | None): hotelBasicInfo.credit。

        Returns:
            str: " / " 区切りの支払い方法。
        """
        if not credit:
            return ""
        lines = [ln.strip() for ln in str(credit).split("\n") if ln.strip()]
        picked = [ln for ln in lines if _PAYMENT_LABEL_RE.match(ln)]
        return " / ".join(picked) if picked else (lines[0] if lines else "")

    @staticmethod
    def _official_site(sites) -> str:
        """site リストから公式サイト URL を選ぶ (無ければ先頭を採用)。"""
        if not sites:
            return ""
        for s in sites:
            if "オフィシャル" in (s.get("text") or "") or "公式" in (s.get("text") or ""):
                return (s.get("url") or "").strip()
        return (sites[0].get("url") or "").strip()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = HappyHotel2()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://happyhotel.jp/search/address/pref/1")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
