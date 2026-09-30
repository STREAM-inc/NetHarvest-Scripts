"""
【STREAMREQ-18923】isleofman.com ビジネスディレクトリ「travel agents」(マン島)
— streamreq_18923isleofman_com_travel_agents

取得対象:
    マン島の地域ポータル isleofman.com のビジネスディレクトリを
    キーワード "travel agents" で検索した結果 (実測 31 件 / 6 ページ / 1 ページ 6 件)。
    そのうち「マン島所在の旅行会社」のみを出力する (後述のフィルタ)。

取得フロー:
    1. 一覧ページ (引数 url) を取得する。サイトは Inertia.js 製で、HTML には
       リンクや店舗情報がほぼ描画されておらず、<div id="app" data-page="{...}"> の
       JSON がデータ本体になっている。
         props.businesses.data[] … 1 ページ分の事業者 (name / slug / categories /
                                   addresses[] {full_address, postcode, town, phones[]})
         props.businesses.meta   … total / last_page / per_page / current_page
    2. 1 件ずつ詳細ページ (https://www.isleofman.com/businesses/{slug}) を取得し、
       props.business から website_url / email / SNS / 営業時間を補って
       その場で yield する (Pattern B: 取得即 yield)。
    3. 2 ページ目以降は meta.last_page まで url の page パラメータを差し替えて巡回する。

フィルタ (依頼の備考「住所がマン島(郵便番号 IM)で旅行会社のものだけを取る」の実装):
    A. マン島所在であること
         - 郵便番号が IM で始まる (英国郵便番号のマン島エリア)、または
         - 郵便番号が空で住所に "Isle of Man" を含み "United Kingdom" を含まない
       → 英国本土の "BirminghamCorporate" (B70 7EA / United Kingdom) を除外する。
    B. 旅行会社であること
         - 宿泊・カジノ系の名称 (hotel / casino / guest house / B&B 等) を除外する
           → "Hilton Hotel & Casino (IOM) Ltd" を除外。
         - 旅行以外のカテゴリ (Financial Services 等) が付く事業者を除外する
           → "Money Express" (Financial Services) を除外。
    C. 掲載が古く現存しない事業者
         - 依頼で名指しされた "Thomas Cook Ltd" (2019 年破綻) を除外する。
           ※ 廃業の有無はサイト上のデータからは判定できないため、
             名指しされた社のみを明示的な定数 (_DEFUNCT_NAMES) で除外する。
             他の掲載も古い可能性があるが、推測で落とさず出力し、
             判断材料として EXTRA カラムにカテゴリ・掲載区分 (tier) を残す。

カラムについて:
    - 国は依頼指示どおり EXTRA カラム「国」に "マン島" を固定で入れる
      (Schema.PREF は日本の都道府県用カラムのため空欄のままとする)。
    - TEL は依頼指示どおり国際表記 (+44-1624-694455 形式) に正規化する。
      原文は EXTRA「TEL(原文)」に残す。phones[].suffix が "Fax" のものは
      Schema.TEL に入れず EXTRA「FAX」に分ける。
    - 郵便番号は英国式 (IM1 2AR) で日本の 7 桁形式ではないため、基盤の正規化で
      空になり得る。原文を EXTRA「郵便番号(英国)」にも保持する。
    - website_url は Schema.HP と Schema.WEBSITE の両方に入れる (実測 14/31 件)。
    - description / short_description は空文字か自由記述の長文であり、
      著作権リスクを避けるため取得しない。
    - tags は全事業者に同じ SEO キーワード列 ("flights", "travel agency" …) が
      入っており事業者固有の情報ではないため取得しない。

利用規約:
    https://www.isleofman.com/robots.txt は「User-agent: * / Disallow:」(全面許可)。
    利用規約ページは存在せず (/terms 等は 404)、スクレイピングを禁止する
    明確な記載は確認できなかった。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/streamreq_18923isleofman_com_travel_agents.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id streamreq_18923isleofman_com_travel_agents
"""

import json
import logging
import re
import sys
from pathlib import Path
from typing import Any, Generator
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

# 依頼指示により固定する国名
_COUNTRY = "マン島"

# 電話番号の国際表記用 (国番号 44 / マン島の市外局番は固定 1624・携帯 7624)
_UK_COUNTRY_CODE = "44"
_IOM_AREA_CODES = ("1624", "7624")

# 英国郵便番号のマン島エリア ("IM1 2AR" 等)
_IOM_POSTCODE_RE = re.compile(r"^IM\d", re.IGNORECASE)

# 郵便番号が無い場合のマン島判定 (住所文字列)
_IOM_IN_ADDRESS_RE = re.compile(r"isle\s*of\s*man", re.IGNORECASE)
_UK_MAINLAND_RE = re.compile(r"united\s*kingdom|\bUK\b|england|scotland|wales", re.IGNORECASE)

# 旅行代理店ではない宿泊・娯楽施設の名称キーワード (フィルタ B)
_NON_AGENCY_NAME_RE = re.compile(
    r"\b(hotel|casino|guest\s*house|guesthouse|b\s*&\s*b|hostel|"
    r"apartments?|cottages?|campsite|caravan)\b",
    re.IGNORECASE,
)

# 旅行系とみなすカテゴリ (これ以外のカテゴリが付く事業者は旅行会社ではないとみなす)
_TRAVEL_CATEGORY_SLUGS = {"travel-accommodation"}

# 掲載が古く現存しない事業者 (依頼で名指しされたもののみ。小文字で比較する)
_DEFUNCT_NAMES = {"thomas cook ltd"}

# Inertia.js のページデータが入る属性
_DATA_PAGE_SELECTOR = "[data-page]"

# 詳細ページのパス
_DETAIL_PATH = "/businesses/"

# 一覧の取得失敗時のリトライ上限 (無限ループ禁止)
_MAX_PAGES_GUARD = 50


def _clean(value: Any) -> str:
    """None / 空白のみを空文字に落とし、連続空白を 1 個に整理する。"""
    if value is None:
        return ""
    text = str(value).replace(" ", " ")
    return re.sub(r"\s+", " ", text).strip()


def _format_phone(value: Any) -> str:
    """英国/マン島の電話番号を国際表記 (+44-1624-694455) に正規化する。

    例: "01624694455"  -> "+44-1624-694455"
        "01624 623123" -> "+44-1624-623123"
        "07624483336"  -> "+44-7624-483336"
        "44121537634"  -> "+44-121537634"
    判定できない値は数字のみに整形して返す (推測で補完しない)。
    """
    raw = _clean(value)
    if not raw:
        return ""
    digits = re.sub(r"\D", "", raw)
    if not digits:
        return ""
    if raw.startswith("+"):
        return f"+{digits}"
    if digits.startswith("0"):
        national = digits[1:]
    elif digits.startswith(_UK_COUNTRY_CODE) and len(digits) > 10:
        national = digits[len(_UK_COUNTRY_CODE):]
    else:
        national = digits
    if not national:
        return ""
    for area in _IOM_AREA_CODES:
        if national.startswith(area):
            return f"+{_UK_COUNTRY_CODE}-{area}-{national[len(area):]}"
    return f"+{_UK_COUNTRY_CODE}-{national}"


def _page_url(url: str, page: int) -> str:
    """引数の一覧 URL の page パラメータだけを差し替える (検索条件は保持する)。"""
    parts = urlsplit(url)
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k != "page"]
    query.append(("page", str(page)))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


class StreamReq18923IsleOfManTravelAgents(StaticCrawler):
    """isleofman.com ビジネスディレクトリ (travel agents / マン島) スクレイパー"""

    DELAY = 1.0
    CONTINUE_ON_ERROR = True
    TIMEOUT = 30

    EXTRA_COLUMNS = [
        "国",
        "郵便番号(英国)",
        "TEL(国際表記)",
        "TEL(原文)",
        "FAX",
        "町名",
        "住所(全文)",
        "LinkedInアカウント",
        "YouTubeアカウント",
        "掲載区分",
        "スラッグ",
    ]

    # ------------------------------------------------------------------ utils

    def _get_page_props(self, page_url: str) -> dict | None:
        """Inertia.js の data-page 属性から props を取り出す。

        取得できない / JSON が壊れている場合は None を返す
        (CONTINUE_ON_ERROR に任せ、黙って握り潰さずログに残す)。
        """
        soup = self.get_soup(page_url)
        if soup is None:
            logger.warning("ページを取得できませんでした: %s", page_url)
            return None

        node = soup.select_one(_DATA_PAGE_SELECTOR)
        if node is None:
            logger.warning("data-page 属性が見つかりません: %s", page_url)
            return None

        try:
            data = json.loads(node["data-page"])
        except (ValueError, KeyError) as exc:
            logger.warning("data-page の JSON を解析できません (%s): %s", page_url, exc)
            return None

        props = data.get("props")
        return props if isinstance(props, dict) else None

    @staticmethod
    def _primary_address(business: dict) -> dict:
        """addresses[] の先頭 (実測でも 1 件のみ) を返す。空なら空 dict。"""
        for address in business.get("addresses") or []:
            if isinstance(address, dict):
                return address
        return {}

    @classmethod
    def _is_isle_of_man(cls, address: dict) -> bool:
        """マン島所在かどうかを判定する (フィルタ A)。"""
        postcode = _clean(address.get("postcode"))
        if postcode:
            return bool(_IOM_POSTCODE_RE.match(postcode))
        full = _clean(address.get("full_address"))
        return bool(_IOM_IN_ADDRESS_RE.search(full)) and not _UK_MAINLAND_RE.search(full)

    @classmethod
    def _is_travel_agency(cls, business: dict) -> bool:
        """旅行会社とみなせるかどうかを判定する (フィルタ B / C)。"""
        name = _clean(business.get("name"))
        if not name:
            return False
        if name.lower() in _DEFUNCT_NAMES:
            return False
        if _NON_AGENCY_NAME_RE.search(name):
            return False
        slugs = {
            _clean(category.get("slug")).lower()
            for category in (business.get("categories") or [])
            if isinstance(category, dict)
        }
        # 旅行以外のカテゴリ (金融など) が付いている事業者は旅行会社ではないとみなす
        return bool(slugs) and slugs.issubset(_TRAVEL_CATEGORY_SLUGS)

    @staticmethod
    def _split_phones(address: dict) -> tuple[str, str]:
        """phones[] を (電話番号の原文, FAX の原文) に分ける。

        suffix が "Fax" のものだけを FAX として扱い、それ以外 ("Phone" / 空) は
        電話番号とする。複数ある場合は " / " で連結する。
        """
        tels: list[str] = []
        faxes: list[str] = []
        for phone in address.get("phones") or []:
            if not isinstance(phone, dict):
                continue
            number = _clean(f"{_clean(phone.get('prefix'))}{_clean(phone.get('tel'))}")
            if not number:
                continue
            if _clean(phone.get("suffix")).lower() == "fax":
                faxes.append(number)
            else:
                tels.append(number)
        return " / ".join(tels), " / ".join(faxes)

    @staticmethod
    def _opening_hours(business: dict) -> str:
        """opening_hours[] を "Mon-Sat 9:00am To 5:30pm" 形式で連結する。"""
        parts: list[str] = []
        for entry in business.get("opening_hours") or []:
            if not isinstance(entry, dict):
                continue
            day = _clean(entry.get("day"))
            hours = _clean(entry.get("hours"))
            joined = " ".join(p for p in (day, hours) if p)
            if joined:
                parts.append(joined)
        return " / ".join(parts)

    # ------------------------------------------------------------------ parse

    def parse(self, url: str) -> Generator[dict, None, None]:
        """一覧 (引数 url) を起点に、マン島の旅行会社を 1 件ずつ yield する。"""
        page = 1
        last_page = 1
        matched = 0
        seen_slugs: set[str] = set()

        while page <= last_page and page <= _MAX_PAGES_GUARD:
            list_url = _page_url(url, page)
            props = self._get_page_props(list_url)
            if props is None:
                # 1 ページ目が取れない場合は構造変化とみなして失敗させる
                if page == 1:
                    raise RuntimeError(f"一覧ページを取得できませんでした: {list_url}")
                logger.warning("ページ %d をスキップします", page)
                page += 1
                continue

            businesses = props.get("businesses") or {}
            meta = businesses.get("meta") or {}
            if page == 1:
                last_page = int(meta.get("last_page") or 1)
                total = meta.get("total")
                if total:
                    # フィルタ前の総件数。ETA 表示の目安として設定する
                    self.total_items = int(total)
                logger.info("検索結果: 全 %s 件 / %d ページ", total, last_page)

            rows = businesses.get("data") or []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                slug = _clean(row.get("slug"))
                if not slug or slug in seen_slugs:
                    continue
                seen_slugs.add(slug)

                address = self._primary_address(row)
                if not self._is_isle_of_man(address):
                    logger.debug("マン島外のため除外: %s", _clean(row.get("name")))
                    continue
                if not self._is_travel_agency(row):
                    logger.debug("旅行会社ではないため除外: %s", _clean(row.get("name")))
                    continue

                detail_url = urljoin(url, f"{_DETAIL_PATH}{slug}")
                item = self._build_item(detail_url, row)
                if item is None:
                    continue
                matched += 1
                yield item

            page += 1

        logger.info("マン島の旅行会社として抽出: %d 件", matched)

    def _build_item(self, detail_url: str, row: dict) -> dict | None:
        """詳細ページを取得し、一覧の情報と合わせて 1 件分の dict を組み立てる。

        詳細が取得できない場合は一覧の情報だけで出力する
        (website_url / メール / SNS は空になる)。
        """
        detail_props = self._get_page_props(detail_url)
        business = (detail_props or {}).get("business")
        if not isinstance(business, dict):
            logger.warning("詳細ページを取得できませんでした (一覧の情報のみ出力): %s", detail_url)
            business = row

        address = self._primary_address(business) or self._primary_address(row)
        name = _clean(business.get("name")) or _clean(row.get("name"))
        if not name:
            return None

        tel_raw, fax_raw = self._split_phones(address)
        tel_intl = _format_phone(tel_raw.split(" / ")[0]) if tel_raw else ""
        postcode = _clean(address.get("postcode"))
        town = _clean((address.get("town") or {}).get("name")) if isinstance(address.get("town"), dict) else ""
        website = _clean(business.get("website_url"))
        categories = " / ".join(
            _clean(category.get("name"))
            for category in (business.get("categories") or [])
            if isinstance(category, dict) and _clean(category.get("name"))
        )

        return {
            Schema.NAME: name,
            # 英国式郵便番号のため基盤の正規化で空になり得る。原文は EXTRA にも残す
            Schema.POST_CODE: postcode,
            Schema.ADDR: _clean(address.get("full_address")),
            Schema.TEL: tel_intl,
            Schema.HP: website,
            Schema.WEBSITE: website,
            Schema.EMAIL: _clean(business.get("email")),
            Schema.FB: _clean(business.get("facebook_url")),
            Schema.INSTA: _clean(business.get("instagram_url")),
            Schema.X: _clean(business.get("twitter_url")),
            Schema.CAT_SITE: categories,
            Schema.TIME: self._opening_hours(business),
            Schema.URL: detail_url,
            "国": _COUNTRY,
            "郵便番号(英国)": postcode,
            "TEL(国際表記)": tel_intl,
            "TEL(原文)": tel_raw,
            "FAX": _format_phone(fax_raw.split(" / ")[0]) if fax_raw else "",
            "町名": town,
            "住所(全文)": _clean(address.get("full_address")),
            "LinkedInアカウント": _clean(business.get("linkedin_url")),
            "YouTubeアカウント": _clean(business.get("youtube_url")),
            "掲載区分": _clean(business.get("tier")),
            "スラッグ": _clean(business.get("slug")) or _clean(row.get("slug")),
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    scraper = StreamReq18923IsleOfManTravelAgents()
    scraper.execute("https://www.isleofman.com/businesses?search=travel%20agents&page=1")
