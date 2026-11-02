"""
ツクリンク（中国・四国2県・九州沖縄 プレミアム13県） (tsukulink.net)
— 鳥取・島根・岡山・広島・山口・香川・愛媛・長崎・熊本・大分・宮崎・鹿児島・沖縄の
  プレミアム会員 建設業者スクレイパー

取得対象:
    備考に従い、対象 13 県の県別一覧 (https://tsukulink.net/<slug>) を巡回し、
    一覧カードに「プレミアム」バッジ (span.c-label-premium) が付いた会社のみ
    詳細ページを取得する (非プレミアムの詳細は取得しない)。

取得フロー:
    1. 引数 url (https://tsukulink.net/tottori) を起点に、他 12 県の一覧 URL を
       urljoin でスラッグから導出する (県ページには他県への都道府県ナビが無いため)
    2. 県ごとに {pref_url}?page=N を 1 ページ目から順に取得する。
       一覧は「おすすめ順」でプレミアム会員が先頭ページに固まる
       (2026-11-02 確認: 鳥取 p1 に 2 社 / 広島 p1 に 20 社・p2 に 6 社、以降 0 社)
    3. 備考どおり「プレミアム会員が 3 ページ連続で 0 件」になった時点でその県を打ち切る
       (範囲外ページ = 200 + 0 件でも打ち切る)
    4. プレミアム会員の詳細ページを 1 件取得するごとに即 yield (Pattern B)
    5. 会社IDで重複排除する (PR 枠混入で 20 件を超えるページがある)

取得カラム (備考指定):
    取得日時 / 取得URL / 名称 / 都道府県 / 郵便番号 / 住所 / 代表者名 / HP /
    設立年月日 / サイト定義業種・ジャンル (Schema) と
    市区町村 / 会社ID / 企業ラベル / 認証・許可ラベル / 建設業許可番号 / 一覧掲載順
    (EXTRA_COLUMNS)。
    電話番号は会員ログイン後のみ表示されるマスク項目で公開ページに存在しないため
    取得対象外 (備考「TELは非公開のため不要」)。
    一覧掲載順 = 県別「おすすめ順」一覧での通し番号 (1 始まり、PR 枠も含めて数える)。

サイト仕様メモ (tsukulink_12 / _15 / _16 と共通):
    - 一覧・詳細とも Accept ヘッダが無いと HTTP 400 を返すため _setup() で
      ブラウザ相当ヘッダ (User-Agent / Accept / Accept-Language: ja) を付与する
    - フレームワークの DELAY はアイテム yield 間にしか効かないため、
      _fetch_soup() で HTTP リクエスト間隔を REQUEST_INTERVAL (1.5 秒) 以上空ける。
      並列処理は行わず完全直列
    - robots.txt の Disallow (/map?* /messages* /evaluation_viewed_logs /likes) には
      アクセスしない (一覧カードの /likes リンクは辿らない。取得は一覧と詳細のみ)
    - 詳細ページの会社概要は h3/h4/h5 見出し駆動レイアウト (table/dl 無し)
    - 企業ラベル・認証ラベルの走査範囲は .p-companies-show-profile に限定する
      (サイドバー「おすすめ会社」の他社バッジ混入防止)。認証ラベルは
      未認証でも出力され class に not-certified が付く
    - JSON-LD (Corporation) の postalCode / addressRegion / addressLocality /
      foundingDate / url を優先し、無い会社は本文・パンくずから補完する
    - 利用規約 (/term) にスクレイピング・クローリングの禁止条項は無い
      (2026-11-02 確認)

著作権配慮 (取得しない項目):
    事業内容 / ご挨拶 / 担当者メッセージ / 募集案件本文 は自由記述の長文のため取得しない。

実行方法:
    # ローカルテスト
    python scripts/sites/construction/2_13.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id 2_13
"""

import json
import logging
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

# 住所から都道府県を切り出す。〒付き郵便番号が前置されるため明示列挙する。
_PREFECTURES = (
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県",
    "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
)
_PREF_PATTERN = re.compile("(" + "|".join(_PREFECTURES) + ")")

# 住所 (都道府県除去後) 先頭の市区町村。郡部は「〇〇郡〇〇町」まで含める。
_CITY_RE = re.compile(r"^((?:.+?郡)?.+?[市区町村])")

# 備考で指定された対象 13 県。key = 正式名 / value = URL スラッグ。備考の順で巡回する。
_TARGET_PREFS = {
    "鳥取県": "tottori",
    "島根県": "shimane",
    "岡山県": "okayama",
    "広島県": "hiroshima",
    "山口県": "yamaguchi",
    "香川県": "kagawa",
    "愛媛県": "ehime",
    "長崎県": "nagasaki",
    "熊本県": "kumamoto",
    "大分県": "oita",
    "宮崎県": "miyazaki",
    "鹿児島県": "kagoshima",
    "沖縄県": "okinawa",
}

# 詳細ページ URL (/tottori/city_314013/1236080)
_DETAIL_PATH = re.compile(r"^/[a-z]+/city_\d+/\d+$")

# 郵便番号 (〒683-0853 / 〒6895532)
_POSTCODE_RE = re.compile(r"〒?\s*(\d{3})-?(\d{4})")

# 設立年月日 (2009年09月07日)
_DATE_RE = re.compile(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日")

# 備考「プレミアム会員が 3 ページ連続で 0 件になったらその県は打ち切り」
_MAX_EMPTY_PREMIUM_PAGES = 3

# 1 県あたりのページ送り安全上限 (通常は上の打ち切り条件で数ページで止まる)
_MAX_PAGES = 2000


class Tsukulink213Premium(StaticCrawler):
    """ツクリンク（中国・四国2県・九州沖縄 プレミアム13県） スクレイパー"""

    DELAY = 3.0

    # HTTP リクエスト間の最小間隔 (秒)。備考「1.5秒間隔」。
    # フレームワークの DELAY はアイテム yield 間にしか効かないため自前で制御する。
    REQUEST_INTERVAL = 1.5
    # 400 / 429 を踏んだ際のページ単位リトライ回数
    FETCH_ATTEMPTS = 4

    EXTRA_COLUMNS = [
        "市区町村",
        "会社ID",
        "企業ラベル",
        "認証・許可ラベル",
        "建設業許可番号",
        "一覧掲載順",
    ]

    # ------------------------------------------------------------------ setup

    def _setup(self):
        """ブラウザ相当のフルヘッダ付与 + 400/429 を含む自動リトライを設定する。"""
        super()._setup()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
                ),
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,"
                    "image/avif,image/webp,*/*;q=0.8"
                ),
                "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
                "Upgrade-Insecure-Requests": "1",
            }
        )
        retries = Retry(
            total=4,
            backoff_factor=1.5,
            status_forcelist=[400, 429, 500, 502, 503, 504],
            respect_retry_after_header=True,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self._last_request_at = 0.0

    # ------------------------------------------------------------------ fetch

    def _fetch_soup(self, url: str):
        """REQUEST_INTERVAL 秒以上の間隔を空けて取得し、失敗時はバックオフして再試行する。

        上限到達時は None を返し、呼び出し側で打ち切り/スキップを判断する。
        """
        for attempt in range(self.FETCH_ATTEMPTS):
            wait = self.REQUEST_INTERVAL - (time.monotonic() - self._last_request_at)
            if wait > 0:
                time.sleep(wait)
            soup = self.get_soup(url)
            self._last_request_at = time.monotonic()
            if soup is not None:
                return soup
            if attempt < self.FETCH_ATTEMPTS - 1:
                backoff = min(2 ** (attempt + 1), 30)
                logger.warning(
                    "取得に失敗したため %s 秒待って再試行 (%s/%s): %s",
                    backoff, attempt + 1, self.FETCH_ATTEMPTS - 1, url,
                )
                time.sleep(backoff)
        logger.warning("リトライ上限に到達したため取得を断念: %s", url)
        return None

    # ------------------------------------------------------------------ parse

    def parse(self, url: str):
        """引数 url (鳥取県の一覧) を唯一の起点に、対象 13 県のプレミアム会員を巡回する。"""
        seen_ids: set[str] = set()

        for pref, list_url in self._target_list_urls(url).items():
            rank = 0  # 県別おすすめ順一覧での通し番号
            empty_streak = 0
            for page in range(1, _MAX_PAGES + 1):
                soup = self._fetch_soup(f"{list_url}?page={page}")
                if soup is None:
                    logger.warning("一覧の取得に失敗したため次の県へ: %s p%s", pref, page)
                    break

                items = soup.select("li.p-companies-list-item")
                if not items:
                    logger.info("一覧の終端に到達: %s (page=%s)", pref, page)
                    break

                premium_count = 0
                for li in items:
                    rank += 1
                    if not _is_premium(li):
                        continue
                    premium_count += 1

                    company_id = (li.get("data-company-id") or "").strip()
                    link = li.select_one("a.p-companies-list-item__name[href]")
                    if not link:
                        continue
                    href = link.get("href", "")
                    if not _DETAIL_PATH.match(href):
                        continue
                    if company_id and company_id in seen_ids:
                        continue
                    if company_id:
                        seen_ids.add(company_id)

                    detail_url = urljoin(url, href)
                    try:
                        item = self._scrape_detail(detail_url, company_id, pref, rank)
                    except Exception as e:  # 個別アイテムの失敗は記録して継続
                        logger.warning("詳細の解析に失敗 (スキップ): %s — %s", detail_url, e)
                        continue
                    if item:
                        yield item

                logger.info("%s p%s: プレミアム %s 社 / %s 社", pref, page, premium_count, len(items))
                if premium_count:
                    empty_streak = 0
                else:
                    empty_streak += 1
                    if empty_streak >= _MAX_EMPTY_PREMIUM_PAGES:
                        logger.info(
                            "プレミアム会員が %s ページ連続 0 件のため打ち切り: %s (page=%s)",
                            _MAX_EMPTY_PREMIUM_PAGES, pref, page,
                        )
                        break

    # -------------------------------------------------------- list discovery

    def _target_list_urls(self, url: str) -> dict:
        """起点 url (鳥取県の一覧) から対象 13 県の一覧 URL を導出する。

        戻り値は {"鳥取県": "https://tsukulink.net/tottori", ...}。
        県別ページには他県への都道府県ナビが無いため、起点 url を基準に
        スラッグを urljoin して組み立てる。
        """
        base_path = (urlparse(url).path.rstrip("/") or "/").lower()
        found: dict[str, str] = {}
        for pref, slug in _TARGET_PREFS.items():
            if base_path == f"/{slug}":
                found[pref] = url.rstrip("/")
            else:
                found[pref] = urljoin(url, f"/{slug}")
        return found

    # ----------------------------------------------------------- detail page

    def _scrape_detail(self, url: str, company_id: str, list_pref: str, rank: int) -> dict | None:
        soup = self._fetch_soup(url)
        if soup is None:
            return None

        name_el = soup.select_one("h1.p-companies-show-profile__title")
        if not name_el:
            logger.warning("会社名が見つからないためスキップ: %s", url)
            return None
        name = _clean(name_el.get_text(" ", strip=True))
        if not name:
            return None

        blocks = _labeled_blocks(soup)
        ld = _corporation_ld(soup)
        ld_addr = ld.get("address") or {}

        # --- 郵便番号 / 住所 / 都道府県 / 市区町村 --------------------------
        raw_addr = _text_of(soup, ".p-companies-show-profile__info-address")
        pm = _POSTCODE_RE.search(raw_addr) or _POSTCODE_RE.search(
            str(ld_addr.get("postalCode") or "")
        )
        post_code = f"{pm.group(1)}-{pm.group(2)}" if pm else ""
        full_addr = _clean(_POSTCODE_RE.sub("", raw_addr, count=1))

        breadcrumbs = [
            _clean(a.get_text(" ", strip=True))
            for a in soup.select("ol.breadcrumbs .breadcrumbs__link")
        ]
        bc_pref = next((b for b in breadcrumbs if b in _PREFECTURES), "")

        m = _PREF_PATTERN.match(full_addr)
        pref = (
            (m.group(1) if m else "")
            or _clean(ld_addr.get("addressRegion") or "")
            or bc_pref
            or list_pref
        )
        # Schema.ADDR は市区町村以降 (先頭の都道府県を除く)
        address = full_addr[len(pref):].strip() if pref and full_addr.startswith(pref) else full_addr

        city = _clean(ld_addr.get("addressLocality") or "")
        if not city and bc_pref and breadcrumbs[-1] not in ("ホーム", bc_pref):
            city = breadcrumbs[-1]
        if not city:
            cm = _CITY_RE.match(address)
            city = cm.group(1) if cm else ""

        # --- ラベル (他社カード混入を避けプロフィール内に限定) --------------
        profile = soup.select_one(".p-companies-show-profile")
        company_labels = _join_texts(
            profile.select(".c-companies-header-labels__label-text") if profile else []
        )
        certified_labels = _join_texts(
            s
            for s in (
                profile.select(".c-companies-certified-labels span.c-label-black-stroke")
                if profile
                else []
            )
            if "not-certified" not in (s.get("class") or [])
        )

        # --- 設立年月日 (YYYY-MM-DD に正規化) -------------------------------
        open_date = _normalize_date(_first_text(blocks, ("会社概要", "設立年月日", "")))
        if not open_date:
            open_date = _clean(ld.get("foundingDate") or "")

        # --- HP -------------------------------------------------------------
        hp = ""
        for el in blocks.get(("会社概要", "ウェブサイト", ""), []):
            a = el.select_one("a[href]") if hasattr(el, "select_one") else None
            if a:
                hp = a.get("href", "").strip()
                break
        if not hp:
            # 未公開企業は「現在公開済み自社ホームページはございません」という
            # 案内文が入るため、URL 形式でなければ捨てる
            fallback = _first_text(blocks, ("会社概要", "ウェブサイト", "")) or _clean(
                ld.get("url") or ""
            )
            hp = fallback if re.match(r"^https?://", fallback) else ""

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: address,
            Schema.REP_NM: _strip_company_suffix(
                _joined(blocks, ("会社概要", "代表者", ""), sep=" "), name
            ),
            Schema.HP: hp,
            Schema.OPEN_DATE: open_date,
            Schema.CAT_SITE: _text_of(soup, ".p-companies-show-profile__info-job-type"),
            "市区町村": city,
            "会社ID": company_id,
            "企業ラベル": company_labels,
            "認証・許可ラベル": certified_labels,
            "建設業許可番号": _first_text(blocks, ("許認可", "建設業許可", "")),
            "一覧掲載順": str(rank),
        }


def _is_premium(li) -> bool:
    """一覧カードにプレミアム会員バッジ (c-label-premium / 「プレミアム」) があるか。"""
    for badge in li.select(".c-companies-header-labels span"):
        if "c-label-premium" in (badge.get("class") or []):
            return True
        if badge.get_text(strip=True) == "プレミアム":
            return True
    return False


# ---------------------------------------------------------------- helpers


def _clean(text: str) -> str:
    """連続空白・NBSP を 1 スペースに畳む。"""
    return re.sub(r"[\s　\xa0]+", " ", text or "").strip()


def _tidy(text: str) -> str:
    """「A 、 B、」のように区切り前後の空白・末尾区切りが残る値を整形する。"""
    text = re.sub(r"\s*、\s*", "、", text or "")
    return text.strip("、 ")


def _text_of(soup, selector: str) -> str:
    el = soup.select_one(selector)
    return _clean(el.get_text(" ", strip=True)) if el else ""


def _join_texts(elements, sep: str = "、") -> str:
    return sep.join(t for t in (_clean(e.get_text(" ", strip=True)) for e in elements) if t)


def _labeled_blocks(soup) -> dict:
    """詳細ページの見出し駆動レイアウトを {(h3, h4, h5): [値要素...]} に変換する。

    h3 (大見出し) / h4 (項目名) / h5 (一般・特定等) はいずれも
    div.p-companies-show__section の直下に兄弟として並んでおり、
    見出しの後ろに続く要素がその項目の値になる。
    """
    blocks: dict[tuple[str, str, str], list] = {}
    for section in soup.select("div.p-companies-show__section"):
        h3 = h4 = h5 = ""
        for el in section.find_all(recursive=False):
            if el.name == "h3":
                h3, h4, h5 = _clean(el.get_text(" ", strip=True)), "", ""
            elif el.name == "h4":
                h4, h5 = _clean(el.get_text(" ", strip=True)), ""
            elif el.name == "h5":
                h5 = _clean(el.get_text(" ", strip=True))
            elif el.name in ("hr", "script", "style"):
                continue
            elif h4 or h3:
                blocks.setdefault((h3, h4, h5), []).append(el)
    return blocks


def _values(blocks: dict, key: tuple) -> list[str]:
    """指定見出しに属する値要素のテキスト (空要素・注記は除く) を返す。"""
    out = []
    for el in blocks.get(key, []):
        classes = el.get("class") or []
        if any("masked" in c or "description" in c for c in classes):
            continue  # 「＊＊＊＊」に関する注記や補足説明は値ではない
        txt = _clean(el.get_text(" ", strip=True))
        if txt:
            out.append(txt)
    return out


def _first_text(blocks: dict, key: tuple) -> str:
    vals = _values(blocks, key)
    return vals[0] if vals else ""


def _joined(blocks: dict, key: tuple, sep: str = "、") -> str:
    return sep.join(_values(blocks, key))


def _normalize_date(text: str) -> str:
    """「1951年02月01日」を 1951-02-01 に正規化する。"""
    m = _DATE_RE.search(text or "")
    if not m:
        return _clean(text)
    return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"


def _strip_company_suffix(value: str, company_name: str) -> str:
    """「小川 博司 株式会社オリバー」のように末尾へ会社名が連結された値から会社名を除く。"""
    if value and company_name and value.endswith(company_name) and value != company_name:
        return value[: -len(company_name)].strip()
    return value


def _corporation_ld(soup) -> dict:
    """JSON-LD の Corporation (住所・設立日・従業員数) を返す。無ければ空 dict。"""
    for script in soup.select('script[type="application/ld+json"]'):
        raw = script.string or script.get_text()
        if not raw or "Corporation" not in raw:
            continue
        try:
            data = json.loads(raw)
        except (ValueError, TypeError):
            continue
        if isinstance(data, dict) and data.get("@type") == "Corporation":
            return data
    return {}


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    scraper = Tsukulink213Premium()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://tsukulink.net/tottori")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
