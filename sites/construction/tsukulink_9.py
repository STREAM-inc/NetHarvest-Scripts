"""
ツクリンク【福岡】 (tsukulink.net) — 福岡県の建設業者スクレイパー

取得対象:
    備考「福岡県の建設業者一覧」に従い、福岡県を拠点とする公開企業を全件取得する。
    2026-10 時点 34,910 社 (/fukuoka の総件数表示)。

取得フロー (備考「トップ(/fukuoka)から市区町村別一覧ページを巡回」に対応):
    1. 引数 url (= https://tsukulink.net/fukuoka) を唯一の起点として取得し、
       ページ内の市区町村ナビから /fukuoka/city_<コード> のリンクを列挙する
    2. 各市区町村一覧を {city_url}?page=N で 1 ページ (20 件) ずつ取得
    3. li.p-companies-list-item から会社ID・詳細URL・企業ラベル(プレミアム等)を拾う
    4. 詳細ページを 1 件取得するごとに即 yield (Pattern B)
    5. 会社IDで重複排除 (北九州市/福岡市は親ページと区ページで重複掲載されるため必須)
    6. 市区町村ナビに現れない会社の取りこぼしを防ぐため、最後に県全体の一覧
       {url}?page=N を同じ要領で流し、未取得の会社IDのみ詳細を取得する

備考への対応:
    - 取得カラム (名称/都道府県/住所/代表者名/HP/設立年月日/会社ID/企業ラベル/
      サイト定義業種・ジャンル) をすべて実装する
    - 「企業ラベル（プレミアム表記の有無を含む）」→ 表示ラベル文字列をそのまま
      EXTRA「企業ラベル」に、プレミアム判定を「プレミアム会員」(1/0) に、
      判定根拠の CSS クラス名を「会員ラベルclass原値」に入れる。絞り込みはしない
    - 「TELは非公開のため取得不要」→ Schema.TEL は出力しない (公開ページに存在しない)
    - 「Referer ヘッダが無いと HTTP 400」→ _setup() で Referer/Accept を付与する
    - 「低並列・リクエスト間隔を空ける」→ 逐次処理 + DELAY = 1.0 秒

サイト仕様メモ:
    - 一覧・詳細とも Accept ヘッダが無いと HTTP 400 を返す (User-Agent だけでは不足)
    - 範囲外ページは 200 + 0 件で返るので終端判定に使う。
      総件数は .c-pagination-entries__total
    - 詳細の会社概要は table/dl ではなく
      h3.p-companies-show-detail__heading (大見出し)
      → h4.p-companies-show-detail__heading--small (項目名)
      → 続く兄弟要素が値 という見出し駆動レイアウト
      (見出し・値はいずれも div.p-companies-show__section の直下に並ぶ)
    - 充実した会社には JSON-LD (Corporation) があり foundingDate / addressRegion /
      url を構造化取得できる。無い会社は本文から補完する
    - プレミアムバッジ span.c-label-premium はサイドバー「おすすめ会社」にも出るため、
      一覧 li 内と詳細 .p-companies-show-profile 配下に限定して判定する
    - 利用規約 (/term) にスクレイピング・クローリングの禁止条項は無い (2026-10-05 再確認)
    - 2026-10-08 初回実行が未到着のため、再実行トリガ目的で再 commit (STREAMREQ-21693)

著作権配慮 (取得しない項目):
    ご挨拶 / 事業内容 / 担当者メッセージ / 募集案件本文 は自由記述の長文プロースのため
    取得しない。

実行方法:
    # ローカルテスト
    python scripts/sites/construction/tsukulink_9.py
    # コンテナ実行
    docker compose exec worker python /app/bin/run_flow.py --site-id tsukulink_9
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

# 住所から都道府県を切り出す。〒付き郵便番号が前置されるため
# 「.{2,3}県」のようなワイルドカードでは番号側を誤って拾う → 47 都道府県を明示列挙する。
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

# 備考「福岡県の建設業者一覧」で指定された対象都道府県
_TARGET_PREF = "福岡県"

# 詳細ページ URL (/fukuoka/city_401315/744116)
_DETAIL_PATH = re.compile(r"^/[a-z]+/city_\d+/\d+$")

# 市区町村別一覧 URL (/fukuoka/city_401315)
_CITY_PATH = re.compile(r"^/[a-z]+/city_\d+$")

# プレミアム会員バッジの CSS クラス (= プレミアム表記の有無の判定根拠)
_PREMIUM_CLASS = "c-label-premium"

# 設立年月日 (2016年11月01日)
_DATE_RE = re.compile(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日")

# ページ送りの安全上限 (2026-10 時点の福岡県全体で 1,746 ページ)
_MAX_PAGES = 3000


class TsukulinkFukuoka(StaticCrawler):
    """ツクリンク【福岡】 — 福岡県の建設業者スクレイパー"""

    DELAY = 1.0

    EXTRA_COLUMNS = [
        "会社ID",
        "市区町村",
        "企業ラベル",
        "プレミアム会員",
        "会員ラベルclass原値",
    ]

    # ------------------------------------------------------------------ setup

    def _setup(self):
        """Accept / Referer が無いと HTTP 400 になるためブラウザ相当のヘッダを付与する。"""
        super()._setup()
        self.session.headers.update(
            {
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,"
                    "image/avif,image/webp,*/*;q=0.8"
                ),
                "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
                "Referer": "https://tsukulink.net/",
            }
        )

    # ------------------------------------------------------------------ parse

    def parse(self, url: str):
        """引数 url (福岡県トップ) を唯一の起点として市区町村別一覧 → 詳細を巡回する。"""
        soup = self.get_soup(url)
        if soup is None:
            logger.error("起点ページの取得に失敗: %s", url)
            return

        total_el = soup.select_one(".c-pagination-entries__total")
        if total_el:
            digits = re.sub(r"[^0-9]", "", total_el.get_text())
            if digits:
                self.total_items = int(digits)

        city_urls = self._city_urls(soup, url)
        logger.info("市区町村別一覧を %d 件検出: %s", len(city_urls), url)

        seen_ids: set[str] = set()

        # 1) 市区町村別一覧 (備考で指定された巡回経路)
        for city_url in city_urls:
            yield from self._crawl_list(city_url, url, seen_ids)

        # 2) 取りこぼし防止: 県全体の一覧も流し、未取得の会社だけ詳細を取る
        yield from self._crawl_list(url, url, seen_ids)

    # --------------------------------------------------------------- listing

    @staticmethod
    def _city_urls(soup, root_url: str) -> list[str]:
        """起点ページの市区町村ナビから /fukuoka/city_<コード> を重複なく列挙する。"""
        root_slug = urlparse(root_url).path.strip("/").split("/")[0]
        urls: list[str] = []
        seen: set[str] = set()
        for a in soup.select("a[href]"):
            href = (a.get("href") or "").split("?")[0].split("#")[0]
            if not _CITY_PATH.match(href):
                continue
            if href.strip("/").split("/")[0] != root_slug:
                continue  # 他県の市区町村リンク (関連リンク等) は除外
            if href in seen:
                continue
            seen.add(href)
            urls.append(urljoin(root_url, href))
        return urls

    def _crawl_list(self, list_url: str, root_url: str, seen_ids: set[str]):
        """一覧 {list_url}?page=N を 1 ページずつ辿り、詳細を取得しては即 yield する。"""
        for page in range(1, _MAX_PAGES + 1):
            page_url = list_url if page == 1 else f"{list_url}?page={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                logger.warning("一覧の取得に失敗したため次へ: %s", page_url)
                break

            items = soup.select("li.p-companies-list-item")
            if not items:
                logger.info("一覧の終端に到達: %s (page=%s)", list_url, page)
                break

            for li in items:
                link = li.select_one("a.p-companies-list-item__name[href]")
                if not link:
                    continue
                href = (link.get("href") or "").split("?")[0]
                if not _DETAIL_PATH.match(href):
                    continue

                company_id = (li.get("data-company-id") or "").strip()
                if not company_id:
                    company_id = href.rsplit("/", 1)[-1]
                if company_id in seen_ids:
                    continue
                seen_ids.add(company_id)

                detail_url = urljoin(root_url, href)
                try:
                    item = self._scrape_detail(detail_url, company_id, _list_labels(li))
                except Exception as e:  # 個別アイテムの失敗は記録して継続
                    logger.warning("詳細の解析に失敗 (スキップ): %s — %s", detail_url, e)
                    continue
                if item:
                    yield item
        else:
            logger.warning("ページ上限 %d に到達: %s", _MAX_PAGES, list_url)

    # ----------------------------------------------------------- detail page

    def _scrape_detail(self, url: str, company_id: str, list_labels: dict) -> dict | None:
        soup = self.get_soup(url)
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
        ld = _json_ld(soup)
        ld_addr = ld.get("address") or {}

        # --- 住所 / 都道府県 / 市区町村 -----------------------------------
        address = _text_of(soup, ".p-companies-show-profile__info-address")

        breadcrumbs = [
            _clean(a.get_text(" ", strip=True))
            for a in soup.select("ol.breadcrumbs .breadcrumbs__link")
        ]
        bc_pref = next((b for b in breadcrumbs if b in _PREFECTURES), "")

        m = _PREF_PATTERN.search(address)
        pref = m.group(1) if m else (_clean(ld_addr.get("addressRegion", "")) or bc_pref)

        # 備考の指示: 福岡県の建設業者が対象 (他県拠点の掲載は除外)
        if pref != _TARGET_PREF:
            logger.debug("対象外の都道府県のためスキップ: %s (%s)", url, pref)
            return None

        city = _clean(ld_addr.get("addressLocality", ""))
        if not city and bc_pref in breadcrumbs:
            idx = breadcrumbs.index(bc_pref)
            if idx + 1 < len(breadcrumbs):
                city = breadcrumbs[idx + 1]

        # --- HP (ウェブサイト) ---------------------------------------------
        hp = ""
        for el in blocks.get(("会社概要", "ウェブサイト", ""), []):
            a = el.select_one("a[href]") if hasattr(el, "select_one") else None
            if a and a.get("href"):
                hp = a["href"].strip()
                break
        if not hp:
            hp = _first_text(blocks, ("会社概要", "ウェブサイト", ""))
        if not hp:
            hp = _clean(ld.get("url", ""))

        # --- 設立年月日 (YYYY-MM-DD へ正規化) -------------------------------
        open_date = _normalize_date(
            _first_text(blocks, ("会社概要", "設立年月日", ""))
        ) or _clean(ld.get("foundingDate", ""))

        # --- 企業ラベル (プレミアム表記の有無を含む) -------------------------
        # 詳細はレイアウト差でプロフィール側にラベルが出ない会社があるため、
        # 一覧 li 側の原値とマージする
        detail_labels = _profile_labels(soup)
        labels = _merge_labels(list_labels["labels"], detail_labels["labels"])
        label_classes = _merge_labels(
            list_labels["classes"], detail_labels["classes"]
        )
        premium = "1" if _PREMIUM_CLASS in label_classes else "0"

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: pref,
            # 〒付き郵便番号は Pipeline 側で住所から分離されるのでそのまま渡す
            Schema.ADDR: address,
            Schema.REP_NM: _strip_company_suffix(
                _joined(blocks, ("会社概要", "代表者", ""), sep=" "), name
            ),
            Schema.HP: hp,
            Schema.OPEN_DATE: open_date,
            # サイト定義業種・ジャンル: プロフィール直下の業種表記 (例: 建築、土木、防水)
            Schema.CAT_SITE: _text_of(soup, ".p-companies-show-profile__info-job-type")
            or _first_text(blocks, ("会社情報", "業種", "")),
            "会社ID": company_id,
            "市区町村": city,
            "企業ラベル": "、".join(labels),
            "プレミアム会員": premium,
            "会員ラベルclass原値": " ".join(label_classes),
        }


# ---------------------------------------------------------------- helpers


def _clean(text: str) -> str:
    """連続空白・NBSP を 1 スペースに畳む。"""
    return re.sub(r"[\s　\xa0]+", " ", text or "").strip()


def _text_of(soup, selector: str) -> str:
    el = soup.select_one(selector)
    return _clean(el.get_text(" ", strip=True)) if el else ""


def _label_pairs(scope) -> dict:
    """ラベル span から表示文字列と判定根拠の CSS クラスを取り出す。"""
    labels: list[str] = []
    classes: list[str] = []
    for span in scope.select(".c-companies-header-labels__label-text"):
        text = _clean(span.get_text(" ", strip=True))
        if not text:
            continue
        labels.append(text)
        parent = span.parent
        classes.extend(parent.get("class") or [] if parent else [])
    return {"labels": labels, "classes": classes}


def _list_labels(li) -> dict:
    """一覧アイテム内 (= 他社バッジが混ざらない範囲) の企業ラベルを拾う。"""
    return _label_pairs(li)


def _profile_labels(soup) -> dict:
    """詳細ページのプロフィール内に限定して企業ラベルを拾う。

    サイドバー「おすすめ会社」にも同じ class のバッジが出るため、
    ページ全体を走査すると他社のラベルを拾ってしまう。
    """
    prof = soup.select_one(".p-companies-show-profile")
    if prof is None:
        return {"labels": [], "classes": []}
    return _label_pairs(prof)


def _merge_labels(*groups) -> list[str]:
    """複数ソースのラベル/クラスを出現順を保って重複排除する。"""
    out: list[str] = []
    for group in groups:
        for value in group:
            if value and value not in out:
                out.append(value)
    return out


def _labeled_blocks(soup) -> dict:
    """詳細ページの見出し駆動レイアウトを {(h3, h4, h5): [値要素...]} に変換する。

    h3 (大見出し) / h4 (項目名) / h5 (一般・特定等) はいずれも
    div.p-companies-show__section の直下に兄弟として並んでおり、
    見出しの後ろに続く要素がその項目の値になる (table も dl も使われていない)。
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
    out = []
    for el in blocks.get(key, []):
        txt = _clean(el.get_text(" ", strip=True))
        if txt:
            out.append(txt)
    return out


def _first_text(blocks: dict, key: tuple) -> str:
    vals = _values(blocks, key)
    return vals[0] if vals else ""


def _joined(blocks: dict, key: tuple, sep: str = "、") -> str:
    return sep.join(_values(blocks, key))


def _json_ld(soup) -> dict:
    """JSON-LD の Corporation を返す (無い会社も多いので空 dict フォールバック)。"""
    for script in soup.select('script[type="application/ld+json"]'):
        raw = script.string or script.get_text()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except (ValueError, TypeError):
            continue
        candidates = data if isinstance(data, list) else [data]
        for d in candidates:
            if isinstance(d, dict) and d.get("@type") == "Corporation":
                return d
    return {}


def _normalize_date(value: str) -> str:
    """「2016年11月01日」を YYYY-MM-DD へ正規化する。"""
    m = _DATE_RE.search(value or "")
    if not m:
        return ""
    return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"


def _strip_company_suffix(value: str, company_name: str) -> str:
    """「小川 博司 株式会社オリバー」のように末尾へ会社名が連結された値から会社名を除く。"""
    if value and company_name and value.endswith(company_name) and value != company_name:
        return value[: -len(company_name)].strip()
    return value


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    scraper = TsukulinkFukuoka()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://tsukulink.net/fukuoka")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
