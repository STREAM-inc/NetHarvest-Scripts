"""
ツクリンク北陸3県 — 富山県・石川県・福井県を拠点とする建設業者の会社概要を収集する。

取得対象:
    - 県別一覧 /{pref_slug} (toyama / ishikawa / fukui) に掲載された全建設業者
      (2026-09 時点 富山 7,681 / 石川 8,569 / 福井 6,229 = 約 22,479 社)
    - 各社の詳細ページ /{pref_slug}/city_{code}/{company_id} の構造化情報

取得フロー:
    1. 一覧 /{pref_slug}?page=N を 1 ページ (20 件) ずつ取得
    2. li.p-companies-list-item から会社ID・詳細URL・主力工事を拾う
    3. 詳細ページを 1 件取得するごとに即 yield (Pattern B)
    4. 富山/石川/福井 の 3 県分を順に巡回。会社IDで重複排除

サイト仕様メモ:
    - 一覧ページは Accept ヘッダが無いと HTTP 400 を返すため _setup() で付与する
    - 詳細ページの会社概要は table/dl ではなく
      h3.p-companies-show-detail__heading (大見出し)
      → h4.p-companies-show-detail__heading--small (項目名)
      → h5.p-companies-show-detail__heading--xs (一般/特定 等)
      → 続く兄弟要素が値 という見出し駆動レイアウト
    - 認証ラベルは未認証でも span が出力され class に not-certified が付く
      (= 付いていないものだけが認証済み)
    - 電話番号は会員限定のため公開ページには存在しない
    - 会社紹介文 (ご挨拶) / 事業内容 / 主要取引先 / 担当者メッセージ /
      募集案件本文 は自由記述の長文のため著作権配慮で取得しない

実行方法:
    # ローカルテスト
    python scripts/sites/construction/tsukulink_4.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id tsukulink_4
"""

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

# 備考で指定された対象 3 県の URL スラッグ (北陸3県)
_PREF_SLUGS = ("toyama", "ishikawa", "fukui")

# 備考で指定された対象都道府県名 (この 3 県以外は parse() で除外する)
_TARGET_PREFS = ("富山県", "石川県", "福井県")

# 住所から都道府県を切り出す。〒付き郵便番号が前置されているため
# 「.{2,3}県」のようなワイルドカードでは番号側を誤って拾う。47都道府県を明示列挙する。
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

# 一覧アイテムの詳細 URL パターン (/toyama/city_162019/857767)
_DETAIL_PATH = re.compile(r"^/[a-z]+/city_\d+/\d+$")


class TsukulinkHokuriku(StaticCrawler):
    """ツクリンク北陸3県 スクレイパー"""

    DELAY = 1.0

    EXTRA_COLUMNS = [
        "会社ID",
        "市区町村",
        "評価点",
        "企業ラベル",
        "認証許可ラベル",
        "ツクリンク認証項目",
        "建設業許可番号",
        "建設業許可業種(一般)",
        "建設業許可業種(特定)",
        "その他許認可",
        "対応可能工事種別",
        "主力工事",
        "スキル・技術",
        "主な施工工事区分",
        "主な建物種別",
        "技術者資格保有状況",
        "対応可能エリア",
        "インボイス登録",
        "表彰歴件数",
        "実績件数",
        "ブログ件数",
        "レビュー件数",
    ]

    # ------------------------------------------------------------------ setup

    def _setup(self):
        """一覧ページが Accept ヘッダ必須 (無いと HTTP 400) なのでブラウザ相当を付与する。"""
        super()._setup()
        self.session.headers.update(
            {
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,"
                    "image/avif,image/webp,*/*;q=0.8"
                ),
                "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            }
        )

    # ------------------------------------------------------------------ parse

    def parse(self, url: str):
        """引数 url (県別一覧) を起点に、北陸3県の一覧→詳細を巡回する。"""
        # 引数 url を唯一のルートとし、そこから他 2 県の一覧 URL を導出する
        root_slug = urlparse(url).path.strip("/").split("/")[0]
        slugs = [root_slug] + [s for s in _PREF_SLUGS if s != root_slug]

        seen_ids: set[str] = set()
        total = 0

        for slug in slugs:
            list_root = urljoin(url, f"/{slug}")
            page = 1

            while True:
                soup = self.get_soup(f"{list_root}?page={page}")
                if soup is None:
                    logger.warning("一覧の取得に失敗したため次の県へ: %s p%s", slug, page)
                    break

                items = soup.select("li.p-companies-list-item")
                if not items:
                    logger.info("一覧の終端に到達: %s (page=%s)", slug, page)
                    break

                if page == 1:
                    # 「N件中」表示から総件数を積み上げて ETA を有効化する
                    total_el = soup.select_one(".c-pagination-entries__total")
                    if total_el:
                        digits = re.sub(r"[^0-9]", "", total_el.get_text())
                        if digits:
                            total += int(digits)
                            self.total_items = total

                for li in items:
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
                        item = self._scrape_detail(detail_url, company_id, self._list_extras(li))
                    except Exception as e:  # 個別アイテムの失敗は記録して継続
                        logger.warning("詳細の解析に失敗 (スキップ): %s — %s", detail_url, e)
                        continue
                    if item:
                        yield item

                page += 1

    # ------------------------------------------------------------- list parts

    @staticmethod
    def _list_extras(li) -> dict:
        """一覧アイテムにしか無い項目 (主力工事) を拾う。"""
        extras = {"主力工事": ""}
        for dl in li.select("dl.p-companies-list-item__job-list-item"):
            dt = dl.select_one(".p-companies-list-item__job-list-item__dt")
            dd = dl.select_one(".p-companies-list-item__job-list-item__dd")
            if not dt or not dd:
                continue
            if dt.get_text(strip=True) == "主力工事":
                extras["主力工事"] = _clean(dd.get_text(" ", strip=True))
        return extras

    # ----------------------------------------------------------- detail parts

    def _scrape_detail(self, url: str, company_id: str, list_extras: dict) -> dict | None:
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

        # --- 住所 / 都道府県 / 市区町村 -----------------------------------
        addr_el = soup.select_one(".p-companies-show-profile__info-address")
        # 〒付き郵便番号は Pipeline が住所から分離するのでそのまま渡す
        address = _clean(addr_el.get_text(" ", strip=True)) if addr_el else ""

        breadcrumbs = [
            _clean(a.get_text(" ", strip=True))
            for a in soup.select("ol.breadcrumbs .breadcrumbs__link")
        ]
        bc_pref = next((b for b in breadcrumbs if b in _TARGET_PREFS), "")
        city = ""
        if bc_pref and bc_pref in breadcrumbs:
            idx = breadcrumbs.index(bc_pref)
            if idx + 1 < len(breadcrumbs):
                city = breadcrumbs[idx + 1]

        m = _PREF_PATTERN.search(address)
        pref = m.group(1) if m else bc_pref

        # 備考の指示: 北陸3県 (富山/石川/福井) を拠点とする業者のみ対象
        if pref not in _TARGET_PREFS:
            logger.debug("対象外の都道府県のためスキップ: %s (%s)", url, pref)
            return None

        # --- 建設業許可 / その他許認可 ------------------------------------
        kensetsu_no = _first_text(blocks, ("許認可", "建設業許可", ""))
        ippan = _tidy(_joined(blocks, ("許認可", "建設業許可", "一般")))
        tokutei = _tidy(_joined(blocks, ("許認可", "建設業許可", "特定")))
        other_permits = _other_permits(blocks)

        # --- ツクリンク認証項目 (is-active = 認証済のみ) --------------------
        certifications = "、".join(
            _clean(li.get_text(" ", strip=True))
            for li in soup.select("li.p-companies-show-detail__certifications-item.is-active")
        )

        # --- ヘッダーの各種ラベル -----------------------------------------
        company_labels = "、".join(
            t
            for t in (
                _clean(s.get_text(" ", strip=True))
                for s in soup.select(".c-companies-header-labels__label-text")
            )
            if t
        )
        certified_labels = "、".join(
            t
            for t in (
                _clean(s.get_text(" ", strip=True))
                for s in soup.select(".c-companies-certified-labels span.c-label-black-stroke")
                if "not-certified" not in (s.get("class") or [])
            )
            if t
        )

        # --- 評価点 --------------------------------------------------------
        rating_el = soup.select_one(".p-companies-show-profile__rating .c-rating__score")
        rating = _clean(rating_el.get_text(" ", strip=True)) if rating_el else ""

        counts = _profile_counts(soup)

        # --- HP ------------------------------------------------------------
        hp = ""
        for el in blocks.get(("会社概要", "ウェブサイト", ""), []):
            a = el.select_one("a[href]") if hasattr(el, "select_one") else None
            if a:
                hp = a.get("href", "").strip()
                break
        if not hp:
            hp = _first_text(blocks, ("会社概要", "ウェブサイト", ""))

        item = {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: address,
            Schema.REP_NM: _strip_company_suffix(
                _joined(blocks, ("会社概要", "代表者", ""), sep=" "), name
            ),
            Schema.HP: hp,
            Schema.EMP_NUM: _first_text(blocks, ("会社概要", "従業員数", "")),
            Schema.CAP: _first_text(blocks, ("会社情報", "資本金", "")),
            Schema.OPEN_DATE: _first_text(blocks, ("会社概要", "設立年月日", "")),
            # サイト定義業種・ジャンル: プロフィール直下の業種表記を採用
            Schema.CAT_SITE: _text_of(soup, ".p-companies-show-profile__info-job-type"),
            "会社ID": company_id,
            "市区町村": city,
            "評価点": rating,
            "企業ラベル": company_labels,
            "認証許可ラベル": certified_labels,
            "ツクリンク認証項目": certifications,
            "建設業許可番号": kensetsu_no,
            "建設業許可業種(一般)": ippan,
            "建設業許可業種(特定)": tokutei,
            "その他許認可": other_permits,
            "対応可能工事種別": _tidy(_joined(blocks, ("会社情報", "対応可能工事種別", ""))),
            "主力工事": list_extras.get("主力工事", ""),
            "スキル・技術": _joined(blocks, ("会社情報", "スキル・技術", "")),
            "主な施工工事区分": _list_items(blocks, ("会社情報", "主な施工工事区分", "")),
            "主な建物種別": _list_items(blocks, ("会社情報", "主な建物種別", "")),
            "技術者資格保有状況": _list_items(blocks, ("会社情報", "技術者資格保有状況", "")),
            "対応可能エリア": _job_areas(blocks),
            "インボイス登録": _first_text(blocks, ("会社情報", "インボイス登録の有無", "")),
            # 売上 は Schema.SALES に既定義のため EXTRA ではなく Schema へ格納する
            Schema.SALES: _joined(blocks, ("会社情報", "売上", ""), sep=" / "),
            "表彰歴件数": counts.get("表彰歴", ""),
            "実績件数": counts.get("実績", ""),
            "ブログ件数": counts.get("ブログ", ""),
            "レビュー件数": counts.get("レビュー", ""),
        }

        # 会社情報に無い項目は会社概要側 (レイアウト差異) も見る
        if not item[Schema.CAP]:
            item[Schema.CAP] = _first_text(blocks, ("会社概要", "資本金", ""))
        if not item["インボイス登録"]:
            # h4 が省略されている = 未登録。ヘッダーラベルから判定する
            item["インボイス登録"] = (
                "登録あり" if "インボイス登録あり" in certified_labels else "登録なし"
            )

        return item


# ---------------------------------------------------------------- helpers


def _clean(text: str) -> str:
    """連続空白・NBSP を 1 スペースに畳む。"""
    return re.sub(r"[\s　\xa0]+", " ", text or "").strip()


def _tidy(text: str) -> str:
    """「A 、 B、」のように区切り文字前後に空白/末尾区切りが残る値を整形する。"""
    text = re.sub(r"\s*、\s*", "、", text or "")
    return text.strip("、 ")


def _text_of(soup, selector: str) -> str:
    el = soup.select_one(selector)
    return _clean(el.get_text(" ", strip=True)) if el else ""


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
    """指定見出しに属する値要素のテキスト (空要素は除く) を返す。"""
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


def _list_items(blocks: dict, key: tuple) -> str:
    """値要素が ul の項目は li 単位で結合する (テキスト直結だと語が繋がるため)。"""
    parts = []
    for el in blocks.get(key, []):
        lis = el.select("li") if hasattr(el, "select") else []
        if lis:
            parts.extend(_clean(li.get_text(" ", strip=True)) for li in lis)
        else:
            parts.append(_clean(el.get_text(" ", strip=True)))
    return "、".join(p for p in parts if p)


def _other_permits(blocks: dict) -> str:
    """許認可セクションのうち建設業許可以外 (産業廃棄物収集運搬業許可 等) をまとめる。"""
    grouped: dict[str, list[str]] = {}
    for (h3, h4, h5), _els in blocks.items():
        if h3 != "許認可" or not h4 or h4 == "建設業許可":
            continue
        vals = _values(blocks, (h3, h4, h5))
        if not vals:
            continue
        label = f"{h4}({h5})" if h5 else h4
        grouped.setdefault(label, []).extend(vals)
    return " / ".join(f"{k}: {'、'.join(v)}" for k, v in grouped.items())


def _strip_company_suffix(value: str, company_name: str) -> str:
    """「小川 博司 株式会社オリバー」のように末尾へ会社名が連結された値から会社名を除く。"""
    if value and company_name and value.endswith(company_name) and value != company_name:
        return value[: -len(company_name)].strip()
    return value


def _job_areas(blocks: dict) -> str:
    """対応可能エリアを「地方: 都道府県、都道府県」形式で結合する。"""
    parts = []
    for el in blocks.get(("会社情報", "対応可能エリア", ""), []):
        divs = [
            _clean(d.get_text(" ", strip=True))
            for d in (el.find_all("div", recursive=False) if hasattr(el, "find_all") else [])
        ]
        divs = [d for d in divs if d]
        if len(divs) >= 2:
            parts.append(f"{divs[0]}: {_tidy(' '.join(divs[1:]))}")
        elif divs:
            parts.append(divs[0])
        else:
            txt = _clean(el.get_text(" ", strip=True))
            if txt:
                parts.append(txt)
    return " / ".join(parts)


def _profile_counts(soup) -> dict:
    """プロフィール上部の「表彰歴 N件 / 実績 N件 / ブログ N件 / レビュー N件」を辞書化する。"""
    counts = {}
    container = soup.select_one(".p-companies-show-profile__counts")
    if not container:
        return counts
    for div in container.find_all("div", recursive=False):
        num_el = div.select_one(".p-companies-show-profile__counts-number")
        if not num_el:
            continue
        label = _clean(div.get_text(" ", strip=True)).split(" ")[0]
        counts[label] = _clean(num_el.get_text(strip=True))
    return counts


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    scraper = TsukulinkHokuriku()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://tsukulink.net/toyama")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
