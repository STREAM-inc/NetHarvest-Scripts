"""
求人ボックス（フードデリバリー代理店・加盟店開拓）スクレイパー

取得対象:
- 求人ボックスのキーワード検索結果一覧 (/{キーワード}の仕事) のみ
- Uber Eats / 出前館 / Wolt / menu / ロケットナウ 等のフードデリバリーの
  「導入提案・代理店営業・加盟店開拓」を行う法人の求人を 14 キーワードで横断収集する

取得カラム:
- 取得日時 / 取得URL (求人パーマリンク)
- 名称 (会社名) / 都道府県 / 住所
- 求人タイトル / 勤務地 / 給与 / 雇用形態 / 求人本文スニペット
- 検索キーワード / 特徴 / 掲載元サイト / 更新日

備考:
- robots.txt で /jb/ ・ /rd/ は Disallow のため詳細ページには一切遷移しない。
  取得は検索結果一覧ページのみで完結する (/jb/ の URL は掲載URLとして記録だけする)。
- ページ送りは一覧ページの「次のページへ」(a.c-pager_btn--next = ?pg=N) を辿る。
  ?page=N はサイト側で無視され 1 ページ目が返るため使わない。
- 自ら配達する飲食店・配達員/ドライバー募集は対象外のため _is_target() で除外し、
  「フードデリバリー系サービス」×「代理店/加盟店開拓/導入提案」の求人だけを残す。
  フィルタを外して全件出力したい場合は FILTER_ENABLED = False にする。
- 会社名が空の求人 (無記名・人材紹介経由) もそのまま出力する。
- 同一キーワード内の重複 (同じ求人が複数ページに現れる) のみ除去し、
  キーワードをまたいだ重複は後工程で除去するためそのまま出力する。
"""

import html
import json
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import quote, urljoin, urlsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# --- 対象判定用パターン ------------------------------------------------------
# フードデリバリー系サービスを一意に指す語 (単独でフードデリバリー文脈と判断できる)
_DELIVERY_STRONG = re.compile(
    r"出前館|出前舘|uber\s*eats|ubereats|ウーバーイーツ|ウーバー|"
    r"フードデリバリー|フードデリバリ|wolt|ロケットナウ|rocketnow|"
    r"モバイルオーダー|飲食店dx|飲食dx|ゴーストレストラン|クラウドキッチン|"
    r"フードテック|foodtech|デリまる|出前",
    re.I,
)
# 単独では IT の「デリバリー」等と紛らわしいため、食の文脈語との共起を要求する語
_DELIVERY_WEAK = re.compile(
    r"デリバリー|デリバリ|宅配|テイクアウト|中食|(?<![a-z])menu(?![a-z])", re.I
)
_FOOD_CONTEXT = re.compile(r"飲食|レストラン|フード|グルメ|外食|料理|食品|食事|店舗|加盟店")

# 代理店・加盟店開拓・導入提案を指す語
_AGENCY = re.compile(
    r"代理店|販売代理|加盟店|加盟開拓|加盟促進|フランチャイズ|(?<![a-z])fc(?![a-z])|"
    r"アライアンス|パートナーセールス|パートナー営業|導入提案|導入支援|導入コンサル|"
    r"店舗開拓|店舗開発|新規開拓",
    re.I,
)
# 営業・提案系の職種語 (フードデリバリー語がタイトルにある場合の補助条件)
_SALES = re.compile(
    r"営業|セールス|コンサル|提案|開拓|事業開発|ビジネスデベロップメント|bizdev|"
    r"business\s*development|カスタマーサクセス|アライアンス|パートナー",
    re.I,
)
# 自ら配達する側・店舗スタッフ側の求人 (対象外)
_EXCLUDE = re.compile(
    r"配達員|配達パートナー|配達スタッフ|配達ドライバー|配達クルー|デリバリースタッフ|"
    r"宅配ドライバー|配送ドライバー|ルート配送|軽貨物|ドライバー|バイク便|タクシー|"
    r"キッチンスタッフ|ホールスタッフ|調理スタッフ|調理補助|皿洗い|洗い場|"
    r"店長候補|料理長|パティシエ|バリスタ|"
    r"警備|介護|看護|保育|工場|倉庫|仕分け|ピッキング|清掃|"
    r"施工管理|電気工事|土木|大工"
)


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def _norm_value(v) -> str:
    """埋め込み JSON の 'None' / 'null' 文字列を空に正規化する。"""
    s = _clean(v)
    return "" if s in ("None", "null", "undefined") else s


def _extract_pref(text: str) -> str:
    m = _PREF_PATTERN.search(_clean(text))
    return m.group(1) if m else ""


def _strip_pref(addr: str) -> str:
    """勤務地表記から先頭の都道府県を取り除いた市区町村以降を返す。"""
    addr = _clean(addr)
    m = _PREF_PATTERN.match(addr)
    return addr[m.end():].strip() if m else addr


def _safe_json_loads(text: str) -> dict:
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _join_tags(value) -> str:
    """allFeatureTags (list もしくは "['A', 'B']" 文字列) を ' / ' 連結にする。"""
    if isinstance(value, list):
        return " / ".join(_clean(v) for v in value if _clean(v))
    raw = _clean(value)
    if not raw:
        return ""
    tags = re.findall(r"'([^']+)'", raw) or re.findall(r'"([^"]+)"', raw)
    return " / ".join(t.strip() for t in tags if t.strip()) if tags else raw.strip("[]")


def _parse_date(raw: str) -> str:
    m = re.search(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", _clean(raw))
    return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}" if m else ""


class KyujinBoxFoodDeliveryAgencyScraper(StaticCrawler):
    """求人ボックス（フードデリバリー代理店・加盟店開拓）スクレイパー"""

    DELAY = 1.0

    EXTRA_COLUMNS = [
        "求人タイトル",
        "勤務地",
        "給与",
        "雇用形態",
        "求人本文スニペット",
        "検索キーワード",
        "特徴",
        "掲載元サイト",
        "更新日",
    ]

    # 巡回する検索キーワード (先頭は sites.yml の url から導出したキーワードに置き換わる)
    KEYWORDS = [
        "出前館代理店",
        "出前館加盟店開拓",
        "UberEats代理店",
        "ウーバーイーツ代理店",
        "フードデリバリー代理店",
        "フードデリバリー導入提案",
        "フードデリバリー加盟店開拓",
        "Wolt代理店",
        "menu代理店",
        "ロケットナウ代理店",
        "デリバリー導入コンサル",
        "デリバリー代理店営業",
        "飲食店DX代理店営業",
        "モバイルオーダー代理店",
    ]

    # フードデリバリーの代理店営業・加盟店開拓に絞り込むか (False で検索結果を全件出力)
    FILTER_ENABLED = True
    # キーワードあたりの最大ページ数 (実測の最長は 13 ページ)
    MAX_PAGES = 40

    def parse(self, url: str) -> Generator[dict, None, None]:
        for index, (keyword, list_url) in enumerate(self._keyword_urls(url)):
            self.logger.info("検索キーワード[%d]: %s -> %s", index + 1, keyword, list_url)
            yield from self._crawl_keyword(keyword, list_url)

    # -------------------------------------------------------------- URL 組み立て
    def _keyword_urls(self, url: str) -> list[tuple[str, str]]:
        """(検索キーワード, 一覧URL) を巡回順に返す。

        引数 url を唯一のルートとし、1 件目は url そのもの、2 件目以降は
        url と同一オリジン上に /{キーワード}の仕事 を組み立てる。
        """
        root_keyword = self._keyword_of(url)
        pairs: list[tuple[str, str]] = [(root_keyword or self.KEYWORDS[0], url)]

        seen = {root_keyword} if root_keyword else set()
        for keyword in self.KEYWORDS:
            if keyword in seen:
                continue
            seen.add(keyword)
            pairs.append((keyword, urljoin(url, "/" + quote(f"{keyword}の仕事"))))
        return pairs

    @staticmethod
    def _keyword_of(url: str) -> str:
        """一覧 URL のパス (/{キーワード}の仕事) から検索キーワードを取り出す。"""
        from urllib.parse import unquote

        path = unquote(urlsplit(url).path).strip("/")
        return path[:-3] if path.endswith("の仕事") else path

    # ------------------------------------------------------------------ 一覧巡回
    def _crawl_keyword(self, keyword: str, list_url: str) -> Generator[dict, None, None]:
        seen_ids: set[str] = set()
        next_url = list_url

        for page in range(1, self.MAX_PAGES + 1):
            soup = self.get_soup(next_url)
            if soup is None:
                self.logger.warning("一覧取得失敗: %s", next_url)
                break

            cards = soup.select("section.p-result_card")
            if not cards:
                self.logger.info("[%s] %dページ目: カードなし、終了", keyword, page)
                break

            kept = 0
            for card in cards:
                try:
                    item = self._parse_card(card, keyword, next_url)
                except Exception as e:  # 1件の失敗で全体を止めない
                    self.logger.warning("カード解析失敗: %s", e)
                    continue
                if not item:
                    continue

                key = item.pop("_unique_id", "") or _clean(item.get(Schema.URL))
                if key and key in seen_ids:
                    continue
                if key:
                    seen_ids.add(key)
                kept += 1
                yield item

            self.logger.info(
                "[%s] %dページ目: カード%d件 / 出力%d件", keyword, page, len(cards), kept
            )

            next_link = soup.select_one("a.c-pager_btn--next")
            href = _clean(next_link.get("href")) if next_link else ""
            if not href:
                self.logger.info("[%s] %dページ目: 次ページなし、終了", keyword, page)
                break
            next_url = urljoin(next_url, href)

    # ------------------------------------------------------------------ カード解析
    def _parse_card(self, card, keyword: str, list_url: str) -> dict | None:
        a_tag = card.select_one("a.p-result_title_link")
        if not a_tag:
            return None

        preview = self._extract_preview_json(a_tag)

        title = _norm_value(preview.get("formatTitle") or preview.get("title")) or self._text(
            card, "span.p-result_name, .p-result_name"
        )
        company = _norm_value(preview.get("company")) or self._text(
            card, ".p-result_companyName"
        )
        work_area = _norm_value(preview.get("workArea")) or self._text(card, ".p-result_area")
        snippet = self._text(card, ".p-result_lines")

        if not title:
            return None
        if self.FILTER_ENABLED and not self._is_target(title, company, snippet):
            return None

        unique_id = _norm_value(preview.get("uniqueId"))
        return {
            Schema.URL: self._job_url(preview, list_url),
            Schema.NAME: company,
            Schema.PREF: _extract_pref(work_area),
            Schema.ADDR: _strip_pref(work_area),
            "求人タイトル": title,
            "勤務地": work_area,
            "給与": _norm_value(preview.get("payment")) or self._text(card, ".p-result_pay"),
            "雇用形態": _norm_value(preview.get("employType"))
            or self._text(card, ".p-result_employType"),
            "求人本文スニペット": snippet,
            "検索キーワード": keyword,
            "特徴": _join_tags(preview.get("allFeatureTags") or preview.get("featureTagSp")),
            "掲載元サイト": _norm_value(preview.get("siteName")),
            "更新日": _parse_date(preview.get("updatedAt") or ""),
            "_unique_id": unique_id,
        }

    def _is_target(self, title: str, company: str, snippet: str) -> bool:
        """フードデリバリーの代理店営業・加盟店開拓の求人かを判定する。

        自ら配達する側 (配達員・ドライバー) や飲食店の店舗スタッフ募集は除外する。
        """
        if _EXCLUDE.search(title):
            return False

        haystack = " ".join([title, company, snippet])
        if not (self._has_delivery(title) or self._has_delivery(haystack)):
            return False

        if _AGENCY.search(title) and self._has_delivery(haystack):
            return True
        if self._has_delivery(title) and (_AGENCY.search(haystack) or _SALES.search(title)):
            return True
        return False

    @staticmethod
    def _has_delivery(text: str) -> bool:
        """フードデリバリー文脈かを判定する。曖昧語は食の文脈語との共起を要求する。"""
        if _DELIVERY_STRONG.search(text):
            return True
        return bool(_DELIVERY_WEAK.search(text) and _FOOD_CONTEXT.search(text))

    def _extract_preview_json(self, a_tag) -> dict:
        raw = a_tag.get("data-func-show-arg", "")
        if not raw:
            return {}
        inner = _safe_json_loads(raw).get("json")
        if isinstance(inner, dict):
            return inner
        if isinstance(inner, str):
            return _safe_json_loads(html.unescape(inner))
        return {}

    def _job_url(self, preview: dict, list_url: str) -> str:
        """求人ごとの掲載 URL (パーマリンク) を組み立てる。

        robots.txt により /jb/ ・ /rd/ は取得しないが、識別子としては記録する。
        """
        rd_url = _norm_value(preview.get("rdUrl"))
        if rd_url and not rd_url.startswith("/rd"):
            return urljoin(list_url, rd_url)
        unique_id = _norm_value(preview.get("uniqueId"))
        return urljoin(list_url, f"/jb/{unique_id}") if unique_id else list_url

    @staticmethod
    def _text(root, selector: str) -> str:
        el = root.select_one(selector)
        return _clean(el.get_text(" ", strip=True)) if el else ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = KyujinBoxFoodDeliveryAgencyScraper()
    scraper.execute("https://xn--pckua2a7gp15o89zb.com/出前館代理店の仕事")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
