"""
ヒカカク！ 買取業者一覧 (hikakaku.com) スクレイパー

取得対象:
    買取形式別の業者一覧 3 本 (出張 / 宅配 / 店頭) の和集合。
    ルート URL (sites.yml の url = 出張一覧) から他の 2 本を派生させる。
        出張: https://hikakaku.com/company/kaitoriinfo/field/keishiki/syuccho/
        宅配: https://hikakaku.com/company/kaitoriinfo/field/keishiki/takuhai/
        店頭: https://hikakaku.com/company/kaitoriinfo/field/keishiki/tentou/

取得フロー:
    1. 3 本の一覧をそれぞれ ?page=N で巡回する。1 ページ 20 件、カードが 0 件になった
       ページで打ち切る (ページ数は固定値で見積もらず実測で最終ページまで回す)。
    2. 一覧カード (section.coSummaryStore-Wrapper) から
       業者名 / 掲載ページURL / 業者ID / 買取形式 / 対応地域 / 古物商許可番号 / 住所 /
       電話番号 (PR 掲載枠のみ) / 評価スコア / クチコミ件数 / 査定実績件数 を抽出
    3. 掲載ページURL (= 業者詳細 URL) をキーに重複除去する。3 本の一覧に同じ業者が
       現れても処理するのは最初の 1 回だけで、詳細ページへも 1 回しかアクセスしない。
    4. 業者詳細ページ (/company/{id}/) から 取扱ジャンル / 運営会社 / 運営会社の住所 /
       屋号の運営代表者名 / 本店の住所 / 本店の営業時間 / 本店の定休日 / 法人買取対応 /
       LINE査定 / 公式サイト / X アカウントを補完
    5. 1 件取得ごとに即 yield (Pattern B)

買取形式について:
    一覧カードの span.keishiki には対応形式に is_true、非対応形式に is_false が付く
    (詳細ページの「買取形式」は非対応の形式も列挙されるため採用しない)。
    よって 1 枚のカードからその業者が対応する全形式が分かり、
    「出張/店頭」のように "/" 区切りで格納する。

フィルター (備考の指示):
    住所の都道府県が 東京都 / 神奈川県 / 埼玉県 / 千葉県 の業者のみを yield する。
    住所が取得できない (「未記載」等) 業者は、対応地域に「東京」「神奈川」「埼玉」
    「千葉」のいずれかを含む場合に限り残す。
    都道府県が確定した時点で対象外と分かる業者は詳細ページを取得しない
    (詳細側の住所は一覧に住所が無いときの補完にしか使わないため結果は変わらない)。

構造上の注意点:
    - 詳細ページのラベル/値は li.companyShow-InfoCompanyUnorderedList_ListItem の
      **同一 li 内**に格納されている。値要素は <p> と <div> の 2 パターンがあるため、
      「ラベルから次の div を探す」実装では <p> の値がスキップされてラベルと値が
      ズレる (例: 「送料」に古物商許可番号の値が入る)。必ず li 単位で対応付けること。
    - 取扱ジャンルは業者情報リストには無く、「{業者名}の取扱い商品」見出し (h2.headline_bu)
      直後の div.tag_list のリンクにある。詳細ページには同クラスの div が十数個あり
      (関連商品・関連業者の枠) 先頭から取ると別業者の商品名を拾うため、必ず
      「取扱い商品」見出しの直後の 1 つだけを採用する。
    - 一覧の住所は「神奈川県神奈川県横浜市…」のように都道府県が重複することがあるため
      正規化してから PREF / ADDR に分割する。
    - 電話番号は PR 掲載枠にのみ表示され、詳細ページには存在しない
      (実測: 1 ページ 20 件中 3 件のみ)。掲載が無い業者は空文字となる。
    - 「未記載」はサイトの空値プレースホルダなので空文字に変換する。
    - 店舗説明文・キャッチコピー・特記事項・キャンセルポリシー・現金化スピード・送料・
      梱包材・振込手数料・コロナウイルス対策・よくある質問と回答・状態別の買取価格・
      口コミ本文は自由記述の文章のため、著作権リスクを避けて取得しない。

実行方法:
    python scripts/sites/service/hikakaku_3.py
    python bin/run_flow.py --site-id hikakaku_3
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


# 住所先頭の都道府県を切り出すためのパターン (長い名称を先に並べて誤マッチを防ぐ)
_PREF_NAMES = [
    "北海道", "東京都", "大阪府", "京都府",
    "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県",
    "滋賀県", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]
_PREF_PATTERN = re.compile(r"^(" + "|".join(_PREF_NAMES) + r")")

# 業者詳細ページ: /company/{数値}/
_COMPANY_ID_RE = re.compile(r"/company/(\d+)/?$")

# サイトが「値なし」を表すプレースホルダ
_EMPTY_TOKENS = {"", "未記載", "ー", "-", "―", "−", "なし・未記載"}

# 巡回する買取形式別一覧 (ルート URL から派生させる。表示順もこの順)
_KEISHIKI_SLUGS = ("syuccho", "takuhai", "tentou")

# 買取形式の並び順 (カードの span 出現順は 宅配/店頭/出張 なので明示的に並べ替える)
_METHOD_ORDER = ("出張", "宅配", "店頭")

# 備考の指示による絞り込み条件
_TARGET_PREFS = {"東京都", "神奈川県", "埼玉県", "千葉県"}
_TARGET_AREA_KEYWORDS = ("東京", "神奈川", "埼玉", "千葉")

# 一覧ページが 0 件になる前に止めるための安全弁 (実測の最終ページ 322 に余裕を持たせた値)
_MAX_PAGES = 500


def _clean(text: str | None) -> str:
    """全角空白・連続空白を潰し、空値プレースホルダを空文字に正規化する。"""
    if not text:
        return ""
    value = re.sub(r"[\s　]+", " ", text).strip()
    return "" if value in _EMPTY_TOKENS else value


def _norm_label(text: str | None) -> str:
    """ラベルは <br> で改行されることがあるため、空白を全て除いて照合用に正規化する。"""
    if not text:
        return ""
    return re.sub(r"[\s　]+", "", text)


class Hikakaku3Scraper(StaticCrawler):
    """ヒカカク！ 買取業者一覧 (出張/宅配/店頭) スクレイパー"""

    # 既定 UA (Chrome/94) は古いため、実際のブラウザに近い UA を明示する
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
    DELAY = 1.0

    EXTRA_COLUMNS = [
        "業者ID",
        "買取形式",
        "取扱ジャンル",
        "対応地域",
        "古物商許可番号",
        "運営会社",
        "運営会社の住所",
        "本店の住所",
        "LINE査定",
        "法人買取対応",
        "査定実績件数",
    ]

    # 一覧カードから読み取るラベル (空白除去後) → 内部キー
    _LIST_LABELS = {
        "買取形式": "buy_methods",
        "地域": "areas",
        "古物商許可番号": "kobutsu",
        "住所": "address",
        "電話番号": "tel",
    }

    # 詳細ページの業者情報リストから読み取るラベル (空白除去後) → 内部キー
    _DETAIL_LABELS = {
        "古物商許可番号": "kobutsu",
        "住所": "address",
        "運営会社": "company",
        "運営会社の住所": "company_address",
        "屋号の運営代表者名": "representative",
        "本店の住所": "head_office_address",
        "本店の営業時間": "business_hours",
        "本店の定休日": "holiday",
        "法人買取対応有無": "corporate",
        "LINE査定の有無": "line",
    }

    def parse(self, url: str):
        """ルート url (= sites.yml の url) を起点に買取形式別 3 一覧を巡回する。"""
        seen: set[str] = set()
        total = 0

        for list_url in self._list_urls(url):
            total += yield from self._crawl_list(list_url, seen)

        self.logger.info("全一覧の巡回を終了しました (出力=%d件 / 一覧出現=%d件)", total, len(seen))

    @staticmethod
    def _list_urls(url: str) -> list[str]:
        """ルート url から買取形式別の一覧 URL 3 本を導出する (ルート自身を先頭に置く)。"""
        root = url if url.endswith("/") else url + "/"
        urls = [urljoin(root, f"../{slug}/") for slug in _KEISHIKI_SLUGS]

        # 引数で渡された URL を必ず起点として扱う (重複は除く)
        ordered = [url] + [u for u in urls if u.rstrip("/") != url.rstrip("/")]
        return ordered

    def _crawl_list(self, list_url: str, seen: set[str]):
        """一覧 1 本をページ送りで最後まで巡回する。戻り値は yield した件数。"""
        count = 0

        for page in range(1, _MAX_PAGES + 1):
            page_url = list_url if page == 1 else f"{list_url}?page={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                self.logger.warning("一覧ページを取得できませんでした (この一覧を中断): %s", page_url)
                return count

            cards = soup.select("section.coSummaryStore-Wrapper")
            if not cards:
                self.logger.info(
                    "カードが 0 件のため一覧を終了しました (%s page=%d, 出力=%d件)",
                    list_url, page, count,
                )
                return count

            self.logger.info("一覧 %s page=%d: %d件", list_url, page, len(cards))

            for card in cards:
                item = self._parse_card(card, page_url)
                if item is None:
                    continue

                # 3 本の一覧に同じ業者が載るため、掲載ページURL で重複除去する
                detail_url = item[Schema.URL]
                if detail_url in seen:
                    continue
                seen.add(detail_url)

                item = self._build_item(item)
                if item is None:
                    continue

                count += 1
                yield item

        self.logger.warning(
            "上限ページ数 %d に到達したため一覧を終了しました (%s, 出力=%d件)",
            _MAX_PAGES, list_url, count,
        )
        return count

    def _build_item(self, item: dict) -> dict | None:
        """絞り込み条件を適用し、対象であれば詳細ページで補完した item を返す。"""
        pref = item[Schema.PREF]

        if pref:
            # 一覧に住所がある場合、詳細側で上書きされることはないのでここで判定できる
            if pref not in _TARGET_PREFS:
                return None
        elif not any(kw in item["対応地域"] for kw in _TARGET_AREA_KEYWORDS):
            # 住所が無く対応地域にも関東 4 都県が出てこない業者は対象外
            return None

        self._enrich_from_detail(item)

        # 詳細ページで住所が判明した場合は、その都道府県で改めて判定する
        if item[Schema.PREF] and item[Schema.PREF] not in _TARGET_PREFS:
            return None

        return item

    # ------------------------------------------------------------------
    # 一覧カード
    # ------------------------------------------------------------------
    def _parse_card(self, card, page_url: str) -> dict | None:
        """一覧カード 1 件を dict に変換する。名称が取れない場合は None。"""
        link = card.select_one("h3.coSummaryStore-HeadingTitle a")
        if link is None:
            return None

        name = _clean(link.get_text())
        if not name:
            return None

        href = link.get("href", "")
        detail_url = urljoin(page_url, href) if href else ""
        matched = _COMPANY_ID_RE.search(detail_url)
        company_id = matched.group(1) if matched else ""

        values = self._extract_list_values(card)

        # 買取形式: is_true が付いた形式のみが対応中 (is_false は非対応)
        methods = {_clean(span.get_text()) for span in card.select("span.keishiki.is_true")}
        buy_methods = "/".join(m for m in _METHOD_ORDER if m in methods)

        # 電話番号は PR 掲載枠の tel: リンクにのみ存在する
        tel_link = card.select_one('a[href^="tel:"]')
        tel = _clean(tel_link.get_text()) if tel_link else values.get("tel", "")

        score = _clean(self._text_of(card.select_one(".review_rate_number")))
        review_count = _clean(
            self._text_of(
                card.select_one(
                    '.coSummaryStore-GradeItem a[href*="reviews"] .coSummaryStore-CountText'
                )
            )
        )
        achievement_count = _clean(
            self._text_of(
                card.select_one(
                    '.coSummaryStore-GradeItem a[href*="assessment_achievements"] '
                    ".coSummaryStore-CountText"
                )
            )
        )

        pref, addr = self._split_address(values.get("address", ""))

        return {
            Schema.URL: detail_url or page_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.REP_NM: "",
            Schema.CAT_SITE: "",
            Schema.HP: "",
            Schema.X: "",
            Schema.TIME: "",
            Schema.HOLIDAY: "",
            Schema.SCORES: score,
            Schema.REV_SCR: review_count,
            "業者ID": company_id,
            "買取形式": buy_methods,
            "取扱ジャンル": "",
            "対応地域": values.get("areas", ""),
            "古物商許可番号": values.get("kobutsu", ""),
            "運営会社": "",
            "運営会社の住所": "",
            "本店の住所": "",
            "LINE査定": "",
            "法人買取対応": "",
            "査定実績件数": achievement_count,
        }

    def _extract_list_values(self, card) -> dict:
        """一覧カードの「ラベル div → 値 div」ブロックから該当ラベルの値を集める。

        一覧側はラベル (.coSummaryStore-Information_Title) と値
        (.coSummaryStore-Information_Value) が div.coSummaryStore-Information 内に
        ペアで入っているため、その親要素を単位に対応付ける。
        """
        values: dict[str, str] = {}
        for title in card.select(".coSummaryStore-Information_Title"):
            key = self._LIST_LABELS.get(_norm_label(title.get_text()))
            if key is None or key in values:
                continue

            block = title.find_parent(
                lambda tag: tag.name == "div"
                and "coSummaryStore-Information" in (tag.get("class") or [])
            )
            if block is None:
                continue

            value_node = block.select_one(".coSummaryStore-Information_Value")
            if value_node is None:
                # 買取形式のように Value クラスを持たず span で表現される場合
                value_node = block.select_one(".coSummaryStore-Information_ValueWrapper")
            if value_node is None:
                continue

            values[key] = _clean(value_node.get_text(" "))
        return values

    # ------------------------------------------------------------------
    # 詳細ページ
    # ------------------------------------------------------------------
    def _enrich_from_detail(self, item: dict) -> None:
        """業者詳細ページから業者情報を補完する (取得失敗時は一覧の値のまま)。"""
        detail_url = item[Schema.URL]
        if not _COMPANY_ID_RE.search(detail_url):
            return

        soup = self.get_soup(detail_url)
        if soup is None:
            self.logger.warning("詳細ページを取得できませんでした (一覧の値を使用): %s", detail_url)
            return

        values = self._extract_detail_values(soup)

        item[Schema.REP_NM] = values.get("representative", "")
        item[Schema.TIME] = values.get("business_hours", "")
        item[Schema.HOLIDAY] = values.get("holiday", "")
        item["運営会社"] = values.get("company", "")
        item["運営会社の住所"] = values.get("company_address", "")
        item["本店の住所"] = values.get("head_office_address", "")
        item["LINE査定"] = values.get("line", "")
        item["法人買取対応"] = values.get("corporate", "")

        # 取扱ジャンル (Schema.CAT_SITE = サイト定義ジャンル にも同じ値を入れる)
        genres = self._extract_genres(soup)
        item["取扱ジャンル"] = genres
        item[Schema.CAT_SITE] = genres

        # 一覧に住所・古物商許可番号が無い場合は詳細側で補完する
        if not item["古物商許可番号"]:
            item["古物商許可番号"] = values.get("kobutsu", "")
        if not item[Schema.PREF] and not item[Schema.ADDR]:
            pref, addr = self._split_address(values.get("address", ""))
            item[Schema.PREF] = pref
            item[Schema.ADDR] = addr

        # 公式サイト / SNS リンク
        for block in soup.select('[class*="InfoCompanySnsanswer"]'):
            for anchor in block.select("a[href]"):
                href = anchor.get("href", "")
                if not href:
                    continue
                if "twitter.com" in href or "x.com" in href:
                    if not item[Schema.X]:
                        item[Schema.X] = href
                elif "公式サイト" in anchor.get_text() and not item[Schema.HP]:
                    item[Schema.HP] = href

    def _extract_detail_values(self, soup) -> dict:
        """詳細ページの業者情報リストから該当ラベルの値を集める。

        ラベルと値は同一の li.companyShow-InfoCompanyUnorderedList_ListItem 内にあり、
        値要素は <p> / <div> の両パターンが存在する。li 単位で対応付けることで
        「ラベルから次の div を探す」方式で起きるラベルと値のズレを防ぐ。
        """
        values: dict[str, str] = {}
        for li in soup.select("li.companyShow-InfoCompanyUnorderedList_ListItem"):
            label_node = li.select_one(".companyShow-InfoCompanyUnorderedList_InfoCompanyItem")
            answer_node = li.select_one(".companyShow-InfoCompanyUnorderedList_InfoCompanyAnswer")
            if label_node is None or answer_node is None:
                continue

            key = self._DETAIL_LABELS.get(_norm_label(label_node.get_text()))
            if key is None or key in values:
                continue

            values[key] = _clean(answer_node.get_text(" "))
        return values

    @staticmethod
    def _extract_genres(soup) -> str:
        """「{業者名}の取扱い商品」見出し直後の div.tag_list からジャンル名を集める。

        同じ div.tag_list は関連商品・関連業者の枠にも使われているため、
        見出しを手がかりに当該業者の 1 ブロックだけを採用する。
        """
        for heading in soup.select("h2.headline_bu, h3.headline_bu"):
            text = _norm_label(heading.get_text())
            if "取扱い商品" not in text and "取扱商品" not in text:
                continue

            tag_list = heading.find_next_sibling("div", class_="tag_list")
            if tag_list is None:
                continue

            genres = [a.get_text(" ", strip=True) for a in tag_list.select("a")]
            genres = [g for g in (_clean(g) for g in genres) if g]
            if genres:
                return "/".join(dict.fromkeys(genres))
        return ""

    # ------------------------------------------------------------------
    # ヘルパー
    # ------------------------------------------------------------------
    @staticmethod
    def _text_of(node) -> str:
        return node.get_text(" ", strip=True) if node else ""

    @staticmethod
    def _split_address(raw: str) -> tuple[str, str]:
        """住所文字列を都道府県と残りに分割する。都道府県の重複表記も正規化する。"""
        address = _clean(raw)
        if not address:
            return "", ""

        matched = _PREF_PATTERN.match(address)
        if not matched:
            return "", address

        pref = matched.group(1)
        rest = address[len(pref):].lstrip()
        # 「神奈川県神奈川県横浜市…」のような重複表記を取り除く
        while rest.startswith(pref):
            rest = rest[len(pref):].lstrip()
        return pref, rest


if __name__ == "__main__":
    scraper = Hikakaku3Scraper()
    scraper.execute("https://hikakaku.com/company/kaitoriinfo/field/keishiki/syuccho/")
