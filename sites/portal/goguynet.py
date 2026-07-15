"""
号外NET — 地域ニュースサイト (goguynet.jp) の「飲食店」情報スクレイパー

取得対象（飲食店のみ）:
    - トップの最新記事フィード (全国横断・?page=N で最大40ページ / 1ページ10件) の
      うち、飲食店（レストラン・カフェ・居酒屋・ラーメン・パン/スイーツ店 等）に
      該当する記事のみを対象とする。
    - 各記事詳細ページの .shop-info（店舗名・住所・営業時間・定休日・最寄り駅・関連リンク）

飲食店フィルタ:
    号外NET はカテゴリが イベント / お店みちゃお / 開店・閉店 等の粒度しか無く、
    「グルメ」「飲食」といったジャンル分類 (タグ・カテゴリ・構造化データ) を持たない。
    コンビニ・ドラッグストア・雑貨店・催事イベント等が同じカテゴリに混在するため、
    記事タイトル（＝店名＋業態を含む文章）に飲食店ジャンル語が含まれるかで判定し、
    フェア・物産展などの一過性イベントは除外する（_is_restaurant / _FOOD_KEYWORDS）。
    ジャンル語の有無は一覧側で先に判定し、飲食店以外は詳細を取得しない。

取得フロー:
    一覧 (a.itemTitle01: タイトル/カテゴリ/掲載日時/詳細URL)
      → 飲食店判定 (_is_restaurant) を通過したものだけ
      → 詳細ページ (.shop-info の dt/dd) を1件ずつ取得して即 yield (Pattern B)

利用規約:
    https://goguynet.jp/about/privacy/ を確認済み。スクレイピング/クローリングを
    明示的に禁止する条項は無し (個人情報・Cookie の取り扱いが中心)。

著作権配慮:
    記事本文 (自由記述プロース) は取得しない。構造化された店舗情報のみを対象とする。

実行方法:
    python scripts/sites/portal/goguynet.py
    docker compose exec worker python /app/bin/run_flow.py --site-id goguynet
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 47 都道府県（住所・パンくずからの都道府県抽出用。京都府/大阪府を切らないよう完全指定）
_PREF_NAMES = (
    "北海道|東京都|京都府|大阪府|"
    "青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|"
    "神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|"
    "滋賀県|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|"
    "愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
_PREF_RE = re.compile("(" + _PREF_NAMES + ")")
# 郵便番号 〒751-0805 / 7510805
_POST_RE = re.compile(r"〒?\s*(\d{3})[-－‐\s]?(\d{4})")
# 日本の電話番号（区切り無しの誤登録 0832561171 にも対応）
_TEL_RE = re.compile(r"0\d{1,3}[-‐−ー－(]?\d{2,4}[-‐−ー－)]?\d{3,4}")
# タイトル先頭の 【新潟市中央区】等の角括弧プレフィックスを除去
_BRACKET_PREFIX = re.compile(r"^[【\[（(][^】\]）)]*[】\]）)]\s*")
# タイトル中の「店名」『イベント名』"..." を店舗・イベント名として抽出
_QUOTE_RE = re.compile(r"[「『“”\"]([^」』“”\"]+)[」』“”\"]")
# 先頭の煽り導入節（「今週末行ける！」「今年も始まります。」等の短い前置き）
_ATTENTION_RE = re.compile(
    r"^(?:今週末|今年も|いよいよ|ついに|遂に|なんと|祝)[^、。！]{0,9}[、。！]\s*"
)
# 先頭の日付・期間トークン列（「7月18日は、」「7/21(火)まで！ 」「2026年7月17日から」）
_DATE_LEAD_RE = re.compile(
    r"^(?:\d{4}年)?\s*\d{1,2}\s*[/月]\s*\d{0,2}\s*日?\s*"
    r"(?:\([^)]*\)|（[^）]*）)?\s*(?:は|から|まで|より|～|~|〜|・|、|！|\s)*"
)
# 末尾の述語（「〜が開催されています。」「〜がオープンします。」等）
_TAIL_RE = re.compile(
    r"(?:が|を|は|も|、)?\s*(?:[0-9０-９/月日\(\)（）〜~\-\s]*)?"
    r"(?:開催|オープン|スタート|リニューアル|グランドオープン|閉店|開店|開業|登場|発売|実施|営業)"
    r"(?:中|されています|されました|されます|される|されて|され|します|しました|する|して|!|！|。)*\s*$"
)
# 先頭の場所プレフィックス「◯◯で」（「カワトク1Fで」「イオン旭川西店で」等）
_LOC_LEAD_RE = re.compile(r"^[^、。！]{1,15}?で")

# 飲食店（レストラン・カフェ・居酒屋・ラーメン店・パン/スイーツ店 等）の業態を示す
# ジャンル語。号外NET には「グルメ」ジャンルの構造化データが無いため、記事タイトル
# （店名＋業態を含む文章）にこれらが含まれるものを飲食店とみなす。誤検出を避けるため、
# 「バー」「パン」「デリ」等の他語に埋没しやすい短い断片は業態が確定する形でのみ列挙する
# （例: ワインバー / パン屋 / ベーカリー）。三軒茶屋・工事の最中 等の地名・慣用句に
# 一致する「茶屋」「最中」等は除外している。
_FOOD_KEYWORDS = re.compile(
    "|".join(
        [
            r"ラーメン", r"らーめん", r"拉麺", r"中華そば", r"つけ麺", r"油そば", r"担々麺", r"麺類", r"製麺", r"麺",
            r"食堂", r"レストラン", r"ダイニング", r"ダイナー", r"食事処", r"お食事処", r"料理店", r"飲食店",
            r"カフェ", r"cafe", r"caf[eé]", r"喫茶", r"珈琲", r"コーヒー", r"茶房", r"甘味処",
            r"居酒屋", r"酒場", r"立呑", r"立ち呑", r"立ち飲", r"呑み", r"ビストロ", r"トラットリア",
            r"スナック", r"ワインバー", r"ビアガーデン", r"ビアバー", r"クラフトビール",
            r"焼肉", r"焼き肉", r"ホルモン", r"もつ鍋", r"もつ焼", r"焼鳥", r"焼き鳥",
            r"串カツ", r"串揚げ", r"串焼", r"やきとり",
            r"寿司", r"鮨", r"寿し", r"回転寿司", r"海鮮", r"刺身",
            r"そば処", r"蕎麦", r"うどん", r"讃岐",
            r"定食", r"牛丼", r"天丼", r"カツ丼", r"海鮮丼", r"丼",
            r"カレー",
            r"パスタ", r"ピザ", r"ピッツァ", r"pizza", r"イタリアン", r"フレンチ",
            r"天ぷら", r"天麩羅", r"とんかつ", r"トンカツ", r"ステーキ", r"ハンバーグ", r"ハンバーガー", r"バーガー",
            r"餃子", r"中華料理", r"韓国料理", r"タイ料理", r"エスニック",
            r"しゃぶしゃぶ", r"すき焼", r"水炊き", r"鉄板焼", r"お好み焼", r"もんじゃ", r"たこ焼", r"たこやき",
            r"パン屋", r"ベーカリー", r"bakery", r"ブーランジェリー", r"ベーグル", r"サンドイッチ",
            r"スイーツ", r"パティスリー", r"ケーキ", r"ドーナツ", r"クレープ", r"たい焼", r"鯛焼",
            r"ジェラート", r"ソフトクリーム", r"かき氷", r"プリン", r"タルト", r"ワッフル", r"パンケーキ",
            r"和菓子", r"洋菓子", r"焼き菓子", r"団子", r"大福",
            r"弁当", r"惣菜", r"おにぎり", r"おむすび",
            r"タピオカ", r"スムージー", r"フルーツパーラー",
            r"屋台", r"フードコート", r"キッチンカー",
            r"割烹", r"料亭", r"懐石", r"会席", r"グリル", r"バイキング", r"食べ放題", r"ビュッフェ",
            r"グルメ",
        ]
    )
)
# 一過性の催事・イベント語。飲食ジャンル語を含んでいても（例: ラーメンフェス、
# ご当地アイスフェア）常設の飲食店ではないため除外する。
_EVENT_WORDS = re.compile(
    r"フェア|フェス|物産展|即売会|マルシェ|マーケット|抽選会|キャンペーン|"
    r"祭り|お祭り|花火|フェスティバル|出店イベント|グルメイベント"
)

# 一覧・詳細のページ送り安全上限（実測は約40ページ。取り逃し防止に余裕を持たせる）
_MAX_PAGES = 60


class GoguynetScraper(StaticCrawler):
    """号外NET (goguynet.jp) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["掲載日時", "地域", "最寄り駅"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        page = 1
        while page <= _MAX_PAGES:
            list_url = url if page == 1 else f"{url}?page={page}"
            soup = self.get_soup(list_url)
            if soup is None:
                break
            anchors = soup.select("a.itemTitle01")
            if not anchors:
                break

            for a in anchors:
                href = a.get("href")
                if not href:
                    continue
                detail_url = urljoin(url, href)

                # 一覧側で確実に取れる情報（カテゴリ・タイトル・掲載日時）を先取り
                title_el = a.select_one("h1.itemTitle01In")
                list_title = title_el.get_text(" ", strip=True) if title_el else ""
                cat_el = a.select_one("span.label-default")
                category = cat_el.get_text(strip=True) if cat_el else ""
                date_el = a.select_one(".listDate01")
                post_date = date_el.get_text(" ", strip=True) if date_el else ""

                # 飲食店以外（イベント・コンビニ・雑貨店等）は詳細を取得せずスキップ。
                # 業態はタイトル（店名＋業態を含む文章）で判定する。
                if not self._is_restaurant(f"{category} {list_title}"):
                    continue

                try:
                    item = self._scrape_detail(detail_url, list_title, category, post_date)
                except Exception as e:  # 個別記事の失敗で全体を止めない
                    self.logger.warning("詳細取得失敗 %s: %s", detail_url, e)
                    continue
                if item:
                    yield item

            page += 1

    @staticmethod
    def _name_from_title(title: str) -> str:
        """記事タイトルから店舗・イベント名を抽出する。

        goguynet の記事タイトルは「【地域】…「店名」…します。」形式の文章で、
        角括弧プレフィックスを除いただけでは日付や煽り文句・述語が混じった
        文章丸ごとが名称になってしまう（例: 「7月18日は、おにクルで手作り
        マルシェ開催！朝採れ野菜の…」）。以下の順で固有名だけを取り出す。

        1. 「」『』"" で囲まれた固有名があれば最優先で採用する。
        2. 角括弧の地域プレフィックス（【茨木市】等）を除去する。
        3. 先頭の煽り導入節（「今週末行ける！」）と日付・期間トークン列
           （「7月18日は、」「7/21(火)まで！ 」）を剥がす。
        4. 最初の節（、。！の手前まで）を取り出し、末尾の述語（「が開催
           されています」「がオープンします」等）を落とす。
        5. 先頭の場所プレフィックス「◯◯で」（「カワトク1Fで」等）を除去する。
        いずれの段階でも空にならないよう、最終的に空なら角括弧除去後の
        タイトルへフォールバックする。
        """
        m = _QUOTE_RE.search(title)
        if m:
            return m.group(1).strip()

        base = _BRACKET_PREFIX.sub("", title).strip()
        s = base
        # 先頭の導入句（煽り節＋日付トークン列）を最大3回まで剥がす
        for _ in range(3):
            stripped = _DATE_LEAD_RE.sub("", _ATTENTION_RE.sub("", s)).strip()
            if stripped == s:
                break
            s = stripped

        # 最初の節のみを名称候補にする（以降は補足説明・列挙のため捨てる）
        first = re.split(r"[、。！\n]", s, maxsplit=1)[0].strip()
        if first:
            s = first

        # 末尾の述語（開催/オープン/…）を除去
        tail_removed = _TAIL_RE.sub("", s).strip()
        if len(tail_removed) >= 3:
            s = tail_removed

        # 先頭の場所プレフィックス「◯◯で」を除去（除去後も3文字以上残る場合のみ）
        lm = _LOC_LEAD_RE.match(s)
        if lm and len(s) - lm.end() >= 3:
            s = s[lm.end():].strip()

        return s or base

    @staticmethod
    def _is_restaurant(text: str) -> bool:
        """記事タイトル（＋カテゴリ）が飲食店に該当するか判定する。

        飲食ジャンル語（_FOOD_KEYWORDS）を含み、かつ一過性の催事・イベント語
        （_EVENT_WORDS。例: ラーメンフェス、ご当地アイスフェア）を含まないものを
        常設の飲食店とみなす。号外NET には飲食ジャンルの構造化データが無いため、
        この文字列判定が唯一の分類手段となる。
        """
        if not text:
            return False
        if _EVENT_WORDS.search(text):
            return False
        return bool(_FOOD_KEYWORDS.search(text))

    def _scrape_detail(
        self, url: str, list_title: str, category: str, post_date: str
    ) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}
        if category:
            data[Schema.CAT_SITE] = category
        if post_date:
            data["掲載日時"] = post_date

        # --- パンくず（都道府県・地域） ---
        crumbs = [
            a.get_text(strip=True)
            for a in soup.select('[class*="bread"] a, [class*="crumb"] a')
        ]
        pref = ""
        region = ""
        for c in crumbs:
            pm = _PREF_RE.search(c)
            if pm and not pref:
                pref = pm.group(1)
                continue
            # 「○○市記事一覧」→ 地域名（都道府県・全国トップ・カテゴリは除く）
            if c.endswith("記事一覧") and "全国" not in c and not pref_only(c):
                region = c.replace("記事一覧", "").strip()
        if region:
            data["地域"] = region

        # --- 店舗情報ブロック ---
        info = soup.select_one(".shop-info")
        name = ""
        if info is not None:
            name_el = info.select_one(".shop-info-name")
            if name_el:
                name = name_el.get_text(" ", strip=True)
            self._parse_shop_info(info, data)

        # 名称: 店舗情報ブロックの店舗名を最優先。無ければ記事タイトルから導出する。
        # タイトルは「南笹口に「麺や大舎厘 南笹口店」がオープンします。」のような文章
        # なので、丸ごと使うと名称が壊れる。まず「」『』で囲まれた店舗・イベント名を
        # 抽出し、それも無ければ角括弧の地域プレフィックスを除いた文章を用いる。
        if not name:
            name = self._name_from_title(list_title)
        if name:
            data[Schema.NAME] = name

        # 都道府県: 住所から導出を優先、無ければパンくずの都道府県
        if data.get(Schema.ADDR):
            am = _PREF_RE.search(data[Schema.ADDR])
            if am:
                pref = am.group(1)
        if pref:
            data[Schema.PREF] = pref

        # TEL 補完: ラベルで取れなかった場合、店舗情報内の電話番号らしき文字列を拾う
        if info is not None and not data.get(Schema.TEL):
            tm = _TEL_RE.search(info.get_text(" ", strip=True))
            if tm:
                data[Schema.TEL] = tm.group(0)

        if not data.get(Schema.NAME):
            return None
        return data

    def _parse_shop_info(self, info, data: dict) -> None:
        """.shop-info-list の dt/dd を走査して各フィールドへ振り分ける。"""
        for row in info.select(".shop-info-row"):
            dt = row.find("dt")
            dd = row.find("dd")
            if dt is None or dd is None:
                continue
            key = dt.get_text(" ", strip=True)
            val = re.sub(r"\s+", " ", dd.get_text(" ", strip=True)).strip()

            if key in ("住所", "所在地"):
                self._set_address(data, val)
            elif key in ("電話番号", "電話", "TEL", "Tel"):
                tm = _TEL_RE.search(val)
                if tm:
                    data[Schema.TEL] = tm.group(0)
                elif val:
                    data[Schema.TEL] = val
            elif key in ("営業時間", "時間"):
                if val:
                    data[Schema.TIME] = val
            elif key == "定休日":
                if val:
                    data[Schema.HOLIDAY] = val
            elif key in ("最寄り駅", "最寄駅", "アクセス"):
                if val:
                    data["最寄り駅"] = val
            elif key in ("関連リンク", "リンク", "URL", "HP", "ホームページ"):
                self._parse_links(dd, data)

    @staticmethod
    def _parse_links(dd, data: dict) -> None:
        """関連リンクの各 href を SNS / 公式サイトに振り分ける。"""
        for a in dd.find_all("a", href=True):
            href = a["href"].strip()
            if not href or href.startswith(("tel:", "mailto:", "#")):
                continue
            low = href.lower()
            if "instagram.com" in low:
                data.setdefault(Schema.INSTA, href)
            elif "twitter.com" in low or "//x.com" in low or ".x.com" in low:
                data.setdefault(Schema.X, href)
            elif "facebook.com" in low:
                data.setdefault(Schema.FB, href)
            elif "tiktok.com" in low:
                data.setdefault(Schema.TIKTOK, href)
            elif "line.me" in low or "lin.ee" in low:
                data.setdefault(Schema.LINE, href)
            else:
                data.setdefault(Schema.HP, href)  # 公式サイト等

    @staticmethod
    def _set_address(data: dict, val: str) -> None:
        m = _POST_RE.search(val)
        if m:
            data[Schema.POST_CODE] = f"{m.group(1)}-{m.group(2)}"
            val = _POST_RE.sub("", val).strip()
        if val:
            data[Schema.ADDR] = val


def pref_only(text: str) -> bool:
    """「新潟県最新記事一覧」のような都道府県のみのパンくずか判定（地域名から除外する）。"""
    return bool(_PREF_RE.fullmatch(text.replace("記事一覧", "").strip()))


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = GoguynetScraper()
    # 🔒 sites.yml に登録する url と完全一致 (SSOT = sites.yml)
    scraper.execute("https://goguynet.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
