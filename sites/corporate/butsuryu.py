"""
一般社団法人日本物流団体連合会 (butsuryu.or.jp) — 正会員名簿スクレイパー

取得対象:
    - 正会員名簿（会社名 79 社 / 団体名 15 団体、計約 94 件）
    - 会員名称・ホームページ URL・会員区分（会社名 / 団体名）
    - 各会員 HP から抽出した TEL・郵便番号・住所（best-effort）

取得フロー:
    会員名簿ページ (/about/members) は 1 ページ完結の静的 HTML。
    「会社名」「団体名」の 2 セクション (.s-member__wrap) を順に走査し、
    各 li.s-member__list 内の a.s-member__link から名称・HP・区分を抽出する
    （ページネーション無し）。
    さらに各会員の HP (a.s-member__link の href) へアクセスし、トップページの
    テキストから TEL・郵便番号・住所を正規表現で抽出して補完し、
    1 件取得ごとに即 yield する。

備考:
    - 会員名称・HP は事実情報。TEL / 住所は各会員 HP のトップページから抽出する。
      HP はサイトごとに構造が異なるため best-effort であり、トップに掲載が無い
      会員は TEL / 住所が空になる（セレクタの黙殺ではなく出典側の未掲載）。
    - HP 取得に失敗 (SSL / 403 / タイムアウト等) しても CONTINUE_ON_ERROR により
      名称・HP・区分のみで 1 件として出力する。
    - 利用規約 (/website) の著作権条項はロゴ・画像・商標の再配布に関する規定で、
      スクレイピング・自動アクセスを明示的に禁止する記載は無し。robots.txt も全許可。

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/butsuryu.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id butsuryu
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

# --- HP トップページからの TEL / 住所抽出用の正規表現 ---
_PREFS = (
    "北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    "埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    "岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    "鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    "佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
# TEL ラベル付き電話番号（優先）／ラベル無しの裸の電話番号（fallback）
_TEL_LABELED = re.compile(
    r"(?:TEL|Tel|tel|電話|℡|ＴＥＬ)[\s:：）\)]*"
    r"(0\d{1,4}[-－(（]?\d{1,4}[-－)）]?\d{3,4})"
)
_TEL_BARE = re.compile(r"0\d{1,3}[-－]\d{1,4}[-－]\d{3,4}")
# 郵便番号（〒省略可） + それに続く住所文字列
_POST = re.compile(r"〒?\s*(\d{3})[-－]?\s*(\d{4})")
# 都道府県以降の住所（住所らしくないフィールドの手前で打ち切る）
_ADDR = re.compile(
    r"(?:" + _PREFS + r")[^\s　]?[0-9０-９一二三四五六七八九十丁目番地号"
    r"ぁ-んァ-ヶ一-龥ー\-－()（）\s　]{2,60}"
)
# 住所として不要な後続キーワード（ここで住所を打ち切る）
_ADDR_STOP = re.compile(
    r"(創業|設立|資本金|従業員|代表|TEL|Tel|電話|FAX|Fax|MAP|地図|事業|決算|年月|株式|会社概要)"
)


class Butsuryu(StaticCrawler):
    """一般社団法人日本物流団体連合会 会員名簿スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = []  # Schema 定数で全フィールドを表現できるため追加カラム無し

    def _fetch_contact(self, hp: str) -> dict:
        """会員 HP のトップページから TEL / 郵便番号 / 住所を best-effort 抽出する。

        HP はサイトごとに構造がまちまちなので、ページ全文テキストに対する
        正規表現マッチで抽出する。取得失敗・未掲載時は空文字を返す。
        """
        result = {Schema.TEL: "", Schema.POST_CODE: "", Schema.ADDR: ""}
        if not hp:
            return result

        soup = self.get_soup(hp)  # 失敗時は CONTINUE_ON_ERROR により None
        if soup is None:
            return result

        # <script>/<style> を除去してから可視テキストを連結
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        text = soup.get_text(" ", strip=True)

        # --- TEL: ラベル付きを優先し、無ければ裸の番号 ---
        m = _TEL_LABELED.search(text)
        if m:
            result[Schema.TEL] = m.group(1).strip()
        else:
            m = _TEL_BARE.search(text)
            if m:
                result[Schema.TEL] = m.group(0).strip()

        # --- 郵便番号 ---
        pm = _POST.search(text)
        if pm:
            result[Schema.POST_CODE] = f"{pm.group(1)}-{pm.group(2)}"

        # --- 住所（都道府県以降） ---
        am = _ADDR.search(text)
        if am:
            addr = am.group(0)
            stop = _ADDR_STOP.search(addr)
            if stop:
                addr = addr[: stop.start()]
            result[Schema.ADDR] = addr.strip("　 -－")

        return result

    def parse(self, url: str):
        soup = self.get_soup(url)

        # 各セクション (会社名 / 団体名) の会員リスト
        wraps = soup.select(".s-member__wrap")
        links = soup.select(".s-member__wrap .s-member__link")
        self.total_items = len(links)

        for wrap in wraps:
            # 直前の見出し (.s-member__title) が会員区分 (会社名 / 団体名)
            title_el = wrap.find_previous(class_="s-member__title")
            member_type = title_el.get_text(strip=True) if title_el else ""

            for link in wrap.select("li.s-member__list a.s-member__link"):
                name = link.get_text(strip=True)
                if not name:
                    continue
                href = link.get("href", "").strip()
                hp = urljoin(url, href) if href else ""

                # 会員 HP のトップページから TEL / 住所を補完
                contact = self._fetch_contact(hp)

                yield {
                    Schema.NAME: name,
                    Schema.HP: hp,
                    Schema.TEL: contact[Schema.TEL],
                    Schema.POST_CODE: contact[Schema.POST_CODE],
                    Schema.ADDR: contact[Schema.ADDR],
                    Schema.CAT_SITE: member_type,
                    Schema.URL: url,
                }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = Butsuryu()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.butsuryu.or.jp/about/members")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
