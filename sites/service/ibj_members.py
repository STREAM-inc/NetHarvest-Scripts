# sites/service/ibj_members.py
"""
IBJメンバーズ（結婚相談所）— 全国店舗詳細スクレイパー

対象サイト: https://www.loungemembers.com/
対象ページ: https://www.loungemembers.com/lounge （全国店舗一覧 / 9店舗）

取得フロー:
    各店舗の詳細ページ /{slug} を GET（UTF-8固定）
    → h1・店舗情報セクションから店舗名・住所・営業時間・定休日・アクセスを抽出
    ※ 詳細ページはクラス名が自動生成（sd-xx 等）で不安定なため、
      ラベル文字列（営業時間 / 定休日 / 住所 / アクセス / 〒）を起点にテキスト抽出する。

取得フィールド（Schema 準拠。同義カラムは必ず Schema を使用する）:
    Schema.NAME      = 店舗名（例: IBJメンバーズ東京店）
    Schema.CAT_SITE  = サイト定義業種・ジャンル（結婚相談所）
    Schema.PREF      = 都道府県
    Schema.POST_CODE = 郵便番号（〒XXX-XXXX）
    Schema.ADDR      = 住所（市区町村以降。都道府県は PREF に分離）
    Schema.TEL       = 電話番号（※当サイトには非掲載。外部ディレクトリで店名/住所一致を
                       確認した検証済み番号を STORE_TEL から補完。出典は STORE_TEL のコメント参照）
    Schema.TIME      = 営業時間（例: 10:00〜19:00）
    Schema.HOLIDAY   = 定休日（例: 火曜日）
    Schema.LINE      = LINE公式アカウント URL
    Schema.URL       = 取得した店舗詳細ページ URL
    EXTRA: アクセス（最寄駅＋徒歩分。Schema に該当カラムが無いため独自カラム）

実行方法:
    python scripts/sites/service/ibj_members.py
"""

import re
import sys
from pathlib import Path
from typing import Generator

import bs4

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://www.loungemembers.com"
LIST_URL = f"{BASE_URL}/lounge"

# 店舗一覧ページのリンク（/lounge から確認した9店舗の slug）
STORE_SLUGS: list[str] = [
    "tokyo", "ginza", "yurakucho", "shinjuku", "yokohama",
    "nagoya", "osaka", "kobe", "fukuoka",
]

# 店舗別固定電話（TEL）。
# loungemembers.com は電話番号を一切掲載していない（tel:リンク・JSON-LDとも無し）ため、
# 外部の電話帳・地図ディレクトリで「店名/住所が一致する実掲載番号」のみを採用した。
# 各番号は番号がURLに埋め込まれた検索結果、または掲載ページの JSON-LD telephone で確認済み。
STORE_TEL: dict[str, str] = {
    # 八重洲1-8-17（同一住所の旧ブランド名「結婚情報センター東京本店」ekiten JSON-LD）
    "tokyo":     "03-3243-0033",
    "ginza":     "03-6679-3816",   # ekiten「ＩＢＪ結婚相談所銀座店」JSON-LD
    "yurakucho": "03-5293-2300",   # jpnumber「結婚相談所IBJメンバーズ有楽町店」
    "shinjuku":  "03-6863-7665",   # jpnumber / ivry「IBJ結婚相談所新宿店」(〒160-0023 西新宿1-13-12 一致)
    "yokohama":  "045-316-2300",   # jpnumber「IBJメンバーズ横浜店」
    "nagoya":    "052-388-7662",   # jpnumber「婚活ラウンジIBJメンバーズ名古屋店」
    "osaka":     "06-6344-3647",   # jpnumber「婚活ラウンジIBJメンバーズ大阪店」
    "kobe":      "078-384-5657",   # ivry「ＩＢＪ結婚相談所神戸店」(磯上通8-3-10 一致)
    "fukuoka":   "092-433-0230",   # ekiten「ＩＢＪ結婚相談所福岡店」JSON-LD (博多駅中央街2-1 一致)
}

CATEGORY = "結婚相談所"

_PREF = (
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


def _norm(s) -> str:
    """空白（半角・全角・改行）を1個の半角スペースに正規化する。None は空文字。"""
    if s is None:
        return ""
    return re.sub(r"[\s　]+", " ", str(s)).strip()


def _search(pattern: str, text: str, group: int = 1) -> str:
    m = re.search(pattern, text)
    return m.group(group).strip() if m else ""


class IbjMembersScraper(StaticCrawler):
    """IBJメンバーズ（loungemembers.com）の全国店舗詳細を取得するクローラー。"""

    DELAY = 1.0  # 店舗間の待機時間（秒）

    EXTRA_COLUMNS = [
        "アクセス",  # 最寄駅＋徒歩分。Schema に該当が無いため独自カラム
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        self.total_items = len(STORE_SLUGS)

        for slug in STORE_SLUGS:
            store_url = f"{BASE_URL}/{slug}"
            soup = self._get_soup_utf8(store_url)
            if soup is None:
                self.logger.warning("取得失敗のためスキップ: %s", store_url)
                continue

            item = self._parse_store(soup, store_url, slug)
            if item:
                yield item

    # -------------------------------------------------------------------------
    # 内部メソッド
    # -------------------------------------------------------------------------

    def _get_soup_utf8(self, url: str) -> bs4.BeautifulSoup | None:
        """
        詳細ページを UTF-8 固定で取得する。
        このサイトは Content-Type に charset が無く、apparent_encoding だと
        日本語 UTF-8 を誤検知することがあるため、UTF-8 を明示する。
        """
        self.logger.info("取得中: %s", url)
        try:
            resp = self.session.get(url, timeout=self.TIMEOUT)
            resp.raise_for_status()
            resp.encoding = "utf-8"
            return bs4.BeautifulSoup(resp.text, "html.parser")
        except Exception as exc:
            if self.CONTINUE_ON_ERROR:
                self.error_count += 1
                self.logger.warning("通信エラー (スキップ): %s — %s", url, exc)
                return None
            raise

    def _parse_store(self, soup: bs4.BeautifulSoup, store_url: str, slug: str = "") -> dict | None:
        h1_el = soup.select_one("h1")
        title_el = soup.title
        h1 = _norm(h1_el.get_text()) if h1_el else ""
        title = _norm(title_el.get_text()) if title_el else ""
        text = _norm(soup.get_text("\n"))

        # ── 店舗名 ──（例: 「IBJメンバーズ 東京店 …」→ IBJメンバーズ東京店）
        name = _search(r"(IBJメンバーズ\s*\S+?店)", h1).replace(" ", "")

        # ── 住所（〒 → 郵便番号 / 都道府県 / 市区町村以降） ──
        addr_m = re.search(
            r"〒\s*(\d{3}-?\d{4})\s*" + _PREF + r"\s*(.+?)\s*(?:大きい|アクセス|地図|よくある|無料カウンセリング)",
            text,
        )
        if addr_m:
            post_code = f"〒{addr_m.group(1)}"
            pref = addr_m.group(2)
            addr = re.sub(r"\s+", " ", addr_m.group(3)).strip()
        else:
            post_code = pref = addr = ""

        # ── 営業時間 / 定休日 ──
        hours = _search(r"営業時間\s*(\d{1,2}:\d{2}\s*[〜~]\s*\d{1,2}:\d{2})", text)
        hours = re.sub(r"\s*[〜~]\s*", "〜", hours) if hours else ""
        holiday = _search(r"定\s*休\s*日\s*(.+?)\s*住\s*所", text)

        # ── アクセス（最寄駅＋徒歩分） ──
        station = _search(r"(\S+?駅)\s*から近く", title) or _search(r"(\S+?駅)\s*から近く", h1)
        walk = _search(r"アクセス\s*徒歩\s*(\d+)\s*分", text)
        if station and walk:
            access = f"{station} 徒歩{walk}分"
        elif walk:
            access = f"徒歩{walk}分"
        else:
            access = ""

        # ── LINE公式アカウント（クエリは除去して正規化） ──
        line_el = soup.select_one('a[href*="line.me"]')
        line = line_el.get("href", "").split("?")[0] if line_el else ""

        # TEL は loungemembers.com に掲載が無いため、外部ディレクトリで検証済みの番号を補完する
        tel = STORE_TEL.get(slug, "")

        return {
            Schema.NAME:      name or h1,
            Schema.CAT_SITE:  CATEGORY,
            Schema.PREF:      pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR:      addr,
            Schema.TEL:       tel,
            Schema.TIME:      hours,
            Schema.HOLIDAY:   holiday,
            Schema.LINE:      line,
            Schema.URL:       store_url,
            "アクセス":        access,
        }


# =============================================================================
# ローカル実行用エントリーポイント
# =============================================================================

if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = IbjMembersScraper()
    scraper.site_name = "ibj_members"
    scraper.execute(LIST_URL)

    print(f"\n取得件数: {scraper.item_count}")
    print(f"出力先:   {scraper.output_filepath}")
