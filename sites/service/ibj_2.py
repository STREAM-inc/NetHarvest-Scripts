# sites/service/ibj_2.py
"""
IBJ（日本結婚相談所連盟）加盟相談所 — 全国エリアブラウズ経由スクレイパー

対象サイト: https://www.ibjapan.com/
起点ページ: トップページ
    トップに並ぶ都道府県リンク /area/{slug}/ (北海道〜沖縄+海外) を辿り、
    各エリアの加盟相談所一覧 → 詳細 を全国分取得する。
詳細ページ例: https://www.ibjapan.com/area/hokkaido/05931/

※ 既存 ibj.py は /area/search/multi/?pref_kbn=00201（北海道のみ・検索経路）。
  本クローラー(ibj_2)は **トップ起点で全47都道府県のエリアブラウズ経路**を辿る点が異なる。
  同一相談所が複数エリアに現れる場合に備え、IBJ公認加盟番号(agency_cd)で全国横断デデュープする。

取得フロー (Pattern B: 一覧を集めてから詳細を1件ずつ取得して即 yield):
    1. 起点 URL（トップ）から都道府県エリア URL /area/{slug}/ を抽出
       （/area/{slug}/{加盟番号}/ の詳細 URL や、guide/review 等の非エリアは除外）
       ※ 起点 URL が既に /area/{slug}/ の場合はその1エリアのみを対象にする（ローカルテスト用）
    2. 各エリア一覧を ?page=N で全ページ巡回し、各カードから
       相談所名 / 詳細URL / 加盟番号 / 口コミ採点 / 口コミ件数 / 対応特徴タグ / 掲載エリア を収集
    3. 各詳細ページ /area/{region}/{加盟番号}/ を GET し、th/td 仕様表から
       住所・電話・営業時間・定休日・支払い方法・Webサイト等の構造化情報を抽出
    4. 一覧カードの口コミ情報と詳細の構造化情報をマージして1件ずつ yield

    ※ 詳細ページの th ラベルは相談所ごとに項目数が異なる（最寄り駅・Webサイト等は欠落あり）。
      そのため位置ではなく「th のラベル文字列」を起点に値を引く方式を採用する。

取得フィールド（Schema 準拠。同義カラムは必ず Schema を使用する）:
    Schema.NAME      = 相談所名
    Schema.TEL       = 電話番号（注記を除いた番号のみ）
    Schema.POST_CODE = 郵便番号（住所の〒XXX-XXXX）
    Schema.PREF      = 都道府県
    Schema.ADDR      = 住所（市区町村以降）
    Schema.TIME      = 営業時間
    Schema.HOLIDAY   = 定休日
    Schema.PAYMENTS  = 支払い方法
    Schema.HP        = Webサイト URL
    Schema.SCORES    = 口コミ採点（一覧カードの data-rate）
    Schema.REV_SCR   = 口コミ件数（一覧カードの (n)）
    Schema.CAT_SITE  = サイト定義業種・ジャンル（固定値「結婚相談所」）
    Schema.URL       = 取得した詳細ページ URL
    EXTRA: IBJ公認加盟番号 / 最寄り駅 / キャッシュレス支払い / 出張面談 /
           オンライン面談 / 駐車場 / 婚活カウンセラー資格 / サービス対応地域 /
           IBJ AWARD受賞履歴 / 対応特徴（一覧の特徴タグ）/ 掲載エリア（巡回元都道府県）

取得しないフィールド（除外）:
    - キャッチコピー（p.catch-copy）: 相談所が書いた宣伝用の自由記述文のため著作権リスクで除外
    - 最寄り駅からの道順: 段落形式の自由記述（道案内文）のため著作権リスクで除外
    - 対応エリア展開（北海道・東北 / 関東 …の都道府県羅列）: 全相談所共通の汎用エリアセレクタで
      相談所固有情報ではなく、要約の「サービス対応地域」で代替できるため除外

実行方法:
    python scripts/sites/service/ibj_2.py
    docker compose exec worker python /app/bin/run_flow.py --site-id ibj_2
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

import bs4

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://www.ibjapan.com"
CATEGORY = "結婚相談所"

# /area/ 配下のうち、都道府県エリアではない（取得対象外の）スラッグ
_NON_PREF_SLUGS = {
    "search", "bookmark", "guide", "seikon_episode", "review",
    "history", "pickup",
}
# トップ等から都道府県エリア URL を抽出する: /area/{slug}/ （末尾が数字=詳細 は除外）
_AREA_SLUG_RE = re.compile(r"^(?:https://www\.ibjapan\.com)?/area/([a-z_]+)/$")

_PREF = (
    r"北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
_ADDR_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})?\s*(" + _PREF + r")\s*(.*)")
_TEL_RE = re.compile(r"(0\d{1,3}[-(]?\d{1,4}[-)]?\d{3,4})")


def _norm(s) -> str:
    """空白（半角・全角・改行）を1個の半角スペースに正規化する。None は空文字。"""
    if s is None:
        return ""
    return re.sub(r"[\s　]+", " ", str(s)).strip()


class Ibj2Scraper(StaticCrawler):
    """IBJ（日本結婚相談所連盟）の全国エリアブラウズ経路を巡回するクローラー。"""

    DELAY = 1.5  # 詳細ページアクセス間の待機時間（秒）

    EXTRA_COLUMNS = [
        "IBJ公認加盟番号",      # 加盟相談所の公認番号（コード）
        "最寄り駅",             # 例: JR函館本線 手稲駅から徒歩1分
        "キャッシュレス支払い",  # 可能 / 不可
        "出張面談",             # 可能 / 不可
        "オンライン面談",        # 可能 / 不可
        "駐車場",               # 有 / 無 など
        "婚活カウンセラー資格",  # 例: 認定婚活カウンセラー（初級）
        "サービス対応地域",      # 例: 日本全国、海外 / 北海道
        "IBJ AWARD受賞履歴",    # 例: IBJ AWARD PREMIUM 2023上期 …
        "対応特徴",             # 一覧カードの特徴タグ（成婚者の声/キャッシュレス 等）
        "掲載エリア",           # 巡回元の都道府県名（例: 北海道）
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # --- 1. 起点 URL から対象エリア URL を決定 ---
        area_urls = self._discover_area_urls(url)
        self.logger.info("対象エリア数: %d", len(area_urls))

        # --- 2. 各エリア一覧を全ページ巡回し、カードのメタ情報を収集（加盟番号でデデュープ） ---
        cards: list[dict] = []
        seen_cd: set[str] = set()
        for area_name, area_url in area_urls:
            area_cards = self._collect_list_cards(area_url, area_name)
            for c in area_cards:
                cd = c.get("agency_cd") or c["detail_url"]
                if cd in seen_cd:
                    continue
                seen_cd.add(cd)
                cards.append(c)
            self.logger.info(
                "エリア %s: %d 件（累計 %d 件）", area_name, len(area_cards), len(cards)
            )

        self.total_items = len(cards)
        self.logger.info("全国で %d 件の相談所を収集しました", len(cards))

        # --- 3. 各詳細ページを取得してマージし、1件ずつ yield (Pattern B) ---
        for card in cards:
            detail_url = card["detail_url"]
            try:
                soup = self.get_soup(detail_url)
                if soup is None:
                    self.logger.warning("詳細取得失敗のためスキップ: %s", detail_url)
                    continue
                item = self._parse_detail(soup, detail_url, card)
                if item:
                    yield item
            except Exception as exc:  # 個別エラーはログして継続
                self.error_count += 1
                self.logger.warning("詳細解析エラー (スキップ): %s — %s", detail_url, exc)
                continue

    # -------------------------------------------------------------------------
    # エリア URL 抽出
    # -------------------------------------------------------------------------

    def _discover_area_urls(self, start_url: str) -> list[tuple[str, str]]:
        """起点 URL から (都道府県名, エリアURL) のリストを作る。

        - 起点が /area/{slug}/ の単一エリアならそのエリアのみ。
        - それ以外（トップ等）はページ内の都道府県エリアリンクを全て抽出する。
        """
        m = _AREA_SLUG_RE.match(start_url.rstrip("/") + "/")
        if m and m.group(1) not in _NON_PREF_SLUGS:
            # 単一エリア指定（ローカルテスト等）
            return [(self._area_name_from_url(start_url), start_url)]

        soup = self.get_soup(start_url)
        if soup is None:
            self.logger.warning("起点ページ取得失敗: %s", start_url)
            return []

        areas: list[tuple[str, str]] = []
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            am = _AREA_SLUG_RE.match(a.get("href"))
            if not am:
                continue
            slug = am.group(1)
            if slug in _NON_PREF_SLUGS or slug in seen:
                continue
            seen.add(slug)
            name = _norm(a.get_text()) or slug
            areas.append((name, urljoin(BASE_URL, f"/area/{slug}/")))
        return areas

    @staticmethod
    def _area_name_from_url(url: str) -> str:
        m = _AREA_SLUG_RE.match(url.rstrip("/") + "/")
        return m.group(1) if m else ""

    # -------------------------------------------------------------------------
    # 一覧ページ
    # -------------------------------------------------------------------------

    def _collect_list_cards(self, list_url: str, area_name: str) -> list[dict]:
        """?page=N を巡回し、カードが無くなるまで全相談所のメタ情報を集める。"""
        cards: list[dict] = []
        page = 1
        sep = "&" if "?" in list_url else "?"
        while True:
            page_url = list_url if page == 1 else f"{list_url}{sep}page={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                break
            items = soup.select("li.search-item")
            if not items:
                break
            for it in items:
                card = self._parse_list_card(it, area_name)
                if card:
                    cards.append(card)
            page += 1
        return cards

    def _parse_list_card(self, it: bs4.element.Tag, area_name: str) -> dict | None:
        a = it.select_one("h2.search-item-title a")
        if not a or not a.get("href"):
            return None
        detail_url = urljoin(BASE_URL, a.get("href"))

        rate_el = it.select_one("span.rate")
        rate = rate_el.get("data-rate", "") if rate_el else ""

        rev_el = it.select_one("span.review-total")
        rev = ""
        if rev_el:
            m = re.search(r"\d+", rev_el.get_text())
            rev = m.group(0) if m else ""

        btn = it.select_one("button.c-button-favorite[data-agency-cd]")
        agency_cd = btn.get("data-agency-cd", "") if btn else ""

        support = [
            _norm(p.get_text())
            for p in it.select(".agency-support-containes.pc p.support")
        ]

        return {
            "name": _norm(a.get_text()),
            "detail_url": detail_url,
            "agency_cd": agency_cd,
            "score": rate,
            "review_count": rev,
            "support": "／".join(t for t in support if t),
            "area": area_name,
        }

    # -------------------------------------------------------------------------
    # 詳細ページ
    # -------------------------------------------------------------------------

    def _detail_table(self, soup: bs4.BeautifulSoup) -> dict:
        """詳細ページの仕様表(th/td)を {ラベル: 値} の辞書にする。"""
        data: dict[str, str] = {}
        for th in soup.find_all("th"):
            td = th.find_next_sibling("td")
            if td is None:
                continue
            key = _norm(th.get_text())
            if key and key not in data:
                data[key] = _norm(td.get_text(" "))
        return data

    def _parse_detail(self, soup: bs4.BeautifulSoup, url: str, card: dict) -> dict | None:
        d = self._detail_table(soup)

        # 名称（詳細の相談所名を優先、無ければ一覧カード名 / h1）
        name = d.get("相談所名", "") or card.get("name", "")
        if not name:
            h1 = soup.select_one("h1")
            name = _norm(h1.get_text()) if h1 else ""

        # 電話番号（注記「※営業を目的とした…」を除き番号のみ抽出）
        tel = ""
        if d.get("電話番号"):
            m = _TEL_RE.search(d["電話番号"])
            tel = m.group(1) if m else ""

        # 住所（〒 → 郵便番号 / 都道府県 / 市区町村以降）
        post_code = pref = addr = ""
        addr_src = d.get("住所", "")
        if addr_src:
            m = _ADDR_RE.search(addr_src)
            if m:
                post_code = m.group(1) or ""
                pref = m.group(2)
                addr = _norm(m.group(3))
            else:
                addr = addr_src

        # Webサイト（HP）
        hp = d.get("Webサイト", "")
        if hp and not hp.startswith("http"):
            hp = ""

        return {
            Schema.NAME:      name,
            Schema.TEL:       tel,
            Schema.POST_CODE: post_code,
            Schema.PREF:      pref,
            Schema.ADDR:      addr,
            Schema.TIME:      d.get("営業時間", ""),
            Schema.HOLIDAY:   d.get("定休日", ""),
            Schema.PAYMENTS:  d.get("支払い方法", ""),
            Schema.HP:        hp,
            Schema.SCORES:    card.get("score", ""),
            Schema.REV_SCR:   card.get("review_count", ""),
            Schema.CAT_SITE:  CATEGORY,
            Schema.URL:       url,
            # --- EXTRA_COLUMNS ---
            "IBJ公認加盟番号":     d.get("IBJ公認加盟番号", "") or card.get("agency_cd", ""),
            "最寄り駅":            d.get("最寄り駅", ""),
            "キャッシュレス支払い": d.get("キャッシュレス支払い", ""),
            "出張面談":            d.get("出張面談", ""),
            "オンライン面談":       d.get("オンライン面談", ""),
            "駐車場":              d.get("駐車場", ""),
            "婚活カウンセラー資格": d.get("婚活カウンセラー資格", ""),
            "サービス対応地域":     d.get("サービス対応地域", ""),
            "IBJ AWARD受賞履歴":   d.get("IBJ AWARD受賞履歴", ""),
            "対応特徴":            card.get("support", ""),
            "掲載エリア":          card.get("area", ""),
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

    # ローカルテストは1エリア（北海道 ≈ 53件）に絞って短時間で検証する。
    # 本番（全国巡回）は sites.yml の url（トップページ）で実行される。
    TEST_URL = "https://www.ibjapan.com/area/hokkaido/"

    scraper = Ibj2Scraper()
    scraper.site_name = "ibj_2"
    scraper.execute(TEST_URL)

    print(f"\n取得件数: {scraper.item_count}")
    print(f"出力先:   {scraper.output_filepath}")
