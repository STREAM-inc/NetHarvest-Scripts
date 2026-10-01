"""
職人働 (求人詳細ベース / 掲載日付・エリア絞り込み版) — www.syokunin-doh.com

取得対象:
    求人詳細ページ (https://www.syokunin-doh.com/recruitpost/{企業ID}/{求人ID}) の
    掲載企業情報。既存の syokunin_doh (全国・募集要項重視) とは異なり、本スクリプトは
    「企業名 / 郵便番号 / 住所 / TEL / 事業内容 / 掲載開始日 / 最終更新日 /
    掲載サイトURL / HP」に絞り、下記フィルタを適用した求人単位の一覧を出力する。

取得フロー:
    求人サイトマップ (recruitpost-sitemap.xml) の <url> から <loc> と <lastmod> を
    取り出し、新しい順に並べ替えて 1 件取得するごとに即 yield する (Pattern B)。
    ページネーションは無く、サイトマップ 1 本で全求人 (2026-10 時点 709 件) を網羅する。

    <lastmod> はページ内の「最終更新日」と一致し、常に 掲載開始日 <= 最終更新日 が
    成り立つため、lastmod が基準日より前の URL は詳細を取得せずにスキップする
    (無駄な通信を避けるための事前フィルタ。最終判定は詳細ページの掲載開始日で行う)。

フィルタ条件 (依頼仕様):
    - エリア: 本社住所の都道府県が関東・甲信越・東海の 14 都県のいずれか
    - 掲載開始日が 2025-01-01 以降

ページ構造:
    - 会社情報   : dl.search-mess-company の th/td 表
                   (会社名 / 本社住所 / 従業員数 / 事業内容 / ホームページ)
    - 掲載日付   : div.recruitpost-box-time time span
                   (「掲載開始日：YYYY年MM月DD日」「最終更新日：YYYY年MM月DD日」)
                   → 出力は YYYY-MM-DD に正規化する
    - TEL        : 応募モーダル内の a[href^="tel:"]。未登録の求人は href="tel:" の
                   空プレースホルダなので桁数で弾く
    - 掲載終了した求人は 200 を返しつつ検索トップの体裁になる (会社情報ブロックが無い)
      → 会社名が取れないものはスキップする

利用規約:
    https://www.syokunin-doh.com/terms — 第四条 (禁止事項) にスクレイピング/
    クローリングの明示的な禁止は無い。robots.txt も /recruitpost/ を許可。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/syokunin_doh_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id syokunin_doh_2
"""

import re
import sys
import urllib.parse
from pathlib import Path
from typing import Generator

import bs4

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

SITE_NAME = "職人働"

# --- 依頼フィルタ ---------------------------------------------------------
# 対象エリア (関東・甲信越・東海の 14 都県)
TARGET_PREFS = {
    "東京都", "神奈川県", "埼玉県", "千葉県", "茨城県", "栃木県", "群馬県",
    "新潟県", "山梨県", "長野県", "岐阜県", "静岡県", "愛知県", "三重県",
}
# 掲載開始日の下限 (YYYY-MM-DD の文字列比較で判定する)
MIN_START_DATE = "2025-01-01"

_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_PATTERN = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_DETAIL_PATTERN = re.compile(r"/recruitpost/\d+/\d+/?$")
_JP_DATE_PATTERN = re.compile(r"(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日")
# HP 判定から除外するドメイン (SNS・地図・自サイト等)
_HP_EXCLUDE = (
    "syokunin-doh.com", "facebook.com", "twitter.com", "x.com", "instagram.com",
    "line.me", "youtube.com", "tiktok.com", "google.com", "goo.gl", "indeed.com",
)


def _clean(text: str) -> str:
    """空白・改行を1スペースに正規化する。"""
    return re.sub(r"[ \t　]+", " ", re.sub(r"\s*\n\s*", " ", text or "")).strip()


def _text(node: bs4.element.Tag | None) -> str:
    """タグのテキストを改行区切りで取り出して正規化する。"""
    if node is None:
        return ""
    return _clean(node.get_text("\n", strip=True))


def _lines(node: bs4.element.Tag | None, sep: str = " / ") -> str:
    """複数行に分かれたセル (事業内容など) を区切り文字で連結する。"""
    if node is None:
        return ""
    parts = [_clean(p) for p in node.get_text("\n", strip=True).split("\n")]
    return sep.join(p for p in parts if p)


def _to_iso_date(text: str) -> str:
    """「2026年08月31日」→「2026-08-31」。取れない場合は空文字。"""
    m = _JP_DATE_PATTERN.search(text or "")
    if not m:
        return ""
    return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"


class SyokuninDoh2Scraper(StaticCrawler):
    """職人働 (エリア・掲載開始日フィルタ付き) スクレイパー"""

    DELAY = 1.0
    CONTINUE_ON_ERROR = True

    EXTRA_COLUMNS = [
        "掲載開始日",
        "最終更新日",
        "掲載サイト名",
        "掲載サイトURL",
    ]

    # ------------------------------------------------------------------
    # 一覧 (サイトマップ) → 詳細
    # ------------------------------------------------------------------
    def parse(self, url: str) -> Generator[dict, None, None]:
        """サイトマップの求人 URL を新しい順に巡回し、1 件ずつ yield する。"""
        soup = self.get_soup(url)
        if soup is None:
            self.logger.error("サイトマップを取得できませんでした: %s", url)
            return

        entries = self._sitemap_entries(soup, url)
        self.logger.info("サイトマップの求人 URL 件数: %d", len(entries))

        # lastmod (= 最終更新日) が基準日より前なら掲載開始日も必ず基準日より前
        targets = [(u, lm) for u, lm in entries if not lm or lm >= MIN_START_DATE]
        # 新しい順 (lastmod 降順) に巡回する
        targets.sort(key=lambda e: e[1], reverse=True)
        self.logger.info("掲載開始日フィルタの候補件数: %d", len(targets))

        for detail_url, _lastmod in targets:
            item = self._parse_detail(detail_url)
            if item:
                yield item

    @staticmethod
    def _sitemap_entries(
        soup: bs4.BeautifulSoup, base_url: str
    ) -> list[tuple[str, str]]:
        """サイトマップから (詳細URL, lastmod の YYYY-MM-DD) の一覧を作る。"""
        entries: list[tuple[str, str]] = []
        seen: set[str] = set()
        for node in soup.find_all("url"):
            loc = node.find("loc")
            if loc is None:
                continue
            detail_url = urllib.parse.urljoin(base_url, _clean(loc.get_text()))
            if not _DETAIL_PATTERN.search(urllib.parse.urlparse(detail_url).path):
                continue
            if detail_url in seen:
                continue
            seen.add(detail_url)
            lastmod_node = node.find("lastmod")
            lastmod = _clean(lastmod_node.get_text())[:10] if lastmod_node else ""
            entries.append((detail_url, lastmod))
        return entries

    # ------------------------------------------------------------------
    # 詳細ページ
    # ------------------------------------------------------------------
    def _parse_detail(self, detail_url: str) -> dict | None:
        """求人詳細ページから 1 件分のデータを組み立てる (フィルタ適用込み)。"""
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        for tag in soup(["script", "style", "noscript", "svg"]):
            tag.decompose()

        company = self._company_block(soup)
        name = company.get("会社名", "")
        if not name:
            # 掲載終了した求人は検索トップの体裁で 200 が返る
            self.logger.debug("会社情報ブロックがありません (掲載終了?): %s", detail_url)
            return None

        start_date, update_date = self._dates(soup)
        if not start_date or start_date < MIN_START_DATE:
            return None

        head_office = company.get("本社住所", "")
        post_code = ""
        m = _POST_PATTERN.search(head_office)
        if m:
            post_code = m.group(1)
            head_office = _clean(_POST_PATTERN.sub("", head_office, count=1))

        m = _PREF_PATTERN.search(head_office)
        pref = m.group(1) if m else ""
        if pref not in TARGET_PREFS:
            return None

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: head_office,
            Schema.TEL: self._pick_tel(soup),
            Schema.EMP_NUM: company.get("従業員数", ""),
            Schema.LOB: company.get("事業内容", ""),
            Schema.HP: self._pick_hp(soup, company),
            "掲載開始日": start_date,
            "最終更新日": update_date,
            "掲載サイト名": SITE_NAME,
            "掲載サイトURL": detail_url,
        }

    # ------------------------------------------------------------------
    # 抽出ヘルパー
    # ------------------------------------------------------------------
    @staticmethod
    def _company_block(soup: bs4.BeautifulSoup) -> dict[str, str]:
        """dl.search-mess-company の th/td 表を {見出し: 値} の辞書にする。"""
        result: dict[str, str] = {}
        block = soup.select_one("dl.search-mess-company")
        if block is None:
            return result
        for row in block.select("tr"):
            th = row.find("th")
            td = row.find("td")
            if th is None or td is None:
                continue
            key = _text(th)
            if key and key not in result:
                result[key] = _lines(td)
        return result

    @staticmethod
    def _dates(soup: bs4.BeautifulSoup) -> tuple[str, str]:
        """div.recruitpost-box-time から (掲載開始日, 最終更新日) を取得する。"""
        start_date = ""
        update_date = ""
        for span in soup.select("div.recruitpost-box-time span"):
            label = _text(span)
            if "掲載開始日" in label and not start_date:
                start_date = _to_iso_date(label)
            elif "最終更新日" in label and not update_date:
                update_date = _to_iso_date(label)
        return start_date, update_date

    @staticmethod
    def _pick_tel(soup: bs4.BeautifulSoup) -> str:
        """応募用モーダルの tel: リンクから電話番号を取得する。"""
        for a in soup.select('a[href^="tel:"]'):
            tel = re.sub(r"[^\d\-+]", "", _clean(a.get("href", "")[4:]))
            # 未登録の求人は href="tel:" の空プレースホルダなので桁数で弾く
            if re.search(r"\d{9,}", re.sub(r"\D", "", tel)):
                return tel
        return ""

    @staticmethod
    def _pick_hp(soup: bs4.BeautifulSoup, company: dict[str, str]) -> str:
        """会社情報表の「ホームページ」→ 本文中の外部リンクの順で企業サイトを取る。"""
        block = soup.select_one("dl.search-mess-company")
        if block is not None:
            for row in block.select("tr"):
                th = row.find("th")
                td = row.find("td")
                if th is None or td is None or "ホームページ" not in _text(th):
                    continue
                a = td.find("a", href=True)
                href = _clean(a["href"]) if a is not None else _text(td)
                if href.startswith("http"):
                    return href

        value = company.get("ホームページ", "")
        if value.startswith("http"):
            return value

        # 本文 (求人詳細ブロック) に企業サイトへの外部リンクがあれば採用する
        for a in soup.select('div.recruitpost a[href^="http"], article a[href^="http"]'):
            href = _clean(a["href"])
            host = urllib.parse.urlparse(href).netloc.lower()
            if host and not any(host.endswith(d) for d in _HP_EXCLUDE):
                return href
        return ""


if __name__ == "__main__":
    scraper = SyokuninDoh2Scraper()
    scraper.execute("https://www.syokunin-doh.com/recruitpost-sitemap.xml")
