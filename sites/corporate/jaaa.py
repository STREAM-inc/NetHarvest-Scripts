"""
JAAA 会員社一覧 — 一般社団法人 日本広告業協会 会員社

取得対象:
    - 会員社名 (NAME)
    - 所在地 (ADDR / PREF) ※各会員社の自社サイトから取得 (会社概要・アクセス・フッター等)
    - URL / HP (会員社の自社サイト URL)
    - 合計 135 社 (2026-07 時点)

取得フロー:
    1. JAAA 会員社一覧ページ (/about/member-companies/) を 1 回取得。
       会員社は `<p><a target="_blank">社名</a></p>` として五十音順に列挙されている。
       リンク先 (href) が会員社の自社サイト URL。
    2. 会員社ごとに自社サイトを訪問し、所在地を抽出 (取得できた 1 件を即 yield)。
       - トップページ本文/フッターに住所が無ければ、会社概要・会社案内・企業情報・
         アクセス・お問い合わせ 等のリンクを 1 つだけ辿って再探索する。
       - 住所は「〒郵便番号」直後、または「都道府県 + 市区郡町村」パターンで抽出する。
       - 自社サイトが 403/タイムアウト等で取得できない場合、所在地は空欄のまま
         (HP に明記された情報のみ取得し、推測・補完はしない)。

備考対応:
    取得カラムは「会員社名／所在地／URL」。所在地は会社概要/アクセス/お問い合わせ/
    特定商取引法に基づく表記/フッターを優先して確認する方針をコードに反映。

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/jaaa.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id jaaa
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 会員社一覧ページのパス (ルート URL から派生させる)
_MEMBER_PATH = "/about/member-companies/"

# 都道府県
_PREF = (
    "北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    "埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    "岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    "鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    "佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
_PREF_RE = re.compile(r"(" + _PREF + r")")
# 「〒NNN-NNNN」形式の郵便番号 (最も信頼できる住所アンカー)
_POSTAL_RE = re.compile(r"〒\s*\d{3}[-−ー－]?\s*\d{4}[\s　]*")
# 都道府県 + 市区郡町村 を含む住所本体
_ADDR_RE = re.compile(
    r"(" + _PREF + r")[^\n<>。｜|、]{0,6}?[市区郡町村][^\n<>。｜|、]{2,45}"
)
# 住所の末尾に紛れ込みやすい情報 (電話・アクセス案内等) を切り落とす
_TRIM_RE = re.compile(
    r"(TEL|Tel|ＴＥＬ|℡|電話|FAX|Fax|ＦＡＸ|MAP|地図|アクセス|お問|"
    r"営業時間|受付|代表|\d{2,4}[-−]\d{2,4}[-−]\d{3,4}).*$"
)
# 会社概要・アクセス等のサブページ候補を判定するキーワード (href / リンクテキスト)
_COMPANY_KW = re.compile(
    r"company|corporate|about|profile|outline|overview|access|contact|"
    r"会社概要|会社情報|企業情報|会社案内|アクセス|お問",
    re.I,
)


class Jaaa(StaticCrawler):
    """JAAA 会員社一覧 スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = []  # Schema (NAME / ADDR / PREF / HP / URL) で充足

    def parse(self, url: str):
        # ルート URL (= sites.yml の url) から会員社一覧ページを派生させる
        list_url = urljoin(url, _MEMBER_PATH)
        soup = self.get_soup(list_url)
        if soup is None:
            return

        links = soup.select('.l-contents p > a[target="_blank"]')
        self.total_items = len(links)

        for a in links:
            name = a.get_text(strip=True)
            hp = (a.get("href") or "").strip()
            if not name or not hp:
                continue

            item = {
                Schema.NAME: name,
                Schema.HP: hp,
                Schema.URL: list_url,
            }

            # 会員社の自社サイトから所在地を取得 (取得できなければ空欄)
            address = self._lookup_address(hp)
            if address:
                m = _PREF_RE.search(address)
                if m:
                    item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = address

            # 1 件取得ごとに即 yield (Pattern B)
            yield item

    def _lookup_address(self, hp: str) -> str:
        """会員社の自社サイトから所在地を抽出する。

        トップページで見つからなければ、会社概要/アクセス/お問い合わせ等の
        リンクを 1 つだけ辿って再探索する。取得不能なら空文字を返す。
        """
        soup = self._safe_get(hp)
        if soup is None:
            return ""

        address = self._extract_address(soup)
        if address:
            return address

        sub_url = self._find_company_link(soup, hp)
        if sub_url and sub_url != hp:
            sub_soup = self._safe_get(sub_url)
            if sub_soup is not None:
                address = self._extract_address(sub_soup)
                if address:
                    return address
        return ""

    def _safe_get(self, url: str):
        """外部サイト取得。403/タイムアウト等は握りつぶして None を返す。"""
        try:
            return self.get_soup(url)
        except Exception as e:  # noqa: BLE001 — 外部サイトの各種失敗を許容
            self.logger.debug("会員社サイト取得失敗 (スキップ): %s — %s", url, e)
            return None

    def _extract_address(self, soup) -> str:
        """ページ本文テキストから住所を抽出する。"""
        text = soup.get_text(" ", strip=True)
        text = re.sub(r"[\t ]+", " ", text)

        # 1) 「〒郵便番号」直後を最優先 (誤検知が少ない)
        for anchor in _POSTAL_RE.finditer(text):
            segment = text[anchor.end():anchor.end() + 80]
            m = _ADDR_RE.search(segment)
            if m:
                return _TRIM_RE.sub("", m.group(0)).strip(" 　:：")

        # 2) フォールバック: 都道府県 + 市区郡町村 パターン
        m = _ADDR_RE.search(text)
        if m:
            return _TRIM_RE.sub("", m.group(0)).strip(" 　:：")
        return ""

    def _find_company_link(self, soup, base_url: str) -> str | None:
        """会社概要・アクセス等の同一ドメイン内リンクを 1 つ返す。

        住所が載りやすい具体ページ (会社概要/アクセス/所在地/outline 等) を、
        汎用メニュー (company トップ/about) より優先する。
        """
        base_host = urlsplit(base_url).netloc
        best = None
        best_rank = 99
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
                continue
            label = href + " " + a.get_text(" ", strip=True)
            if not _COMPANY_KW.search(label):
                continue
            full = urljoin(base_url, href)
            if urlsplit(full).netloc != base_host:
                continue
            # 具体的な所在地系ページほど優先度を高く (rank 小)
            if re.search(r"outline|profile|access|会社概要|会社情報|会社案内|アクセス|所在", label, re.I):
                rank = 0
            elif re.search(r"contact|お問|特定商取引", label, re.I):
                rank = 1
            else:  # company トップ / about 等の汎用
                rank = 2
            if rank < best_rank:
                best, best_rank = full, rank
                if rank == 0:
                    break
        return best


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Jaaa()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.jaaa.ne.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
