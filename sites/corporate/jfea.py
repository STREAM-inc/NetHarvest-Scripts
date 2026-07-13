"""
日本厨房工業会 (JFEA) 会員名簿 — 全国会員一覧スクレイパー

取得対象:
    - 全国の支部会員 + 賛助会員 (会員社名 / 会社名カナ / ホームページ URL / 支部 / 会員番号)
    - 所在地・電話番号は各会員企業の公式サイトを訪問して取得 (備考の指示)

取得フロー:
    1. ルート (list.html) は各支部の会員名簿ページ (list_*.html) への索引。
       索引から支部ページのリンクを列挙する (list_50.html=50音順の重複ビュー、
       list.html 自身は除外)。関東だけでなく関西・九州・北海道など全国分を巡回する。
    2. 各支部ページの会員ブロック (th.list / td.list のペア) から
       会員番号・会社名・会社名カナ・ホームページを取得する。
    3. ホームページが記載されている場合のみ、その公式サイト (トップ +
       会社概要/アクセス/お問い合わせ 等のサブページ) にアクセスし、
       所在地 (住所) と電話番号を抽出する。見つからなければ空欄。
    4. 会員 1 件を取得するごとに即 yield する (Pattern B)。

備考の遵守:
    - HP に明記された情報のみ取得し、推測・補完はしない。見つからない項目は空欄。
    - robots.txt は存在せず (404)、スクレイピング禁止の明示的記述は確認されなかった。

実行方法:
    python scripts/sites/corporate/jfea.py
    docker compose exec worker python /app/bin/run_flow.py --site-id jfea
"""

import re
import sys
import logging
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import urllib3
from bs4 import BeautifulSoup

from src.framework.static import StaticCrawler
from src.const.schema import Schema

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

# 都道府県 (住所抽出用)
_PREF = (
    r"(?:北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_PREF_RE = re.compile(_PREF)
# 〒NNN-NNNN に続く都道府県以降の住所
_ADDR_POST_RE = re.compile(r"〒?\s*\d{3}[-－]?\d{4}[\s　]*(" + _PREF + r"[^\n\r]{2,50})")
# 都道府県 + 市区町村 + 番地らしき並び (〒が無い場合のフォールバック)
_ADDR_PREF_RE = re.compile(
    "(" + _PREF + r"[^\n\r]{2,40}?[0-9０-９][-−ー－0-9０-９]{0,12}(?:番地?|号|丁目)?)"
)
# 電話番号 (3 セグメント。郵便番号=2 セグメントと区別できる)
_TEL_LABEL_RE = re.compile(
    r"(?:TEL|Tel|tel|ＴＥＬ|電話|℡|Phone|PHONE)[)：:\s　（(]*?(0\d{1,3}[-(（]\d{1,4}[-)）]\d{3,4})"
)
_TEL_PLAIN_RE = re.compile(r"(?<![0-9-])(0\d{1,3}-\d{1,4}-\d{3,4})(?![0-9-])")

# 会社概要系サブページを探すキーワード
_SUBPAGE_KW = (
    "会社概要", "会社案内", "会社情報", "企業情報", "企業概要", "会社紹介",
    "アクセス", "お問い合わせ", "お問合せ", "問い合わせ", "特定商取引",
    "company", "about", "access", "contact", "profile", "corporate", "outline",
)


class Jfea(StaticCrawler):
    """日本厨房工業会 会員名簿スクレイパー"""

    DELAY = 1.0
    CONTINUE_ON_ERROR = True
    # 会員番号 / 会社名カナ / 支部 は Schema 外のためサイト固有カラムとして宣言
    EXTRA_COLUMNS = ["会員番号", "会社名カナ", "支部"]

    def parse(self, url: str):
        index = self.get_soup(url)
        if index is None:
            logger.error("索引ページを取得できませんでした: %s", url)
            return

        chapters = self._chapter_pages(index, url)
        logger.info("巡回対象の支部ページ数: %d", len(chapters))
        self.total_items = 0

        for chapter_url, chapter_name in chapters:
            soup = self.get_soup(chapter_url)
            if soup is None:
                logger.warning("支部ページ取得失敗 (スキップ): %s", chapter_url)
                continue

            members = self._parse_members(soup)
            self.total_items += len(members)
            logger.info("%s: %d 件", chapter_name, len(members))

            for m in members:
                item = {
                    Schema.URL: chapter_url,      # 取得元 (支部名簿ページ)
                    Schema.NAME: m["name"],
                    Schema.HP: m["hp"],           # 会員企業の公式サイト URL
                    Schema.ADDR: "",
                    Schema.PREF: "",
                    Schema.TEL: "",
                    "会員番号": m["memno"],
                    "会社名カナ": m["kana"],
                    "支部": chapter_name,
                }
                # 所在地・電話番号は公式サイトから取得する (備考の指示)
                if m["hp"]:
                    addr, tel = self._scrape_company_site(m["hp"])
                    if addr:
                        pref_m = _PREF_RE.match(addr)
                        if pref_m:
                            item[Schema.PREF] = pref_m.group(0)
                        item[Schema.ADDR] = addr
                    if tel:
                        item[Schema.TEL] = tel
                yield item

    # ------------------------------------------------------------------
    # 索引: 支部名簿ページの列挙
    # ------------------------------------------------------------------
    def _chapter_pages(self, index_soup, root_url: str):
        """索引ページ (list.html) から支部名簿ページの (URL, 名称) を列挙する。"""
        seen = set()
        pages = []
        root_norm = root_url.rstrip("/")
        for a in index_soup.find_all("a", href=True):
            href = a["href"].strip()
            if not re.search(r"list_[a-z0-9]+\.html", href, re.I):
                continue
            # 50音順ページは支部別ページの重複ビューなので除外
            if "list_50" in href.lower():
                continue
            full = urljoin(root_url, href)
            if full.rstrip("/") == root_norm:
                continue
            if full in seen:
                continue
            seen.add(full)
            name = a.get_text(strip=True) or full
            pages.append((full, name))
        return pages

    # ------------------------------------------------------------------
    # 支部ページ: 会員ブロックの解析
    # ------------------------------------------------------------------
    def _parse_members(self, soup):
        """th.list / td.list ペアの並びから会員レコードを組み立てる。

        HTML は <tr> の閉じ忘れが多いが、th.list は会員データ表にのみ出現するため、
        出現順に走査し「会員番号」を各レコードの開始とみなして区切る。
        """
        members = []
        cur = None

        def flush():
            if cur and cur.get("name"):
                members.append(cur)

        for th in soup.select("th.list"):
            label = th.get_text(strip=True)
            td = th.find_next_sibling("td")
            if td is None:
                continue

            if label == "会員番号":
                flush()
                cur = {"memno": td.get_text(strip=True), "name": "", "kana": "", "hp": ""}
                continue
            if cur is None:
                cur = {"memno": "", "name": "", "kana": "", "hp": ""}

            if label == "会社名":
                # <td> の閉じ忘れで後続要素が入れ子になるため直下テキストのみ採用
                cur["name"] = self._direct_text(td)
            elif label == "会社名カナ":
                cur["kana"] = self._direct_text(td)
            elif label == "ホームページ":
                a = td.find("a", href=True)
                if a:
                    cur["hp"] = a["href"].strip()

        flush()
        return members

    @staticmethod
    def _direct_text(td) -> str:
        """td の直下テキストノードのみ連結 (入れ子要素のテキストは無視)。"""
        text = "".join(td.find_all(string=True, recursive=False))
        return re.sub(r"[\s　]+", " ", text).strip()

    # ------------------------------------------------------------------
    # 会員企業の公式サイトから所在地・電話番号を抽出
    # ------------------------------------------------------------------
    def _scrape_company_site(self, home_url: str):
        """公式サイトのトップ + 会社概要系サブページから (住所, 電話) を抽出する。"""
        soup = self._fetch_external(home_url)
        if soup is None:
            return "", ""

        text = soup.get_text("\n", strip=True)
        addr = self._extract_addr(text)
        tel = self._extract_tel(text)

        # トップで欠けた項目のみサブページで補完
        if not addr or not tel:
            sub_url = self._find_subpage(soup, home_url)
            if sub_url and sub_url.rstrip("/") != home_url.rstrip("/"):
                sub_soup = self._fetch_external(sub_url)
                if sub_soup is not None:
                    sub_text = sub_soup.get_text("\n", strip=True)
                    if not addr:
                        addr = self._extract_addr(sub_text)
                    if not tel:
                        tel = self._extract_tel(sub_text)
        return addr, tel

    def _fetch_external(self, url: str):
        """外部 (会員企業) サイトを取得。失敗は握り潰して None を返す (項目は空欄)。"""
        try:
            resp = self.session.get(url, timeout=15, verify=False)
            resp.raise_for_status()
            ct = resp.headers.get("Content-Type", "")
            if "charset=" not in ct.lower():
                resp.encoding = resp.apparent_encoding
            return BeautifulSoup(resp.text, "html.parser")
        except Exception as e:  # noqa: BLE001  外部サイトは何が起きても致命にしない
            logger.info("外部サイト取得失敗 (空欄で継続): %s — %s", url, type(e).__name__)
            return None

    @staticmethod
    def _clean(s: str) -> str:
        return re.sub(r"[\s　]+", " ", s).strip()

    def _extract_addr(self, text: str) -> str:
        m = _ADDR_POST_RE.search(text)
        if m:
            return self._clean(m.group(1))
        m = _ADDR_PREF_RE.search(text)
        if m:
            return self._clean(m.group(1))
        return ""

    @staticmethod
    def _extract_tel(text: str) -> str:
        m = _TEL_LABEL_RE.search(text)
        if m:
            return m.group(1)
        m = _TEL_PLAIN_RE.search(text)
        if m:
            return m.group(1)
        return ""

    @staticmethod
    def _find_subpage(soup, base_url: str):
        for a in soup.find_all("a", href=True):
            hint = (a.get_text(" ", strip=True) + " " + a["href"]).lower()
            for kw in _SUBPAGE_KW:
                if kw.lower() in hint:
                    return urljoin(base_url, a["href"])
        return None


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Jfea()
    # 🔒 sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.jfea.or.jp/list/list.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
