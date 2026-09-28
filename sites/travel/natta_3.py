"""
ネパール旅行代理店協会 (NATTA) 会員名簿 — natta_3

取得対象:
    Nepal Association of Tour & Travel Agents (NATTA) の会員名簿 (約 823 件)。
    会員詳細ページ (/member/{slug}/) から
    会社名 / 住所 / TEL / Web サイト URL を主カラムとして取得し、
    併せて メールアドレス・代表者名/役職・NATTA 会員ID・所属支部 を補完する。

取得フロー:
    1. ルート (会員名簿 = 引数 url) を取得し、支部フィルタ
       <select id="membertypeid"> の <option value> から支部 slug と表示名を集める
       (2026-09 時点: Headquarter 658 / Regional Association Western 122 /
        Regional Association Eastern 20 / Regional Association Far Western 15 /
        Palpa Chapter 8 — 和集合 823 件)
    2. 支部ごとに {url}?slug={支部slug} を取得する。1 支部 = 1 ページで
       ページ送りは無い (ページ上部の a-z リンクは同一ページ内アンカー #a … #z で
       あってページャではない)。会員リンクは
       <pre class="member-detail-link"><a href="/member/{slug}/"> に並ぶ
    3. 会員リンクを 1 件取り出すたびに詳細ページを取得して即 yield する (Pattern B)
    4. 最後に支部指定なしのルートページを走査し、支部一覧に現れない会員を補完する
       (2026-09 時点ではルート = Headquarter と同一集合。将来の取りこぼし防止用)

備考:
    - robots.txt は `User-agent: *` / `Allow: /` / `Crawl-delay:3`。DELAY = 3.0 で厳守する。
      サイト内に利用規約ページは存在せず、スクレイピングを禁ずる記述も見当たらない。
    - 同一会員が複数の支部ページに重複掲載され得る。本スクレイパーは
      「同一詳細ページ URL = 同一レコード」の単位でのみ重複取得を避ける
      (同じページを 2 度取得しないため)。名称・住所による名寄せは行わず、
      後工程のクレンジングに委ねる (依頼仕様)。
    - 詳細ページの <dl> は掲載がある項目だけが出力される (Telephone / Email /
      Website / Natta Member ID はいずれも欠落し得る)。欠落項目は空文字とする。
    - サイトはコールドスタート時に HTTP 500 / WordPress エラー画面を返すことが
      あるため、_get_soup_retry() で上限付き (MAX_ATTEMPTS) のリトライを行う。
    - 会員は全てネパール国内のため Schema.PREF (都道府県) は使用しない。
    - 長文の自由記述 (紹介文等) は掲載が無く、取得対象にも含めない。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/natta_3.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id natta_3
"""

import logging
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlsplit, urlunsplit

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 詳細ページ URL の判定 (/member/{slug}/ ※一覧の /members/ とは別パス)
_MEMBER_PATH_PATTERN = re.compile(r"^/member/[^/]+/?$")

# 代表者見出し "Mr. Arvind K. Jha ( Managing Director )" の括弧内 (= 役職)
_POSITION_PATTERN = re.compile(r"[（(]\s*(.*?)\s*[)）]\s*$")


class Natta3(StaticCrawler):
    """ネパール旅行代理店協会 (NATTA) 会員名簿 スクレイパー"""

    DELAY = 3.0   # robots.txt の Crawl-delay:3 を厳守
    TIMEOUT = 60  # コールドスタート時のレスポンスが遅い

    EXTRA_COLUMNS = [
        "NATTA会員ID",
        "所属支部",
    ]

    # 一時エラー (HTTP 500 / WordPress エラー画面) からの復帰リトライ上限
    MAX_ATTEMPTS = 4

    # 詳細ページ <dl><dt> のラベル (小文字・末尾コロン除去後) → 出力先キー
    _LABEL_MAP = {
        "natta member id": "NATTA会員ID",
        "member id": "NATTA会員ID",
        "company name": Schema.NAME,
        "telephone": Schema.TEL,
        "phone": Schema.TEL,
        "office address": Schema.ADDR,
        "address": Schema.ADDR,
        "email address": Schema.EMAIL,
        "email": Schema.EMAIL,
        "website": Schema.HP,
        "web site": Schema.HP,
    }

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str):
        """引数 url (会員名簿ルート) を唯一の起点として全会員を巡回する。"""
        root = self._get_soup_retry(url)
        if root is None:
            raise RuntimeError(f"会員名簿ルートを取得できませんでした: {url}")

        chapters = self._extract_chapters(root)
        logger.info("支部フィルタ: %s", [label for _, label in chapters] or "(取得できず)")

        base = self._base_list_url(url)
        # 支部別ページを先に、支部指定なしのルートを最後に巡回する。
        # こうすると支部に属する会員には必ず支部名が付き、
        # どの支部にも現れない会員だけがルート走査で拾われる。
        pages: list[tuple[str, str]] = [
            (f"{base}?slug={slug}", label) for slug, label in chapters
        ]
        pages.append((url, ""))

        seen: set[str] = set()

        for page_url, chapter in pages:
            # ルートは既に取得済みなので再取得しない
            soup = root if page_url == url else self._get_soup_retry(page_url)
            if soup is None:
                logger.warning("一覧ページを取得できませんでした (スキップ): %s", page_url)
                continue

            links = [
                (detail_url, list_name)
                for detail_url, list_name in self._extract_member_links(soup, page_url)
                if detail_url not in seen
            ]
            logger.info("%s: 未取得の会員リンク %d 件", chapter or "(支部指定なし)", len(links))

            # 進捗表示 (ETA) 用の見込み件数を加算していく
            self.total_items = (self.total_items or 0) + len(links)

            for detail_url, list_name in links:
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                try:
                    item = self._scrape_detail(detail_url, list_name, chapter)
                except Exception as e:  # 1 件の失敗で全体を止めない
                    self.error_count += 1
                    logger.warning("詳細ページの取得に失敗 (スキップ): %s — %s", detail_url, e)
                    continue
                if item:
                    yield item  # 1 件取得ごとに即 yield (Pattern B)

    # ------------------------------------------------------------------ #
    # 一覧ページ
    # ------------------------------------------------------------------ #
    @staticmethod
    def _base_list_url(url: str) -> str:
        """引数 url からクエリ・フラグメントを落とした一覧ベース URL を作る。"""
        parts = urlsplit(url)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))

    @staticmethod
    def _extract_chapters(soup: bs4.BeautifulSoup) -> list[tuple[str, str]]:
        """支部フィルタ <select id="membertypeid"> から (slug, 表示名) を取り出す。"""
        chapters: list[tuple[str, str]] = []
        select = soup.select_one("select#membertypeid, select[name='slug']")
        if not select:
            return chapters
        for opt in select.select("option"):
            slug = (opt.get("value") or "").strip()
            if not slug:
                continue  # 先頭の "--Select Member by Location--"
            chapters.append((slug, opt.get_text(" ", strip=True)))
        return chapters

    @staticmethod
    def _extract_member_links(
        soup: bs4.BeautifulSoup, page_url: str
    ) -> list[tuple[str, str]]:
        """一覧ページから (詳細URL, 一覧上の会員名) をページ内重複なしで取り出す。"""
        links: list[tuple[str, str]] = []
        seen: set[str] = set()
        for a in soup.select("pre.member-detail-link a[href], a[href*='/member/']"):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            absolute = urljoin(page_url, href)
            if not _MEMBER_PATH_PATTERN.match(urlsplit(absolute).path):
                continue  # 一覧 (/members/) やヘッダ・フッタのリンクを除外
            if absolute in seen:
                continue
            seen.add(absolute)
            links.append((absolute, a.get_text(" ", strip=True)))
        return links

    # ------------------------------------------------------------------ #
    # 詳細ページ
    # ------------------------------------------------------------------ #
    def _scrape_detail(self, detail_url: str, list_name: str, chapter: str) -> dict | None:
        soup = self._get_soup_retry(detail_url)
        if soup is None:
            return None

        item = {
            Schema.URL: detail_url,
            Schema.NAME: "",
            Schema.ADDR: "",
            Schema.TEL: "",
            Schema.HP: "",
            Schema.EMAIL: "",
            Schema.REP_NM: "",
            Schema.POS_NM: "",
            "NATTA会員ID": "",
            "所属支部": chapter,
        }

        block = soup.select_one("div.single-member")
        for dl in (block.select("dl") if block else []):
            dt = dl.find("dt")
            dd = dl.find("dd")
            if not dt or not dd:
                continue
            label = dt.get_text(" ", strip=True).rstrip(":： ").strip().lower()
            key = self._LABEL_MAP.get(label)
            if not key:
                continue
            value = self._clean(dd.get_text(" ", strip=True))
            if key == Schema.HP:
                # Website は <a href> が正 (表示文字列は "www.foo.com" のようにスキーム無し)
                anchor = dd.find("a", href=True)
                if anchor:
                    value = anchor["href"].strip()
            if value and not item.get(key):
                item[key] = value

        # 代表者名 / 役職 (<div class="member-image-widgets"><h4>氏名<span>(役職)</span></h4>)
        item[Schema.REP_NM], item[Schema.POS_NM] = self._extract_representative(soup)

        # Company Name の <dl> が無いページは一覧上の会員名 → <h1> の順で補う
        if not item[Schema.NAME]:
            heading = soup.select_one("div.page-intro h1")
            item[Schema.NAME] = self._clean(
                list_name or (heading.get_text(" ", strip=True) if heading else "")
            )

        if not item[Schema.NAME]:
            logger.warning("会員名を取得できませんでした (スキップ): %s", detail_url)
            return None
        return item

    def _extract_representative(self, soup: bs4.BeautifulSoup) -> tuple[str, str]:
        """代表者ウィジェットから (氏名, 役職) を取り出す。掲載が無ければ ("", "")。"""
        for h4 in soup.select("div.member-image-widgets h4"):
            span = h4.find("span")
            position = self._clean(span.get_text(" ", strip=True)) if span else ""
            if span:
                span.extract()  # 氏名だけを残すため役職 span を切り離す
            name = self._clean(h4.get_text(" ", strip=True))
            if not name:
                continue  # 2 個目の空ウィジェットを読み飛ばす
            m = _POSITION_PATTERN.search(position)
            if m:
                position = m.group(1).strip()
            return name, position.strip("()（） ")
        return "", ""

    # ------------------------------------------------------------------ #
    # 取得 (コールドスタートの HTTP 500 対策付き)
    # ------------------------------------------------------------------ #
    def _get_soup_retry(self, url: str) -> bs4.BeautifulSoup | None:
        """get_soup() を最大 MAX_ATTEMPTS 回まで再試行する。

        サイトはコールドスタート時に HTTP 500 (WordPress Error 画面) を返すことが
        あるため、指数バックオフで取り直す。全試行失敗時は None を返し、
        呼び出し元が一覧なら raise、詳細ならスキップする (無限リトライはしない)。
        """
        for attempt in range(self.MAX_ATTEMPTS):
            soup = self.get_soup(url)
            if soup is not None and not self._is_error_page(soup):
                return soup
            if attempt < self.MAX_ATTEMPTS - 1:
                wait = min(2 ** attempt, 8)
                logger.info(
                    "再取得します (%d/%d, %d秒待機): %s",
                    attempt + 1, self.MAX_ATTEMPTS, wait, url,
                )
                time.sleep(wait)
        logger.warning("リトライ上限に達しました: %s", url)
        return None

    @staticmethod
    def _is_error_page(soup: bs4.BeautifulSoup) -> bool:
        """WordPress のエラー画面 (HTTP 200 で返ることがある) を検出する。"""
        title = soup.title.get_text(strip=True) if soup.title else ""
        return "WordPress" in title and "Error" in title

    @staticmethod
    def _clean(text: str) -> str:
        """NBSP・改行・連続空白を畳み、末尾の区切り記号を落とす。"""
        if not text:
            return ""
        text = text.replace("\xa0", " ")
        text = re.sub(r"\s+", " ", text).strip()
        return text.strip(" ;,")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Natta3()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://natta.org.np/members/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
