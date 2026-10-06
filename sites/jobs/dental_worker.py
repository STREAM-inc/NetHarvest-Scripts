"""
対象サイト: https://dental-worker.com/batch/sitemap-detail-job/

デンタルワーカー (株式会社トライトキャリア) の求人詳細ページを
サイトマップから全件列挙して巡回する。1求人 = 1行。

- 列挙元: https://dental-worker.com/batch/sitemap-detail-job/ (約6,567件)
- 詳細  : https://dental-worker.com/job-detail/{ID}/
- 施設詳細ページ (https://dental-worker.com/facility-detail/{ID}/) は巡回せず、
  求人ページ内の「施設詳細情報へ」リンクから URL / ID のみを取得する。
- robots.txt の `Crawl-delay: 10` を遵守するため DELAY=10.0 を parse() 内で
  リクエスト前に待機する。基盤側の二重待機を避けるため ITEM_DELAY=0 とする。
"""

import re
import time
import warnings
from typing import Generator
from urllib.parse import urljoin

import bs4

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# サイトマップ (XML) を基盤の html.parser で読むため、bs4 の警告を抑制する
warnings.filterwarnings("ignore", category=bs4.XMLParsedAsHTMLWarning)

# サイトマップ中の求人詳細 URL
_JOB_URL_PATTERN = re.compile(r"/job-detail/(\d+)/?$")
# 施設詳細 URL から施設 ID を取り出す
_FACILITY_ID_PATTERN = re.compile(r"/facility-detail/(\d+)/?")
# 「更新日：2026/06/12」から日付だけを取り出す
_UPDATE_PATTERN = re.compile(r"(\d{4}[/-]\d{1,2}[/-]\d{1,2})")
# 住所先頭の都道府県 (breadcrumb が取れなかった場合のフォールバック)
_PREF_PATTERN = re.compile(r"^(北海道|東京都|(?:大阪|京都)府|.{2,3}県)")

# 詳細ページの見出し (h3.p-detail-info__term) → 出力カラム名
# 見出しは <br> を含むことがあるため、空白を除去した文字列で突き合わせる。
_TERM_TO_COLUMN = {
    "募集職種": "募集職種",
    "雇用形態": "雇用形態",
    "求人概要": Schema.DESCRIPTION,
    "給与": "給与",
    "昇給・賞与": "昇給・賞与",
    "諸手当": "諸手当",
    "勤務時間": "勤務時間",
    "休日": "休日",
    "社会保険": "社会保険",
    "その他福利厚生": "福利厚生",
    "備考": "備考",
    "施設名": Schema.NAME,
    "施設形態": Schema.CAT_SITE,
    "勤務地": Schema.ADDR,
    "アクセス": "アクセス",
    "最寄駅": "最寄駅",
}

# 求人概要は先頭 200 字のみ保存する
_DESCRIPTION_MAX_LEN = 200


class DentalWorkerScraper(StaticCrawler):
    """デンタルワーカー 求人詳細 スクレイパー (静的)"""

    DELAY = 10.0        # robots.txt の Crawl-delay: 10 に合わせる
    ITEM_DELAY = 0.0    # 待機は parse() 内で行うため基盤側の待機は無効化

    EXTRA_COLUMNS = [
        "施設詳細URL",
        "施設ID",
        "アクセス",
        "最寄駅",
        "募集職種",
        "雇用形態",
        "給与",
        "昇給・賞与",
        "諸手当",
        "勤務時間",
        "休日",
        "社会保険",
        "福利厚生",
        "備考",
        "更新日",
    ]

    # ------------------------------------------------------------------ #
    # ヘルパー
    # ------------------------------------------------------------------ #
    @staticmethod
    def _text(node: bs4.element.Tag | None) -> str:
        """<br> を改行として扱いつつ、タグ内のテキストを整形して返す。"""
        if node is None:
            return ""
        raw = node.get_text("\n", strip=True)
        # 行単位で空行を潰し、連続空白を 1 つにまとめる
        lines = [re.sub(r"[ \t　]+", " ", ln).strip() for ln in raw.split("\n")]
        return "\n".join(ln for ln in lines if ln)

    def _collect_job_urls(self, url: str) -> list[str]:
        """サイトマップから求人詳細 URL を列挙する。"""
        soup = self.get_soup(url)
        if soup is None:
            self.logger.error("サイトマップを取得できませんでした: %s", url)
            return []

        job_urls: list[str] = []
        seen: set[str] = set()
        for loc in soup.select("loc"):
            href = loc.get_text(strip=True)
            if not href or not _JOB_URL_PATTERN.search(href):
                continue
            # サイトマップは絶対 URL だが、念のため起点 URL を基準に解決する
            absolute = urljoin(url, href)
            if absolute in seen:
                continue
            seen.add(absolute)
            job_urls.append(absolute)

        self.logger.info("サイトマップから %d 件の求人 URL を取得しました", len(job_urls))
        return job_urls

    def _parse_detail(self, soup: bs4.BeautifulSoup, job_url: str) -> dict:
        """求人詳細ページ 1 枚から 1 行分のデータを組み立てる。"""
        # 出現しない項目があるページも多いため、全カラムを空文字で初期化しておく
        item: dict = {Schema.URL: job_url}
        for column in (
            Schema.NAME, Schema.PREF, Schema.ADDR, Schema.CAT_SITE, Schema.DESCRIPTION,
        ):
            item[column] = ""
        for column in self.EXTRA_COLUMNS:
            item[column] = ""

        # --- 募集内容 / 待遇 / 施設情報 の各項目 (h3 見出し + p 本文) ---
        for block in soup.select("div.p-detail-info__item"):
            term_node = block.select_one("h3.p-detail-info__term")
            if term_node is None:
                continue
            # 見出しは "その他<br>福利厚生" のように改行を含むので空白を除去して照合
            term = re.sub(r"\s+", "", term_node.get_text(" ", strip=True))
            column = _TERM_TO_COLUMN.get(term)
            if column is None:
                continue
            value = self._text(block.select_one("p.p-detail-info__desc"))
            if not value:
                # タグ形式 (ul) で値が入る項目のフォールバック
                value = self._text(block.select_one("ul.p-detail-info__tag"))
            if value:
                item[column] = value

        # 求人概要は先頭 200 字のみ保存する
        if item.get(Schema.DESCRIPTION):
            item[Schema.DESCRIPTION] = item[Schema.DESCRIPTION][:_DESCRIPTION_MAX_LEN]

        # --- 施設名 (施設情報に無い場合は h1 のリンクテキストで補完) ---
        if not item[Schema.NAME]:
            name_link = soup.select_one("h1.c-job__name a")
            if name_link is not None:
                item[Schema.NAME] = name_link.get_text(strip=True)

        # --- 施設詳細 URL / 施設 ID ---
        facility_link = soup.select_one(
            'div.p-detail-info__btn a[href*="/facility-detail/"]'
        ) or soup.select_one('h1.c-job__name a[href*="/facility-detail/"]')
        if facility_link is not None:
            facility_url = urljoin(job_url, facility_link.get("href", ""))
            item["施設詳細URL"] = facility_url
            matched = _FACILITY_ID_PATTERN.search(facility_url)
            if matched:
                item["施設ID"] = matched.group(1)

        # --- 都道府県 (パンくずの /area/ リンク優先、無ければ勤務地の先頭から) ---
        pref_link = soup.select_one('ul.c-breadcrumbs__list a[href*="/area/"]')
        if pref_link is not None:
            item[Schema.PREF] = pref_link.get_text(strip=True)
        if item[Schema.ADDR]:
            matched = _PREF_PATTERN.search(item[Schema.ADDR])
            if matched:
                if not item[Schema.PREF]:
                    item[Schema.PREF] = matched.group(1)
                # Schema.ADDR は「市区町村以降」が規約のため、先頭の都道府県を除く
                item[Schema.ADDR] = item[Schema.ADDR][matched.end():].strip()

        # --- 更新日 ---
        update_node = soup.select_one("div.c-job__update")
        if update_node is not None:
            matched = _UPDATE_PATTERN.search(update_node.get_text(" ", strip=True))
            if matched:
                item["更新日"] = matched.group(1).replace("/", "-")

        return item

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str) -> Generator[dict, None, None]:
        """サイトマップ (引数 url) を起点に、求人詳細を 1 件ずつ取得して yield する。"""
        job_urls = self._collect_job_urls(url)

        for job_url in job_urls:
            # robots.txt の Crawl-delay: 10 を守るため、通信前に必ず待機する
            time.sleep(self.DELAY)

            soup = self.get_soup(job_url)
            if soup is None:
                # CONTINUE_ON_ERROR により取得失敗時は None。次の求人へ進む
                continue

            item = self._parse_detail(soup, job_url)
            if not item[Schema.NAME]:
                # 施設名が取れないページ (掲載終了等) は行として残さない
                self.logger.warning("施設名を取得できないためスキップ: %s", job_url)
                continue

            # 1 件取得するごとに即 yield する (全件バッファしない)
            yield item


if __name__ == "__main__":
    scraper = DentalWorkerScraper()
    scraper.execute("https://dental-worker.com/batch/sitemap-detail-job/")
