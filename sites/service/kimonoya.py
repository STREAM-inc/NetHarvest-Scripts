"""
着物屋さんナビ (kimonoya.org) — 全国の着物店・呉服店ディレクトリスクレイパー

取得対象:
    - トップページから 47 都道府県のインデックスページ (/1XX{name}/) を列挙
    - 各都道府県ページに掲載される店舗の詳細ページ (/1XX{name}/{digits}.html)
    - 詳細ページの「:DATA」テーブル (th: td.sr30 / 値: td.sr70) から店舗情報を抽出

取得フロー:
    1. サイトルートから /1\\d{2}[a-z]+/ 形式の都道府県インデックス URL を列挙 (備考「全国で取得」)
    2. 各都道府県ページから店舗詳細リンク (/{pref}/{digits}.html) を重複除去して収集
    3. 詳細ページを 1 件取得するごとに即 yield (Pattern B — 途中 break でも無駄通信なし)
    4. 「お役立ち情報」(長文の自由記述プロース) は著作権リスクのため取得しない

実行方法:
    # ローカルテスト
    python scripts/sites/service/kimonoya.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id kimonoya
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


_PREF_PATTERN = re.compile(
    r"(北海道|東京都|大阪府|京都府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 都道府県インデックス: コード 1XX (101-147)。記事ページ (020/040/050 等) は除外。
# 一部リンクは末尾スラッシュ無し (例: /107fukushi) のため /? で許容し、後で正規化する。
_PREF_INDEX_RE = re.compile(r"^https://www\.kimonoya\.org/1\d{2}[a-z]+/?$")
# 店舗詳細: 都道府県ディレクトリ配下の数字.html
_DETAIL_RE = re.compile(r"^https://www\.kimonoya\.org/1\d{2}[a-z]+/\d+\.html$")
_POST_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")


class KimonoyaScraper(StaticCrawler):
    """着物屋さんナビ (kimonoya.org) スクレイパー"""

    DELAY = 1.5

    EXTRA_COLUMNS = [
        "FAX",
        "駐車場",
        "アクセス",
    ]

    _BASE_URL = "https://www.kimonoya.org/"

    def parse(self, url: str):
        # 備考「全国で取得」: サイトルートから全 47 都道府県インデックスを列挙する
        root = urljoin(url, "/")
        soup = self.get_soup(root)
        if not soup:
            self.logger.warning("ルートページ取得失敗: %s", root)
            return

        pref_urls: list[str] = []
        seen_pref: set[str] = set()
        for a in soup.select("a[href]"):
            absolute = urljoin(self._BASE_URL, a.get("href") or "")
            if not _PREF_INDEX_RE.match(absolute):
                continue
            # 末尾スラッシュを正規化 (相対リンク解決と重複除去のため)
            if not absolute.endswith("/"):
                absolute += "/"
            if absolute not in seen_pref:
                seen_pref.add(absolute)
                pref_urls.append(absolute)
        self.logger.info("都道府県インデックス数: %d", len(pref_urls))

        seen_detail: set[str] = set()
        for pref_url in pref_urls:
            yield from self._scrape_pref(pref_url, seen_detail)

    def _scrape_pref(self, pref_url: str, seen_detail: set[str]):
        self.logger.info("都道府県ページ取得: %s", pref_url)
        soup = self.get_soup(pref_url)
        if not soup:
            return

        detail_urls: list[str] = []
        for a in soup.select("a[href]"):
            absolute = urljoin(self._BASE_URL, a.get("href") or "")
            if _DETAIL_RE.match(absolute) and absolute not in seen_detail:
                seen_detail.add(absolute)
                detail_urls.append(absolute)

        for detail_url in detail_urls:
            try:
                record = self._scrape_detail(detail_url)
            except Exception as e:
                self.logger.warning("詳細取得失敗: %s — %s", detail_url, e)
                continue
            if record:
                self.total_items = len(seen_detail)
                yield record

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if not soup:
            return None

        item: dict = {Schema.URL: detail_url}

        # 店舗名: h1
        h1 = soup.find("h1")
        if h1:
            item[Schema.NAME] = h1.get_text(strip=True)

        # :DATA テーブル — 各行 <td class="sr30">ラベル</td><td class="sr70">値</td>
        for label_td in soup.select("td.sr30"):
            value_td = label_td.find_next_sibling("td")
            if value_td is None:
                continue
            label = label_td.get_text(strip=True)
            # <br> を改行に置換してテキスト化
            value = value_td.get_text(" ", strip=True)

            if label == "所在地":
                raw = value
                pm = _POST_RE.search(raw)
                if pm:
                    item[Schema.POST_CODE] = pm.group(1)
                    raw = _POST_RE.sub("", raw).strip()
                item[Schema.ADDR] = raw
                prefm = _PREF_PATTERN.search(raw)
                if prefm:
                    item[Schema.PREF] = prefm.group(1)
            elif label == "TEL":
                item[Schema.TEL] = value
            elif label == "FAX":
                item["FAX"] = value
            elif label == "URL":
                a = value_td.find("a", href=True)
                item[Schema.HP] = a["href"] if a else value
            elif label == "主な取り扱い業務":
                item[Schema.CAT_SITE] = value
            elif label == "営業時間など":
                item[Schema.TIME] = value
            elif label == "駐車場":
                item["駐車場"] = value
            elif label == "アクセス":
                item["アクセス"] = value
            # 「お役立ち情報」は長文の自由記述プロース → 著作権リスクのため取得しない

        if not item.get(Schema.NAME):
            return None

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = KimonoyaScraper()
    scraper.execute("https://www.kimonoya.org/aboutme.html")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
