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

BASE_URL = "http://www.hamanight.com"

AREA_URLS = [
    "http://www.hamanight.com/location/kannai",
    "http://www.hamanight.com/location/fukutomicho",
    "http://www.hamanight.com/location/sakuragicho",
    "http://www.hamanight.com/location/yokohama",
    "http://www.hamanight.com/location/shin-yokohama",
    "http://www.hamanight.com/location/other-area",
]

# 店舗詳細ページのURLパターン (例: /location/kannai/loc00007.html)
DETAIL_RE = re.compile(r"/location/[^/]+/loc[\w\-]*\.html$", re.IGNORECASE)

# このサイト独自の追加カラム (Schema に対応する定義が無いもの)
SEATS = "座席数"
KARAOKE = "カラオケ"
STAFF_NUM = "接客人数"
BUDGET = "予算目安"

# 詳細ページの <em>ラベル</em><span>値</span> ラベル → 出力カラム名 のマッピング
LABEL_MAP = {
    "住所": Schema.ADDR,
    "所在地": Schema.ADDR,
    "電話番号": Schema.TEL,
    "TEL": Schema.TEL,
    "営業時間": Schema.TIME,
    "定休日": Schema.HOLIDAY,
    "座席数": SEATS,
    "カラオケ": KARAOKE,
    "接客人数": STAFF_NUM,
    "予算目安": BUDGET,
}


def _clean(s) -> str:
    """空白を正規化し、元サイトが未入力箇所に出力する不正値 'Array' を除去する。"""
    if s is None:
        return ""
    # 全角スペース(　)を含む空白を 1 つの半角スペースへ正規化する
    text = re.sub(r"\s+", " ", str(s)).strip()
    # 元サイトのテンプレートが未入力箇所に "Array" を出力するため取り除く
    text = re.sub(r"\bArray\b", "", text).strip()
    return re.sub(r"\s+", " ", text).strip()


# 横浜市の行政区（このサイトの掲載エリアは全て横浜市内）
YOKOHAMA_WARDS = (
    "鶴見区", "神奈川区", "西区", "中区", "南区", "港南区", "保土ケ谷区",
    "保土ヶ谷区", "旭区", "磯子区", "金沢区", "港北区", "緑区", "青葉区",
    "都筑区", "戸塚区", "栄区", "泉区", "瀬谷区",
)

# 既に都道府県から始まっているか判定する
_PREF_RE = re.compile(r"^(?:北海道|東京都|京都府|大阪府|.{2,3}県)")


def _normalize_addr(value: str) -> str:
    """住所が必ず「都道府県・市区町村」から始まる完全な住所になるよう補完する。

    対象サイトの住所は「中区相生町1-5 真和関内第一ビル4F」のように
    区（市区町村の途中）から始まり、都道府県・市名が欠落しているため、
    横浜市の区で始まる場合は「神奈川県横浜市」を、「横浜市」で始まる場合は
    「神奈川県」を前置して補う。建物名以降はそのまま保持する。
    """
    v = (value or "").strip()
    if not v:
        return v
    # 既に都道府県から始まっていればそのまま
    if _PREF_RE.match(v):
        return v
    # 「横浜市…」で始まる場合は都道府県のみ補う
    if v.startswith("横浜市"):
        return "神奈川県" + v
    # 「○○区…」で始まる場合は都道府県＋市名を補う
    if any(v.startswith(ward) for ward in YOKOHAMA_WARDS):
        return "神奈川県横浜市" + v
    return v


class HamanightScraper(StaticCrawler):
    """ハマんナイト 横浜ナイト店舗情報スクレイパー（hamanight.com）"""

    DELAY = 1.0
    EXTRA_COLUMNS = [SEATS, KARAOKE, STAFF_NUM, BUDGET]

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls: list[str] = []
        seen: set[str] = set()
        for area_url in AREA_URLS:
            self.logger.info("エリア取得: %s", area_url)
            for u in self._collect_area_urls(area_url):
                if u not in seen:
                    seen.add(u)
                    detail_urls.append(u)
        self.total_items = len(detail_urls)
        self.logger.info("詳細URL収集完了: %d 件", len(detail_urls))
        for detail_url in detail_urls:
            item = self._scrape_detail(detail_url)
            if item:
                yield item

    def _collect_area_urls(self, area_url: str) -> list[str]:
        """エリアページをページネーション追跡しながら店舗詳細URLを収集する。"""
        urls: list[str] = []
        seen: set[str] = set()
        visited_pages: set[str] = set()
        current = area_url

        while current and current not in visited_pages:
            visited_pages.add(current)
            soup = self.get_soup(current)
            if soup is None:
                break

            for a in soup.select("a[href]"):
                href = (a.get("href") or "").strip()
                if not href:
                    continue
                full = urljoin(BASE_URL, href).split("#")[0].split("?")[0]
                if DETAIL_RE.search(full) and full not in seen:
                    seen.add(full)
                    urls.append(full)

            # 「次の12件>>」リンク、無ければ page-numbers から次ページを辿る
            next_a = soup.select_one("span.next a[href]")
            if not next_a:
                next_a = self._find_next_page_link(soup, visited_pages)
            current = urljoin(BASE_URL, next_a["href"]) if next_a else None

        return urls

    def _find_next_page_link(self, soup, visited_pages: set[str]):
        """page-numbers の中から未訪問のページリンクを返す。"""
        for a in soup.select("a.page-numbers[href]"):
            full = urljoin(BASE_URL, a["href"])
            if full not in visited_pages:
                return a
        return None

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        # --- 店名 ---
        name = ""
        h1 = soup.select_one("div.content div.main h1")
        if h1:
            name = _clean(h1.get_text())
        if not name:
            # #detail 内 1つ目の <dt> が店名
            dt = soup.select_one("#detail dl dt")
            if dt:
                name = _clean(dt.get_text())
        if not name:
            title = soup.select_one("title")
            if title:
                name = _clean(title.get_text().split("｜")[0])
        if name:
            data[Schema.NAME] = name

        # --- ジャンル (サイト定義業種) ---
        detail = soup.select_one("#detail")
        genre_a = detail.find("a", href=lambda h: h and "/genre/" in h) if detail else None
        if genre_a is None:
            genre_a = soup.find("a", href=lambda h: h and "/genre/" in h)
        if genre_a:
            genre = _clean(genre_a.get_text())
            if genre:
                data[Schema.CAT_SITE] = genre

        # --- 基本情報リスト (<em>ラベル</em><span>値</span>) ---
        self._extract_em_span(soup, data)

        if not data.get(Schema.NAME):
            return None
        return data

    def _extract_em_span(self, soup, data: dict):
        """詳細ページの em/span 形式の項目を可能な限り全て取得する。

        値（住所等）が複数のタグ（例: <span>...</span><p>...</p>）に
        分割されている場合でも欠落しないよう、ラベルである <em> 以外の
        全ての子要素・テキストを連結して値とする。
        """
        # 通常店舗ページは基本情報を <div id="detail"> 内に持つが、ピックアップ
        # 店舗ページ（例: クラブ リアン /location/kannai/loc00085.html）は
        # テンプレートが異なり #detail が存在せず <div id="pickup"> 内に持つ。
        # 従来は #detail のみを対象にしていたため、ピックアップ店舗では住所・
        # 電話番号等が丸ごと欠落していた。両テンプレートを対象に収集する。
        items = soup.select("#detail li, #pickup li")
        if not items:
            # 想定外テンプレートでも取りこぼさないよう全 li をフォールバック対象にする
            # （LABEL_MAP に無いラベルは後段で無視されるため無関係な li は問題にならない）
            items = soup.select("li")
        for li in items:
            em = li.find("em")
            if not em:
                continue
            label = _clean(em.get_text()).rstrip("：:").strip()

            # <em>（ラベル）以外の全ての子要素・テキストを連結する。
            # span だけでなく p や br で分割されていても全て拾える。
            value_parts = [
                child.get_text(" ") if hasattr(child, "get_text") else str(child)
                for child in li.children
                if child is not em
            ]
            value = _clean(" ".join(value_parts))
            if not label or not value:
                continue

            column = LABEL_MAP.get(label)
            if column is None:
                continue

            if column == Schema.TEL:
                m = re.search(r"0\d{1,4}[-(]?\d{1,4}[-)]?\d{3,4}", value)
                value = m.group(0) if m else value
            elif column == Schema.ADDR:
                # 都道府県・市区町村から始まる完全な住所に補完する
                value = _normalize_addr(value)
            data[column] = value


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    HamanightScraper().execute("http://www.hamanight.com")
