"""
農園ナビ（全国貸し農園一覧） — https://farm-navi.com/

取得対象:
    - 全国 47 都道府県の市民農園・貸し農園（体験農園） 約 3,000 件
    - 農園名, 都道府県, 市区町村, 住所（詳細: 丁目・番地まで）, 電話番号, 最寄駅/アクセス
    - 運営会社名は掲載が無いため取得しない（空欄）

取得フロー:
    1. 引数 url（例 /farms/tokyo/）から一覧トップ /farms/ を導出し、
       ページ内の都道府県セレクト <select data-prefecture-select> から
       47 都道府県のスラッグ（hokkaido, tokyo, ...）を動的に取得する
       （スラッグはハードコードしない）
    2. 引数 url の都道府県を先頭に、全都道府県の一覧ページ /farms/{pref}/ を
       ページ送り (/page/N/) で巡回。最終ページ番号は 1 ページ目の
       ページャリンクから読み取る（12 件/ページ）
    3. 一覧カードには農園名とリンクしか無いため、各農園の詳細ページ
       (/farm/id/{id}/) へ遷移して住所・アクセス等を取得し都度 yield
       （Pattern B: 1 件取得ごとに即 yield）
    4. 取りこぼし防止として、最後に farm サイトマップ
       (/farm-sitemap*.xml, url と同一ホストから派生) の未取得分を補完する

実行方法:
    # ローカルテスト
    python scripts/sites/leisure/farm_navi.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id farm_navi
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 詳細ページ URL の正規表現（一覧・サイトマップ共通で抽出に使う）
_DETAIL_RE = re.compile(r"https?://[^\s\"'<>]*?/farm/id/\d+/")
# 一覧ページ URL から都道府県スラッグを取り出す（/farms/tokyo/ → tokyo）
_PREF_SLUG_RE = re.compile(r"/farms/([a-z\-]+)/?$")


class FarmNaviScraper(StaticCrawler):
    """農園ナビ（全国貸し農園一覧）スクレイパー"""

    DELAY = 1.5
    # 都道府県は Schema.PREF、住所（詳細）は Schema.ADDR、電話番号は Schema.TEL に格納。
    # 市区町村・最寄駅/アクセスは Schema に無いため EXTRA_COLUMNS で定義。
    EXTRA_COLUMNS = ["市区町村", "最寄駅/アクセス"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        base = urljoin(url, "/")
        seen: set[str] = set()

        # 引数 url から一覧トップ (/farms/) を導出し、都道府県スラッグを動的取得
        top_url = self._farms_top(url)
        slugs = self._prefecture_slugs(top_url)

        # 引数 url の都道府県を先頭に持ってくる（ローカル/スモークテストで即結果が出るように）
        current = self._slug_of(url)
        if current:
            slugs = [current] + [s for s in slugs if s != current]
        if not slugs:
            # セレクトが取れなかった場合でも引数 url だけは必ず巡回する
            self.logger.warning("都道府県スラッグを取得できませんでした: %s", top_url)
            yield from self._crawl_pref_list(url, seen)
        else:
            self.logger.info("都道府県 %d 件を巡回します", len(slugs))
            for slug in slugs:
                list_url = urljoin(top_url, f"{slug}/")
                yield from self._crawl_pref_list(list_url, seen)

        # 取りこぼし防止: farm サイトマップ（同一ホストから派生）の未取得分を補完
        yield from self._crawl_sitemaps(base, seen)

    # ------------------------------------------------------------------
    # 都道府県リンク一覧（トップ /farms/ のセレクトから動的取得）
    # ------------------------------------------------------------------
    @staticmethod
    def _farms_top(url: str) -> str:
        """引数 url（/farms/tokyo/ 等）から一覧トップ /farms/ を導出する。"""
        path = urlparse(url).path
        idx = path.find("/farms/")
        if idx >= 0:
            return urljoin(url, path[: idx + len("/farms/")])
        return urljoin(url, "/farms/")

    @staticmethod
    def _slug_of(url: str) -> str:
        m = _PREF_SLUG_RE.search(urlparse(url).path)
        return m.group(1) if m else ""

    def _prefecture_slugs(self, top_url: str) -> list[str]:
        """トップページの都道府県セレクト/リンクからスラッグを収集する。"""
        try:
            soup = self.get_soup(top_url)
        except Exception as e:  # noqa: BLE001
            self.logger.warning("一覧トップの取得に失敗 (%s): %s", top_url, e)
            return []
        if soup is None:
            return []

        slugs: list[str] = []
        select = soup.select_one("select[data-prefecture-select], select#prefecture")
        if select:
            for opt in select.find_all("option"):
                value = (opt.get("value") or "").strip()
                if value:
                    slugs.append(value)

        # セレクトが無い/空のときは本文中の /farms/{slug}/ リンクから拾う
        if not slugs:
            for a in soup.select('a[href*="/farms/"]'):
                slug = self._slug_of(urljoin(top_url, a.get("href") or ""))
                if slug and slug not in ("page", "feed"):
                    slugs.append(slug)

        return list(dict.fromkeys(slugs))

    # ------------------------------------------------------------------
    # 一覧ページ巡回（/farms/{pref}/ → /farms/{pref}/page/N/）
    # ------------------------------------------------------------------
    def _crawl_pref_list(
        self, list_url: str, seen: set[str]
    ) -> Generator[dict, None, None]:
        if not list_url.endswith("/"):
            list_url += "/"

        page = 1
        last_page: int | None = None
        while True:
            page_url = list_url if page == 1 else urljoin(list_url, f"page/{page}/")
            try:
                soup = self.get_soup(page_url)
            except Exception as e:  # noqa: BLE001 — ページ範囲外(404等)は正常終了として扱う
                self.logger.info("一覧ページ取得終了 (%s): %s", page_url, e)
                break
            if soup is None:
                break

            if page == 1:
                last_page = self._last_page(soup, list_url)
                self.logger.info(
                    "一覧巡回開始: %s (最終ページ=%s)", list_url, last_page or "?"
                )

            detail_urls = [
                urljoin(page_url, a.get("href"))
                for a in soup.select('a[href*="/farm/id/"]')
                if a.get("href")
            ]
            # 順序を保ちつつ重複除去し、既取得分を除外
            fresh = [u for u in dict.fromkeys(detail_urls) if u not in seen]
            if not detail_urls:
                # リンクが 1 件も無ければ最終ページ（or 範囲外リダイレクト）とみなす
                break

            for detail_url in fresh:
                seen.add(detail_url)
                item = self._scrape_detail(detail_url)
                if item:
                    yield item

            if last_page is not None and page >= last_page:
                break
            page += 1

    @staticmethod
    def _last_page(soup, list_url: str) -> int | None:
        """ページャの /page/N/ リンクから最終ページ番号を取得する。"""
        base_path = urlparse(list_url).path
        pattern = re.compile(re.escape(base_path) + r"page/(\d+)/?")
        pages = [
            int(m.group(1))
            for a in soup.select('a[href*="/page/"]')
            if (m := pattern.search(a.get("href") or ""))
        ]
        return max(pages) if pages else None

    # ------------------------------------------------------------------
    # サイトマップ列挙（/sitemap.xml → /farm-sitemap*.xml → /farm/id/{id}/）
    # ------------------------------------------------------------------
    def _crawl_sitemaps(
        self, base: str, seen: set[str]
    ) -> Generator[dict, None, None]:
        try:
            index = self.get_soup(urljoin(base, "sitemap.xml"))
        except Exception as e:  # noqa: BLE001
            self.logger.warning("サイトマップ索引の取得に失敗: %s", e)
            return
        if index is None:
            return

        farm_maps = [
            loc
            for loc in re.findall(r"https?://[^\s\"'<>]+", index.decode())
            if re.search(r"/farm-sitemap\d*\.xml", loc)
        ]
        # 順序保持で重複除去
        farm_maps = list(dict.fromkeys(farm_maps))

        for sm_url in farm_maps:
            try:
                sm = self.get_soup(sm_url)
            except Exception as e:  # noqa: BLE001
                self.logger.warning("サイトマップ取得に失敗 (%s): %s", sm_url, e)
                continue
            if sm is None:
                continue

            detail_urls = list(dict.fromkeys(_DETAIL_RE.findall(sm.decode())))
            for detail_url in detail_urls:
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                item = self._scrape_detail(detail_url)
                if item:
                    yield item

    # ------------------------------------------------------------------
    # 詳細ページ解析
    # ------------------------------------------------------------------
    def _scrape_detail(self, url: str) -> dict | None:
        try:
            soup = self.get_soup(url)
        except Exception as e:  # noqa: BLE001
            self.logger.warning("詳細ページ取得に失敗 (%s): %s", url, e)
            return None
        if soup is None:
            return None

        # 農園名
        h1 = soup.select_one("h1")
        name = h1.get_text(strip=True) if h1 else ""

        # 住所（詳細）: <div><strong>住所</strong><p>東京都調布市仙川町1丁目28</p></div>
        addr = self._value_after_label(soup, "住所")

        # 電話番号: <div><strong>電話番号</strong><p>011-782-8130</p></div>（掲載が無い農園もある）
        tel = self._value_after_label(soup, "電話番号")

        # 最寄駅/アクセス: <strong>電車でお越しの方</strong><p>京王線｜仙川駅</p>
        access = self._value_after_label(soup, "電車でお越しの方")

        # 都道府県 / 市区町村: <div><small>エリア</small><strong>東京都 / 調布市</strong></div>
        pref, city = self._area(soup)
        # エリア表記が無い場合は住所（詳細）から都道府県を補完
        if not pref and addr:
            m = _PREF_RE.match(addr)
            if m:
                pref = m.group(1)

        if not name:
            return None

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            "市区町村": city,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            "最寄駅/アクセス": access,
            Schema.URL: url,
        }

    @staticmethod
    def _value_after_label(soup, label: str) -> str:
        """<strong>{label}</strong> の直後の <p> テキストを返す。"""
        for st in soup.find_all("strong"):
            if st.get_text(strip=True) == label:
                p = st.find_next_sibling("p")
                if p:
                    return p.get_text(" ", strip=True)
        return ""

    @staticmethod
    def _area(soup) -> tuple[str, str]:
        """<small>エリア</small><strong>都道府県 / 市区町村</strong> を分解して返す。"""
        for sm in soup.find_all("small"):
            if sm.get_text(strip=True) == "エリア":
                st = sm.find_next_sibling("strong")
                if st:
                    txt = st.get_text(strip=True)
                    parts = [p.strip() for p in txt.split("/", 1)]
                    if len(parts) == 2:
                        return parts[0], parts[1]
                    return txt, ""
        return "", ""


_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = FarmNaviScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://farm-navi.com/farms/tokyo/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
