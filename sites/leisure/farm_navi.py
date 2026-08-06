"""
農園ナビ（全国貸し農園一覧） — https://farm-navi.com/

取得対象:
    - 全国の市民農園・貸し農園（体験農園）
    - 農園名, 都道府県, 市区町村, 住所（詳細: 丁目・番地まで）, 最寄駅/アクセス
    - TEL・運営会社名は掲載が無いため取得しない（空欄）

取得フロー:
    1. 引数 url（指定都道府県の一覧ページ, 例 /farms/tokyo/）を起点にページ送り
       (/page/N/) で巡回し、各農園の詳細ページ (/farm/id/{id}/) を都度取得・yield
    2. 全国 47 都道府県を漏れなく網羅するため、続けて farm サイトマップ
       (/farm-sitemap*.xml, url と同一ホストから派生) に載る全詳細ページを列挙し、
       未取得分を都度取得・yield する（約 3,000 件）
    ※ 一覧カードには農園名とリンクしか無く、住所等は詳細ページのみに存在するため
       一覧→詳細（Pattern B: 1 件取得ごとに即 yield）で実装する。

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
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 詳細ページ URL の正規表現（一覧・サイトマップ共通で抽出に使う）
_DETAIL_RE = re.compile(r"https?://[^\s\"'<>]*?/farm/id/\d+/")


class FarmNaviScraper(StaticCrawler):
    """農園ナビ（全国貸し農園一覧）スクレイパー"""

    DELAY = 1.5
    # 都道府県は Schema.PREF、住所（詳細）は Schema.ADDR に格納。
    # 市区町村・最寄駅/アクセスは Schema に無いため EXTRA_COLUMNS で定義。
    EXTRA_COLUMNS = ["市区町村", "最寄駅/アクセス"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        base = urljoin(url, "/")
        seen: set[str] = set()

        # 1) 引数 url（指定都道府県の一覧）を起点にページ送り巡回して詳細を都度 yield
        yield from self._crawl_pref_list(url, seen)

        # 2) 全国の残り農園を farm サイトマップ（同一ホストから派生）で網羅
        yield from self._crawl_sitemaps(base, seen)

    # ------------------------------------------------------------------
    # 一覧ページ巡回（/farms/{pref}/ → /farms/{pref}/page/N/）
    # ------------------------------------------------------------------
    def _crawl_pref_list(
        self, list_url: str, seen: set[str]
    ) -> Generator[dict, None, None]:
        if not list_url.endswith("/"):
            list_url += "/"

        page = 1
        while True:
            page_url = list_url if page == 1 else urljoin(list_url, f"page/{page}/")
            try:
                soup = self.get_soup(page_url)
            except Exception as e:  # noqa: BLE001 — ページ範囲外(404等)は正常終了として扱う
                self.logger.info("一覧ページ取得終了 (%s): %s", page_url, e)
                break
            if soup is None:
                break

            detail_urls = [
                urljoin(page_url, a.get("href"))
                for a in soup.select('a[href*="/farm/id/"]')
                if a.get("href")
            ]
            # 順序を保ちつつ重複除去し、既取得分を除外
            fresh = [u for u in dict.fromkeys(detail_urls) if u not in seen]
            if not fresh:
                # 新規リンクが無ければ最終ページ（or 範囲外リダイレクト）とみなす
                break

            for detail_url in fresh:
                seen.add(detail_url)
                item = self._scrape_detail(detail_url)
                if item:
                    yield item

            page += 1

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
