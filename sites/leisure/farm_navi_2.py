"""
農園ナビ（全国の市民農園・貸し農園検索サイト） — https://farm-navi.com/farms/

取得対象:
    - 全国の市民農園・貸し農園（体験農園） 約 3,028 件
      （/farms/ 全国一覧 = 12 件/ページ × 253 ページ）
    - 農園名 / 都道府県 / 市区町村 / 住所 / 電話番号 / 募集状況 /
      最寄駅・アクセス / 特徴タグ / 設備 / サポート

サイト構造 (2026-08 調査):
    1. 一覧 (全国) : https://farm-navi.com/farms/
       ページ送りは WordPress 標準の /farms/page/{N}/
       （最終ページ番号は 1 ページ目のページャリンクから読み取る。253 ページ）
       カードは <article class="entry-card nn-card ..."> で
       詳細リンクは <a href="https://farm-navi.com/farm/id/{id}/">
    2. 詳細 : /farm/id/{id}/
       - 農園名        : <h1>
       - エリア        : <small>エリア</small><strong>北海道 / 札幌市北区</strong>
                         （<div class="nn-detail-meta"> にも同じ表記あり）
       - 募集状況      : <span class="farm-status farm-status--open">募集中</span>
       - 住所 / 電話番号 / 電車でお越しの方 :
                         <div class="nn-info-row">…<strong>ラベル</strong><p>値</p></div>
       - 設備 / サポート: <div class="nn-info-card"> の <h3> で判別し
                         <span class="nn-icon-list__label"> を列挙
       - 特徴タグ      : <div class="nn-detail-tags"> の <span class="tag-chip">
    3. 取りこぼし防止として farm サイトマップ
       (/sitemap.xml → /farm-sitemap*.xml) の未取得分を最後に補完する

サイトに存在しないため取得しないカラム:
    郵便番号 / 法人番号 / 代表者 / 役職 / 資本金 / 売上 / 従業員数 / 設立日 /
    FAX / メール / HP / SNS（Instagram・Facebook・X・LINE）
    → いずれも農園ナビの一覧・詳細ページに掲載が無い
    「農園について」(nn-prose) は運営者による長文の自由記述のため
    著作権リスクを避けて取得しない。

出現率メモ (実ページ 24 件サンプリング):
    エリア 24/24 / 電話番号 23/24 / 住所 18/24 / アクセス 1/24
    → 出現率が低いカラムも実装し、値が無い場合は空文字を返す

実行方法:
    # ローカルテスト
    python scripts/sites/leisure/farm_navi_2.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id farm_navi_2
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

# 詳細ページ URL（一覧カード・サイトマップ共通）
_DETAIL_RE = re.compile(r"https?://[^\s\"'<>]*?/farm/id/\d+/")

_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


class FarmNavi2Scraper(StaticCrawler):
    """農園ナビ（全国貸し農園一覧）スクレイパー"""

    DELAY = 1.0
    # 都道府県=Schema.PREF / 住所=Schema.ADDR / 電話番号=Schema.TEL /
    # 募集状況=Schema.STS_NM。Schema に無い項目は EXTRA_COLUMNS で定義する。
    EXTRA_COLUMNS = ["市区町村", "最寄駅/アクセス", "特徴タグ", "設備", "サポート"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 🔒 引数 url（= sites.yml の url）を唯一のルートとして使う
        list_url = url if url.endswith("/") else url + "/"
        seen: set[str] = set()

        # 1. 全国一覧をページ送りで巡回（詳細は 1 件取得ごとに即 yield）
        yield from self._crawl_list(list_url, seen)

        # 2. 取りこぼし防止: farm サイトマップの未取得分を補完
        yield from self._crawl_sitemaps(urljoin(list_url, "/"), seen)

    # ------------------------------------------------------------------
    # 一覧巡回 (/farms/ → /farms/page/N/)
    # ------------------------------------------------------------------
    def _crawl_list(
        self, list_url: str, seen: set[str]
    ) -> Generator[dict, None, None]:
        page = 1
        last_page: int | None = None

        while True:
            page_url = list_url if page == 1 else urljoin(list_url, f"page/{page}/")
            try:
                soup = self.get_soup(page_url)
            except Exception as e:  # noqa: BLE001 — 範囲外(404)は正常終了として扱う
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
                for a in soup.select('article.entry-card a[href*="/farm/id/"]')
                if a.get("href")
            ]
            if not detail_urls:
                # カードが 1 件も無ければ最終ページ（or 範囲外リダイレクト）
                self.logger.info("カードが見つからないため一覧巡回を終了: %s", page_url)
                break

            for detail_url in dict.fromkeys(detail_urls):
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                item = self._scrape_detail(detail_url)
                if item:
                    yield item

            if last_page is not None and page >= last_page:
                break
            page += 1

    @staticmethod
    def _last_page(soup, list_url: str) -> int | None:
        """ページャの /page/N/ リンクから最終ページ番号を読み取る。"""
        base_path = urlparse(list_url).path
        pattern = re.compile(re.escape(base_path) + r"page/(\d+)/?")
        pages = [
            int(m.group(1))
            for a in soup.select('a[href*="/page/"]')
            if (m := pattern.search(a.get("href") or ""))
        ]
        return max(pages) if pages else None

    # ------------------------------------------------------------------
    # サイトマップ補完 (/sitemap.xml → /farm-sitemap*.xml)
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

        farm_maps = list(
            dict.fromkeys(
                loc
                for loc in re.findall(r"https?://[^\s\"'<>\]]+", index.decode())
                if re.search(r"/farm-sitemap\d*\.xml", loc)
            )
        )
        if farm_maps:
            self.logger.info("サイトマップ補完: %d ファイル", len(farm_maps))

        for sm_url in farm_maps:
            try:
                sm = self.get_soup(sm_url)
            except Exception as e:  # noqa: BLE001
                self.logger.warning("サイトマップ取得に失敗 (%s): %s", sm_url, e)
                continue
            if sm is None:
                continue

            for detail_url in dict.fromkeys(_DETAIL_RE.findall(sm.decode())):
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

        # 「近隣の農園」カードを拾わないよう、詳細本体のスコープに限定する
        main = soup.select_one("div.nn-detail-layout") or soup
        title = soup.select_one("section.nn-detail-title") or soup

        h1 = soup.select_one("h1")
        name = h1.get_text(strip=True) if h1 else ""
        if not name:
            return None

        # 住所 / 電話番号 / アクセス（<strong>ラベル</strong><p>値</p>）
        addr = self._value_after_label(main, "住所")
        tel = self._value_after_label(main, "電話番号")
        access = self._value_after_label(main, "電車でお越しの方")
        if not access:
            # サイドバーの <small>アクセス</small><strong>値</strong> をフォールバック
            access = self._side_spec(main, "アクセス")

        # 都道府県 / 市区町村（<small>エリア</small><strong>北海道 / 札幌市北区</strong>）
        pref, city = self._area(main, title)
        if not pref and addr:
            if m := _PREF_RE.match(addr):
                pref = m.group(1)

        # 募集状況（募集中 / 募集終了 など）
        status_el = title.select_one("span.farm-status") or main.select_one(
            "span.farm-status"
        )
        status = status_el.get_text(strip=True) if status_el else ""

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            "市区町村": city,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.STS_NM: status,
            "最寄駅/アクセス": access,
            "特徴タグ": self._chips(main),
            "設備": self._icon_list(main, "設備"),
            "サポート": self._icon_list(main, "サポート"),
            Schema.URL: url,
        }

    # ------------------------------------------------------------------
    # 詳細ページ内の小ヘルパー
    # ------------------------------------------------------------------
    @staticmethod
    def _value_after_label(scope, label: str) -> str:
        """<strong>{label}</strong> の直後の <p> テキストを返す。"""
        for st in scope.find_all("strong"):
            if st.get_text(strip=True) == label:
                p = st.find_next_sibling("p")
                if p:
                    return p.get_text(" ", strip=True)
        return ""

    @staticmethod
    def _side_spec(scope, label: str) -> str:
        """<small>{label}</small><strong>値</strong> の値を返す。"""
        for sm in scope.find_all("small"):
            if sm.get_text(strip=True) == label:
                st = sm.find_next_sibling("strong")
                if st:
                    return st.get_text(" ", strip=True)
        return ""

    def _area(self, main, title) -> tuple[str, str]:
        """「北海道 / 札幌市北区」表記を都道府県・市区町村に分解する。"""
        txt = self._side_spec(main, "エリア")
        if not txt:
            # サイドバーが無い場合はタイトル直下のメタ表記から拾う
            for div in title.select("div.nn-detail-meta div"):
                candidate = div.get_text(" ", strip=True)
                if _PREF_RE.match(candidate):
                    txt = candidate
                    break
        if not txt:
            return "", ""

        parts = [p.strip() for p in txt.split("/", 1)]
        if len(parts) == 2:
            return parts[0], parts[1]
        return parts[0], ""

    @staticmethod
    def _chips(scope) -> str:
        """特徴タグ（サポートあり / 駐車場あり 等）を「/」連結で返す。"""
        tags = [
            s.get_text(strip=True)
            for s in scope.select("div.nn-detail-tags span.tag-chip")
        ]
        return " / ".join(dict.fromkeys(t for t in tags if t))

    @staticmethod
    def _icon_list(scope, heading: str) -> str:
        """<h3>{heading}</h3> を持つ nn-info-card 内のラベルを「/」連結で返す。"""
        for card in scope.select("div.nn-info-card"):
            h3 = card.select_one("h3")
            if h3 and h3.get_text(strip=True) == heading:
                labels = [
                    s.get_text(strip=True)
                    for s in card.select("span.nn-icon-list__label")
                ]
                return " / ".join(dict.fromkeys(l for l in labels if l))
        return ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = FarmNavi2Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://farm-navi.com/farms/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
