"""
Retty (レッティ) — 実名型グルメサービスの店舗情報スクレイパー

取得対象:
    - 全国の掲載飲食店の店舗情報 (名称/読み/住所/TEL/ジャンル/営業時間/定休日/
      支払い方法/予算/座席/最寄駅 など)

取得フロー:
    トップURL (https://retty.me/) を起点に、都道府県ページ
    /area/PRE{JISコード}/ を組み立てて巡回する。
      1. 各都道府県ページのページャ (/area/PRE{n}/page-N/) から最終ページ数を把握
      2. page-1 から順にエリア一覧ページを取得し、店舗詳細リンク
         (/area/PRE{n}/ARE{a}/SUB{s}/{shop_id}/) を抽出
      3. 店舗詳細ページを 1 件取得するごとに即 yield (Pattern B)
    重複店舗は shop_id で除去し、ページャ外 (存在しないページは先頭ページを
    返す) 対策として「新規 0 件のページが連続したら」その都道府県を打ち切る。

備考:
    - requests の TLS フィンガープリントは WAF に 406 で弾かれるため、
      httpx クライアントを self.session として使う (_setup をオーバーライド)。
    - 口コミ本文 (reviewBody) 等の自由記述プロースは著作権リスクのため取得しない。
    - robots.txt は /area/PRE13/... 等の店舗ページを許可。
      利用規約 (https://retty.me/announce/tos/) にスクレイピングの明示禁止は無い
      (第14条(19) のサーバー過負荷禁止のみ) ため、DELAY を空けて巡回する。

実行方法:
    # ローカルテスト
    python scripts/sites/food/retty.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id retty
"""

import json
import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import httpx
from bs4 import BeautifulSoup

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 店舗詳細ページ: /area/PRE{pref}/ARE{area}/SUB{sub}/{shop_id}/
_SHOP_PATTERN = re.compile(r"^/area/PRE\d+/ARE\d+/SUB\d+/(\d+)/?$")
# ページャリンク: /area/PRE{pref}/page-N/
_PAGE_PATTERN = re.compile(r"/area/PRE\d+/page-(\d+)/?$")

# 都道府県 JIS コード (retty は先頭ゼロ無し PRE1..PRE47)
_PREF_CODES = [str(n) for n in range(1, 48)]

# 1 都道府県あたりのページ上限 (安全弁)。実データはページャから取得する。
_PAGE_CAP = 6000
# 「新規店舗 0 件」ページがこの回数連続したら都道府県を打ち切る
_MAX_EMPTY_PAGES = 2

# /area/ 配下はリクエスト過多で 429 を返す (バーストで数分ロックされる)。
# 毎リクエスト前に必ず間隔を空け、429 を食らったら指数バックオフで再試行する。
_REQUEST_DELAY = 2.0      # 実取得ごとの最小待機秒 (キャッシュヒットはスキップ)
_MAX_FETCH_ATTEMPTS = 4   # 429 リトライ上限 (無限ループ禁止)
_BACKOFF_CAP = 16.0       # バックオフ待機の上限秒


class RettyScraper(StaticCrawler):
    """Retty グルメ 店舗情報スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["予算", "座席", "カウンター席", "喫煙", "個室", "最寄駅", "利用シーン"]

    # ------------------------------------------------------------------
    # セッション: requests は WAF に 406 で弾かれるため httpx を使う
    # ------------------------------------------------------------------
    def _setup(self):
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "accept-language": "ja,en-US;q=0.9,en;q=0.8",
        }
        self.session = httpx.Client(
            headers=headers, timeout=self.TIMEOUT, follow_redirects=True
        )

    def get_soup(self, url: str) -> BeautifulSoup | None:
        """httpx で HTML を取得し BeautifulSoup を返す。エラー時は None。"""

        def _fetch() -> str:
            # 429 (rate limit) は回数上限付きバックオフで再試行する。
            # 毎回まず _REQUEST_DELAY 待ってからアクセスし、バーストを避ける。
            for attempt in range(_MAX_FETCH_ATTEMPTS):
                time.sleep(min(_REQUEST_DELAY * (2 ** attempt), _BACKOFF_CAP))
                self.logger.info("取得中: %s", url)
                resp = self.session.get(url)
                if resp.status_code == 429:
                    self.logger.warning(
                        "429 レート制限 (試行 %d/%d): %s",
                        attempt + 1, _MAX_FETCH_ATTEMPTS, url,
                    )
                    continue
                resp.raise_for_status()
                return resp.text
            raise RuntimeError(f"429 が {_MAX_FETCH_ATTEMPTS} 回続きました: {url}")

        try:
            html = self._fetch_html_cached(url, variant="", fetcher=_fetch)
            if html is None:
                return None
            return BeautifulSoup(html, "html.parser")
        except httpx.HTTPError as e:
            if self.CONTINUE_ON_ERROR:
                self.error_count += 1
                self.logger.warning("通信エラー (スキップして継続): %s — %s", url, e)
                return None
            self.logger.error("通信エラー: %s", e)
            raise

    # ------------------------------------------------------------------
    # メイン: 都道府県 → ページ → 店舗詳細
    # ------------------------------------------------------------------
    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_shops: set[str] = set()
        for code in _PREF_CODES:
            pref_url = urljoin(url, f"area/PRE{code}/")
            yield from self._parse_prefecture(pref_url, seen_shops)

    def _parse_prefecture(
        self, pref_url: str, seen_shops: set[str]
    ) -> Generator[dict, None, None]:
        # 1 ページ目でページャ最終ページ数を把握
        first = self.get_soup(f"{pref_url}page-1/")
        if first is None:
            return
        max_page = self._max_page(first)
        max_page = min(max_page, _PAGE_CAP)

        empty_streak = 0
        for page in range(1, max_page + 1):
            soup = first if page == 1 else self.get_soup(f"{pref_url}page-{page}/")
            if soup is None:
                empty_streak += 1
                if empty_streak >= _MAX_EMPTY_PAGES:
                    break
                continue

            new_urls = []
            for path in self._shop_paths(soup):
                shop_id = _SHOP_PATTERN.match(path).group(1)
                if shop_id in seen_shops:
                    continue
                seen_shops.add(shop_id)
                # path はサイト絶対パス (/area/PRE01/...) なので host に対して解決する
                new_urls.append(urljoin(pref_url, path))

            if not new_urls:
                # 範囲外ページは先頭ページ (既出店舗) を返すためここで打ち切る
                empty_streak += 1
                if empty_streak >= _MAX_EMPTY_PAGES:
                    break
                continue
            empty_streak = 0

            for shop_url in new_urls:
                item = self._scrape_detail(shop_url)
                if item:
                    yield item

    @staticmethod
    def _max_page(soup: BeautifulSoup) -> int:
        pages = [1]
        for a in soup.select("a[href]"):
            m = _PAGE_PATTERN.search(a.get("href", "").split("?")[0])
            if m:
                pages.append(int(m.group(1)))
        return max(pages)

    @staticmethod
    def _shop_paths(soup: BeautifulSoup) -> list[str]:
        out, seen = [], set()
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            path = re.sub(r"^https?://retty\.me", "", href).split("?")[0].split("#")[0]
            if _SHOP_PATTERN.match(path) and path not in seen:
                seen.add(path)
                out.append(path)
        return out

    # ------------------------------------------------------------------
    # 詳細ページ解析
    # ------------------------------------------------------------------
    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        ld = self._restaurant_ld(soup)
        data = {Schema.URL: url}

        # --- th/td 情報テーブル (定休日/座席/決済 等) ---
        tmap: dict[str, str] = {}
        for tr in soup.select("table.restaurant-info-table tr"):
            th = tr.select_one("th")
            td = tr.select_one("td")
            if th and td:
                key = th.get_text(strip=True)
                if key and key not in tmap:
                    tmap[key] = td.get_text(" ", strip=True)

        # --- 名称 / 読み ---
        name = (ld.get("name") or "").strip()
        if not name and tmap.get("店名"):
            # テーブルは "店名 ローマ字" の形式 → 先頭トークンのみ
            name = tmap["店名"].split()[0]
        if name:
            data[Schema.NAME] = name
        kana = ld.get("alternateName")
        if kana:
            data[Schema.NAME_KANA] = kana.strip()

        # --- 住所 / 都道府県 / 郵便番号 ---
        addr = ld.get("address") or {}
        region = addr.get("addressRegion", "").strip()
        if region:
            data[Schema.PREF] = region
        post = re.sub(r"\D", "", addr.get("postalCode", "") or "")
        if len(post) == 7:
            data[Schema.POST_CODE] = f"{post[:3]}-{post[3:]}"
        full_addr = (region + addr.get("addressLocality", "") + addr.get("streetAddress", "")).strip()
        if not full_addr:
            raw = tmap.get("住所", "")
            full_addr = raw.replace("大きな地図をみる", "").strip()
        if full_addr:
            data[Schema.ADDR] = full_addr

        # --- TEL ---
        tel = ld.get("telephone") or tmap.get("TEL", "")
        if tel:
            data[Schema.TEL] = tel.strip()

        # --- ジャンル (サイト定義) ---
        cuisine = ld.get("servesCuisine")
        if isinstance(cuisine, list):
            genre = " / ".join(c for c in cuisine if c)
        else:
            genre = (cuisine or "").strip() or tmap.get("ジャンル", "")
        if genre:
            data[Schema.CAT_SITE] = genre

        # --- 営業時間 / 定休日 ---
        if tmap.get("営業時間"):
            data[Schema.TIME] = tmap["営業時間"]
        if tmap.get("定休日"):
            data[Schema.HOLIDAY] = tmap["定休日"]

        # --- 支払い方法 (クレジットカード + QRコード決済) ---
        pays = [tmap.get("クレジットカード", ""), tmap.get("QRコード決済", "")]
        pay = " / ".join(p for p in pays if p and p != "不可")
        if not pay and (tmap.get("クレジットカード") == "不可" or tmap.get("QRコード決済") == "不可"):
            pay = "不可"
        if pay:
            data[Schema.PAYMENTS] = pay

        # --- HP (掲載があれば) ---
        hp_th = soup.find("th", string=re.compile(r"(ホームページ|公式サイト|URL)"))
        if hp_th:
            td = hp_th.find_next("td")
            a = td.find("a", href=True) if td else None
            if a and a["href"].startswith("http") and "retty.me" not in a["href"]:
                data[Schema.HP] = a["href"].strip()

        # --- EXTRA (短い構造化フィールド) ---
        for label, col in [
            ("予算", "予算"),
            ("座席", "座席"),
            ("カウンター席", "カウンター席"),
            ("喫煙", "喫煙"),
            ("個室", "個室"),
            ("利用シーン", "利用シーン"),
        ]:
            if tmap.get(label):
                data[col] = tmap[label]

        # --- 最寄駅 (footer 情報リストの dt/dd) ---
        for dt in soup.select("dl dt"):
            if dt.get_text(strip=True) == "最寄駅":
                dd = dt.find_next_sibling("dd")
                if dd:
                    data["最寄駅"] = dd.get_text(" ", strip=True)
                break

        if not data.get(Schema.NAME):
            return None
        return data

    @staticmethod
    def _restaurant_ld(soup: BeautifulSoup) -> dict:
        for sc in soup.select('script[type="application/ld+json"]'):
            try:
                d = json.loads(sc.string or sc.get_text())
            except Exception:
                continue
            for x in (d if isinstance(d, list) else [d]):
                if isinstance(x, dict) and x.get("@type") == "Restaurant":
                    return x
        return {}


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = RettyScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://retty.me/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
