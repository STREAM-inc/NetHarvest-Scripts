"""
MOTA車買取 加盟店一覧スクレイパー

取得対象:
    - 全国 47 都道府県の MOTA車買取 加盟店 (約 1,580 店)

取得フロー:
    1. 都道府県別一覧 `{root}pr{1..47}/` を巡回し、`article.p-shop-infocard` 内の
       `a.p-shop-infocard__link` から店舗詳細 URL (`/ullo/shop/{店舗ID}/`) を収集
       (一覧ページは市区町村セクションでは上位3件のみ表示だが、ページ内の
        ランキング等も含めた全カードを拾うと当該都道府県の全店舗が揃う。
        群馬県で市区町村ページ全件との突合を行い一致を確認済み)
    2. 収集した詳細 URL を 1 件取得するごとに即 yield する
    3. 詳細ページからは以下を抽出
       - `h1.c-headline-title` = 店舗名
       - `.p-shop-detail__main > p` = 店舗紹介文
       - `table.p-shop-data` の th/td = 住所 / 電話 / 営業 / 休日 / 運営
         (「運営」は「運営会社名（古物商許可番号：xxxx）」の複合表記のため分割する)
       - `.p-shop-star-parts__value.is-total` = 口コミ評価 (総合評価)
       - `.p-shop-star-parts__review` = 口コミ件数
       - `.p-shop-star-parts__list` = 査定価格 / 連絡・対応 / おすすめ度 の個別評価
    4. 都道府県別一覧に現れない店舗 (掲載一時停止等) を取りこぼさないよう、
       sitemap (`/sitemap_xml/sitemap_13_kaitori.xml`) の店舗 URL を補完に使う

備考:
    - TEL はサイト上ハイフン無しの連結形式 (例: 0272151855) で掲載されている。
      本スクリプトでは整形せず生データのまま格納する。
    - 都道府県はページ側に独立した項目が無いため住所の先頭から導出する。

実行方法:
    # ローカルテスト
    python scripts/sites/auto/mota.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id mota
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


# 正規 URL (sites.yml 登録値): https://autoc-one.jp/ullo/shopList/
# parse() は引数 url を唯一のルートとして扱うため、ここでは定数化しない。

# 都道府県別一覧は /ullo/shopList/pr1/ 〜 /pr47/ (MOTA 独自採番)
_PREF_COUNT = 47

# 店舗詳細 URL: /ullo/shop/{店舗ID}/
_SHOP_URL_PATTERN = re.compile(r"/ullo/shop/(\d+)/")
# 「株式会社Light（古物商許可番号：421202024031）」形式を分割する
_OPERATOR_PATTERN = re.compile(r"^(.*?)\s*[（(]\s*古物商許可番号\s*[：:]\s*([^）)]*)[）)]\s*$")
# 口コミ件数は "(37)" / "（37）" 表記
_REVIEW_COUNT_PATTERN = re.compile(r"[\d,]+")
# 総合評価は "4.8PT" 表記
_SCORE_PATTERN = re.compile(r"\d+(?:\.\d+)?")
# 住所先頭の都道府県
_PREF_PATTERN = re.compile(r"^(北海道|東京都|(?:京都|大阪)府|[^\s]{2,3}県)")
# 補完用サイトマップ (買取カテゴリ)。root からの相対で解決する
_SITEMAP_PATH = "/sitemap_xml/sitemap_13_kaitori.xml"

_MULTI_WS = re.compile(r"\s+")


class MotaScraper(StaticCrawler):
    """MOTA車買取 加盟店一覧スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "運営会社名",
        "古物商許可番号",
        "査定価格評価",
        "連絡・対応評価",
        "おすすめ度評価",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url を唯一のルートとして扱い、以降の URL は全てここから派生させる
        root = url if url.endswith("/") else url + "/"

        # サイトマップ上の全店舗 ID (取りこぼし補完 + 進捗表示用)。失敗しても致命的ではない
        sitemap_urls = self._fetch_sitemap_shops(root)
        if sitemap_urls:
            self.total_items = len(sitemap_urls)

        seen: set[str] = set()

        for pref_no in range(1, _PREF_COUNT + 1):
            pref_url = urljoin(root, f"pr{pref_no}/")
            soup = self.get_soup(pref_url)
            if soup is None:
                self.logger.warning("都道府県一覧の取得に失敗: %s", pref_url)
                continue

            detail_urls = []
            for card in soup.select("article.p-shop-infocard"):
                link = card.select_one('a.p-shop-infocard__link[href*="/ullo/shop/"]')
                if link is None:
                    link = card.select_one('a[href*="/ullo/shop/"]')
                if link is None:
                    continue
                href = link.get("href", "")
                m = _SHOP_URL_PATTERN.search(href)
                if not m:
                    continue
                shop_id = m.group(1)
                if shop_id in seen:
                    continue
                seen.add(shop_id)
                detail_urls.append((shop_id, urljoin(root, f"/ullo/shop/{shop_id}/")))

            self.logger.info("pr%d: 店舗 %d 件", pref_no, len(detail_urls))

            for shop_id, detail_url in detail_urls:
                item = self._scrape_detail(detail_url, shop_id)
                if item:
                    yield item

        # 都道府県別一覧に出てこなかった店舗をサイトマップから補完する
        for shop_id, detail_url in sitemap_urls:
            if shop_id in seen:
                continue
            seen.add(shop_id)
            item = self._scrape_detail(detail_url, shop_id)
            if item:
                yield item

    def _fetch_sitemap_shops(self, root: str) -> list[tuple[str, str]]:
        """サイトマップから店舗詳細 URL の一覧を取得する (失敗時は空リスト)。"""
        sitemap_url = urljoin(root, _SITEMAP_PATH)
        try:
            res = self.session.get(sitemap_url, timeout=self.TIMEOUT)
            res.raise_for_status()
        except Exception as exc:  # noqa: BLE001 — 補完用途のため取得失敗は警告に留める
            self.logger.warning("サイトマップの取得に失敗: %s (%s)", sitemap_url, exc)
            return []

        result: list[tuple[str, str]] = []
        seen: set[str] = set()
        for shop_id in _SHOP_URL_PATTERN.findall(res.text):
            if shop_id in seen:
                continue
            seen.add(shop_id)
            result.append((shop_id, urljoin(root, f"/ullo/shop/{shop_id}/")))
        self.logger.info("サイトマップ掲載店舗: %d 件", len(result))
        return result

    def _scrape_detail(self, detail_url: str, shop_id: str) -> dict | None:
        """店舗詳細ページから 1 店舗分のデータを抽出する。"""
        try:
            soup = self.get_soup(detail_url)
        except Exception as exc:  # noqa: BLE001 — 1 店舗の失敗で全体を止めない
            self.logger.warning("詳細取得エラー: %s (%s)", detail_url, exc)
            return None
        if soup is None:
            self.logger.warning("詳細取得に失敗: %s", detail_url)
            return None

        name_el = soup.select_one("h1.c-headline-title") or soup.select_one("h1")
        name = self._clean(name_el.get_text(strip=True)) if name_el else ""
        if not name:
            self.logger.warning("店舗名を取得できません: %s", detail_url)
            return None

        # 店舗データテーブル (住所 / 電話 / 営業 / 休日 / 運営)
        data: dict[str, str] = {}
        for tr in soup.select("table.p-shop-data tr"):
            th = tr.find("th")
            td = tr.find("td")
            if th is None or td is None:
                continue
            data[self._clean(th.get_text(strip=True))] = self._clean(
                td.get_text(" ", strip=True)
            )

        addr = data.get("住所", "")
        pref_match = _PREF_PATTERN.match(addr)
        pref = pref_match.group(1) if pref_match else ""

        operator_raw = data.get("運営", "")
        operator, license_no = self._split_operator(operator_raw)

        # 口コミ評価 (総合評価) と口コミ件数
        score = ""
        score_el = soup.select_one(".p-shop-star-parts__value.is-total")
        if score_el:
            m = _SCORE_PATTERN.search(score_el.get_text(strip=True))
            if m:
                score = m.group(0)

        review_count = ""
        review_el = soup.select_one(".p-shop-star-parts__review")
        if review_el:
            m = _REVIEW_COUNT_PATTERN.search(review_el.get_text(strip=True))
            if m:
                review_count = m.group(0).replace(",", "")

        # 個別評価 (査定価格 / 連絡・対応 / おすすめ度)
        sub_scores = self._parse_sub_scores(soup)

        # 店舗紹介文 (h1 直後のリード文)
        description = ""
        main = soup.select_one(".p-shop-detail__main")
        if main:
            lead = main.find("p")
            if lead:
                description = self._clean(lead.get_text(" ", strip=True))

        return {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: data.get("電話", ""),
            Schema.TIME: data.get("営業", ""),
            Schema.HOLIDAY: data.get("休日", ""),
            Schema.SCORES: score,
            Schema.REV_SCR: review_count,
            Schema.DESCRIPTION: description,
            "運営会社名": operator,
            "古物商許可番号": license_no,
            "査定価格評価": sub_scores.get("査定価格", ""),
            "連絡・対応評価": sub_scores.get("連絡・対応", ""),
            "おすすめ度評価": sub_scores.get("おすすめ度", ""),
        }

    def _parse_sub_scores(self, soup) -> dict[str, str]:
        """`.p-shop-star-parts__list` の項目名と点数を対応付ける。"""
        result: dict[str, str] = {}
        block = soup.select_one(".p-shop-star-parts__list")
        if block is None:
            return result
        titles = [
            self._clean(e.get_text(strip=True))
            for e in block.select(".p-shop-star-parts__title")
        ]
        values = [
            self._clean(e.get_text(strip=True))
            for e in block.select(".p-shop-star-parts__value")
        ]
        for title, value in zip(titles, values):
            m = _SCORE_PATTERN.search(value)
            result[title] = m.group(0) if m else ""
        return result

    @staticmethod
    def _split_operator(raw: str) -> tuple[str, str]:
        """「運営会社名（古物商許可番号：xxxx）」を会社名と許可番号に分割する。"""
        if not raw:
            return "", ""
        m = _OPERATOR_PATTERN.match(raw)
        if m:
            return m.group(1).strip(), m.group(2).strip()
        return raw, ""

    @staticmethod
    def _clean(text: str) -> str:
        return _MULTI_WS.sub(" ", (text or "").replace("　", " ")).strip()


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = MotaScraper()
    scraper.execute("https://autoc-one.jp/ullo/shopList/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
