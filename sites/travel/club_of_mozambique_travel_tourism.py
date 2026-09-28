"""
Club of Mozambique ビジネスディレクトリ (Travel & Tourism) — スクレイパー

対象サイト: https://clubofmozambique.com/business-directory/

取得対象:
    clubofmozambique.com の Business Directory (WordPress + Directorist プラグイン /
    投稿タイプ at_biz_dir、全 250 社) のうち、旅行・観光カテゴリ
    (Travel & Tourism 系) に属する事業者のみ。

    会社名 / 住所 / 郵便番号 / TEL / 電話番号(2) / FAX / メールアドレス /
    公式サイト URL / サイト定義カテゴリ

取得フロー:
    1. ルート URL (/business-directory/) から Directorist REST API を導出する。
       - カテゴリ分類: /wp-json/directorist/v1/listings/categories
       - 掲載企業一覧: /wp-json/directorist/v1/listings?categories={cat_id}
    2. カテゴリ分類から旅行・観光系カテゴリ (_TARGET_CATEGORY_RE) の ID と、
       その配下 (子孫) カテゴリ ID を解決する。
    3. 対象カテゴリごとに per_page=50 で 1 ページずつ取得し、1 件ずつ即 yield する
       (Pattern B / 早期 yield)。複数カテゴリに重複掲載された企業は ID で除外する。

調査メモ (2026-09-28 時点):
    - 全パスが Cloudflare マネージドチャレンジ配下で requests は 403。Playwright
      (headless, 既定 UA) はチャレンジを通過できるため DynamicCrawler を使う。
      連続遷移で再チャレンジに落ちないよう goto 直前に context.clear_cookies() する。
    - カテゴリ絞り込み UI は一覧ページの検索フォーム (select[name="in_cat"]) で、
      HTML 側は /business-directory/?in_cat={cat_id}。同じ絞り込みが REST API の
      ?categories={cat_id} で行えるため、API 側を使う (一覧 HTML は JS 描画で
      静的 HTML にカードが存在しない)。
    - カテゴリのタクソノミーは全 56 件すべてフラット (parent=0)。「Travel & Tourism」
      という名称のカテゴリは現時点で存在せず、対象は「Hospitality & Tourism」
      (id=692, 3 件) のみ。旅行代理店・ツアーオペレーター専用カテゴリはサイト側に
      無いため、名称に travel / tourism / tour operator / travel agency を含む
      カテゴリを対象とする (将来 "Travel & Tourism" が追加されれば自動で拾う)。
      子カテゴリが追加された場合に備えて親子解決も実装してある。
    - 詳細ページ (/directory/{slug}/) は「Single listing view is disabled」で
      掲載内容が一切描画されない。掲載情報は REST API にのみ存在する。
    - tagline / description (事業紹介の長文) は著作権リスクのため取得しない。
    - 利用規約 (/terms-of-use/) にスクレイピング・クローリングの明示禁止は無い
      (禁止されているのは配信元ニュース記事の全文転載)。robots.txt も
      /business-directory/ と /wp-json/ を許可している。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/club_of_mozambique_travel_tourism.py

    # Prefect Flow 経由 (全件)
    docker compose exec worker python /app/bin/run_flow.py --site-id club_of_mozambique_travel_tourism
"""

import html as html_lib
import json
import logging
import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

# REST API のパス (ルート URL からの相対。urljoin でルート URL のオリジンに結合する)
LISTINGS_PATH = "/wp-json/directorist/v1/listings"
CATEGORIES_PATH = "/wp-json/directorist/v1/listings/categories"

# 1 リクエストあたりの取得件数 (read timeout 回避のため 50 件以下に抑える)
PER_PAGE = 50

# 無限ループ防止の上限 (掲載総数 250 社に対し十分大きい)
MAX_PAGES = 30
MAX_CATEGORY_PAGES = 5

# 取得対象カテゴリの判定 (正規化後のカテゴリ名に対して部分一致)
# 「Travel & Tourism」「Hospitality & Tourism」「Tour Operators」等を拾う
_TARGET_CATEGORY_RE = re.compile(r"(travel|tourism|tour operator|tourist)", re.IGNORECASE)

# 通信リトライ回数 (上限到達で raise する。無限再帰・無限ループは禁止)
MAX_ATTEMPTS = 3

# FAX は Schema に定義が無いためサイト固有カラムとして持つ
COL_FAX = "FAX"


def _norm_name(value: str) -> str:
    """カテゴリ名を比較用に正規化する (HTML エンティティ解除 → 空白圧縮 → 小文字)。"""
    text = html_lib.unescape(value or "").replace(" ", " ")
    # 「and」表記ゆれを「&」に寄せる (例: "Hospitality and Tourism")
    text = re.sub(r"\s+and\s+", " & ", text, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", text).strip().lower()


def _clean(value) -> str:
    """API の値を表示用の文字列に整形する (None / 数値も安全に文字列化)。"""
    if value is None:
        return ""
    text = html_lib.unescape(str(value)).replace(" ", " ")
    return re.sub(r"\s+", " ", text).strip()


def _normalize_url(value) -> str:
    """スキーム無しで登録されている公式サイト URL (例: www.example.com) を補う。"""
    text = _clean(value)
    if not text:
        return ""
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", text):
        text = "http://" + text.lstrip("/")
    return text


class ClubOfMozambiqueTravelTourism(DynamicCrawler):
    """Club of Mozambique ビジネスディレクトリ (Travel & Tourism) スクレイパー"""

    # ページ取得間の待機は parse() 内で明示的に行うため、アイテム間待機は 0 にする
    DELAY = 2.0
    ITEM_DELAY = 0
    EXTRA_COLUMNS = [COL_FAX]

    # -------------------------------------------------------------------
    # 通信
    # -------------------------------------------------------------------
    def _fetch_json(self, url: str):
        """Playwright で JSON エンドポイントを取得してデコードする。

        Cloudflare のマネージドチャレンジ対策として goto 直前に Cookie を破棄し、
        毎回「初回訪問」扱いにする。失敗時は指数バックオフ付きで MAX_ATTEMPTS 回まで
        再試行し、それでも駄目なら例外を送出する (黙って無限リトライしない)。
        """
        last_error = None

        for attempt in range(MAX_ATTEMPTS):
            if attempt:
                time.sleep(min(2 ** attempt, 10))

            def _fetch() -> str:
                self.logger.info("取得中 (API): %s", url)
                self.context.clear_cookies()
                response = self.page.goto(url, wait_until="domcontentloaded", timeout=60000)
                if response is None:
                    raise RuntimeError(f"レスポンスがありません: {url}")
                if response.status != 200:
                    raise RuntimeError(f"HTTP {response.status}: {url}")
                return response.text()

            try:
                body = self._fetch_html_cached(url, variant="json", fetcher=_fetch)
                if not body:
                    raise RuntimeError(f"レスポンスが空です: {url}")
                return json.loads(body)
            except (RuntimeError, ValueError) as e:
                last_error = e
                self.logger.warning("API 取得に失敗 (%d/%d): %s", attempt + 1, MAX_ATTEMPTS, e)
            except Exception as e:  # Playwright のタイムアウト等
                last_error = e
                self.logger.warning("API 取得中にエラー (%d/%d): %s", attempt + 1, MAX_ATTEMPTS, e)

        raise RuntimeError(
            f"API を取得できませんでした ({MAX_ATTEMPTS} 回失敗): {url} — {last_error}"
        )

    # -------------------------------------------------------------------
    # カテゴリ解決
    # -------------------------------------------------------------------
    def _resolve_target_category_ids(self, root_url: str) -> list[int]:
        """旅行・観光系カテゴリとその配下カテゴリの ID 一覧を返す。

        取得に失敗した場合は空リストを返し、呼び出し側は全件走査 +
        カテゴリ名での直接判定にフォールバックする。
        """
        categories: list[dict] = []
        for page in range(1, MAX_CATEGORY_PAGES + 1):
            api = urljoin(root_url, f"{CATEGORIES_PATH}?per_page=100&page={page}")
            try:
                chunk = self._fetch_json(api)
            except RuntimeError as e:
                self.logger.warning("カテゴリ分類を取得できませんでした: %s", e)
                break
            if not isinstance(chunk, list) or not chunk:
                break
            categories.extend(chunk)
            if len(chunk) < 100:
                break

        if not categories:
            return []

        # 親 ID → 子カテゴリ ID のマップを作り、対象カテゴリから子孫まで辿る
        children: dict[int, list[int]] = {}
        for cat in categories:
            children.setdefault(int(cat.get("parent") or 0), []).append(int(cat["id"]))

        target_ids: list[int] = []
        seen: set[int] = set()
        queue = [
            int(cat["id"])
            for cat in categories
            if _TARGET_CATEGORY_RE.search(_norm_name(cat.get("name", "")))
        ]
        while queue:
            cat_id = queue.pop(0)
            if cat_id in seen:
                continue
            seen.add(cat_id)
            target_ids.append(cat_id)
            queue.extend(children.get(cat_id, []))

        self.logger.info(
            "対象カテゴリ: %s (全カテゴリ %d 件中)", target_ids, len(categories)
        )
        return target_ids

    @staticmethod
    def _is_target_by_name(listing: dict) -> bool:
        """カテゴリ ID を解決できなかった場合のフォールバック判定 (カテゴリ名で直接判定)。"""
        for cat in listing.get("categories") or []:
            if _TARGET_CATEGORY_RE.search(_norm_name(cat.get("name", ""))):
                return True
        return False

    # -------------------------------------------------------------------
    # 変換
    # -------------------------------------------------------------------
    def _to_item(self, listing: dict, root_url: str) -> dict:
        """API の 1 件を出力用の辞書に変換する。"""
        categories = " / ".join(
            _clean(c.get("name")) for c in listing.get("categories") or [] if c.get("name")
        )
        return {
            Schema.URL: _clean(listing.get("permalink")) or root_url,
            Schema.NAME: _clean(listing.get("name")),
            Schema.POST_CODE: _clean(listing.get("zip")),
            Schema.ADDR: _clean(listing.get("address")),
            Schema.TEL: _clean(listing.get("phone")),
            Schema.PHONE: _clean(listing.get("phone_2")),
            Schema.EMAIL: _clean(listing.get("email")),
            Schema.HP: _normalize_url(listing.get("website")),
            Schema.CAT_SITE: categories,
            COL_FAX: _clean(listing.get("fax")),
        }

    def _iter_listing_pages(self, root_url: str, query: str) -> Generator[list, None, None]:
        """掲載企業一覧 API をページ送りしながら 1 ページ分ずつ返す。"""
        for page in range(1, MAX_PAGES + 1):
            api = urljoin(root_url, f"{LISTINGS_PATH}?per_page={PER_PAGE}&page={page}{query}")
            try:
                listings = self._fetch_json(api)
            except RuntimeError as e:
                self.logger.warning("一覧ページ %d を取得できませんでした: %s", page, e)
                return

            if not isinstance(listings, list) or not listings:
                return

            yield listings

            # 最終ページに到達したら終了
            if len(listings) < PER_PAGE:
                return

            if self.DELAY > 0:
                time.sleep(self.DELAY)

    # -------------------------------------------------------------------
    # メイン
    # -------------------------------------------------------------------
    def parse(self, url: str) -> Generator[dict, None, None]:
        """ルート URL から REST API を導出し、旅行・観光カテゴリの掲載企業を 1 件ずつ返す。"""
        target_ids = self._resolve_target_category_ids(url)
        seen_ids: set[int] = set()

        if target_ids:
            # カテゴリ絞り込み (?categories={id}) で対象だけを取得する
            for cat_id in target_ids:
                for listings in self._iter_listing_pages(url, f"&categories={cat_id}"):
                    for listing in listings:
                        listing_id = int(listing.get("id") or 0)
                        if listing_id and listing_id in seen_ids:
                            continue  # 複数カテゴリに重複掲載されている企業
                        seen_ids.add(listing_id)
                        yield self._to_item(listing, url)
            return

        # フォールバック: カテゴリ分類を取得できなかった場合は全件走査 + 名前判定
        self.logger.warning("カテゴリ ID を解決できないため全件走査にフォールバックします")
        for listings in self._iter_listing_pages(url, ""):
            for listing in listings:
                if not self._is_target_by_name(listing):
                    continue
                listing_id = int(listing.get("id") or 0)
                if listing_id and listing_id in seen_ids:
                    continue
                seen_ids.add(listing_id)
                yield self._to_item(listing, url)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = ClubOfMozambiqueTravelTourism()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://clubofmozambique.com/business-directory/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
