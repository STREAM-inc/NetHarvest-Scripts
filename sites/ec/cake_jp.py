"""
CAKE*JP (Cake.jp) — 店舗受取予約 お店一覧

取得対象:
    - Cake.jp に出店しているケーキ・スイーツ店舗
      (起点 URL のカテゴリ絞り込み = デコレーションケーキ を販売するお店)
    - 店舗名 / 郵便番号 / 都道府県 / 住所 / 運営責任者 (販売会社情報)
    - 口コミ件数 / 口コミ採点
    - 販売業者・適格請求書発行事業者の登録・最短受取日 (EXTRA)

取得フロー:
    - 一覧ページ (https://cake.jp/shops/?category=decoration-cake) は React SPA で、
      店舗データは内部 API `/shops_v2/list/` から JSON で配信される。
    - parse() はまず一覧ページ HTML を取得して h1 からサイト定義ジャンル
      (例: 「デコレーションケーキ」) を導出する。
    - 続いて起点 URL のクエリ (category / area 等) をそのまま引き継いで
      `/shops_v2/list/?<起点クエリ>&page=N` を 1 ページずつ叩き、
      1 店舗ごとに詳細ページ /shops/{id}/ を取得して即 yield する
      (全件バッファしない)。
    - ページ送りは paginator.nextPage / pageCount で終端判定する。

備考:
    - 「ブランドの紹介」等の長文プロースは著作権リスクのため取得しない。
    - 電話番号・営業時間・HP はサイト上に掲載が無いため取得できない。

実行方法:
    # ローカルテスト
    python scripts/sites/ec/cake_jp.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id cake_jp
"""

import logging
import re
import sys
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import requests

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 店舗一覧を返す内部 API (SPA の XHR 先)。1 ページ 10 件固定。
_API_PATH = "/shops_v2/list/"

# 暴走防止のページ上限 (decoration-cake は 14 ページ / 全店舗でも 1000 ページ未満)。
_MAX_PAGES = 1000

# 郵便番号 (〒955-0094 形式)
_POST_CODE_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")

# 都道府県
_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|"
    r"千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|"
    r"愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|"
    r"広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|"
    r"宮崎県|鹿児島県|沖縄県)"
)

# 一覧ページ h1「デコレーションケーキを販売しているお店一覧」→「デコレーションケーキ」
_H1_SUFFIX_RE = re.compile(r"を(?:販売しているお店|扱うお店|取り扱うお店)?一覧\s*$")

# EXTRA カラム名
_COL_SHOP_ID = "店舗ID"
_COL_SELLER = "販売業者"
_COL_INVOICE = "適格請求書発行事業者の登録"
_COL_SHORTEST = "最短受取日"


class CakeJp(StaticCrawler):
    """CAKE*JP (Cake.jp) 店舗一覧 スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        _COL_SHOP_ID,
        _COL_SELLER,
        _COL_INVOICE,
        _COL_SHORTEST,
    ]

    # ------------------------------------------------------------------ #
    # メイン
    # ------------------------------------------------------------------ #
    def parse(self, url: str):
        # サイト定義ジャンル (起点 URL の category 絞り込み) を h1 から導出する
        cat_site = self._extract_category_label(url)

        api_url = urljoin(url, _API_PATH)
        # 起点 URL のクエリ (category / area / how_to_receive 等) をそのまま引き継ぐ
        base_params = [(k, v) for k, v in parse_qsl(urlparse(url).query) if k != "page"]

        seen: set[str] = set()
        page = 1
        while page <= _MAX_PAGES:
            data = self._get_json(f"{api_url}?{urlencode(base_params + [('page', page)])}")
            if not data:
                break

            shops = data.get("shops") or []
            if not shops:
                break

            paginator = data.get("paginator") or {}
            page_count = paginator.get("pageCount")
            if page == 1 and isinstance(page_count, int) and page_count > 0:
                # ETA 表示用 (最終ページは端数だが概算で十分)
                self.total_items = page_count * len(shops)

            for shop in shops:
                shop_id = str(shop.get("id") or shop.get("domain") or "").strip()
                if not shop_id or shop_id in seen:
                    continue
                seen.add(shop_id)

                item = self._build_item(shop, shop_id, url, cat_site)
                if item:
                    yield item

            if not paginator.get("nextPage"):
                break
            page += 1

    # ------------------------------------------------------------------ #
    # 1 店舗分の組み立て (一覧 API + 詳細ページ)
    # ------------------------------------------------------------------ #
    def _build_item(self, shop: dict, shop_id: str, root_url: str, cat_site: str) -> dict | None:
        detail_url = urljoin(root_url, f"/shops/{shop_id}/")
        detail = self._scrape_detail(detail_url)

        name = (detail.get("_title") or shop.get("title") or "").strip()
        if not name:
            return None

        # 住所は詳細ページの「住所」を優先し、無ければ一覧 API の address を使う
        raw_addr = detail.get("住所") or shop.get("address") or ""
        post_code, pref, addr = self._split_address(raw_addr)

        review = shop.get("review") or {}
        review_total = review.get("total")
        review_avg = review.get("average")

        return {
            Schema.NAME: name,
            Schema.URL: detail_url,
            Schema.POST_CODE: post_code,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.REP_NM: detail.get("運営責任者", ""),
            Schema.CAT_SITE: cat_site,
            _COL_SHOP_ID: shop_id,
            _COL_SELLER: detail.get("販売業者", ""),
            _COL_INVOICE: detail.get("適格請求書発行事業者の登録", ""),
            Schema.REV_SCR: str(review_total) if review_total is not None else "",
            Schema.SCORES: str(review_avg) if review_avg else "",
            _COL_SHORTEST: shop.get("shortestDate") or "",
        }

    def _scrape_detail(self, detail_url: str) -> dict:
        """詳細ページの「販売会社情報」定義リストと店舗名を辞書で返す。"""
        soup = self.get_soup(detail_url)
        if soup is None:
            return {}

        result: dict[str, str] = {}

        title_el = soup.select_one("h1.p-shops-detail-header__title, h1")
        if title_el:
            result["_title"] = title_el.get_text(strip=True)

        for dl in soup.select("dl.p-shops-detail-information__defs"):
            dt = dl.find("dt")
            dd = dl.find("dd")
            if not dt or not dd:
                continue
            label = dt.get_text(strip=True)
            value = re.sub(r"\s+", " ", dd.get_text(" ", strip=True)).strip()
            if label:
                result[label] = value

        return result

    # ------------------------------------------------------------------ #
    # ユーティリティ
    # ------------------------------------------------------------------ #
    def _extract_category_label(self, url: str) -> str:
        """一覧ページ h1 からサイト定義ジャンル名を取り出す。"""
        soup = self.get_soup(url)
        if soup is None:
            return ""
        # SPA なので h1 は data-args (JSON) 内の h1Text にある
        app = soup.select_one("#app")
        h1_text = ""
        if app and app.get("data-args"):
            m = re.search(r'"h1Text"\s*:\s*"(.*?)"', app["data-args"])
            if m:
                h1_text = m.group(1)
        if not h1_text:
            title = soup.select_one("h1")
            h1_text = title.get_text(strip=True) if title else ""
        h1_text = h1_text.strip()
        label = _H1_SUFFIX_RE.sub("", h1_text).strip()
        return label or h1_text

    def _get_json(self, api_url: str) -> dict | None:
        """内部 API を叩いて JSON を返す (通信エラー時は None)。"""
        try:
            logger.info("取得中(API): %s", api_url)
            res = self.session.get(
                api_url,
                timeout=self.TIMEOUT,
                headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"},
            )
            res.raise_for_status()
            return res.json()
        except (requests.exceptions.RequestException, ValueError) as e:
            if self.CONTINUE_ON_ERROR:
                self.error_count += 1
                logger.warning("API 取得エラー (スキップ): %s — %s", api_url, e)
                return None
            raise

    @staticmethod
    def _split_address(raw: str) -> tuple[str, str, str]:
        """「〒955-0094 新潟県 三条市 大島960」→ (郵便番号, 都道府県, 住所)。"""
        text = re.sub(r"\s+", " ", (raw or "")).strip()
        if not text:
            return "", "", ""

        post_code = ""
        m = _POST_CODE_RE.search(text)
        if m:
            post_code = m.group(1)
            if "-" not in post_code:
                post_code = f"{post_code[:3]}-{post_code[3:]}"
            text = (text[: m.start()] + text[m.end():]).strip()

        pref = ""
        m = _PREF_RE.search(text)
        if m:
            pref = m.group(1)
            text = (text[: m.start()] + text[m.end():]).strip()

        # 「三条市 大島960」のような市区町村とその先の間の空白は詰める
        addr = re.sub(r"\s+", "", text).strip()
        return post_code, pref, addr


if __name__ == "__main__":
    import logging as _logging

    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = CakeJp()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://cake.jp/shops/?category=decoration-cake")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
