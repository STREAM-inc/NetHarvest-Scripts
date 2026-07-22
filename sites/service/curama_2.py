"""
くらしのマーケット2 (curama.jp) — サービス情報 + 店舗プロフィール収集スクレイパー

取得対象:
    - /category/ 一覧に掲載される全カテゴリ (/aircon/, /moving/, /pest/ …) の
      各サービス詳細ページ (SER{n})
    - 各サービス詳細ページのトラッキング JS 内 item_brand を店舗ID(9桁)として取得
    - 店舗ID から生成した店舗プロフィール (/{店舗ID}/) の店舗情報

取得フロー (カテゴリ一覧 → カテゴリ別サービス一覧 → サービス詳細 → 店舗プロフィール;
            Pattern B: 1件ごとに即 yield):
    1. 引数 url (= https://curama.jp/) から /category/ を urljoin で導出し全カテゴリを列挙
       (ヘッダ/フッタの about/category/magazine/privacy/terms は除外)
    2. 各カテゴリで ?page=N& によりページネーション。SERリンクが無い or「次のN件」リンクが
       無い or 直前ページと同一一覧になったら次カテゴリへ
       ※ 一部カテゴリ(例 /moving/)は service-details アンカーを持たないため、
         id ではなく href の /SER{n}/ で抽出する
    3. サービスURLは SER から始まるサービスIDで重複排除 (カテゴリ横断でグローバルに)
    4. 各サービス詳細ページで item_name(SER)/item_brand(9桁店舗ID)/item_category/price と
       microdata (ratingValue/reviewCount/name) と店舗画像URL(/store/{9桁}/)を取得
    5. item_brand を正式な店舗IDとし、店舗ID から /{店舗ID}/ プロフィールURLを生成
    6. 店舗プロフィールは店舗IDで重複排除し1回だけ取得 (キャッシュ)、
       サービス行に店舗情報を付与して yield

注記:
    - サービス説明・店舗紹介文は「自由記述の文章」のため著作権リスク回避で取得しない
    - 電話番号は店舗連絡先として本文に明示されている場合のみ (format-detection は除外)

実行方法:
    python scripts/sites/service/curama_2.py
    python bin/run_flow.py --site-id curama_2
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

_ZENKAKU = "０１２３４５６７８９－（）"
_HANKAKU = "0123456789-()"
_TRANS = str.maketrans(_ZENKAKU, _HANKAKU)

_START_PATH = "/aircon/"
_CATEGORY_INDEX_PATH = "/category/"

# /category/ から列挙する単一セグメントのカテゴリパス (/aircon/ 等)。
_CATEGORY_PATH_RE = re.compile(r"^/([a-z0-9-]+)/$")
# サービス/カテゴリではないヘッダ・フッタ導線を除外する。
_NON_CATEGORY_SLUGS = {"about", "category", "magazine", "privacy", "terms"}

_SER_RE = re.compile(r"(SER\d+)")
# サービス詳細リンクは href に /SER{n}/ を含む (id=service-details を持たない
# カテゴリ /moving/ 等にも対応するため id ではなく href で抽出する)。
_SER_HREF_RE = re.compile(r"/SER\d+/")
_NEXT_BTN_RE = re.compile(r"次の\d+件|次へ")
_STORE_ID_RE = re.compile(r"/store/(\d{9})/")
_POST_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")

_ITEM_NAME_RE = re.compile(r"item_name:\s*'(SER\d+)'")
_ITEM_BRAND_RE = re.compile(r"item_brand:\s*'(\d{6,})'")
_ITEM_CATEGORY_RE = re.compile(r"item_category:\s*'([^']+)'")
_PRICE_RE = re.compile(r"price:\s*(\d+)")
_STORE_IMG_RE = re.compile(
    r"(//[\w.-]*curama\.jp/[^\"' ]*?/store/(\d{9})/[^\"' ]+\.(?:jpg|jpeg|png|webp))"
)


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


class Curama2Scraper(StaticCrawler):
    """くらしのマーケット2 (curama.jp) サービス+店舗スクレイパー"""

    DELAY = 1.5

    EXTRA_COLUMNS = [
        "price",
        "service_rating",
        "service_review_count",
        "service_url",
        "store_rating",
        "store_review_count",
        "profile_url",
    ]

    def parse(self, url: str):
        seen_services: set[str] = set()
        store_cache: dict[str, dict] = {}

        category_paths = self._extract_categories(url)
        if not category_paths:
            # フォールバック: 一覧が取れなくとも最低限エアコンは巡回する
            self.logger.warning("カテゴリ一覧が取得できず /aircon/ のみ巡回します")
            category_paths = [_START_PATH]
        self.logger.info("巡回カテゴリ数: %d", len(category_paths))

        for cat_path in category_paths:
            start_url = urljoin(url, cat_path)
            yield from self._iter_category(start_url, url, seen_services, store_cache)

    def _extract_categories(self, url: str) -> list[str]:
        """/category/ から全カテゴリの単一セグメントパスを順序保持で列挙する。"""
        index_url = urljoin(url, _CATEGORY_INDEX_PATH)
        self.logger.info("カテゴリ一覧取得: %s", index_url)
        soup = self.get_soup(index_url)
        if not soup:
            return []

        paths: list[str] = []
        seen: set[str] = set()
        for a in soup.find_all("a", href=_CATEGORY_PATH_RE):
            m = _CATEGORY_PATH_RE.match(a.get("href", ""))
            if not m:
                continue
            slug = m.group(1)
            if slug in _NON_CATEGORY_SLUGS:
                continue
            path = f"/{slug}/"
            if path in seen:
                continue
            seen.add(path)
            paths.append(path)
        return paths

    def _iter_category(
        self, start_url: str, root_url: str, seen_services: set, store_cache: dict
    ):
        """1カテゴリを ?page=N& で巡回し、サービス行を1件ずつ yield する。"""
        prev_hrefs: list[str] | None = None
        page = 1

        while True:
            page_url = f"{start_url}?page={page}&"
            self.logger.info("一覧ページ取得: %s", page_url)
            soup = self.get_soup(page_url)
            if not soup:
                break

            # id=service-details を持たないカテゴリもあるため href の /SER{n}/ で抽出
            anchors = soup.find_all("a", href=_SER_HREF_RE)
            hrefs = [a.get("href") for a in anchors if a.get("href")]
            if not hrefs:
                break
            # 直前ページと同一一覧になったら終了 (ページ送りが効いていない)
            if prev_hrefs is not None and hrefs == prev_hrefs:
                break
            prev_hrefs = hrefs

            for href in hrefs:
                m = _SER_RE.search(href)
                if not m:
                    continue
                service_id = m.group(1)
                if service_id in seen_services:
                    continue
                seen_services.add(service_id)

                detail_url = urljoin(root_url, href)
                try:
                    record = self._scrape_service(detail_url, root_url, store_cache)
                except Exception as e:  # noqa: BLE001
                    self.logger.warning("サービス取得失敗: %s — %s", detail_url, e)
                    continue
                if record:
                    self.total_items = len(seen_services)
                    yield record

            next_btn = soup.find("a", string=_NEXT_BTN_RE)
            if not next_btn:
                break
            page += 1

    def _scrape_service(self, detail_url: str, root_url: str, store_cache: dict) -> dict | None:
        soup = self.get_soup(detail_url)
        if not soup:
            return None

        html = str(soup)

        # --- トラッキング JS ブロック (item_name の後方に item_brand/category/price) ---
        # store_id (item_brand) は店舗プロフィール取得の起点として内部利用のみ
        store_id = ""
        category = ""
        price = ""
        nm = _ITEM_NAME_RE.search(html)
        if nm:
            block = html[nm.start(): nm.start() + 400]
            bm = _ITEM_BRAND_RE.search(block)
            if bm:
                store_id = bm.group(1)
            cm = _ITEM_CATEGORY_RE.search(block)
            if cm:
                category = cm.group(1)
            pm = _PRICE_RE.search(block)
            if pm:
                price = pm.group(1)

        if not category:
            # URL パスの第1階層をカテゴリとしてフォールバック
            parts = [p for p in detail_url.split("curama.jp/")[-1].split("/") if p]
            if parts:
                category = parts[0]

        # --- 店舗画像 URL から店舗ID を補助取得 (item_brand が無い場合の内部fallback) ---
        if not store_id:
            im = _STORE_IMG_RE.search(html)
            if im:
                store_id = im.group(2)

        # --- サービス項目 (microdata) ---
        service_rating = ""
        rv = soup.find(attrs={"itemprop": "ratingValue"})
        if rv:
            service_rating = rv.get("content") or _clean(rv.get_text(strip=True))
        service_review = ""
        rc = soup.find(attrs={"itemprop": "reviewCount"})
        if rc:
            service_review = rc.get("content") or _clean(rc.get_text(strip=True))

        record: dict = {
            Schema.URL: detail_url,
            Schema.CAT_SITE: category,
            "price": price,
            "service_rating": service_rating,
            "service_review_count": service_review,
            "service_url": detail_url,
        }

        # --- 店舗プロフィール (店舗IDで重複排除・1回だけ取得) ---
        if store_id:
            if store_id not in store_cache:
                store_cache[store_id] = self._scrape_store(store_id, root_url)
            record.update(store_cache[store_id])

        return record

    def _scrape_store(self, store_id: str, root_url: str) -> dict:
        profile_url = urljoin(root_url, f"/{store_id}/")
        info: dict = {
            Schema.NAME: "",
            Schema.PREF: "",
            Schema.POST_CODE: "",
            Schema.ADDR: "",
            Schema.TIME: "",
            Schema.HOLIDAY: "",
            "store_rating": "",
            "store_review_count": "",
            "profile_url": profile_url,
        }
        soup = self.get_soup(profile_url)
        if not soup:
            return info

        # 店舗名: プロフィール本文 (h1[itemprop=name]) を優先し title と照合
        h1 = soup.find("h1", attrs={"itemprop": "name"}) or soup.find("h1")
        name = _clean(h1.get_text(strip=True)) if h1 else ""
        if name and soup.title:
            title = soup.title.get_text(strip=True)
            # title に店舗名が含まれていれば整合とみなす (照合のみ、値は本文優先)
            if name not in title:
                self.logger.debug("店舗名と title 不一致: %s / %s", name, title)
        info[Schema.NAME] = name

        # 住所: meta[itemprop=address] の content を優先
        addr = ""
        addr_meta = soup.find("meta", attrs={"itemprop": "address"})
        if addr_meta and addr_meta.get("content"):
            addr = _clean(addr_meta["content"]).translate(_TRANS)

        # 所在地 h3 ブロック (郵便番号・住所フォールバック)
        for h3 in soup.find_all("h3"):
            if _clean(h3.get_text(strip=True)) == "所在地":
                nd = h3.find_next_sibling("div")
                if nd:
                    raw = _clean(nd.get_text(" ", strip=True)).translate(_TRANS)
                    pmt = _POST_RE.search(raw)
                    if pmt:
                        info[Schema.POST_CODE] = pmt.group(1)
                    if not addr:
                        addr = re.sub(r"〒?\s*\d{3}-?\d{4}\s*", "", raw).strip()
                break

        if addr:
            info[Schema.ADDR] = addr
            pm = _PREF_PATTERN.search(addr)
            if pm:
                info[Schema.PREF] = pm.group(1)

        # 評価・口コミ数 (microdata 優先)
        rv = soup.find(attrs={"itemprop": "ratingValue"})
        if rv:
            info["store_rating"] = rv.get("content") or _clean(rv.get_text(strip=True))
        rc = soup.find("meta", attrs={"itemprop": "reviewCount"}) or soup.find(
            attrs={"itemprop": "reviewCount"}
        )
        if rc:
            info["store_review_count"] = rc.get("content") or _clean(rc.get_text(strip=True))

        # 営業時間・定休日 (h3 ラベル → 直後 div)
        for h3 in soup.find_all("h3"):
            label = _clean(h3.get_text(strip=True))
            if label == "営業時間":
                nd = h3.find_next_sibling("div")
                if nd:
                    info[Schema.TIME] = _clean(nd.get_text(" ", strip=True))
            elif label == "定休日":
                nd = h3.find_next_sibling("div")
                if nd:
                    info[Schema.HOLIDAY] = _clean(nd.get_text(" ", strip=True))

        return info


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Curama2Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://curama.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
