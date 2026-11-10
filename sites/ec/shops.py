"""
対象サイト: メルカリShops (https://jp.mercari.com/search?keyword=...&item_types=beyond_shops)

取得対象:
    - メルカリShops に出店しているショップ (ショップ単位で 1 行に集約)
    - ショップ名 / ショップURL / ショップID / 発送元地域 (都道府県) / 紹介文
    - 紹介文に記載された TEL・メールアドレス (正規表現で抽出)
    - 評価 / レビュー件数 / フォロワー数 / 出品商品数

取得フロー:
    1. キーワード検索 (メルカリShops 商品に絞り込み) を全ページ巡回し、
       /shops/product/<ID> を収集する。起点 URL のキーワードを最初に使い、
       続いて _KEYWORDS (カテゴリ語・商品語) に差し替えて網羅的に巡回する。
    2. 未知のショップの商品ページから ショップID・「発送元の地域」を取得する。
    3. ショップページ /shops/profile/<ID> から ショップ名・評価・レビュー件数・
       フォロワー数・紹介文 (「ショップ情報」タブ)・出品商品数 を取得する。
       商品タブの商品 ID は既知集合に入れ、以降の検索結果で同ショップの商品を再訪しない。
    4. 発送元地域はショップ内の商品を最大 _REGION_SAMPLE 件見て最多の都道府県を採用し、
       見つかった全地域は「発送元地域」カラムに「/」区切りで出力する。
    1 ショップ処理するごとに即 yield する (全件バッファしない)。

備考:
    - サイトは Next.js の CSR。SSR HTML に商品・ショップのデータは含まれないため
      Playwright で描画後の DOM を読む。描画完了待ち・スクロールが必要なため
      get_soup() ではなく self.page を直接操作する。
    - 起点 URL の item_types=beyond_shops は現行 UI では無効値 (個人出品が返る)。
      有効値は item_types=beyond なので、起点 URL のクエリを引き継いだうえで値のみ正規化する。
    - /shops/profile/<ID>/details (特商法・ログイン限定) と api.mercari.jp の内部 API
      (/v1/ /v2/) は直接叩かない (robots.txt Disallow / 依頼者指示)。
    - 出品商品数はページ上に総数表示が無いため、商品タブに表示される商品カード数
      (スクロールで追加読込が止まるまで) を数える。
    - 紹介文は依頼者の備考で明示的に出力指定があるため DESCRIPTION に出力する。
    - エリア絞り込み (近畿等) は後工程で行うため全国分を出力する。

実行方法:
    # ローカルテスト
    python scripts/sites/ec/shops.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id shops
"""

import logging
import re
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Generator
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 起点キーワードの後に巡回するカテゴリ語・商品語 (メルカリShops の主要カテゴリを網羅)
_KEYWORDS = [
    # 食品・飲料
    "野菜", "果物", "りんご", "いちご", "ぶどう", "米", "肉", "牛肉", "豚肉", "鶏肉",
    "魚", "海鮮", "カニ", "干物", "お菓子", "和菓子", "スイーツ", "パン", "チョコレート",
    "コーヒー", "紅茶", "日本茶", "お茶", "日本酒", "ワイン", "ジュース", "調味料",
    "味噌", "醤油", "はちみつ", "漬物", "麺", "そば", "うどん", "ラーメン", "健康食品",
    "サプリメント", "ドライフルーツ", "ナッツ", "惣菜", "冷凍食品", "ギフト",
    # 花・植物
    "花", "観葉植物", "多肉植物", "苗", "種", "盆栽",
    # ファッション
    "レディース", "メンズ", "キッズ", "ベビー服", "バッグ", "財布", "靴", "帽子",
    "アクセサリー", "ピアス", "ネックレス", "指輪", "腕時計", "着物", "古着", "下着",
    # 美容・健康
    "コスメ", "スキンケア", "化粧水", "香水", "ヘアケア", "ネイル", "石鹸",
    # 家電・スマホ・カメラ
    "家電", "スマホ", "スマホケース", "パソコン", "タブレット", "カメラ", "イヤホン",
    "ゲーム", "PC周辺機器", "充電器",
    # インテリア・日用品
    "インテリア", "家具", "照明", "カーテン", "ラグ", "寝具", "収納", "キッチン用品",
    "食器", "雑貨", "日用品", "掃除用品", "タオル", "文房具", "手帳",
    # ハンドメイド・ホビー
    "ハンドメイド", "手作り", "陶器", "木工", "レジン", "布", "生地", "手芸",
    "おもちゃ", "フィギュア", "ぬいぐるみ", "プラモデル", "トレーディングカード",
    "本", "漫画", "CD", "DVD", "楽器", "アート", "絵画",
    # スポーツ・アウトドア・車
    "スポーツ", "ゴルフ", "釣り", "キャンプ", "アウトドア", "自転車", "車用品",
    "バイク用品", "工具", "DIY",
    # ペット・ベビー
    "ペット用品", "ドッグフード", "キャットフード", "ベビー用品", "マタニティ",
]

# 起点 URL の item_types を有効値へ正規化 (beyond_shops は現行 UI で無効)
_ITEM_TYPES_FIX = {"beyond_shops": "beyond"}

# 1 キーワードあたりのページ上限 (暴走防止)
_MAX_PAGES = 200

# 発送元地域の多数決に使うショップ内商品の最大件数 (起点商品を含む)
_REGION_SAMPLE = 3

# 出品商品数カウント時のスクロール上限
_MAX_SCROLLS = 30

# 描画待ちタイムアウト (ms)
_WAIT_MS = 20000

_PRODUCT_RE = re.compile(r"/shops/product/([A-Za-z0-9]+)")
_PROFILE_RE = re.compile(r"/shops/profile/([A-Za-z0-9]+)")
_TEL_RE = re.compile(r"(?<!\d)(0\d{1,4}[-‐－ー―(（ ]?\d{1,4}[-‐－ー―)） ]?\d{3,4})(?!\d)")
_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|"
    r"千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|"
    r"愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|"
    r"広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|"
    r"宮崎県|鹿児島県|沖縄県)$"
)

# EXTRA カラム名
_COL_SHOP_URL = "ショップURL"
_COL_SHOP_ID = "ショップID"
_COL_REGIONS = "発送元地域"
_COL_FOLLOWERS = "フォロワー数"
_COL_PRODUCTS = "出品商品数"


def _zen2han(text: str) -> str:
    return text.translate(str.maketrans("０１２３４５６７８９－（）", "0123456789-()"))


def _extract_tel(text: str) -> str:
    tels = []
    for m in _TEL_RE.finditer(_zen2han(text)):
        tel = re.sub(r"[‐－ー―(（)） ]", "-", m.group(1)).strip("-")
        digits = re.sub(r"\D", "", tel)
        if 10 <= len(digits) <= 11 and tel not in tels:
            tels.append(tel)
    return " / ".join(tels)


def _extract_email(text: str) -> str:
    emails = []
    for m in _EMAIL_RE.finditer(text):
        if m.group(0) not in emails:
            emails.append(m.group(0))
    return " / ".join(emails)


class ShopsScraper(DynamicCrawler):
    """メルカリShops スクレイパー (動的)"""

    DELAY = 3.0  # 待機時間（秒）
    EXTRA_COLUMNS = [_COL_SHOP_URL, _COL_SHOP_ID, _COL_REGIONS, _COL_FOLLOWERS, _COL_PRODUCTS]

    def parse(self, url: str) -> Generator[dict, None, None]:
        parsed = urlparse(url)
        query = [
            (k, _ITEM_TYPES_FIX.get(v, v) if k == "item_types" else v)
            for k, v in parse_qsl(parsed.query)
            if k != "page_token"
        ]
        first_kw = dict(query).get("keyword", "")
        keywords = [first_kw] + [kw for kw in _KEYWORDS if kw != first_kw]

        seen_shops: set[str] = set()
        known_products: set[str] = set()

        for kw in keywords:
            for page_no in range(_MAX_PAGES):
                params = [(k, kw if k == "keyword" else v) for k, v in query]
                if page_no:
                    params.append(("page_token", f"v1:{page_no}"))
                search_url = parsed._replace(query=urlencode(params)).geturl()
                product_ids, has_next = self._fetch_search(search_url)
                logger.info("検索 kw=%s page=%d 商品=%d", kw, page_no, len(product_ids))

                for pid in product_ids:
                    if pid in known_products:
                        continue
                    known_products.add(pid)
                    product_url = urljoin(url, f"/shops/product/{pid}")
                    shop_id, region = self._fetch_product(product_url)
                    if not shop_id or shop_id in seen_shops:
                        continue
                    seen_shops.add(shop_id)
                    item = self._build_shop(url, shop_id, pid, region, known_products)
                    if item:
                        yield item

                if not has_next or not product_ids:
                    break

    # ------------------------------------------------------------------
    # ページ取得
    # ------------------------------------------------------------------
    def _goto(self, url: str, selector: str) -> bool:
        time.sleep(self.DELAY)
        logger.info("取得中 (Playwright): %s", url)
        self.page.goto(url, wait_until="domcontentloaded")
        try:
            self.page.wait_for_selector(selector, timeout=_WAIT_MS)
            return True
        except PlaywrightTimeoutError:
            logger.warning("描画待ちタイムアウト: %s (%s)", url, selector)
            return False

    def _scroll_until_stable(self, selector: str) -> int:
        """仮想スクロールの追加読込が止まるまでスクロールし、要素数を返す。"""
        count = self.page.locator(selector).count()
        for _ in range(_MAX_SCROLLS):
            self.page.mouse.wheel(0, 5000)
            self.page.wait_for_timeout(1500)
            new_count = self.page.locator(selector).count()
            if new_count == count:
                break
            count = new_count
        return count

    def _fetch_search(self, url: str) -> tuple[list[str], bool]:
        if not self._goto(url, 'a[href*="/shops/product/"]'):
            return [], False
        self._scroll_until_stable('a[href*="/shops/product/"]')
        html = self.page.content()
        ids = list(dict.fromkeys(_PRODUCT_RE.findall(html)))
        has_next = "page_token=" in html and self.page.locator(
            'a[href*="page_token="]', has_text="次へ"
        ).count() > 0
        return ids, has_next

    def _fetch_product(self, url: str) -> tuple[str, str]:
        if not self._goto(url, "text=発送元の地域"):
            return "", ""
        soup = bs4.BeautifulSoup(self.page.content(), "html.parser")
        shop_ids = list(dict.fromkeys(
            _PROFILE_RE.search(a["href"]).group(1)
            for a in soup.select('a[href*="/shops/profile/"]')
            if _PROFILE_RE.search(a["href"])
        ))
        for tag in soup(["script", "style"]):
            tag.decompose()
        lines = soup.get_text("\n", strip=True).split("\n")
        region = ""
        for i, line in enumerate(lines[:-1]):
            if line == "発送元の地域":
                region = lines[i + 1]
                break
        return (shop_ids[0] if shop_ids else ""), region

    # ------------------------------------------------------------------
    # ショップ
    # ------------------------------------------------------------------
    def _build_shop(
        self, url: str, shop_id: str, first_pid: str, first_region: str, known_products: set
    ) -> dict | None:
        shop_url = urljoin(url, f"/shops/profile/{shop_id}")
        if not self._goto(shop_url, "h1"):
            return None
        try:
            self.page.wait_for_selector('[role="tab"]', timeout=_WAIT_MS)
        except PlaywrightTimeoutError:
            logger.warning("タブ描画待ちタイムアウト: %s", shop_url)

        soup = bs4.BeautifulSoup(self.page.content(), "html.parser")
        h1 = soup.select_one("h1")
        name = h1.get_text(strip=True) if h1 else ""

        score = review_count = ""
        rating = soup.select_one('[data-testid="shops-profile-rating"]')
        if rating:
            m = re.search(r"([\d.]+)\s*/\s*5", rating.get_text(" ", strip=True))
            score = m.group(1) if m else ""
            spans = [s.get_text(strip=True) for s in rating.find_all("span")]
            nums = [s for s in spans if re.fullmatch(r"[\d,]+", s)]
            review_count = nums[-1].replace(",", "") if nums else ""

        followers = ""
        for p in soup.find_all("p"):
            m = re.fullmatch(r"フォロワー\s*([\d,]+)", p.get_text(strip=True))
            if m:
                followers = m.group(1).replace(",", "")
                break

        # 商品タブ: 商品数 + 同ショップ商品 ID (検索結果での再訪防止 / 地域サンプル)
        product_count = ""
        shop_pids: list[str] = []
        if self.page.locator('[data-testid="item-thumbnail-link"]').count():
            product_count = str(self._scroll_until_stable('[data-testid="item-thumbnail-link"]'))
            shop_pids = list(dict.fromkeys(_PRODUCT_RE.findall(self.page.content())))
        known_products.update(shop_pids)

        # 紹介文 (ショップ情報タブ)
        description = ""
        tab = self.page.locator('[role="tab"]', has_text="ショップ情報")
        if tab.count():
            tab.first.click()
            self.page.wait_for_timeout(1500)
            panel = self.page.locator('[role="tabpanel"]').last
            if panel.locator("p").count():
                description = panel.locator("p").first.inner_text().strip()

        # 発送元地域: ショップ内商品を最大 _REGION_SAMPLE 件見て多数決
        regions = [first_region] if first_region else []
        for pid in [p for p in shop_pids if p != first_pid][: _REGION_SAMPLE - 1]:
            _, region = self._fetch_product(urljoin(url, f"/shops/product/{pid}"))
            if region:
                regions.append(region)
        counter = Counter(regions)
        top_region = counter.most_common(1)[0][0] if counter else ""

        return {
            Schema.URL: shop_url,
            Schema.NAME: name,
            Schema.PREF: top_region if _PREF_RE.match(top_region) else "",
            Schema.TEL: _extract_tel(description),
            Schema.EMAIL: _extract_email(description),
            Schema.DESCRIPTION: description,
            Schema.SCORES: score,
            Schema.REV_SCR: review_count,
            _COL_SHOP_URL: shop_url,
            _COL_SHOP_ID: shop_id,
            _COL_REGIONS: "/".join(counter.keys()),
            _COL_FOLLOWERS: followers,
            _COL_PRODUCTS: product_count,
        }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scraper = ShopsScraper()
    scraper.execute("https://jp.mercari.com/search?keyword=%E3%81%BF%E3%81%8B%E3%82%93&item_types=beyond_shops")
