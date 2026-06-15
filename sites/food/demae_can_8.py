"""
出前館 (demae-can) — フードデリバリーポータル 店舗スクレイパー
(demae_can_8 / 起点 = /shopDetail ・ID 列挙型 — 一覧ページなし)

取得対象 (店舗詳細ページ /shopDetail/{id} を一次ソースに構造化情報のみ抽出):
    - 店舗名 / ジャンル / 都道府県 / 住所 / 郵便番号 / TEL /
      営業時間 / 定休日 / 支払い方法 / 取得URL
    - EXTRA: テイクアウト可否、配達形態

🔒 URL 一貫性 (SSOT = sites.yml の url):
    起点 url = https://demae-can.com/shopDetail  (= sites.yml の url / __main__ の execute 引数)。
    店舗詳細 URL は f"{url}/{shop_id}" で url から直接派生させる。別 URL をハードコードしない。

取得フロー (ID ブロック列挙型 — 一覧ページなし):
    1. _START_SHOP_ID (3,000,000) から上方向にスキャン開始
    2. 連続ミスが _BLOCK_MISS_LIMIT に達したらそのブロック終了
    3. 1,000,000 戻して次ブロックをスキャン (3M→, 2M-3M, 1M-2M, 0-1M の順)
    4. 取得成功時は INFO ログで店名・都道府県・URL を出力 (取得確認用)
    ※ 実績 ID 例: 3110319 → 3M 帯を優先探索
    ※ 一覧ページが存在しないため ID 直接アクセスで全件を網羅する。

注意 (重要):
    - demae-can.com は Akamai WAF 配下。requests ベースは 403 全拒否 → Playwright 必須。
    - 有効 ID が疎な分布の場合、全件収集に長時間を要する。_BLOCK_MISS_LIMIT を
      調整することで早期終了のしきい値を変更できる。

実行方法:
    # ローカルテスト
    python scripts/sites/food/demae_can_8.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id demae_can_8
"""

import json
import re
import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema


_PREF_PATTERN = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POSTCODE_PATTERN = re.compile(r"\d{3}-?\d{4}")
_TAKEOUT_PATTERN = re.compile(r"テイクアウト|お持ち帰り|お持帰り|持ち帰り")

_PAYMENT_NAMES = {
    "visa": "VISA", "master": "Master", "jcb": "JCB", "amex": "AMEX",
    "diners": "Diners", "amazon": "Amazon Pay", "paypay": "PayPay",
    "docomo": "d払い", "carrier": "キャリア決済", "apple": "Apple Pay",
    "google": "Google Pay", "rakuten": "楽天ペイ", "linepay": "LINE Pay",
    "aupay": "au PAY", "merpay": "メルペイ",
}

_SECTION_HEADINGS = {
    "営業時間", "定休日", "住所", "ご利用できるお支払い方法", "お支払い方法",
    "配達員", "店舗からのコメント", "Information", "配達エリア", "店舗からのお知らせ",
}

# ブロック内で連続してこの件数だけ invalid だったらそのブロックを終了
_BLOCK_MISS_LIMIT = 20_000
# 探索開始 ID (実績 ID 3110319 が存在する 3M 帯を優先)
_START_SHOP_ID = 3_000_000
# ブロックサイズ (連続ミス上限到達後に戻す幅)
_BLOCK_SIZE = 1_000_000
# ID 列挙の絶対上限
_MAX_SHOP_ID = 10_000_000

# 404/エラーページ判定に使うタイトル文言
_ERROR_TITLE_PATTERNS = [
    "ページが見つかりません", "お探しのページは", "店舗が見つかりません",
    "404", "Not Found", "エラー",
]


class DemaeCan8Scraper(DynamicCrawler):
    """出前館 (demae-can) スクレイパー — /shopDetail 起点・ID 列挙型"""

    DELAY = 2.0
    CONTINUE_ON_ERROR = True

    EXTRA_COLUMNS = [
        "テイクアウト可",   # テキスト/JSON-LD 検出: 記載があれば "可"
        "配達形態",        # 「配達員」セクションの値 (例: 出前館スタッフ)
    ]

    # ------------------------------------------------------------------ utils

    def _iter_jsonld(self, soup):
        """ページ内の JSON-LD ブロックを dict 単位で列挙する。"""
        for tag in soup.select('script[type="application/ld+json"]'):
            raw = (tag.string or tag.get_text() or "").strip()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except (ValueError, TypeError):
                continue
            candidates = data if isinstance(data, list) else [data]
            for d in candidates:
                if isinstance(d, dict):
                    if isinstance(d.get("@graph"), list):
                        for g in d["@graph"]:
                            if isinstance(g, dict):
                                yield g
                    yield d

    @staticmethod
    def _lines(soup) -> list[str]:
        text = soup.get_text("\n", strip=True)
        return [ln.strip() for ln in text.split("\n") if ln.strip()]

    @classmethod
    def _section_value(cls, lines: list[str], label: str) -> str:
        try:
            idx = next(i for i, ln in enumerate(lines) if ln == label)
        except StopIteration:
            return ""
        collected: list[str] = []
        for ln in lines[idx + 1:]:
            if ln in _SECTION_HEADINGS:
                break
            collected.append(ln)
        return " / ".join(collected).strip()

    @staticmethod
    def _first_text(soup, selectors: list[str]) -> str:
        for sel in selectors:
            el = soup.select_one(sel)
            if el:
                txt = el.get_text(" ", strip=True)
                if txt:
                    return txt
        return ""

    @classmethod
    def _extract_payments(cls, soup) -> str:
        found: list[str] = []
        for img in soup.select('img[src*="payment-method"], [class*="payment" i] img'):
            label = (img.get("alt") or "").strip()
            if not label:
                src = (img.get("src") or "").lower()
                for key, name in _PAYMENT_NAMES.items():
                    if key in src:
                        label = name
                        break
            if label and label not in found:
                found.append(label)
        return " / ".join(found)

    @classmethod
    def _extract_genre(cls, soup, name: str) -> str:
        for a in soup.select('a[href*="/chain/list/"]'):
            txt = a.get_text(" ", strip=True)
            if txt and "一覧" not in txt and len(txt) <= 30:
                return txt
        m = re.search(r"[（(]([^）)]{1,30})[）)]\s*の店舗詳細", name)
        if m:
            return m.group(1).strip()
        m = re.search(r"[（(]([^）)]{1,30})[）)]", name)
        return m.group(1).strip() if m else ""

    @staticmethod
    def _clean_name(raw: str) -> str:
        n = raw.strip()
        n = re.sub(r"[（(][^）)]*[）)]\s*の店舗詳細$", "", n)
        n = re.sub(r"\s*の店舗詳細$", "", n)
        m = re.search(r"店舗詳細[｜|]\s*(.+?)\s*の宅配", n)
        if m:
            n = m.group(1)
        return n.strip()

    @classmethod
    def _detect_takeout(cls, soup, jsonld_dicts: list[dict], page_text: str) -> str:
        for d in jsonld_dicts:
            actions = d.get("potentialAction")
            action_list = actions if isinstance(actions, list) else [actions]
            for act in action_list:
                if not isinstance(act, dict):
                    continue
                dm = act.get("deliveryMethod")
                dm_str = json.dumps(dm, ensure_ascii=False) if dm is not None else ""
                if re.search(r"takeout|pickup|pick[-_]?up|お持ち帰り|テイクアウト", dm_str, re.I):
                    return "可"
        if soup.select_one(
            '[class*="takeout" i], [class*="takeOut" i], [class*="pickup" i], '
            'a[href*="takeout" i], a[href*="pickup" i]'
        ):
            return "可"
        if _TAKEOUT_PATTERN.search(page_text):
            return "可"
        return ""

    # ------------------------------------------------------------------ parse

    def parse(self, url: str) -> Generator[dict, None, None]:
        """
        起点 _START_SHOP_ID (3,000,000) から上方向にスキャンし、有効な店舗を即 yield。
        連続ミスが _BLOCK_MISS_LIMIT に達したら _BLOCK_SIZE (1,000,000) 戻して次ブロックへ。
        スキャン順: 3M→MAX, 2M→3M-1, 1M→2M-1, 1→1M-1 (重複なし)。
        取得成功時は INFO ログで店名・都道府県・URL を出力して取得確認できるようにする。
        """
        count = 0

        # ブロック境界リスト: (block_start, block_end_exclusive) の降順
        # 3M から上は上限 MAX まで、以降は 1M ずつ下げた範囲を重複なしでカバー
        blocks: list[tuple[int, int]] = []
        s = _START_SHOP_ID
        blocks.append((s, _MAX_SHOP_ID + 1))
        while s > 1:
            s -= _BLOCK_SIZE
            block_start = max(1, s)
            block_end = s + _BLOCK_SIZE  # = 前ブロックの start (重複なし)
            blocks.append((block_start, block_end))

        for block_start, block_end in blocks:
            self.logger.info(
                "=== ブロック開始: ID %d 〜 %d ===", block_start, block_end - 1
            )
            consecutive_misses = 0
            block_count = 0

            for shop_id in range(block_start, block_end):
                shop_url = f"{url}/{shop_id}"
                try:
                    item = self._scrape_shop(shop_url)
                except Exception as exc:  # noqa: BLE001
                    self.logger.debug("例外スキップ: ID=%d — %s", shop_id, exc)
                    consecutive_misses += 1
                    if consecutive_misses % 1_000 == 0:
                        self.logger.info(
                            "連続ミス中: ID=%d, 連続=%d件 (ブロック取得=%d件 / 累計=%d件)",
                            shop_id, consecutive_misses, block_count, count,
                        )
                    if consecutive_misses >= _BLOCK_MISS_LIMIT:
                        self.logger.info(
                            "連続 %d 件 invalid → ブロック終了 (ID: %d)",
                            _BLOCK_MISS_LIMIT, shop_id,
                        )
                        break
                    continue

                if item and item.get(Schema.NAME):
                    consecutive_misses = 0
                    count += 1
                    block_count += 1
                    self.total_items = count
                    self.logger.info(
                        "✓ 取得 [累計%d件 / ブロック%d件]: %s | %s | %s",
                        count, block_count,
                        item[Schema.NAME],
                        item.get(Schema.PREF, ""),
                        shop_url,
                    )
                    yield item
                else:
                    consecutive_misses += 1
                    if consecutive_misses % 1_000 == 0:
                        self.logger.info(
                            "連続ミス中: ID=%d, 連続=%d件 (ブロック取得=%d件 / 累計=%d件)",
                            shop_id, consecutive_misses, block_count, count,
                        )
                    if consecutive_misses >= _BLOCK_MISS_LIMIT:
                        self.logger.info(
                            "連続 %d 件 invalid → ブロック終了 (ID: %d)",
                            _BLOCK_MISS_LIMIT, shop_id,
                        )
                        break

            self.logger.info(
                "=== ブロック完了: ID %d 〜 %d, 取得 %d 件 (累計 %d 件) ===",
                block_start, block_end - 1, block_count, count,
            )

        self.total_items = count

    # --------------------------------------------------------------- detail

    def _scrape_shop(self, shop_url: str) -> dict | None:
        """店舗詳細ページ (/shopDetail/{id}) から構造化情報を抽出する。
        有効な店舗でない場合 (404・エラーページ) は None を返す。
        """
        soup = self.get_soup(shop_url, wait_until="domcontentloaded")
        if soup is None:
            return None

        # 404/エラーページの早期判定
        title_el = soup.select_one("title")
        title_text = title_el.get_text(strip=True) if title_el else ""
        if any(pat in title_text for pat in _ERROR_TITLE_PATTERNS):
            return None
        page_text = soup.get_text(" ", strip=True)
        if len(page_text) < 100:
            return None

        jsonld_dicts = list(self._iter_jsonld(soup))
        lines = self._lines(soup)

        # --- 名称 ---
        name = self._first_text(
            soup, ["h1", '[class*="shopName" i]', '[class*="storeName" i]']
        )
        if not name and title_el:
            name = title_text
        name = self._clean_name(name)
        if not name:
            for d in jsonld_dicts:
                if isinstance(d.get("name"), str) and d["name"].strip():
                    name = d["name"].strip()
                    break
        if not name:
            return None  # 店名なし = 有効な店舗ページではない

        # --- ジャンル ---
        cuisine = self._extract_genre(soup, self._first_text(soup, ["h1"]) or name)

        # --- 各セクション (見出しベース抽出) ---
        hours = self._section_value(lines, "営業時間")
        holiday = self._section_value(lines, "定休日")
        address = self._section_value(lines, "住所")
        delivery_form = self._section_value(lines, "配達員")

        if not address:
            address = self._first_text(
                soup, ['[class*="address" i]', '[itemprop="address"]']
            )
        # JSON-LD フォールバック
        if not address:
            for d in jsonld_dicts:
                addr = d.get("address")
                if isinstance(addr, dict):
                    parts = [
                        (addr.get("addressRegion") or "").strip(),
                        (addr.get("addressLocality") or "").strip(),
                        (addr.get("streetAddress") or "").strip(),
                    ]
                    address = "".join(p for p in parts if p)
                    if address:
                        break
                elif isinstance(addr, str) and addr.strip():
                    address = addr.strip()
                    break

        # --- 都道府県・郵便番号 ---
        pref = ""
        postcode = ""
        if address:
            pm = _PREF_PATTERN.search(address)
            if pm:
                pref = pm.group(1)
                address = address[pm.start():].strip()
            mc = _POSTCODE_PATTERN.search(address)
            if mc:
                postcode = mc.group(0)

        # --- TEL (JSON-LD 優先; プラットフォーム仕様上ほぼ非掲載) ---
        tel = ""
        for d in jsonld_dicts:
            tel = (d.get("telephone") or "").strip()
            if tel:
                break

        # --- 支払い方法 ---
        payments = self._extract_payments(soup)

        # --- テイクアウト可否 ---
        takeout = self._detect_takeout(soup, jsonld_dicts, page_text)

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: postcode,
            Schema.ADDR: address,
            Schema.TEL: tel,
            Schema.CAT_SITE: cuisine,
            Schema.TIME: hours,
            Schema.HOLIDAY: holiday,
            Schema.PAYMENTS: payments,
            Schema.URL: shop_url,
            "テイクアウト可": takeout,
            "配達形態": delivery_form,
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = DemaeCan8Scraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えると挙動がズレる。
    scraper.execute("https://demae-can.com/shopDetail")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
