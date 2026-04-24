"""
SAVOR JAPAN — 日本の飲食店予約ガイド (英語圏向けポータル)

取得対象:
    - 飲食店情報（日本語/英語名・住所・料理ジャンル・営業時間・定休日・
      支払い方法・アクセス・特徴・緯度経度 等）

取得フロー:
    一覧ページ (https://savorjapan.com/search?page=N) を巡回
      → 各店舗詳細ページ (https://savorjapan.com/{10桁code}) を取得
      → JSON-LD + ページ内 .card-info-separate セクションを解析

実行方法:
    python scripts/sites/food/savor_japan.py
    python bin/run_flow.py --site-id savor_japan
"""

import json
import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

_BASE = "https://savorjapan.com"
_LIST_URL = f"{_BASE}/search"

_PREF_RE = re.compile(
    r"^(北海道|東京都|大阪府|京都府|"
    r"神奈川県|埼玉県|千葉県|兵庫県|福岡県|愛知県|静岡県|茨城県|広島県|"
    r"宮城県|新潟県|長野県|群馬県|栃木県|岡山県|島根県|山口県|香川県|"
    r"徳島県|愛媛県|高知県|福井県|石川県|富山県|滋賀県|奈良県|和歌山県|"
    r"鳥取県|岐阜県|三重県|山梨県|長崎県|佐賀県|熊本県|大分県|宮崎県|"
    r"鹿児島県|沖縄県|青森県|岩手県|秋田県|山形県|福島県)"
)


class SavorJapanScraper(StaticCrawler):
    """SAVOR JAPAN スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "名称_英語",
        "住所_英語",
        "アクセス",
        "平均価格",
        "英語対応",
        "特徴",
        "紹介文",
        "緯度",
        "経度",
    ]

    def parse(self, url: str):
        page = 1
        while True:
            list_url = url if page == 1 else f"{_BASE}/search?page={page}"
            soup = self.get_soup(list_url)
            if soup is None:
                break

            if page == 1:
                total_el = soup.select_one(".search-result-text")
                if total_el:
                    m = re.search(r"of\s+([\d,]+)\s+restaurants", total_el.get_text(strip=True))
                    if m:
                        try:
                            self.total_items = int(m.group(1).replace(",", ""))
                        except ValueError:
                            pass

            anchors = soup.select("a.card-list-cassette[data-restaurant-code]")
            if not anchors:
                break

            for a in anchors:
                detail_url = a.get("href")
                if not detail_url:
                    continue
                if not detail_url.startswith("http"):
                    detail_url = _BASE + detail_url
                try:
                    result = self._scrape_detail(detail_url)
                    if result:
                        yield result
                except Exception as e:
                    self.logger.warning("詳細取得エラー %s: %s", detail_url, e)
                    continue

            next_btn = soup.select_one(".pagination a.button-arrow-right:not(.button-disabled)")
            if not next_btn:
                break
            page += 1

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        ld = self._find_restaurant_jsonld(soup)
        if ld:
            name_en = (ld.get("name") or "").strip()
            name_jp = (ld.get("alternateName") or "").strip()
            data[Schema.NAME] = name_jp or name_en
            data["名称_英語"] = name_en
            addr = ld.get("address") or {}
            data["住所_英語"] = (addr.get("streetAddress") or "").strip()
            post_code = (addr.get("postalCode") or "").strip()
            if post_code:
                data[Schema.POST_CODE] = post_code

            data[Schema.TIME] = (ld.get("openingHours") or "").strip()

            cuisines = ld.get("servesCuisine")
            if isinstance(cuisines, list):
                data[Schema.CAT_SITE] = ", ".join(str(c) for c in cuisines if c)
            elif isinstance(cuisines, str):
                data[Schema.CAT_SITE] = cuisines.strip()

            payments = ld.get("paymentAccepted")
            if isinstance(payments, list):
                data[Schema.PAYMENTS] = ", ".join(str(p) for p in payments if p)
            elif isinstance(payments, str):
                data[Schema.PAYMENTS] = payments.strip()

            data["紹介文"] = (ld.get("description") or "").strip()

            geo = ld.get("geo") or {}
            data["緯度"] = str(geo.get("latitude") or "").strip()
            data["経度"] = str(geo.get("longitude") or "").strip()

        if not data.get(Schema.NAME):
            h1 = soup.select_one("h1")
            if h1:
                data[Schema.NAME] = h1.get_text(strip=True)

        for row in soup.select(".card-info.card-info-separate"):
            parts = row.select(":scope > .card-info-separate-inner")
            if len(parts) < 2:
                continue
            label = parts[0].get_text(" ", strip=True)
            value = parts[1].get_text(" ", strip=True)
            value = re.sub(r"\s+", " ", value).strip()
            if not label:
                continue

            if label == "Access":
                data["アクセス"] = value
            elif label == "Closed":
                data[Schema.HOLIDAY] = value
            elif label == "Open" and not data.get(Schema.TIME):
                data[Schema.TIME] = value
            elif label == "Average price":
                data["平均価格"] = value
            elif label == "English services":
                data["英語対応"] = value
            elif label == "Features":
                data["特徴"] = re.sub(r"\s*\*.*", "", value).strip()
            elif label == "Address (for taxi driver)":
                addr_jp = value
                pm = _PREF_RE.match(addr_jp)
                if pm:
                    data[Schema.PREF] = pm.group(1)
                    data[Schema.ADDR] = addr_jp[pm.end():].strip()
                else:
                    data[Schema.ADDR] = addr_jp
            elif label == "Address" and not data.get("住所_英語"):
                data["住所_英語"] = re.sub(r"\s*map\s*$", "", value).strip()
            elif label == "Method of payment" and not data.get(Schema.PAYMENTS):
                data[Schema.PAYMENTS] = value

        return data if data.get(Schema.NAME) else None

    @staticmethod
    def _find_restaurant_jsonld(soup) -> dict | None:
        for script in soup.select('script[type="application/ld+json"]'):
            raw = script.string or script.get_text() or ""
            try:
                parsed = json.loads(raw)
            except (ValueError, json.JSONDecodeError):
                continue
            candidates = parsed if isinstance(parsed, list) else [parsed]
            for entry in candidates:
                if isinstance(entry, dict) and entry.get("@type") == "Restaurant":
                    return entry
        return None


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = SavorJapanScraper()
    scraper.execute(_LIST_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
