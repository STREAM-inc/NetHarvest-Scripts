"""
ガルバパラダイス — 首都圏 ガールズバー＆コンカフェ専門サイト スクレイパー

取得対象:
    - girlsbar-prds.net 掲載の全店舗詳細ページ (首都圏 ガールズバー / コンカフェ)

取得フロー:
    1. ルート URL の netloc から sitemap.xml を取得
    2. sitemap 内の店舗 URL (/tokyo/{area}/{shop}/) を直接 / cast URL 由来の双方から抽出・正規化
    3. 各店舗詳細ページを解析し、1 件取得するごとに即 yield

備考対応:
    - 「取れそうなカラムは全てとる」方針。ただし長文の自由記述 (店舗紹介文 description)・
      ラベルが不定の料金/SYSTEM テーブルは著作権リスク/構造化困難のため除外。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/girlsbar_prds.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id girlsbar_prds
"""

from __future__ import annotations

import json
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

_PREF_RE = re.compile(
    r"^(北海道|東京都|大阪府|京都府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|和歌山|"
    r"鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県)"
)

# <title> 例: "新橋 Ma Cherie・マシェリ（ガールズバー）  【ガルパラ】"
_TITLE_RE = re.compile(r"^\s*\S+?\s+(?P<name>.+?)・(?P<kana>.+?)（(?P<genre>.+?)）")

_SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

# /tokyo/{area}/{shop}/ (店舗詳細) を直接 / cast URL 由来で抽出するためのパターン
_SHOP_DIRECT_RE = re.compile(r"^https?://[^/]+/tokyo/([^/]+)/([^/]+)/?$")
_SHOP_FROM_CAST_RE = re.compile(r"^https?://[^/]+/tokyo/([^/]+)/([^/]+)/cast/[^/]+/?$")

# 店舗ではない area / shop スラッグ (ジャンル一覧・ランキング等) を除外する
_SKIP_AREA = {"cast"}


class GirlsbarPrdsScraper(StaticCrawler):
    """ガルバパラダイス スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "エリア",
        "衣装",
        "アクセス",
        "緯度",
        "経度",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        shop_urls = self._collect_shop_urls(url)
        self.total_items = len(shop_urls)
        self.logger.info("対象店舗 URL 数: %d", self.total_items)

        for shop_url in shop_urls:
            try:
                soup = self.get_soup(shop_url)
                if soup is None:
                    continue
                item = self._parse_shop_page(shop_url, soup)
                if item.get(Schema.NAME):
                    yield item
            except Exception as e:
                self.logger.warning("スキップ %s: %s", shop_url, e)
                continue

    # ------------------------------------------------------------------
    # Shop URL collection (sitemap)
    # ------------------------------------------------------------------

    def _collect_shop_urls(self, root_url: str) -> list[str]:
        """ルート URL の netloc から sitemap.xml を辿り店舗 URL を収集する。"""
        parsed = urlparse(root_url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        sitemap_url = urljoin(base, "/sitemap.xml")

        try:
            response = self.session.get(sitemap_url, timeout=self.TIMEOUT)
            response.raise_for_status()
            xml_root = ET.fromstring(response.content)
        except Exception as e:
            self.logger.warning("サイトマップ取得失敗: %s (%s)", sitemap_url, e)
            return []

        shops: dict[str, None] = {}  # 挿入順保持の dedupe
        for node in xml_root.findall(".//sm:loc", _SITEMAP_NS):
            loc = (node.text or "").strip()
            if not loc:
                continue
            shop_path = self._extract_shop_path(loc)
            if shop_path:
                shops.setdefault(urljoin(base, shop_path), None)

        return list(shops.keys())

    def _extract_shop_path(self, loc: str) -> str | None:
        """sitemap の loc から店舗詳細パス /tokyo/{area}/{shop}/ を抽出する。"""
        for pattern in (_SHOP_DIRECT_RE, _SHOP_FROM_CAST_RE):
            m = pattern.match(loc)
            if not m:
                continue
            area, shop = m.group(1), m.group(2)
            if not self._is_shop(area, shop):
                return None
            return f"/tokyo/{area}/{shop}/"
        return None

    @staticmethod
    def _is_shop(area: str, shop: str) -> bool:
        if area in _SKIP_AREA or re.fullmatch(r"g\d+", area):
            return False
        if re.fullmatch(r"sta\d+", shop) or re.fullmatch(r"g\d+", shop):
            return False
        if shop.startswith(("ranking", "shopSort", "cos", "search")):
            return False
        return True

    # ------------------------------------------------------------------
    # Detail page parsing
    # ------------------------------------------------------------------

    def _parse_shop_page(self, page_url: str, soup) -> dict:
        labels = self._extract_shopdata(soup)
        ld = self._extract_ld(soup)
        title_meta = self._parse_title(soup)
        info_parts = self._extract_info_parts(soup)

        name = labels.get("店名") or title_meta.get("name") or ld.get("name", "")
        if not name:
            nm = soup.select_one(".header_shop_name")
            name = self._c(nm.get_text(" ", strip=True)) if nm else ""

        address = labels.get("住所") or self._ld_street_address(ld)
        pref, addr_body = self._split_pref(address)

        tel = labels.get("TEL") or self._c(str(ld.get("telephone", "")))
        tel = self._clean_tel(tel)

        sns = self._extract_sns(soup)
        genre = title_meta.get("genre") or self._info_genre(info_parts)
        geo = ld.get("geo") if isinstance(ld.get("geo"), dict) else {}

        return {
            Schema.URL: page_url,
            Schema.NAME: name,
            Schema.NAME_KANA: title_meta.get("kana", ""),
            Schema.PREF: pref,
            Schema.ADDR: addr_body or address,
            Schema.TEL: tel,
            Schema.TIME: labels.get("営業時間", ""),
            Schema.HOLIDAY: labels.get("定休日", ""),
            Schema.CAT_SITE: genre,
            Schema.HP: labels.get("WEB", ""),
            Schema.INSTA: sns["insta"],
            Schema.X: sns["x"],
            Schema.FB: sns["fb"],
            Schema.LINE: sns["line"],
            Schema.TIKTOK: sns["tiktok"],
            "エリア": self._info_area(info_parts),
            "衣装": labels.get("衣装", ""),
            "アクセス": info_parts[3] if len(info_parts) >= 4 else "",
            "緯度": self._c(str(geo.get("latitude", ""))),
            "経度": self._c(str(geo.get("longitude", ""))),
        }

    def _extract_shopdata(self, soup) -> dict[str, str]:
        """dl.shopdata_list の dt/dd ペアを辞書化する。"""
        data: dict[str, str] = {}
        dl = soup.select_one("dl.shopdata_list")
        if not dl:
            return data
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            key = self._c(dt.get_text(" ", strip=True))
            if not key or key in data:
                continue
            data[key] = self._c(dd.get_text(" ", strip=True))
        return data

    def _extract_ld(self, soup) -> dict:
        for s in soup.select('script[type="application/ld+json"]'):
            raw = s.string or s.get_text()
            if not raw:
                continue
            raw = re.sub(r"[\x00-\x1f]+", " ", raw)
            try:
                d = json.loads(raw)
            except (json.JSONDecodeError, ValueError):
                continue
            if isinstance(d, dict) and d.get("@type") == "LocalBusiness":
                return d
        return {}

    def _ld_street_address(self, ld: dict) -> str:
        addr = ld.get("address")
        if isinstance(addr, dict):
            return self._c(str(addr.get("streetAddress", "")))
        return ""

    def _parse_title(self, soup) -> dict[str, str]:
        if not soup.title:
            return {}
        m = _TITLE_RE.match(soup.title.get_text())
        if not m:
            return {}
        return {
            "name": self._c(m.group("name")),
            "kana": self._c(m.group("kana")),
            "genre": self._c(m.group("genre")),
        }

    def _extract_info_parts(self, soup) -> list[str]:
        """.shop_detail_info_area の 区切りテキスト
        例: [ '新橋・銀座のガールズバー', 'Ma Cherie', '18：00〜05：00', '各線「新橋駅」...' ]
        """
        el = soup.select_one(".shop_detail_info_area")
        if not el:
            return []
        return [self._c(p) for p in el.get_text("|", strip=True).split("|") if self._c(p)]

    def _info_area(self, parts: list[str]) -> str:
        """'新橋・銀座のガールズバー' → '新橋・銀座'"""
        if not parts:
            return ""
        head = parts[0]
        m = re.match(r"^(.*?)の(?:ガールズバー|コンカフェ|.+)$", head)
        return self._c(m.group(1)) if m else ""

    def _info_genre(self, parts: list[str]) -> str:
        if not parts:
            return ""
        m = re.search(r"の(ガールズバー|コンカフェ|[^の|]+)$", parts[0])
        return self._c(m.group(1)) if m else ""

    def _split_pref(self, address: str) -> tuple[str, str]:
        if not address:
            return "", ""
        m = _PREF_RE.match(address)
        if m:
            return m.group(1), address[m.end():].strip()
        return "", address

    def _clean_tel(self, raw: str) -> str:
        if not raw:
            return ""
        m = re.search(r"0\d{1,4}-?\d{1,4}-?\d{3,4}", raw)
        return m.group(0) if m else self._c(raw)

    def _extract_sns(self, soup) -> dict[str, str]:
        sns = {"insta": "", "x": "", "fb": "", "line": "", "tiktok": ""}
        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            low = href.lower()
            if "instagram.com" in low and not sns["insta"]:
                sns["insta"] = href
            elif (("x.com" in low) or ("twitter.com" in low)) and "intent/tweet" not in low and not sns["x"]:
                sns["x"] = href
            elif "facebook.com" in low and "sharer" not in low and not sns["fb"]:
                sns["fb"] = href
            elif "line.me" in low and not sns["line"]:
                sns["line"] = href
            elif "tiktok.com" in low and not sns["tiktok"]:
                sns["tiktok"] = href
        return sns

    def _c(self, value: str | None) -> str:
        if not value:
            return ""
        return re.sub(r"\s+", " ", str(value)).strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    scraper = GirlsbarPrdsScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://girlsbar-prds.net/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
