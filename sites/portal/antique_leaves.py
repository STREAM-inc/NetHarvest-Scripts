# -*- coding: utf-8 -*-
"""
アンティークリーブス (antique-leaves) — 古着ショップ案内ディレクトリ

取得対象:
    - カテゴリー一覧ページ (Color Me Shop / shop-pro ベース) に掲載された
      古着ショップの店舗情報 (名称・住所・TEL・営業時間・定休日・Instagram 等)

取得フロー:
    1. 一覧ページ (?mode=cate&cbid=...) から各店舗の詳細ページ (?pid=...) リンクを収集
    2. 各詳細ページ (div.shop-info の schema.org 構造化データ) を 1 件ずつ取得して即 yield

備考対応:
    - 「取れるカラムは全部取る」方針。ただし店舗紹介文 (自由記述プロース) は
      著作権リスク回避のため取得対象外。

実行方法:
    python scripts/sites/portal/antique_leaves.py
    docker compose exec worker python /app/bin/run_flow.py --site-id antique_leaves
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

root_path = Path(__file__).resolve()
while not (root_path / "src").exists() and root_path != root_path.parent:
    root_path = root_path.parent
sys.path.insert(0, str(root_path))

from src.const.schema import Schema
from src.framework.static import StaticCrawler


class AntiqueLeavesCrawler(StaticCrawler):
    """アンティークリーブス スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["エリア", "ショップID", "掲載媒体名"]

    _MEDIA_NAME = "アンティークリーブス"

    def parse(self, url: str) -> Generator[dict, None, None]:
        list_soup = self.get_soup(url)
        if list_soup is None:
            return

        # エリア (例: 「関東地方のおすすめ古着ショップ（全39件）」 → 関東地方)
        area = ""
        h1 = list_soup.select_one("h1")
        if h1:
            m = re.match(r"^(.+?)の", h1.get_text(strip=True))
            if m:
                area = m.group(1)

        # 一覧 → 詳細ページリンクを収集
        detail_urls = []
        seen = set()
        for a in list_soup.select("ul.c-item-list li.event a.event-link[href]"):
            href = urljoin(url, a["href"])
            if "pid=" in href and href not in seen:
                seen.add(href)
                detail_urls.append(href)

        self.total_items = len(detail_urls)
        self.logger.info("詳細ページ件数: %s (エリア=%s)", len(detail_urls), area)

        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url, area)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("詳細取得失敗 (スキップ): %s — %s", detail_url, exc)
                continue
            if item:
                yield item

    def _scrape_detail(self, url: str, area: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None
        si = soup.select_one("div.shop-info")
        if si is None:
            return None

        def dd_for(label: str):
            for dt in si.select("dt"):
                if dt.get_text(strip=True) == label:
                    return dt.find_next_sibling("dd")
            return None

        def sp(prop: str, scope=None):
            node = (scope or si).select_one(f"span[itemprop={prop}]")
            return node.get_text(strip=True) if node else ""

        item = {Schema.URL: url}

        # 名称 / 名称カナ (例: 「COELACANTH（シーラカンス）」)
        name = ""
        name_kana = ""
        dd_name = dd_for("ショップ名")
        if dd_name:
            raw = dd_name.get_text(strip=True)
            mk = re.search(r"（(.+?)）", raw)
            # （…）はカナ読みの場合と支店名等の場合がある。
            # カタカナのみのときだけ 名称_カナ として分離し、それ以外は名称に残す。
            if mk and re.fullmatch(r"[゠-ヿー・\s]+", mk.group(1)):
                name_kana = mk.group(1).strip()
                name = re.sub(r"（.+?）", "", raw).strip()
            else:
                name = raw
        item[Schema.NAME] = name
        item[Schema.NAME_KANA] = name_kana

        # ショップID
        dd_id = dd_for("ID")
        item["ショップID"] = dd_id.get_text(strip=True) if dd_id else ""

        # 住所 (郵便番号 / 都道府県 / 市区町村+番地+建物)
        item[Schema.POST_CODE] = sp("postalCode")
        item[Schema.PREF] = sp("addressRegion")
        dd_addr = dd_for("住所")
        addr = ""
        if dd_addr:
            locality = sp("addressLocality", dd_addr)
            street = sp("streetAddress", dd_addr)
            building_node = dd_addr.select_one("span[itemprop=name]")
            building = building_node.get_text(strip=True) if building_node else ""
            addr = "".join(p for p in (locality, street, building) if p)
        item[Schema.ADDR] = addr

        # TEL
        item[Schema.TEL] = sp("telephone")

        # 営業時間 (opens - closes)
        opens = sp("opens")
        closes = sp("closes")
        if opens or closes:
            item[Schema.TIME] = f"{opens} - {closes}".strip(" -")
        else:
            item[Schema.TIME] = ""

        # 定休日
        item[Schema.HOLIDAY] = sp("dayOfWeek")

        # 業種 (詳細 H1 の「[古着]」等の接頭タグ)
        cat = ""
        dh1 = soup.select_one("h1")
        if dh1:
            mc = re.match(r"^\s*\[([^\]]+)\]", dh1.get_text(strip=True))
            if mc:
                cat = mc.group(1).strip()
        item[Schema.CAT_SITE] = cat

        # SNS / HP (shop-info 内のリンクのみ。フッターのサイト公式アカウントは除外)
        item[Schema.INSTA] = ""
        item[Schema.X] = ""
        item[Schema.FB] = ""
        item[Schema.LINE] = ""
        item[Schema.TIKTOK] = ""
        item[Schema.HP] = ""
        for a in si.select("a[href]"):
            href = a.get("href", "")
            host = urlparse(href).netloc.lower()
            if href.startswith("tel:") or href.startswith("#") or "javascript" in href:
                continue
            if "instagram.com" in host:
                item[Schema.INSTA] = item[Schema.INSTA] or href
            elif "twitter.com" in host or host == "x.com" or host.endswith(".x.com"):
                item[Schema.X] = item[Schema.X] or href
            elif "facebook.com" in host:
                item[Schema.FB] = item[Schema.FB] or href
            elif "line.me" in host or "lin.ee" in host:
                item[Schema.LINE] = item[Schema.LINE] or href
            elif "tiktok.com" in host:
                item[Schema.TIKTOK] = item[Schema.TIKTOK] or href
            elif "google" in host or "map" in host:
                continue

        # EXTRA
        item["エリア"] = area
        item["掲載媒体名"] = self._MEDIA_NAME

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = AntiqueLeavesCrawler()
    # 🔒 sites.yml に登録する url と完全一致 (SSOT = sites.yml)
    scraper.execute("https://antique-leaves.com/?mode=cate&cbid=2927760&csid=0")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
