"""
求とぴ (cute-p.info) — 北関東(群馬・栃木・茨城・埼玉)ナイトワーク系求人スクレイパー

取得対象:
    - cute-p.info 配下の キャバクラ/セクキャバ/デリヘル/コンパニオン/チャットレディ
      の全店舗詳細ページ

取得フロー:
    1. sitemap.xml → 各カテゴリの per-category sitemap を辿って店舗URLを収集
       (一覧ページは1ページしか出力されず /page/2/ は 500 を返すため利用しない)
    2. 各詳細ページの 2 つの <table> (求人情報 / 店舗情報) を
       <td class="rth"> をキー、隣の <td> を値として辞書化し、Schema にマッピング

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/cute_p.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id cute_p
"""

from __future__ import annotations

import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Generator
from urllib.parse import urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler


_PREF_RE = re.compile(
    r"^(北海道|東京都|大阪府|京都府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|新潟|富山|"
    r"石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|和歌山|鳥取|島根|岡山|"
    r"広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県)"
)

_SHOP_PATH_RE = re.compile(r"^/(nightclub|sexycaba|escort|companion|chatlady)/\d+\.html$")

_CATEGORY_SITEMAPS = (
    "/nightclub-sitemap.xml",
    "/sexycaba-sitemap.xml",
    "/escort-sitemap.xml",
    "/companion-sitemap.xml",
    "/chatlady-sitemap.xml",
)

_SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


class CutePScraper(StaticCrawler):
    """求とぴ スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS: list[str] = []

    def parse(self, url: str) -> Generator[dict, None, None]:
        shop_urls = self._collect_shop_urls(url)
        self.total_items = len(shop_urls)
        self.logger.info("対象詳細URL数: %d", self.total_items)

        for shop_url in shop_urls:
            try:
                soup = self.get_soup(shop_url)
                if soup is None:
                    continue
                item = self._parse_shop_page(shop_url, soup)
                if not item.get(Schema.NAME):
                    continue
                yield item
            except Exception as e:
                self.logger.warning("スキップ %s: %s", shop_url, e)
                continue

    # ------------------------------------------------------------------
    # Sitemap collection
    # ------------------------------------------------------------------

    def _collect_shop_urls(self, seed_url: str) -> list[str]:
        parsed = urlparse(seed_url)
        if _SHOP_PATH_RE.match(parsed.path):
            return [seed_url]

        base = f"{parsed.scheme}://{parsed.netloc}"
        shop_urls: list[str] = []
        seen: set[str] = set()
        for sub in _CATEGORY_SITEMAPS:
            sitemap_url = base + sub
            for u in self._fetch_sitemap_locs(sitemap_url):
                p = urlparse(u)
                if p.netloc != parsed.netloc:
                    continue
                if not _SHOP_PATH_RE.match(p.path):
                    continue
                if u not in seen:
                    seen.add(u)
                    shop_urls.append(u)
        return shop_urls

    def _fetch_sitemap_locs(self, sitemap_url: str) -> list[str]:
        try:
            response = self.session.get(sitemap_url, timeout=self.TIMEOUT)
            response.raise_for_status()
            root = ET.fromstring(response.content)
        except Exception as e:
            self.logger.warning("サイトマップ取得失敗: %s (%s)", sitemap_url, e)
            return []
        return [
            node.text.strip()
            for node in root.findall(".//sm:loc", _SITEMAP_NS)
            if node.text
        ]

    # ------------------------------------------------------------------
    # Detail page parsing
    # ------------------------------------------------------------------

    def _parse_shop_page(self, page_url: str, soup) -> dict:
        labels = self._extract_table_labels(soup)
        name = self._extract_name(soup)
        kana = self._extract_kana(soup)
        address = self._c(labels.get("住所", ""))
        pref, addr_body = self._split_pref(address)
        tel = self._extract_tel(soup)
        line_id = self._extract_line_id(soup)
        hp = self._extract_hp(soup)
        # オフィシャル サイト ラベルは <br> を含むため
        # "オフィシャルサイト" / "オフィシャル サイト" のいずれにも対応
        # → labels には正規化済みキーで入っている

        return {
            Schema.URL: page_url,
            Schema.NAME: name,
            Schema.NAME_KANA: kana,
            Schema.PREF: pref,
            Schema.ADDR: addr_body or address,
            Schema.TEL: tel,
            Schema.TIME: self._c(labels.get("営業時間", "")),
            Schema.HOLIDAY: self._c(labels.get("定休日", "")),
            Schema.CAT_SITE: self._c(labels.get("業種", "")),
            Schema.LOB: self._c(labels.get("職種", "")),
            Schema.LINE: line_id,
            Schema.HP: hp,
        }

    def _extract_table_labels(self, soup) -> dict[str, str]:
        """<td class="rth">label</td><td>value</td> 形式の表をすべて辞書化"""
        data: dict[str, str] = {}
        for tr in soup.select("table tr"):
            cells = tr.find_all("td", recursive=False)
            if len(cells) < 2:
                continue
            label_cell = cells[0]
            if "rth" not in (label_cell.get("class") or []):
                continue
            key = self._c(label_cell.get_text(" ", strip=True))
            if not key:
                continue
            # オフィシャル サイト 系は href も拾うので、ここではテキストのみ
            value = self._c(cells[1].get_text(" ", strip=True))
            data.setdefault(key, value)
        return data

    def _extract_name(self, soup) -> str:
        # 詳細ページ内の店舗名は class 無しの <h2> (1つ目は h2.tax_list なので 2つ目)
        for h2 in soup.find_all("h2"):
            if h2.get("class"):
                continue
            text = self._c(h2.get_text(" ", strip=True))
            if text:
                return text
        # フォールバック: アイキャッチ画像の alt/title
        img = soup.select_one("img.wp-post-image")
        if img:
            return self._c(img.get("alt") or img.get("title") or "")
        return ""

    def _extract_kana(self, soup) -> str:
        og = soup.find("meta", attrs={"property": "og:title"})
        if not og or not og.get("content"):
            return ""
        # 例: "club Grow（クラブ グロウ）求人情報｜..." → カナは括弧内
        m = re.search(r"（([^）]+)）", og["content"])
        return self._c(m.group(1)) if m else ""

    def _split_pref(self, address: str) -> tuple[str, str]:
        if not address:
            return "", ""
        m = _PREF_RE.match(address)
        if m:
            return m.group(1), address[m.end() :].strip()
        return "", address

    def _extract_tel(self, soup) -> str:
        a = soup.select_one('a.oubo_tel[href^="tel:"], a[href^="tel:"]')
        if not a:
            return ""
        raw = a.get("href", "").replace("tel:", "")
        return re.sub(r"[^\d\-]", "", raw)

    def _extract_line_id(self, soup) -> str:
        """応募方法 行内の "ID： xxx" / "ラインID： xxx" 形式を抽出"""
        p_line = soup.select_one("p.oubo_line")
        if not p_line:
            return ""
        # p.oubo_line と同じ <td> 配下のテキストを取得
        td = p_line.find_parent("td")
        if not td:
            return ""
        text = td.get_text(" ", strip=True)
        # "LINEでのお問い合わせ" 以降に出現する "ID：xxx"
        m = re.search(r"(?:ライン)?ID[：:]\s*([A-Za-z0-9_.\-@]+)", text)
        return self._c(m.group(1)) if m else ""

    def _extract_hp(self, soup) -> str:
        """店舗情報テーブル内 "オフィシャル サイト" 行のリンク href"""
        for tr in soup.select("table tr"):
            cells = tr.find_all("td", recursive=False)
            if len(cells) < 2:
                continue
            if "rth" not in (cells[0].get("class") or []):
                continue
            label = self._c(cells[0].get_text(" ", strip=True))
            if "オフィシャル" not in label:
                continue
            if "求人サイト" in label:
                continue
            a = cells[1].find("a", href=True)
            if a and a["href"].strip().startswith("http"):
                return a["href"].strip()
        return ""

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
    scraper = CutePScraper()
    scraper.execute("https://cute-p.info/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
