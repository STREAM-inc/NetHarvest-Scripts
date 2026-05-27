"""
G-CHANNEL (www.g-channel.jp) — 群馬・埼玉・栃木・茨城のキャバクラ／ガールズバー等 店舗スクレイパー

取得対象:
    - 店舗名 / 名称カナ / 都道府県 / 住所 / TEL
    - サイト定義業種 (キャバクラ/セクキャバ/パブ・スナック/ガールズバー/クラブ・ラウンジ)
    - 営業時間 / 定休日
    - SNS (Instagram / X / Facebook / TikTok / LINE) / HP

取得フロー:
    1. 各都道府県の店舗一覧ページ (/shop/{1,2,3,6}_list.html) を巡回
    2. 一覧ページから /shop/{slug}/index.html を全件収集 (重複は除外)
    3. 各詳細ページ shop-sub-info / 蛇身 (navi) breadcrumb / shop-sns を解析

実行方法:
    python scripts/sites/nightlife/g_channel.py
    python bin/run_flow.py --site-id g_channel
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup

from src.const.schema import Schema
from src.framework.static import StaticCrawler

_TEL_PATTERN = re.compile(r"\d{2,4}-\d{2,4}-\d{4}")

# G-CHANNEL の breadcrumb 形式 "群馬県 - 太田市" から都道府県を抽出
_PREF_FROM_AREA = re.compile(
    r"^(北海道|東京都|京都府|大阪府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 業種ラベルの有効値 (breadcrumb 第2階層から取得する想定)
_SHOP_TYPES = {
    "キャバクラ",
    "セクキャバ",
    "パブ・スナック",
    "ガールズバー",
    "クラブ・ラウンジ",
}


class GChannelScraper(StaticCrawler):
    """G-CHANNEL スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS: list[str] = []

    BASE_URL = "https://www.g-channel.jp"
    # 都道府県別の店舗一覧ページ (1=群馬, 2=埼玉, 3=栃木, 6=茨城)
    AREA_LIST_PATHS = (
        "/shop/1_list.html",
        "/shop/2_list.html",
        "/shop/3_list.html",
        "/shop/6_list.html",
    )

    _SHOP_HREF_RE = re.compile(r"^/shop/[a-zA-Z0-9][a-zA-Z0-9_-]*/index\.html$")

    def parse(self, url: str) -> Generator[dict, None, None]:
        shop_urls = self._collect_shop_urls()
        self.total_items = len(shop_urls)
        self.logger.info("対象店舗URL数: %d", self.total_items)

        saved = 0
        failed = 0
        for index, shop_url in enumerate(shop_urls, start=1):
            try:
                soup = self.get_soup(shop_url)
                if soup is None:
                    failed += 1
                    self.logger.warning(
                        "詳細取得失敗 (soup=None): %d/%d URL=%s",
                        index,
                        self.total_items,
                        shop_url,
                    )
                    continue
                record = self._parse_shop_page(shop_url, soup)
            except Exception as e:
                failed += 1
                self.logger.warning(
                    "詳細取得例外: %d/%d URL=%s (%s)",
                    index,
                    self.total_items,
                    shop_url,
                    e,
                )
                continue

            if record:
                saved += 1
                self.logger.info(
                    "詳細取得OK: %d/%d 店舗=%s",
                    index,
                    self.total_items,
                    record.get(Schema.NAME) or shop_url,
                )
                yield record
            else:
                failed += 1
                self.logger.warning(
                    "詳細取得スキップ: %d/%d URL=%s", index, self.total_items, shop_url
                )

        self.logger.info(
            "詳細取得完了: 候補%d件 取得%d件 失敗/スキップ%d件",
            self.total_items,
            saved,
            failed,
        )

    def _collect_shop_urls(self) -> list[str]:
        """4都道府県の一覧ページから shop URL を全件収集 (重複除外)"""
        seen: set[str] = set()
        shop_urls: list[str] = []

        for path in self.AREA_LIST_PATHS:
            list_url = urljoin(self.BASE_URL, path)
            try:
                soup = self.get_soup(list_url)
            except Exception as e:
                self.logger.warning("一覧ページ取得失敗: %s (%s)", list_url, e)
                continue
            if soup is None:
                continue

            for anchor in soup.find_all("a", href=True):
                href = anchor["href"]
                if not self._SHOP_HREF_RE.match(href):
                    continue
                full = urljoin(self.BASE_URL, href)
                if full in seen:
                    continue
                seen.add(full)
                shop_urls.append(full)

            self.logger.info("一覧収集: %s から累計 %d 件", list_url, len(shop_urls))

        return shop_urls

    def _parse_shop_page(self, shop_url: str, soup: BeautifulSoup) -> dict | None:
        info_box = soup.select_one("div.shop-sub-info")
        if info_box is None:
            self.logger.warning("shop-sub-info が見つかりません: %s", shop_url)
            return None

        name_el = info_box.select_one("div.shop h1")
        name = self._clean(name_el.get_text()) if name_el else ""

        kana_el = info_box.select_one("div.kana")
        kana = self._clean(kana_el.get_text()) if kana_el else ""

        labels = self._extract_labels(info_box)
        address = self._clean(labels.get("所在地", ""))
        tel = self._extract_tel(labels.get("電話番号", ""))
        time_open = self._clean(labels.get("営業時間", ""))
        holiday = self._clean(labels.get("定休日", ""))

        if not name:
            self.logger.warning("店舗名が空です: %s", shop_url)
            return None

        pref, shop_type = self._parse_breadcrumb(soup)
        sns = self._extract_sns(soup)

        return {
            Schema.URL: shop_url,
            Schema.NAME: name,
            Schema.NAME_KANA: kana,
            Schema.PREF: pref,
            Schema.ADDR: address,
            Schema.TEL: tel,
            Schema.CAT_SITE: shop_type,
            Schema.TIME: time_open,
            Schema.HOLIDAY: holiday,
            Schema.INSTA: sns["insta"],
            Schema.X: sns["x"],
            Schema.FB: sns["fb"],
            Schema.TIKTOK: sns["tiktok"],
            Schema.LINE: sns["line"],
            Schema.HP: sns["hp"],
        }

    def _extract_labels(self, info_box) -> dict[str, str]:
        """shop-sub-info 内の <div class="tit">ラベル</div><div class="cmt">値</div> ペアを抽出"""
        labels: dict[str, str] = {}
        children = list(info_box.find_all("div", recursive=False))
        for idx, div in enumerate(children):
            classes = div.get("class") or []
            if "tit" not in classes:
                continue
            label = self._clean(div.get_text())
            if not label:
                continue
            value_div = children[idx + 1] if idx + 1 < len(children) else None
            if value_div is None:
                continue
            value_classes = value_div.get("class") or []
            if "cmt" not in value_classes:
                continue
            value = self._clean(value_div.get_text(separator=" "))
            if label and label not in labels:
                labels[label] = value
        return labels

    def _extract_tel(self, raw: str) -> str:
        raw = self._clean(raw)
        match = _TEL_PATTERN.search(raw)
        if match:
            return match.group(0)
        return raw

    def _parse_breadcrumb(self, soup: BeautifulSoup) -> tuple[str, str]:
        """
        breadcrumb 例: HOME ≫ ガールズバー ≫ 群馬県 - 太田市 ≫ 店名
        2番目の階層が業種、3番目が "都道府県 - 市区町村"
        """
        navi = soup.select_one("div.navi")
        if navi is None:
            return "", ""

        items: list[str] = []
        for li in navi.find_all("li"):
            text = self._clean(li.get_text())
            if not text or text == "≫":
                continue
            items.append(text)

        shop_type = ""
        pref = ""
        for item in items:
            if not shop_type and item in _SHOP_TYPES:
                shop_type = item
                continue
            if not pref:
                m = _PREF_FROM_AREA.match(item)
                if m:
                    pref = m.group(1)

        return pref, shop_type

    def _extract_sns(self, soup: BeautifulSoup) -> dict[str, str]:
        sns = {"insta": "", "x": "", "fb": "", "tiktok": "", "line": "", "hp": ""}

        sns_box = soup.select_one("div.shop-sns")
        anchors = sns_box.find_all("a", href=True) if sns_box else []

        for anchor in anchors:
            href = (anchor.get("href") or "").strip()
            if not href or href.startswith("javascript"):
                continue
            low = href.lower()
            if "instagram.com" in low and not sns["insta"]:
                sns["insta"] = href
            elif (
                ("x.com/" in low or "twitter.com" in low)
                and "intent/tweet" not in low
                and not sns["x"]
            ):
                sns["x"] = href
            elif "facebook.com" in low and not sns["fb"]:
                sns["fb"] = href
            elif "tiktok.com" in low and not sns["tiktok"]:
                sns["tiktok"] = href
            elif "line.me" in low and not sns["line"]:
                sns["line"] = href
            elif href.startswith("http") and "g-channel.jp" not in low and not sns["hp"]:
                sns["hp"] = href

        return sns

    def _clean(self, value: str | None) -> str:
        if not value:
            return ""
        return re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = GChannelScraper()
    scraper.execute(GChannelScraper.BASE_URL + "/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
