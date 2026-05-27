# https://www.takasaki-fun.jp/ 用
"""
TAKASAKI NIGHT PRIVITY FUN (www.takasaki-fun.jp) — 群馬・高崎キャバクラ等紹介ポータル

取得対象:
    - 店舗名 / 名称_カナ / 都道府県 / 住所 / TEL / サイト定義業種

取得フロー:
    1. トップページ単一ページ (ページネーション無し) を取得
    2. div.shop_box > ul > li を順に走査し、空エントリ (href が /s/ のみ等) は除外
    3. 各 li から アンカー / p.add / p.tel をパースし、住所先頭の都道府県を分離

実行方法:
    python scripts/sites/nightlife/takasaki_night_privity_fun.py
    python bin/run_flow.py --site-id takasaki_night_privity_fun
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup, Tag

from src.const.schema import Schema
from src.framework.static import StaticCrawler

_PREF_PATTERN = re.compile(
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
_TEL_PATTERN = re.compile(r"\d{2,4}-\d{2,4}-\d{4}")

_SECTION_CAT_MAP = {
    "店舗紹介": "キャバクラ",
    "居酒屋・ガールズバー紹介": "居酒屋・ガールズバー",
}

# /s/<digits> 形式のみ有効。/s/ や /s 単体はプレースホルダ扱いで除外
_VALID_SHOP_HREF_RE = re.compile(r"^https?://www\.caba2\.net/s/\d+/?$")


class TakasakiNightPrivityFunScraper(StaticCrawler):
    """TAKASAKI NIGHT PRIVITY FUN スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS: list[str] = []

    BASE_URL = "https://www.takasaki-fun.jp"

    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)
        if soup is None:
            self.logger.error("トップページ取得失敗: %s", url)
            return

        boxes = soup.select("div.shop_box")
        candidates: list[tuple[str, Tag]] = []
        for box in boxes:
            heading = box.find("h2")
            section_title = self._clean(heading.get_text(strip=True)) if heading else ""
            cat_site = _SECTION_CAT_MAP.get(section_title, section_title)
            for li in box.select("ul > li"):
                candidates.append((cat_site, li))

        self.total_items = len(candidates)
        self.logger.info(
            "対象候補数: %d (空エントリ含む。次工程で除外)", self.total_items
        )

        saved_count = 0
        skipped_count = 0
        for index, (cat_site, li) in enumerate(candidates, start=1):
            try:
                record = self._parse_item(url, cat_site, li)
            except Exception as e:  # 個別エラーは握りつぶして継続
                skipped_count += 1
                self.logger.warning(
                    "アイテム解析失敗: %d/%d (%s)", index, self.total_items, e
                )
                continue

            if record is None:
                skipped_count += 1
                continue

            saved_count += 1
            self.logger.info(
                "取得OK: %d/%d 店舗=%s",
                index,
                self.total_items,
                record.get(Schema.NAME) or record.get(Schema.URL),
            )
            yield record

        self.logger.info(
            "取得完了: 候補%d件 取得%d件 スキップ%d件",
            self.total_items,
            saved_count,
            skipped_count,
        )

    def _parse_item(self, source_url: str, cat_site: str, li: Tag) -> dict | None:
        anchor = li.select_one("div.name a.link")
        href = (anchor.get("href", "").strip() if anchor else "")

        if not _VALID_SHOP_HREF_RE.match(href):
            # 「居酒屋・ガールズバー紹介」セクションの空プレースホルダ等を除外
            return None

        anchor_text = self._clean(anchor.get_text(strip=True))
        name, kana = self._split_name_kana(anchor_text)
        if not name:
            return None

        address_raw = self._extract_address(li.select_one("p.add"))
        pref, addr_body = self._split_pref(address_raw)
        tel = self._extract_tel(li.select_one("p.tel"))

        # ポータル側の取得元 URL は同一なので、店舗識別可能な caba2.net 詳細 URL を採用
        return {
            Schema.URL: href,
            Schema.NAME: name,
            Schema.NAME_KANA: kana,
            Schema.PREF: pref,
            Schema.ADDR: addr_body or address_raw,
            Schema.TEL: tel,
            Schema.CAT_SITE: cat_site,
        }

    def _split_name_kana(self, text: str) -> tuple[str, str]:
        if not text:
            return "", ""
        if " - " in text:
            name, kana = text.split(" - ", 1)
            return self._clean(name), self._clean(kana)
        return text, ""

    def _extract_address(self, p: Tag | None) -> str:
        if p is None:
            return ""
        node = p.__copy__() if hasattr(p, "__copy__") else p
        # <br> をスペース化して読みやすく結合
        for br in p.find_all("br"):
            br.replace_with(" ")
        return self._clean(p.get_text(" ", strip=True))

    def _extract_tel(self, p: Tag | None) -> str:
        if p is None:
            return ""
        raw = self._clean(p.get_text(" ", strip=True))
        # 「TEL 027-...」プレフィックスを除去
        raw = re.sub(r"^TEL\s*", "", raw, flags=re.IGNORECASE)
        match = _TEL_PATTERN.search(raw)
        return match.group(0) if match else raw

    def _split_pref(self, address: str) -> tuple[str, str]:
        if not address:
            return "", ""
        match = _PREF_PATTERN.match(address)
        if not match:
            return "", address
        pref = match.group(1)
        return pref, address[match.end():].strip()

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

    scraper = TakasakiNightPrivityFunScraper()
    scraper.execute(TakasakiNightPrivityFunScraper.BASE_URL + "/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
