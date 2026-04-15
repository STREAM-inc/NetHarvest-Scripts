"""
比較ビズ — 全業種 企業スクレイパー
対象URL: https://www.biz.ne.jp/list/{category}/

取得対象 (一覧ページのみで完結):
    - 会社名 / 詳細URL / 代表者名 / 住所 (都道府県分割) / TEL / HP
    - 対応業務 (サイト内業種, CAT_SITE)
    - 入口カテゴリ (どの親カテゴリから収集したか)
    - 特色 / 特徴

取得フロー:
    親カテゴリ (tax, judicial, oa, system, ...) を順に巡回し、
    各カテゴリで /list/{slug}/?datacnt=N#list を全ページ取得。
    会社の詳細URLで重複排除する。

実行方法:
    python scripts/sites/portal/biz_all.py
    python bin/run_flow.py --site-id biz_all
"""

import re
import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


BASE_URL = "https://www.biz.ne.jp"
ITEMS_PER_PAGE = 10

# トップページ「全て見る」アグリゲータカテゴリ (slug, 表示名)
PARENT_CATEGORIES: list[tuple[str, str]] = [
    ("tax", "税務・財務"),
    ("judicial", "司法書士・行政書士"),
    ("patent", "特許・知財"),
    ("consulting", "経営コンサル"),
    ("oa", "OA機器・オフィス用品"),
    ("printing", "印刷"),
    ("web", "Web制作・マーケ"),
    ("advertising", "広告・販促"),
    ("creator", "クリエイター"),
    ("system", "システム開発"),
    ("officework", "事務代行"),
    ("logistics", "物流"),
    ("administrative", "総務・庶務"),
    ("architect", "建築・内装"),
    ("training", "研修・教育"),
    ("temporary", "人材派遣・紹介"),
    ("lassa", "弁護士"),
    ("construction", "建設・工事"),
    ("industry", "製造・加工"),
    ("finance", "資金調達・保険"),
]

_PREF_PATTERN = re.compile(
    r"^(北海道|(?:東京|大阪|京都|神奈川|愛知|兵庫|福岡|埼玉|千葉"
    r"|静岡|広島|宮城|茨城|新潟|栃木|群馬|長野|岐阜|福島|三重"
    r"|熊本|鹿児島|岡山|山口|愛媛|長崎|滋賀|奈良|沖縄|青森|岩手"
    r"|秋田|山形|富山|石川|福井|山梨|和歌山|鳥取|島根|香川|高知"
    r"|徳島|佐賀|大分|宮崎)都?道?府?県?)"
)


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class BizAllScraper(StaticCrawler):
    """比較ビズ 全業種スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["入口カテゴリ", "特色", "特徴"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_urls: set[str] = set()

        for slug, label in PARENT_CATEGORIES:
            cat_url = f"{BASE_URL}/list/{slug}/"
            self.logger.info("=== カテゴリ [%s] %s ===", slug, label)

            try:
                first_soup = self.get_soup(cat_url)
            except Exception as e:
                self.logger.warning("カテゴリ %s 取得失敗: %s", slug, e)
                continue
            if first_soup is None:
                continue

            total = self._extract_total_count(first_soup)
            if total > 0:
                self.logger.info("  カテゴリ総件数: %d 件", total)

            offset = 0
            consecutive_empty = 0
            while True:
                page_url = cat_url if offset == 0 else f"{cat_url}?datacnt={offset}#list"
                soup = first_soup if offset == 0 else self.get_soup(page_url)
                if soup is None:
                    consecutive_empty += 1
                    if consecutive_empty >= 3:
                        break
                    offset += ITEMS_PER_PAGE
                    continue

                boxes = [b for b in soup.select("li.box") if b.select_one(".re_tl h3")]
                if not boxes:
                    consecutive_empty += 1
                    if consecutive_empty >= 3:
                        self.logger.info("  3回連続データなしのため終了")
                        break
                    offset += ITEMS_PER_PAGE
                    continue

                consecutive_empty = 0
                for box in boxes:
                    try:
                        item = self._parse_box(box, page_url, label)
                    except Exception as e:
                        self.logger.warning("ボックス解析失敗: %s", e)
                        continue
                    if not item:
                        continue
                    key = item.get(Schema.URL) or item.get(Schema.NAME)
                    if key in seen_urls:
                        continue
                    seen_urls.add(key)
                    yield item

                if total > 0 and offset + ITEMS_PER_PAGE >= total:
                    break
                offset += ITEMS_PER_PAGE

            self.logger.info("  累計取得済: %d 社", len(seen_urls))

    def _extract_total_count(self, soup) -> int:
        el = soup.select_one("span.count_total")
        if el:
            try:
                return int(_clean(el.get_text()))
            except ValueError:
                pass
        m = re.search(r"(\d+)\s*件中", soup.get_text())
        return int(m.group(1)) if m else 0

    def _parse_box(self, box, page_url: str, entry_category: str) -> dict | None:
        item: dict = {"入口カテゴリ": entry_category}

        h3_a = box.select_one(".re_tl h3 a")
        if not h3_a:
            return None
        item[Schema.NAME] = _clean(h3_a.get_text())
        item[Schema.URL] = h3_a.get("href", "").strip() or page_url

        ceo = box.select_one("li.ceo span:not(.svg_icon)")
        if ceo:
            item[Schema.REP_NM] = _clean(ceo.get_text())

        addr_li = box.select_one("li.address span:not(.svg_icon)")
        if addr_li:
            raw = _clean(addr_li.get_text())
            m = _PREF_PATTERN.match(raw)
            if m:
                item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = raw[m.end():].strip()
            else:
                item[Schema.ADDR] = raw

        tel = box.select_one("li.tel span:not(.svg_icon)")
        if tel:
            item[Schema.TEL] = _clean(tel.get_text())

        hp = box.select_one("li.url a")
        if hp:
            item[Schema.HP] = hp.get("href", "").strip()

        feats = [_clean(li.get_text()) for li in box.select("ul.sub_list.sub_1 li")]
        if feats:
            item["特色"] = "、".join(filter(None, feats))

        cats = [_clean(li.get_text()) for li in box.select("ul.sub_list.sub_2 li")]
        if cats:
            item[Schema.CAT_SITE] = "、".join(filter(None, cats))

        chars = [_clean(li.get_text()) for li in box.select(".summaly_area .check_list_area li")]
        if chars:
            item["特徴"] = "、".join(filter(None, chars))

        return item if item.get(Schema.NAME) else None


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = BizAllScraper()
    scraper.execute(BASE_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
