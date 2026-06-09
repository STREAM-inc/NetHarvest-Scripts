import re
import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class RakutenTravelScraper(StaticCrawler):
    """楽天トラベル 宿泊施設一覧スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["キャッチコピー", "アクセス", "設備・サービス", "最安値", "地区"]

    _SEARCH_BASE = "https://search.travel.rakuten.co.jp/ds/undated/search"

    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)
        if soup is None:
            return

        # 都道府県コードを収集（長い順にソートして前方一致マッチング精度を上げる）
        pref_codes = sorted(
            {
                m.group(1)
                for a in soup.find_all("a", href=True)
                if (m := re.search(r"/02japan([a-z]+)\.html", a.get("href", "")))
            },
            key=len,
            reverse=True,
        )

        # 03レベルエリアリンクと地区名を収集
        seen_hrefs: set[str] = set()
        area_links: list[tuple[str, str]] = []
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if not re.search(r"/03japan[a-z]+\.html", href):
                continue
            if href in seen_hrefs:
                continue
            seen_hrefs.add(href)

            sub_name = a.get_text(strip=True)
            parent_td = a.find_parent("td")
            pref_name = ""
            if parent_td:
                pl = parent_td.find("a", href=lambda h: h and "/02japan" in h)
                if pl:
                    pref_name = pl.get_text(strip=True)
            display = f"{pref_name} {sub_name}".strip() if pref_name else sub_name
            area_links.append((href, display))

        self.logger.info("エリアリンク収集完了: %d 件", len(area_links))

        seen_ids: set[str] = set()
        for href, area_name in area_links:
            chu, shou = self._parse_area_code(href, pref_codes)
            if not chu:
                self.logger.warning("エリアコード解析失敗: %s", href)
                continue
            for item in self._scrape_area(chu, shou, area_name, seen_ids):
                yield item

    def _parse_area_code(self, href: str, pref_codes: list[str]) -> tuple[str, str]:
        """03レベルURLから f_chu / f_shou を抽出する"""
        m = re.search(r"/03japan([a-z]+)\.html", href)
        if not m:
            return "", ""
        combined = m.group(1)
        for pref in pref_codes:
            if combined.startswith(pref):
                shou = combined[len(pref) :]
                return pref, shou
        return "", ""

    def _scrape_area(self, chu: str, shou: str, area_name: str, seen_ids: set[str]) -> Generator[dict, None, None]:
        """エリアの全ページを巡回してホテルを取得"""
        page = 1
        while True:
            qs = f"f_dai=japan&f_chu={chu}"
            if shou:
                qs += f"&f_shou={shou}"
            search_url = f"{self._SEARCH_BASE}?{qs}&f_sort=hotel&f_page={page}&f_hyoji=30&f_tab=hotel"

            soup = self.get_soup(search_url)
            if soup is None:
                break

            cards = soup.find_all(class_="htl-list-card")
            if not cards:
                break

            for card in cards:
                hotel_id = card.get("data-map-modal-hotel-no", "")
                if hotel_id in seen_ids:
                    continue
                seen_ids.add(hotel_id)
                item = self._extract_card(card, area_name)
                if item:
                    yield item

            if not soup.select_one(".pagination__control-btn--next"):
                break
            page += 1

    def _extract_card(self, card, area_name: str) -> dict | None:
        title_el = card.select_one(".hotel-list__title-text a")
        if not title_el:
            return None

        data: dict = {}
        data[Schema.NAME] = title_el.get_text(strip=True)
        data[Schema.URL] = title_el.get("href", "")

        score_el = card.select_one(".cstmrEvl strong")
        if score_el:
            data[Schema.SCORES] = score_el.get_text(strip=True)
            evl_spans = card.select(".cstmrEvl span")
            if len(evl_spans) >= 2:
                count_m = re.search(r"（(\d+)件）", evl_spans[1].get_text(strip=True))
                if count_m:
                    data[Schema.REV_SCR] = count_m.group(1)

        catchphrase_el = card.select_one(".htlSpecial")
        data["キャッチコピー"] = catchphrase_el.get_text(strip=True) if catchphrase_el else ""

        access_el = card.select_one(".htlAccess span")
        if access_el:
            data["アクセス"] = re.sub(r"^アクセス\s*[：:]\s*", "", access_el.get_text(strip=True))
        else:
            data["アクセス"] = ""

        features = [lb.get_text(strip=True) for lb in card.select(".hotelInfo_features label")]
        data["設備・サービス"] = "、".join(features)

        price_el = card.select_one(".htlLowprice strong")
        data["最安値"] = price_el.get_text(strip=True).replace(",", "") if price_el else ""

        data["地区"] = area_name
        return data


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = RakutenTravelScraper()
    scraper.execute("https://travel.rakuten.co.jp/group/TIKU/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
