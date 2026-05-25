import sys
from pathlib import Path
import time
from typing import Generator
from urllib.parse import urljoin

# sys.path を調整（4階層上へ）
base_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(base_dir))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema


class PachiTownCrawler(DynamicCrawler):
    """
    DMMぱちタウン - Pachinko Parlor Information
    https://p-town.dmm.com/shops/{prefecture}
    全国のパチンコ・パチスロ店舗情報を取得（全47都道府県）
    """

    SITE_ID = "pachi_town"
    BASE_URL = "https://p-town.dmm.com"
    START_URL = "https://p-town.dmm.com/shops/tokyo"
    DELAY = 1.0
    EXTRA_COLUMNS = ["prefecture", "shop_id", "pachi_info", "slot_info", "facilities"]

    # 47都道府県コード
    PREFECTURES = [
        "hokkaido",
        "aomori",
        "iwate",
        "miyagi",
        "akita",
        "yamagata",
        "fukushima",
        "tokyo",
        "kanagawa",
        "saitama",
        "chiba",
        "ibaraki",
        "tochigi",
        "gunma",
        "niigata",
        "toyama",
        "ishikawa",
        "fukui",
        "yamanashi",
        "nagano",
        "shizuoka",
        "aichi",
        "gifu",
        "mie",
        "shiga",
        "kyoto",
        "osaka",
        "hyogo",
        "wakayama",
        "nara",
        "tottori",
        "shimane",
        "okayama",
        "hiroshima",
        "yamaguchi",
        "tokushima",
        "kagawa",
        "ehime",
        "kochi",
        "fukuoka",
        "saga",
        "nagasaki",
        "kumamoto",
        "oita",
        "miyazaki",
        "kagoshima",
        "okinawa",
    ]

    def prepare(self):
        pass

    def parse(self, url: str) -> Generator[dict, None, None]:
        """
        全47都道府県のパチンコ店舗情報を取得
        各都道府県の地区別ページから取得
        """
        item_count = 0

        for prefecture in self.PREFECTURES:
            pref_url = f"{self.BASE_URL}/shops/{prefecture}"
            self.logger.info(f"Processing prefecture: {prefecture}")

            try:
                soup = self.get_soup(pref_url)
                if not soup:
                    self.logger.warning(f"Failed to fetch {prefecture}")
                    continue

                # 地区別リンクを収集（重複排除）
                area_links = {
                    urljoin(self.BASE_URL, a["href"])
                    for a in soup.select("a[href]")
                    if f"/shops/{prefecture}/area/" in a.get("href", "")
                }

                self.logger.info(f"Found {len(area_links)} areas in {prefecture}")

                # 各地区のページから店舗を取得
                for area_url in area_links:
                    try:
                        area_soup = self.get_soup(area_url)
                        if not area_soup:
                            continue

                        shops = area_soup.select("li.unit")
                        for shop in shops:
                            try:
                                item = self._parse_shop(shop, prefecture)
                                if item:
                                    yield item
                                    item_count += 1
                            except Exception as e:
                                self.logger.warning(f"Error parsing shop: {e}")
                                continue

                        time.sleep(self.DELAY)

                    except Exception as e:
                        self.logger.error(f"Error processing area {area_url}: {e}")
                        continue

            except Exception as e:
                self.logger.error(f"Error processing prefecture {prefecture}: {e}")
                continue

        self.total_items = item_count
        self.logger.info(f"Total shops scraped: {item_count}")

    def _parse_shop(self, shop_elem, prefecture: str) -> dict | None:
        """
        個別の店舗情報をパース
        構造:
          <li class="unit">
            <div class="link js-card-button shop-card-button" data-url="/shops/tokyo/154">
              <a class="title card-link" href="/shops/tokyo/154">店舗名</a>
              <p class="lead">住所</p>
              <p class="lead">営業時間</p>
              <div class="shop-machine-rate">...</div>
              <ul class="facilityicons">...</ul>
            </div>
          </li>
        """
        try:
            # Shop name
            title_elem = shop_elem.select_one("a.title.card-link")
            shop_name = (
                title_elem.get_text(strip=True) if title_elem else ""
            )
            if not shop_name:
                return None

            # Extract shop ID from href or data-url
            shop_id = ""
            if title_elem:
                href = title_elem.get("href", "")
                if "/shops/" in href:
                    parts = href.split("/")
                    shop_id = parts[-1] if parts else ""

            # Address and operating hours
            leads = shop_elem.select("p.lead")
            address = leads[0].get_text(strip=True) if len(leads) > 0 else ""
            operating_time = (
                leads[1].get_text(strip=True) if len(leads) > 1 else ""
            )

            # Detail page URL
            detail_url = ""
            card_div = shop_elem.select_one("div.shop-card-button")
            if card_div:
                data_url = card_div.get("data-url", "")
                detail_url = data_url if data_url else ""

            # Machine rate information
            pachi_info = ""
            slot_info = ""
            machine_rate = shop_elem.select_one("div.shop-machine-rate")
            if machine_rate:
                pachi_info = self._extract_machine_info(
                    machine_rate, "pachi"
                )
                slot_info = self._extract_machine_info(
                    machine_rate, "slot"
                )

            # Facilities
            facilities_list = []
            facilities_elem = shop_elem.select("ul.facilityicons li span")
            for fac in facilities_elem:
                fac_text = fac.get_text(strip=True)
                if fac_text:
                    facilities_list.append(fac_text)
            facilities = ", ".join(facilities_list)

            item = {
                Schema.NAME: shop_name,
                Schema.ADDR: address,
                Schema.TIME: operating_time,
                Schema.HP: detail_url,
                # EXTRA_COLUMNS
                "prefecture": prefecture,
                "shop_id": shop_id,
                "pachi_info": pachi_info,
                "slot_info": slot_info,
                "facilities": facilities,
            }

            return item

        except Exception as e:
            self.logger.warning(f"Error parsing shop: {e}")
            return None

    def _extract_machine_info(self, machine_elem, machine_type: str) -> str:
        """
        機種情報を抽出（パチンコまたはスロット）
        フォーマット: "レート1-台数1, レート2-台数2, ..."
        """
        try:
            type_elem = machine_elem.select_one(
                f".machine-type-name.{machine_type}"
            )
            if not type_elem:
                return ""

            info_list = []
            parent = type_elem.parent
            if parent:
                rates = parent.select(".machine-rate")
                for rate_elem in rates:
                    span_text = rate_elem.get_text(strip=True).replace(
                        "\n", ""
                    )
                    if span_text:
                        info_list.append(span_text)

            return " | ".join(info_list) if info_list else ""

        except Exception as e:
            self.logger.warning(f"Error extracting machine info: {e}")
            return ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    crawler = PachiTownCrawler()
    crawler.execute(PachiTownCrawler.START_URL)
