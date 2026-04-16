"""
対象サイト: https://shisha-suitai.com/sitemap.xml
"""

import argparse
import json
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Generator

import requests

root_path = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(root_path))

from src.const.schema import Schema
from src.framework.static import StaticCrawler


class ShishaSuitaiScraper(StaticCrawler):
    """Shisha Suitai 店舗詳細クローラー"""

    DELAY = 3.0
    EXTRA_COLUMNS = [
        "予算",
        "料金メニュー",
        "お店の特徴",
        "設備",
        "予約方法",
        "備考",
        "アクセス",
    ]
    MAX_ITEMS = 0
    DETAIL_RETRY_ATTEMPTS = 2
    DETAIL_RETRY_DELAY = 5.0

    _DAY_MAP = {
        "Monday": Schema.TIME_MON,
        "Tuesday": Schema.TIME_TUE,
        "Wednesday": Schema.TIME_WED,
        "Thursday": Schema.TIME_THU,
        "Friday": Schema.TIME_FRI,
        "Saturday": Schema.TIME_SAT,
        "Sunday": Schema.TIME_SUN,
    }

    _RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

    def parse(self, url: str) -> Generator[dict, None, None]:
        shop_urls = self._extract_shop_urls_from_xml(url)
        if self.MAX_ITEMS and self.MAX_ITEMS > 0:
            shop_urls = shop_urls[: self.MAX_ITEMS]
        self.total_items = len(shop_urls)

        for shop_url in shop_urls:
            detail_soup = self._fetch_detail_soup(shop_url)
            if detail_soup is None:
                continue

            next_data = self._extract_next_data(detail_soup)
            shop_data = self._extract_shop_data(next_data)

            if not shop_data:
                shop_data = self._extract_shop_data_from_rows(detail_soup)

            if not shop_data.get("name"):
                self.logger.warning("店舗名を取得できないためスキップ: %s", shop_url)
                continue

            weekday_hours = self._parse_business_hours(shop_data.get("businessHours", []))
            facilities = shop_data.get("facilities", [])
            if isinstance(facilities, list):
                facilities_text = ",".join(
                    [
                        facility.get("name", "") if isinstance(facility, dict) else str(facility)
                        for facility in facilities
                        if facility
                    ]
                )
            else:
                facilities_text = str(facilities or "")

            sns_accounts = shop_data.get("shopSnsAccounts", [])
            insta = ""
            x_account = ""
            for sns in sns_accounts:
                if not isinstance(sns, dict):
                    continue
                service_id = sns.get("serviceId", "")
                sns_url = sns.get("url", "")
                if service_id == "instagram":
                    insta = sns_url
                elif service_id == "twitter":
                    x_account = sns_url

            pref_obj = shop_data.get("prefecture", {})
            pref_name = pref_obj.get("name", "") if isinstance(pref_obj, dict) else ""

            yield {
                Schema.URL: shop_url,
                Schema.NAME: shop_data.get("name", ""),
                Schema.PREF: pref_name,
                Schema.ADDR: shop_data.get("address", ""),
                Schema.TEL: shop_data.get("phoneNumber", ""),
                Schema.INSTA: insta,
                Schema.X: x_account,
                Schema.HP: shop_data.get("homepageUrl", ""),
                Schema.TIME_MON: weekday_hours.get(Schema.TIME_MON, ""),
                Schema.TIME_TUE: weekday_hours.get(Schema.TIME_TUE, ""),
                Schema.TIME_WED: weekday_hours.get(Schema.TIME_WED, ""),
                Schema.TIME_THU: weekday_hours.get(Schema.TIME_THU, ""),
                Schema.TIME_FRI: weekday_hours.get(Schema.TIME_FRI, ""),
                Schema.TIME_SAT: weekday_hours.get(Schema.TIME_SAT, ""),
                Schema.TIME_SUN: weekday_hours.get(Schema.TIME_SUN, ""),
                Schema.HOLIDAY: shop_data.get("regularClosingDay", ""),
                "料金メニュー": shop_data.get("menu", ""),
                "予算": shop_data.get("budget", ""),
                "予約方法": shop_data.get("reservation", ""),
                "お店の特徴": shop_data.get("description", ""),
                "設備": facilities_text,
                "備考": shop_data.get("note", ""),
                "アクセス": shop_data.get("accessDescription", ""),
            }

    def _fetch_detail_soup(self, shop_url: str):
        for attempt in range(1, self.DETAIL_RETRY_ATTEMPTS + 1):
            try:
                return self.get_soup(shop_url)
            except requests.exceptions.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else None
                if status_code == 404:
                    self.logger.warning("404のためスキップ: %s", shop_url)
                    return None
                if status_code in self._RETRYABLE_STATUS_CODES:
                    if self._should_retry_detail_request(attempt):
                        self.logger.warning(
                            "詳細ページ取得に失敗(%s)。%s/%s 回目の再試行を行います: %s",
                            status_code,
                            attempt,
                            self.DETAIL_RETRY_ATTEMPTS,
                            shop_url,
                        )
                        time.sleep(self.DETAIL_RETRY_DELAY * attempt)
                        continue
                    self.logger.error(
                        "詳細ページ取得に失敗(%s)。再試行上限に達したためスキップ: %s",
                        status_code,
                        shop_url,
                    )
                    return None
                raise
            except (
                requests.exceptions.RetryError,
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
            ) as exc:
                if self._should_retry_detail_request(attempt):
                    self.logger.warning(
                        "詳細ページ取得で通信エラー。%s/%s 回目の再試行を行います: %s (%s)",
                        attempt,
                        self.DETAIL_RETRY_ATTEMPTS,
                        shop_url,
                        exc,
                    )
                    time.sleep(self.DETAIL_RETRY_DELAY * attempt)
                    continue
                self.logger.error(
                    "詳細ページ取得で通信エラー。再試行上限に達したためスキップ: %s (%s)",
                    shop_url,
                    exc,
                )
                return None
            except requests.exceptions.RequestException as exc:
                self.logger.error("詳細ページ取得で想定外の通信エラー: %s (%s)", shop_url, exc)
                return None

        return None

    def _should_retry_detail_request(self, attempt: int) -> bool:
        return attempt < self.DETAIL_RETRY_ATTEMPTS

    def _extract_shop_urls_from_xml(self, sitemap_url: str) -> list[str]:
        response = self.session.get(sitemap_url, timeout=self.TIMEOUT)
        response.raise_for_status()

        root = ET.fromstring(response.text)
        shop_urls = []
        seen = set()
        for elem in root.iter():
            if not str(elem.tag).endswith("loc"):
                continue
            loc_url = (elem.text or "").strip()
            if "/shop/" not in loc_url:
                continue
            if loc_url in seen:
                continue
            seen.add(loc_url)
            shop_urls.append(loc_url)

        return shop_urls

    def _extract_next_data(self, soup) -> dict:
        tag = soup.select_one("#__NEXT_DATA__")
        if not tag:
            return {}
        text = tag.get_text(strip=True)
        if not text:
            return {}
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return {}

    def _extract_shop_data(self, next_data: dict) -> dict:
        return next_data.get("props", {}).get("pageProps", {}).get("shop", {}).get("shop", {})

    def _extract_shop_data_from_rows(self, soup) -> dict:
        data = {
            "name": "",
            "accessDescription": "",
            "description": "",
            "address": "",
            "phoneNumber": "",
            "regularClosingDay": "",
            "menu": "",
            "budget": "",
            "reservation": "",
            "facilities": [],
            "note": "",
            "businessHours": [],
            "shopSnsAccounts": [],
            "homepageUrl": "",
            "prefecture": {},
        }

        name_el = soup.select_one("p.page_shopHeaderPrimaryName__jGtq4")
        if name_el:
            data["name"] = name_el.get_text(strip=True)

        access_el = soup.select_one("p.page_shopHeaderPrimaryAccess__3pg8E")
        if access_el:
            data["accessDescription"] = access_el.get_text(strip=True)

        for row in soup.select("div.page_shopDetailListRow__379Xi"):
            label_el = row.select_one("div.page_shopDetailListRowLabel__2ZZRs")
            value_el = row.select_one("div.page_shopDetailListRowValue__294kV")
            if not label_el or not value_el:
                continue

            label = label_el.get_text(strip=True)
            value = value_el.get_text("\n", strip=True)

            if label == "住所":
                data["address"] = value
            elif label == "電話番号":
                data["phoneNumber"] = value
            elif label == "営業時間":
                data["businessHours"] = self._parse_business_hours_text(value)
            elif label == "休日":
                data["regularClosingDay"] = value
            elif label == "料金":
                data["menu"] = value
            elif label == "予算":
                data["budget"] = value
            elif label == "予約":
                data["reservation"] = value
            elif label == "設備":
                chips = [chip.get_text(strip=True) for chip in row.select("span.Chip____2Fg2Y")]
                data["facilities"] = chips
            elif label == "備考":
                data["note"] = value

        return data

    def _parse_business_hours(self, business_hours: list) -> dict:
        weekday_hours = {
            Schema.TIME_MON: "",
            Schema.TIME_TUE: "",
            Schema.TIME_WED: "",
            Schema.TIME_THU: "",
            Schema.TIME_FRI: "",
            Schema.TIME_SAT: "",
            Schema.TIME_SUN: "",
        }

        for row in business_hours:
            if not isinstance(row, dict):
                continue
            day = row.get("dayOfWeek")
            key = self._DAY_MAP.get(day)
            if not key:
                continue

            start = self._format_hhmm(row.get("start", ""))
            end = self._format_hhmm(row.get("end", ""))
            if start and end:
                weekday_hours[key] = f"{start} 〜 {end}"

        return weekday_hours

    def _parse_business_hours_text(self, text: str) -> list[dict]:
        jp_to_en = {
            "月曜日": "Monday",
            "火曜日": "Tuesday",
            "水曜日": "Wednesday",
            "木曜日": "Thursday",
            "金曜日": "Friday",
            "土曜日": "Saturday",
            "日曜日": "Sunday",
        }
        rows = []
        for line in text.splitlines():
            parts = line.split("：", 1)
            if len(parts) != 2:
                continue
            day_jp = parts[0].strip()
            range_text = parts[1].strip()
            if "〜" not in range_text:
                continue
            start, end = [s.strip() for s in range_text.split("〜", 1)]
            day_en = jp_to_en.get(day_jp)
            if not day_en:
                continue
            rows.append({"dayOfWeek": day_en, "start": f"{start}:00", "end": f"{end}:00"})
        return rows

    def _format_hhmm(self, value: str) -> str:
        if not value:
            return ""
        parts = value.split(":")
        if len(parts) < 2:
            return value
        return f"{parts[0]}:{parts[1]}"


def main() -> None:
    parser = argparse.ArgumentParser(description="ShishaSuitaiScraper 実行用エントリーポイント")
    parser.add_argument("--url", default="https://shisha-suitai.com/sitemap.xml", help="サイトマップURL")
    parser.add_argument("--limit", type=int, default=0, help="取得件数上限（0以下で全件）")
    parser.add_argument("--site-name", default="シーシャスイタイ", help="出力CSVのサイト名")
    parser.add_argument("--site-id", default="shisha-suitai", help="出力CSVのサイトID")
    args = parser.parse_args()

    scraper = ShishaSuitaiScraper()
    scraper.site_name = args.site_name
    scraper.site_id = args.site_id
    scraper.MAX_ITEMS = args.limit
    scraper.execute(args.url)

    print(f"output_path={scraper.output_filepath}")
    print(f"item_count={scraper.item_count}")


if __name__ == "__main__":
    main()
