"""
対象サイト: https://www.cookdoor.jp/

クックドア 飲食店情報スクレイパー

取得方針:
    - 全国一覧では 9999 ページ上限があるため、47都道府県の一覧を巡回する
    - 口コミ本文や紹介文などの文章は保存せず、基礎情報テーブルと件数のみを取得する

ローカル検証用の環境変数:
    COOKDOOR_PREFS=tokyo,osaka        # 対象都道府県を絞り込み
    COOKDOOR_MAX_ITEMS=30             # 最大取得件数
    COOKDOOR_MAX_PAGES=2              # 都道府県ごとの最大一覧ページ数
"""

import math
import os
import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import parse_qs, urljoin, urlparse

import bs4

_here = Path(__file__).resolve()
for _candidate in (
    _here.parents[3],
    _here.parents[3] / "NetHarvest",
    _here.parents[3].parent / "NetHarvest",
):
    if (_candidate / "src").exists():
        if str(_candidate) not in sys.path:
            sys.path.insert(0, str(_candidate))
        break

from src.const.schema import Schema
from src.framework.static import StaticCrawler


BASE_URL = "https://www.cookdoor.jp"
MEDIA_NAME = "クックドア"
PAGE_SIZE = 30

PREFECTURES = [
    ("hokkaido", "北海道"),
    ("aomori", "青森県"),
    ("iwate", "岩手県"),
    ("miyagi", "宮城県"),
    ("akita", "秋田県"),
    ("yamagata", "山形県"),
    ("fukushima", "福島県"),
    ("ibaraki", "茨城県"),
    ("tochigi", "栃木県"),
    ("gunma", "群馬県"),
    ("saitama", "埼玉県"),
    ("chiba", "千葉県"),
    ("tokyo", "東京都"),
    ("kanagawa", "神奈川県"),
    ("niigata", "新潟県"),
    ("toyama", "富山県"),
    ("ishikawa", "石川県"),
    ("fukui", "福井県"),
    ("yamanashi", "山梨県"),
    ("nagano", "長野県"),
    ("gifu", "岐阜県"),
    ("shizuoka", "静岡県"),
    ("aichi", "愛知県"),
    ("mie", "三重県"),
    ("shiga", "滋賀県"),
    ("kyoto", "京都府"),
    ("osaka", "大阪府"),
    ("hyogo", "兵庫県"),
    ("nara", "奈良県"),
    ("wakayama", "和歌山県"),
    ("tottori", "鳥取県"),
    ("shimane", "島根県"),
    ("okayama", "岡山県"),
    ("hiroshima", "広島県"),
    ("yamaguchi", "山口県"),
    ("tokushima", "徳島県"),
    ("kagawa", "香川県"),
    ("ehime", "愛媛県"),
    ("kochi", "高知県"),
    ("fukuoka", "福岡県"),
    ("saga", "佐賀県"),
    ("nagasaki", "長崎県"),
    ("kumamoto", "熊本県"),
    ("oita", "大分県"),
    ("miyazaki", "宮崎県"),
    ("kagoshima", "鹿児島県"),
    ("okinawa", "沖縄県"),
]

PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|"
    r"三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
ZIP_PATTERN = re.compile(r"〒?\s*(\d{3}-\d{4})")
TOTAL_COUNT_PATTERN = re.compile(r"[\/／]\s*([\d,]+)\s*店舗")
DETAIL_PATH_PATTERN = re.compile(r"^/dtl/\d+/$")
WS_PATTERN = re.compile(r"\s+")
ORDER_STOP_PATTERN = re.compile(r"オーダーストップ\s*([^\)）\s]+(?:\s*[0-9:：]+)?)")

CATEGORY_LV2_MAP = {
    "ファミレス": "レストラン・食堂",
    "ファーストフード": "ファストフード",
    "ステーキハウス": "洋食・レストラン",
    "レストラン": "洋食・レストラン",
    "和食・日本料理": "和食",
    "うどん・そば屋": "麺類",
    "寿司": "寿司",
    "居酒屋": "居酒屋",
    "お好み焼き": "粉もの",
    "焼肉・韓国料理": "焼肉・韓国料理",
    "中華料理・中国料理": "中華料理",
    "ラーメン": "ラーメン",
    "喫茶店・カフェ": "カフェ・喫茶",
}


class CookdoorScraper(StaticCrawler):
    """クックドア 飲食店情報スクレイパー"""

    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"
    )
    TIMEOUT = 30
    DELAY = 1.0
    LIST_DELAY = 0.7
    EXTRA_COLUMNS = [
        "掲載媒体名",
        "エリア",
        "交通アクセス",
        "平均予算",
        "座席",
        "予約",
        "貸切",
        "禁煙・喫煙",
        "駐車場",
        "カード",
        "口コミ数",
        "写真数",
        "動画数",
        "緯度",
        "経度",
        "オーダーストップ",
        "名寄せキー",
        "取得元一覧URL",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        max_items = _int_env("COOKDOOR_MAX_ITEMS")
        max_pages = _int_env("COOKDOOR_MAX_PAGES")
        selected_prefs = self._select_prefectures(url)

        yielded = 0
        estimated_total = 0
        seen: set[str] = set()
        for slug, pref_name in selected_prefs:
            first_list_url = self._list_url(slug, 1)
            first_soup = self.get_soup(first_list_url)
            if first_soup is None:
                self.logger.warning("都道府県一覧取得失敗: %s", first_list_url)
                continue

            total_count = self._extract_total_count(first_soup)
            if total_count == 0:
                self.logger.warning("総件数を取得できませんでした: %s", first_list_url)
            estimated_total += total_count
            self.total_items = min(estimated_total, max_items) if max_items else estimated_total
            self.logger.info("%s: 推定 %d 件 (累計推定 %d 件)", pref_name, total_count, self.total_items or 0)

            page_count = max(1, math.ceil(total_count / PAGE_SIZE)) if total_count else 1
            if max_pages:
                page_count = min(page_count, max_pages)

            for page_no in range(1, page_count + 1):
                list_url = self._list_url(slug, page_no)
                soup = first_soup if page_no == 1 else self.get_soup(list_url)
                if soup is None:
                    self.logger.warning("一覧ページ取得失敗: %s", list_url)
                    break

                detail_urls = self._extract_detail_urls(soup)
                if not detail_urls:
                    self.logger.info("詳細URLなしで終了: %s", list_url)
                    break

                self.logger.info(
                    "%s page=%d/%d: 詳細URL %d 件",
                    pref_name,
                    page_no,
                    page_count,
                    len(detail_urls),
                )

                for detail_url in detail_urls:
                    if detail_url in seen:
                        continue
                    seen.add(detail_url)

                    item = self._scrape_detail(detail_url, pref_name, list_url)
                    if item:
                        yield item
                        yielded += 1
                        if max_items and yielded >= max_items:
                            return

                if page_no < page_count:
                    time.sleep(self.LIST_DELAY)

    def _select_prefectures(self, url: str) -> list[tuple[str, str]]:
        pref_by_slug = dict(PREFECTURES)
        pref_by_name = {name: (slug, name) for slug, name in PREFECTURES}

        env_value = os.getenv("COOKDOOR_PREFS", "").strip()
        if env_value:
            selected = []
            for raw in env_value.split(","):
                key = raw.strip()
                if not key:
                    continue
                if key in pref_by_slug:
                    selected.append((key, pref_by_slug[key]))
                elif key in pref_by_name:
                    selected.append(pref_by_name[key])
                else:
                    self.logger.warning("未知の都道府県指定をスキップ: %s", key)
            return selected or PREFECTURES

        path_parts = [p for p in urlparse(url).path.split("/") if p]
        if path_parts and path_parts[0] in pref_by_slug:
            slug = path_parts[0]
            return [(slug, pref_by_slug[slug])]
        return PREFECTURES

    def _list_url(self, slug: str, page_no: int) -> str:
        if page_no <= 1:
            return f"{BASE_URL}/{slug}/list/"
        return f"{BASE_URL}/{slug}/list/{page_no}/"

    def _extract_total_count(self, soup: bs4.BeautifulSoup) -> int:
        text = self._clean(soup.get_text(" ", strip=True))
        match = TOTAL_COUNT_PATTERN.search(text)
        if not match:
            return 0
        return int(match.group(1).replace(",", ""))

    def _extract_detail_urls(self, soup: bs4.BeautifulSoup) -> list[str]:
        urls = []
        seen = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            detail_url = urljoin(BASE_URL, href)
            parsed = urlparse(detail_url)
            if parsed.netloc != "www.cookdoor.jp":
                continue
            if not DETAIL_PATH_PATTERN.match(parsed.path):
                continue
            normalized = f"{BASE_URL}{parsed.path}"
            if normalized not in seen:
                seen.add(normalized)
                urls.append(normalized)
        return urls

    def _scrape_detail(self, detail_url: str, pref_hint: str, list_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        fields, field_cells = self._extract_fields(soup)
        location_info = self._extract_location_info(soup)

        data = {
            Schema.URL: detail_url,
            Schema.CAT_LV1: "飲食",
            "掲載媒体名": MEDIA_NAME,
            "取得元一覧URL": list_url,
        }

        name = fields.get("店名") or self._text_first(soup, "h1")
        if name:
            data[Schema.NAME] = name

        pref, address, post_code = self._split_address(fields.get("所在地", ""))
        data[Schema.PREF] = pref or location_info.get("pref") or pref_hint
        if post_code:
            data[Schema.POST_CODE] = post_code
        if address:
            data[Schema.ADDR] = address

        area = location_info.get("area")
        if area:
            data["エリア"] = area

        tel = self._valid_value(fields.get("TEL", ""))
        if tel:
            data[Schema.TEL] = tel

        category = location_info.get("category")
        if category:
            data[Schema.CAT_SITE] = category
            data[Schema.CAT_LV2] = CATEGORY_LV2_MAP.get(category, "飲食店")
            data[Schema.CAT_LV3] = category

        label_to_schema = {
            "営業時間": Schema.TIME,
            "定休日": Schema.HOLIDAY,
        }
        for label, schema_key in label_to_schema.items():
            value = self._valid_value(fields.get(label, ""))
            if value:
                data[schema_key] = value

        extra_label_map = {
            "交通アクセス": "交通アクセス",
            "平均予算": "平均予算",
            "座席": "座席",
            "席数": "座席",
            "予約": "予約",
            "貸切": "貸切",
            "禁煙/喫煙": "禁煙・喫煙",
            "禁煙・喫煙": "禁煙・喫煙",
            "駐車場": "駐車場",
            "カード": "カード",
        }
        for label, out_col in extra_label_map.items():
            value = self._valid_value(fields.get(label, ""))
            if value and not data.get(out_col):
                data[out_col] = value

        if data.get("カード"):
            data[Schema.PAYMENTS] = data["カード"]

        order_stop = self._extract_order_stop(data.get(Schema.TIME, ""))
        if order_stop:
            data["オーダーストップ"] = order_stop

        counts = self._extract_counts(soup)
        data.update(counts)

        coords = self._extract_coordinates(soup)
        if coords:
            data["緯度"], data["経度"] = coords

        hp = self._extract_homepage(field_cells.get("ホームページ"), detail_url)
        if hp:
            data[Schema.HP] = hp

        if not data.get(Schema.TEL):
            match_key_parts = [data.get(Schema.NAME, ""), data.get(Schema.PREF, ""), data.get(Schema.ADDR, "")]
            match_key = self._clean(" ".join(p for p in match_key_parts if p))
            if match_key:
                data["名寄せキー"] = match_key

        if not data.get(Schema.NAME):
            return None
        return data

    def _extract_fields(self, soup: bs4.BeautifulSoup) -> tuple[dict[str, str], dict[str, bs4.Tag]]:
        fields: dict[str, str] = {}
        field_cells: dict[str, bs4.Tag] = {}
        for table in soup.find_all("table"):
            classes = table.get("class") or []
            if "table01" not in classes:
                continue
            for tr in table.find_all("tr"):
                cells = [c for c in tr.find_all(["th", "td"], recursive=False)]
                for idx in range(0, len(cells) - 1):
                    if cells[idx].name != "th" or cells[idx + 1].name != "td":
                        continue
                    label = self._clean(cells[idx].get_text(" ", strip=True))
                    value = self._clean_cell_value(label, cells[idx + 1])
                    if label and value and label not in fields:
                        fields[label] = value
                        field_cells[label] = cells[idx + 1]
        return fields, field_cells

    def _extract_location_info(self, soup: bs4.BeautifulSoup) -> dict[str, str]:
        info: dict[str, str] = {}
        el = soup.select_one("p.ttl_location")
        if not el:
            return info
        text = self._clean(el.get_text(" ", strip=True)).strip("（）() ")
        if "／" in text:
            area_text, category = [self._clean(part) for part in text.split("／", 1)]
        elif "/" in text:
            area_text, category = [self._clean(part) for part in text.split("/", 1)]
        else:
            area_text, category = text, ""

        pref_match = PREF_PATTERN.match(area_text)
        if pref_match:
            info["pref"] = pref_match.group(1)
            area = area_text[pref_match.end():].strip()
            if area:
                info["area"] = area
        elif area_text:
            info["area"] = area_text

        if category:
            info["category"] = category.strip("（）() ")
        return info

    def _split_address(self, raw_address: str) -> tuple[str, str, str]:
        text = self._clean(raw_address.replace("地図を見る", ""))
        if not text:
            return "", "", ""

        post_code = ""
        zip_match = ZIP_PATTERN.search(text)
        if zip_match:
            post_code = zip_match.group(1)
            text = self._clean(text[: zip_match.start()] + " " + text[zip_match.end() :])

        text = text.lstrip("〒").strip()
        pref = ""
        pref_match = PREF_PATTERN.match(text)
        if pref_match:
            pref = pref_match.group(1)
            text = text[pref_match.end():].strip()
        return pref, text, post_code

    def _clean_cell_value(self, label: str, cell: bs4.Tag) -> str:
        text = cell.get_text(" ", strip=True)
        if label == "交通アクセス":
            text = re.split(r"※|実際の正確な道路距離|経路検索", text)[0]
        return self._clean(text)

    def _extract_order_stop(self, hours: str) -> str:
        match = ORDER_STOP_PATTERN.search(hours)
        if not match:
            return ""
        return self._clean(match.group(1))

    def _extract_counts(self, soup: bs4.BeautifulSoup) -> dict[str, str]:
        text = self._clean(soup.get_text(" ", strip=True))
        result: dict[str, str] = {}
        patterns = {
            "口コミ数": r"口コミ\s*([\d,]+)件",
            "写真数": r"写真\s*([\d,]+)枚",
            "動画数": r"動画\s*([\d,]+)本",
        }
        for col, pattern in patterns.items():
            match = re.search(pattern, text)
            if match:
                result[col] = match.group(1).replace(",", "")
        return result

    def _extract_coordinates(self, soup: bs4.BeautifulSoup) -> tuple[str, str] | None:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "destination=" not in href:
                continue
            parsed = urlparse(href)
            destination = parse_qs(parsed.query).get("destination", [""])[0]
            if "," not in destination:
                continue
            lat, lng = [p.strip() for p in destination.split(",", 1)]
            if lat and lng:
                return lat, lng
        return None

    def _extract_homepage(self, cell: bs4.Tag | None, detail_url: str) -> str:
        if cell is None:
            return ""
        for a in cell.find_all("a", href=True):
            href = urljoin(detail_url, a["href"].strip())
            parsed = urlparse(href)
            if parsed.netloc and "cookdoor.jp" not in parsed.netloc and "homemate-research" not in parsed.netloc:
                return href
        return ""

    def _text_first(self, soup: bs4.BeautifulSoup, selector: str) -> str:
        el = soup.select_one(selector)
        return self._clean(el.get_text(" ", strip=True)) if el else ""

    def _valid_value(self, value: str) -> str:
        value = self._clean(value)
        if value in {"", "-", "―", "ー"}:
            return ""
        return value

    @staticmethod
    def _clean(text: str) -> str:
        return WS_PATTERN.sub(" ", text or "").strip()


def _int_env(name: str) -> int | None:
    raw_value = os.getenv(name, "").strip()
    if not raw_value:
        return None
    try:
        value = int(raw_value)
    except ValueError:
        return None
    return value if value > 0 else None


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    scraper = CookdoorScraper()
    output_label = re.sub(r"[^0-9A-Za-z_-]+", "_", os.getenv("COOKDOOR_OUTPUT_LABEL", "").strip())
    scraper.site_id = f"cookdoor_{output_label}" if output_label else "cookdoor"
    scraper.site_name = f"クックドア_{output_label}" if output_label else "クックドア"
    scraper.execute("https://www.cookdoor.jp/")
    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
