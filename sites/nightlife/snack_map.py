"""
Target site: https://www.snack-map.com/
"""

from __future__ import annotations

import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if (_project_root / "src").exists() and str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
elif (_project_root / "NetHarvest" / "src").exists():
    net_harvest_root = _project_root / "NetHarvest"
    if str(net_harvest_root) not in sys.path:
        sys.path.insert(0, str(net_harvest_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler


SUPABASE_URL = "https://vyhuxallcaciedrmxdtf.supabase.co"
SUPABASE_ANON_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZ5aHV4YWxsY2FjaWVkcm14ZHRmIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MDAzMjkyMzQsImV4cCI6MjAxNTkwNTIzNH0."
    "JuayrVK-eQQbgGpELHpZSAdoBBGapKg6xxM6S-h-2es"
)
API_BASE = f"{SUPABASE_URL}/rest/v1"
SITE_BASE = "https://www.snack-map.com"
MEDIA_NAME = "スナックマップ"

PREFECTURES = {
    1: "北海道",
    2: "青森県",
    3: "岩手県",
    4: "宮城県",
    5: "秋田県",
    6: "山形県",
    7: "福島県",
    8: "茨城県",
    9: "栃木県",
    10: "群馬県",
    11: "埼玉県",
    12: "千葉県",
    13: "東京都",
    14: "神奈川県",
    15: "新潟県",
    16: "富山県",
    17: "石川県",
    18: "福井県",
    19: "山梨県",
    20: "長野県",
    21: "岐阜県",
    22: "静岡県",
    23: "愛知県",
    24: "三重県",
    25: "滋賀県",
    26: "京都府",
    27: "大阪府",
    28: "兵庫県",
    29: "奈良県",
    30: "和歌山県",
    31: "鳥取県",
    32: "島根県",
    33: "岡山県",
    34: "広島県",
    35: "山口県",
    36: "徳島県",
    37: "香川県",
    38: "愛媛県",
    39: "高知県",
    40: "福岡県",
    41: "佐賀県",
    42: "長崎県",
    43: "熊本県",
    44: "大分県",
    45: "宮崎県",
    46: "鹿児島県",
    47: "沖縄県",
}

SNACK_COLUMNS = ",".join(
    [
        "id",
        "uuid",
        "name",
        "prefecture",
        "address",
        "tel",
        "business_hours",
        "business_hours_detail",
        "regular_holiday",
        "website",
        "instagram",
        "twitter",
        "tiktok",
        "youtube",
        "line",
        "nearest_station",
        "transpotation",
        "min_budget",
        "rating",
        "lat",
        "lng",
        "area_id",
        "googlemap_url",
        "created_at",
        "updated_at",
        "publish_status",
    ]
)

EXTRA_MEDIA = "掲載媒体名"
EXTRA_AREA = "エリア"
EXTRA_AREA_GROUP = "広域エリア"
EXTRA_STORE_PHONE = "店舗番号"
EXTRA_MOBILE_PHONE = "携帯番号"
EXTRA_RAW_PHONE = "電話番号_原文"
EXTRA_STORE_ID = "店舗ID"
EXTRA_NEAREST_STATION = "最寄駅"
EXTRA_ACCESS = "アクセス"
EXTRA_BUDGET = "予算"
EXTRA_RATING = "評価"
EXTRA_LAT = "緯度"
EXTRA_LNG = "経度"
EXTRA_GOOGLE_MAP = "Google Maps URL"
EXTRA_BUSINESS_HOURS_DETAIL = "営業時間詳細"
EXTRA_YOUTUBE = "YouTube"
EXTRA_CREATED_AT = "掲載作成日時"
EXTRA_UPDATED_AT = "更新日時"

_FULLWIDTH_TRANS = str.maketrans(
    "０１２３４５６７８９－ー−―ｰ　",
    "0123456789----- ",
)
_PHONE_RE = re.compile(r"0\d{1,4}[-\s]?\d{1,4}[-\s]?\d{3,4}")
_MOBILE_RE = re.compile(r"0[789]0[-\s]?\d{4}[-\s]?\d{4}")


def _clean(value) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).translate(_FULLWIDTH_TRANS)).strip()


def _join_unique(values: list[str]) -> str:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        value = _clean(value)
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return " / ".join(result)


def _content_range_total(content_range: str | None) -> int | None:
    if not content_range or "/" not in content_range:
        return None
    total = content_range.rsplit("/", 1)[1]
    return int(total) if total.isdigit() else None


def _phone_groups(raw_tel: str) -> tuple[str, str, str]:
    raw = _clean(raw_tel)
    if not raw or raw in {".", "-", "ー", "なし"}:
        return "", "", ""

    normalized = re.sub(r"\s*-\s*", "-", raw)
    normalized = re.sub(r"[()（）]", "-", normalized)

    mobile_matches = _MOBILE_RE.findall(normalized)
    all_matches = _PHONE_RE.findall(normalized)
    mobile_digits = {re.sub(r"\D", "", value) for value in mobile_matches}

    store_matches: list[str] = []
    for value in all_matches:
        digits = re.sub(r"\D", "", value)
        if digits not in mobile_digits:
            store_matches.append(value)

    store_phone = _join_unique(store_matches)
    mobile_phone = _join_unique(mobile_matches)
    return store_phone, mobile_phone, normalized if store_phone or mobile_phone else ""


class SnackMapScraper(StaticCrawler):
    """Snack Map crawler for published snack bar shop records."""

    DELAY = 0.0
    TIMEOUT = 60
    CONTINUE_ON_ERROR = False
    PAGE_SIZE = 250
    REQUEST_DELAY = 0.2
    EXTRA_COLUMNS = [
        EXTRA_MEDIA,
        EXTRA_AREA,
        EXTRA_AREA_GROUP,
        EXTRA_STORE_PHONE,
        EXTRA_MOBILE_PHONE,
        EXTRA_RAW_PHONE,
        EXTRA_STORE_ID,
        EXTRA_NEAREST_STATION,
        EXTRA_ACCESS,
        EXTRA_BUDGET,
        EXTRA_RATING,
        EXTRA_LAT,
        EXTRA_LNG,
        EXTRA_GOOGLE_MAP,
        EXTRA_BUSINESS_HOURS_DETAIL,
        EXTRA_YOUTUBE,
        EXTRA_CREATED_AT,
        EXTRA_UPDATED_AT,
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        area_groups = self._area_groups()
        areas = self._areas(area_groups)

        uuid = self._uuid_from_url(url)
        filters = {"publish_status": "eq.published"}
        if uuid:
            filters["uuid"] = f"eq.{uuid}"

        total = None
        for row in self._fetch_paginated("snacks", SNACK_COLUMNS, filters, order="id.asc"):
            if total is None and self.total_items:
                total = self.total_items
                self.logger.info("スナックマップ取得対象: %d 件", total)
            yield self._row_to_item(row, areas)

    def _fetch_paginated(
        self,
        table: str,
        columns: str,
        filters: dict[str, str] | None = None,
        order: str = "id.asc",
    ) -> Generator[dict, None, None]:
        start = 0
        total: int | None = None
        filters = filters or {}

        while True:
            end = start + self.PAGE_SIZE - 1
            headers = self._api_headers()
            headers["Range-Unit"] = "items"
            headers["Range"] = f"{start}-{end}"
            params = {"select": columns, "order": order}
            params.update(filters)

            response = self.session.get(
                f"{API_BASE}/{table}",
                headers=headers,
                params=params,
                timeout=self.TIMEOUT,
            )
            response.raise_for_status()
            rows = response.json()

            if total is None:
                total = _content_range_total(response.headers.get("content-range"))
                if table == "snacks" and total is not None:
                    self.total_items = total

            if not rows:
                break

            for row in rows:
                yield row

            start += self.PAGE_SIZE
            if total is not None and start >= total:
                break
            time.sleep(self.REQUEST_DELAY)

    def _fetch_all(self, table: str, columns: str, order: str = "id.asc") -> list[dict]:
        return list(self._fetch_paginated(table, columns, order=order))

    def _api_headers(self) -> dict[str, str]:
        return {
            "apikey": SUPABASE_ANON_KEY,
            "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
            "Prefer": "count=exact",
        }

    def _area_groups(self) -> dict[int, str]:
        groups: dict[int, str] = {}
        for row in self._fetch_all("area_groups", "id,name,prefecture_id"):
            group_id = row.get("id")
            name = _clean(row.get("name"))
            if isinstance(group_id, int) and name:
                groups[group_id] = name
        return groups

    def _areas(self, area_groups: dict[int, str]) -> dict[int, tuple[str, str]]:
        areas: dict[int, tuple[str, str]] = {}
        for row in self._fetch_all("areas", "id,name,area_group_id"):
            area_id = row.get("id")
            name = _clean(row.get("name"))
            group_id = row.get("area_group_id")
            group_name = area_groups.get(group_id, "") if isinstance(group_id, int) else ""
            if isinstance(area_id, int):
                areas[area_id] = (name, group_name)
        return areas

    def _row_to_item(self, row: dict, areas: dict[int, tuple[str, str]]) -> dict:
        uuid = _clean(row.get("uuid"))
        detail_url = f"{SITE_BASE}/snack/{uuid}" if uuid else SITE_BASE
        pref = PREFECTURES.get(row.get("prefecture"), "")
        area, area_group = areas.get(row.get("area_id"), ("", ""))
        store_phone, mobile_phone, raw_phone = _phone_groups(row.get("tel", ""))
        main_tel = store_phone or mobile_phone

        item = {
            Schema.URL: detail_url,
            Schema.NAME: _clean(row.get("name")),
            Schema.PREF: pref,
            Schema.ADDR: _clean(row.get("address")),
            Schema.TEL: main_tel,
            Schema.CAT_LV1: "飲食店",
            Schema.CAT_LV2: "ナイトレジャー",
            Schema.CAT_LV3: "スナック・パブ・ラウンジ",
            Schema.CAT_NM: "スナック",
            Schema.CAT_SITE: "スナック",
            Schema.TIME: _clean(row.get("business_hours")),
            Schema.HOLIDAY: _clean(row.get("regular_holiday")),
            Schema.HP: _clean(row.get("website")),
            Schema.LINE: _clean(row.get("line")),
            Schema.INSTA: _clean(row.get("instagram")),
            Schema.X: _clean(row.get("twitter")),
            Schema.TIKTOK: _clean(row.get("tiktok")),
            EXTRA_MEDIA: MEDIA_NAME,
            EXTRA_AREA: area,
            EXTRA_AREA_GROUP: area_group,
            EXTRA_STORE_PHONE: store_phone,
            EXTRA_MOBILE_PHONE: mobile_phone,
            EXTRA_RAW_PHONE: raw_phone,
            EXTRA_STORE_ID: str(row.get("id") or ""),
            EXTRA_NEAREST_STATION: _clean(row.get("nearest_station")),
            EXTRA_ACCESS: _clean(row.get("transpotation")),
            EXTRA_BUDGET: self._budget(row.get("min_budget")),
            EXTRA_RATING: _clean(row.get("rating")),
            EXTRA_LAT: _clean(row.get("lat")),
            EXTRA_LNG: _clean(row.get("lng")),
            EXTRA_GOOGLE_MAP: _clean(row.get("googlemap_url")),
            EXTRA_BUSINESS_HOURS_DETAIL: _clean(row.get("business_hours_detail")),
            EXTRA_YOUTUBE: _clean(row.get("youtube")),
            EXTRA_CREATED_AT: _clean(row.get("created_at")),
            EXTRA_UPDATED_AT: _clean(row.get("updated_at")),
        }

        return item

    def _budget(self, value) -> str:
        try:
            amount = int(value or 0)
        except (TypeError, ValueError):
            return ""
        return str(amount) if amount > 0 else ""

    def _uuid_from_url(self, url: str) -> str:
        path = urlparse(url).path.rstrip("/")
        parts = path.split("/")
        if len(parts) >= 3 and parts[-2] == "snack":
            return parts[-1]
        return ""
