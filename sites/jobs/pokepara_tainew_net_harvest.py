# -*- coding: utf-8 -*-
"""
Pokepara Tainew - NetHarvest用 URL収集スクリプト

目的:
- 地域ページから都道府県トップを発見
- 都道府県トップ / エリア一覧 / ページネーションのみ巡回
- 店舗詳細URL（.../shop12345/）だけを収集
- NetHarvestのSchema列 + EXTRA_COLUMNS形式でCSV出力しやすい形にする

出力列:
- 取得URL
- 取得日時
- 取得サイト名
- 店舗詳細URL
- 発見元URL
"""

from __future__ import annotations

import csv
import os
import random
import re
import sys
import time
from collections import deque
from datetime import datetime
from typing import Dict, Iterable, List, Optional
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

# NetHarvest上で動かす場合はこちらを優先
try:
    from src.models.schema import Schema  # type: ignore
except Exception:
    # ローカル単体実行でも動くように最低限のフォールバック
    class Schema:  # type: ignore
        URL = "取得URL"
        FETCHED_AT = "取得日時"
        SITE_NAME = "取得サイト名"


SITE_NAME = "ポケパラ体入"
BASE = "https://www.pokepara-tainew.jp"

REGION_SEEDS = [
    "https://www.pokepara-tainew.jp/hokkaido/",
    "https://www.pokepara-tainew.jp/tohoku/",
    "https://www.pokepara-tainew.jp/nn/",
    "https://www.pokepara-tainew.jp/hokuriku/",
    "https://www.pokepara-tainew.jp/kanto/",
    "https://www.pokepara-tainew.jp/shizuoka/",
    "https://www.pokepara-tainew.jp/tokai/",
    "https://www.pokepara-tainew.jp/kansai/",
    "https://www.pokepara-tainew.jp/chugoku/",
    "https://www.pokepara-tainew.jp/kyushu/",
    "https://www.pokepara-tainew.jp/okinawa/",
]

# NetHarvest側で追加列として扱いやすいように定義
EXTRA_COLUMNS = [
    "店舗詳細URL",
    "発見元URL",
]

OUTPUT_DIR = "output"
OUT_CSV = os.path.join(OUTPUT_DIR, "pokepara_tainew_detail_urls.csv")

SLEEP_MIN = 0.2
SLEEP_MAX = 0.5
TIMEOUT = 30
RETRIES = 2
CHECKPOINT_EVERY_PAGES = 200

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

SHOP_PATH_RE = re.compile(r"^/.*/shop\d+/$")
PAGINATION_RE = re.compile(r"/p_?\d+\.html$")
AREA_PATH_RE = re.compile(r"^/(_?[a-z0-9]+)/m\d+(/a\d+)?(/g\d+)?/?$")
PREF_TOP_RE = re.compile(r"^/(_?[a-z0-9]+)/$")

REGION_ROOTS = {
    "/",
    "/hokkaido/", "/tohoku/", "/nn/", "/hokuriku/", "/kanto/",
    "/shizuoka/", "/tokai/", "/kansai/", "/chugoku/", "/kyushu/", "/okinawa/",
}

EXCLUDE_SUBSTRINGS = [
    "mailto:", "tel:", "javascript:",
    "/login/", "/register", "/identity",
    "/search", "/reserve",
    "/favorite", "/keep", "/entry",
    "/tg_", "/pickup", "/girl_", "/blog", "/photo",
    "/js/", "/css/", "/Images/", "/img/",
]


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def sleep_jitter() -> None:
    time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))


def normalize_url(abs_url: str) -> str:
    p = urlparse(abs_url)
    path = p.path
    while "//" in path:
        path = path.replace("//", "/")
    return urlunparse((p.scheme, p.netloc, path, "", "", ""))


def is_same_domain(url: str) -> bool:
    return urlparse(url).netloc == urlparse(BASE).netloc


def should_exclude(href: str) -> bool:
    h = (href or "").strip()
    if not h:
        return True
    return any(s in h for s in EXCLUDE_SUBSTRINGS)


def extract_links(html: str, base_url: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: List[str] = []
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if should_exclude(href):
            continue
        abs_url = urljoin(base_url, href)
        if is_same_domain(abs_url):
            links.append(normalize_url(abs_url))
    return unique_keep_order(links)


def canonical_shop_only(url: str) -> str:
    p = urlparse(url)
    if SHOP_PATH_RE.match(p.path):
        return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))
    return ""


def is_region_root_path(path: str) -> bool:
    return path in REGION_ROOTS


def is_pref_top_path(path: str) -> bool:
    return bool(PREF_TOP_RE.match(path)) and not is_region_root_path(path)


def is_area_list_path(path: str) -> bool:
    return bool(AREA_PATH_RE.match(path))


def is_pagination_path(path: str) -> bool:
    return bool(PAGINATION_RE.search(path))


def unique_keep_order(seq: Iterable[str]) -> List[str]:
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


class Fetcher:
    def __init__(self) -> None:
        self.sess = requests.Session()
        self.sess.headers.update(
            {
                "User-Agent": UA,
                "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
                "Referer": BASE + "/",
            }
        )

    def get(self, url: str) -> Optional[str]:
        last_err: Optional[Exception] = None
        for i in range(RETRIES + 1):
            try:
                res = self.sess.get(url, timeout=TIMEOUT, allow_redirects=True)
                if res.status_code in (404, 410):
                    return None
                res.raise_for_status()
                res.encoding = res.apparent_encoding or res.encoding
                return res.text
            except Exception as e:
                last_err = e
                if i < RETRIES:
                    sleep_jitter()
        print(f"[WARN] fetch failed url={url} err={last_err}", file=sys.stderr)
        return None


def row_from_shop(detail_url: str, found_on: str) -> Dict[str, str]:
    return {
        Schema.URL: found_on,
        Schema.FETCHED_AT: now_str(),
        Schema.SITE_NAME: SITE_NAME,
        "店舗詳細URL": detail_url,
        "発見元URL": found_on,
    }


def load_existing_rows(out_csv: str) -> Dict[str, str]:
    """checkpoint再開用: {店舗詳細URL: 発見元URL}"""
    if not os.path.exists(out_csv):
        return {}

    found: Dict[str, str] = {}
    try:
        with open(out_csv, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                detail_url = (row.get("店舗詳細URL") or row.get("detail_url") or "").strip()
                found_on = (row.get("発見元URL") or row.get("found_on") or "").strip()
                if detail_url:
                    found[detail_url] = found_on
    except Exception as e:
        print(f"[WARN] failed to load existing csv: {e}", file=sys.stderr)
    return found


def save_rows(found_map: Dict[str, str], out_csv: str) -> None:
    os.makedirs(os.path.dirname(out_csv) or ".", exist_ok=True)
    fieldnames = [Schema.URL, Schema.FETCHED_AT, Schema.SITE_NAME] + EXTRA_COLUMNS

    rows = [row_from_shop(detail_url, found_on) for detail_url, found_on in found_map.items()]
    rows.sort(key=lambda r: r["店舗詳細URL"])

    with open(out_csv, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def collect_prefecture_tops(fetcher: Fetcher) -> List[str]:
    pref_urls: List[str] = []

    for seed in REGION_SEEDS:
        html = fetcher.get(seed)
        sleep_jitter()
        if not html:
            continue
        for url in extract_links(html, seed):
            if is_pref_top_path(urlparse(url).path):
                pref_urls.append(url)

    pref_urls = unique_keep_order(pref_urls)

    if not pref_urls:
        html = fetcher.get(BASE + "/")
        sleep_jitter()
        if html:
            for url in extract_links(html, BASE + "/"):
                if is_pref_top_path(urlparse(url).path):
                    pref_urls.append(url)

    return unique_keep_order(pref_urls)


def crawl_shops(fetcher: Fetcher, pref_urls: List[str], out_csv: str = OUT_CSV) -> Dict[str, str]:
    queue = deque(pref_urls)
    visited = set()
    found_map = load_existing_rows(out_csv)

    if found_map:
        print(f"[RESUME] loaded existing shops: {len(found_map):,}", file=sys.stderr)

    pages = 0
    last_report = time.time()

    while queue:
        url = queue.popleft()
        if url in visited:
            continue
        visited.add(url)

        html = fetcher.get(url)
        pages += 1
        if not html:
            sleep_jitter()
            continue

        links = extract_links(html, url)

        for link in links:
            shop_url = canonical_shop_only(link)
            if shop_url and shop_url not in found_map:
                found_map[shop_url] = url

        for link in links:
            path = urlparse(link).path
            if is_pref_top_path(path) or is_area_list_path(path) or is_pagination_path(path):
                if link not in visited:
                    queue.append(link)

        now = time.time()
        if pages % 100 == 0 or now - last_report > 10:
            last_report = now
            print(
                f"[PROGRESS] pages={pages:,} shops={len(found_map):,} queue={len(queue):,} visited={len(visited):,}",
                file=sys.stderr,
            )

        if pages % CHECKPOINT_EVERY_PAGES == 0:
            save_rows(found_map, out_csv)
            print(f"[CHECKPOINT] saved -> {out_csv} shops={len(found_map):,}", file=sys.stderr)

        sleep_jitter()

    save_rows(found_map, out_csv)
    return found_map


def parse() -> List[Dict[str, str]]:
    """
    NetHarvest用の入口。
    NetHarvestからこの関数を呼べば、Schema列付きのlist[dict]を返す。
    同時に checkpoint/final CSV も output 配下へ保存する。
    """
    fetcher = Fetcher()

    print("[1/2] Discover prefecture top pages from region seeds...", file=sys.stderr)
    pref_urls = collect_prefecture_tops(fetcher)
    print(f"[INFO] prefecture tops discovered: {len(pref_urls)}", file=sys.stderr)

    print("[2/2] Crawl and collect canonical shop URLs...", file=sys.stderr)
    found_map = crawl_shops(fetcher, pref_urls, OUT_CSV)

    rows = [row_from_shop(detail_url, found_on) for detail_url, found_on in found_map.items()]
    rows.sort(key=lambda r: r["店舗詳細URL"])
    print(f"[DONE] out={OUT_CSV} shops={len(rows):,}", file=sys.stderr)
    return rows


def main() -> None:
    parse()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[ABORT] KeyboardInterrupt", file=sys.stderr)
        sys.exit(130)
