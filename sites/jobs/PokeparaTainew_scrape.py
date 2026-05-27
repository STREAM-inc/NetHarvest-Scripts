# -*- coding: utf-8 -*-
"""
Target site: https://www.pokepara-tainew.jp/
NetHarvest用 ポケパラ体入 店舗URL収集
"""

import re
import sys
from collections import deque
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse, urlunparse

root_path = Path(__file__).resolve()
while not (root_path / "src").exists() and root_path != root_path.parent:
    root_path = root_path.parent

sys.path.insert(0, str(root_path))

from src.const.schema import Schema
from src.framework.static import StaticCrawler


class PokeparaTainewScraper(StaticCrawler):
    DELAY = 1.0
    EXTRA_COLUMNS = ["found_on"]

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

    SHOP_PATH_RE = re.compile(r"^/.*/shop\d+/$")
    PAGINATION_RE = re.compile(r"/p_?\d+\.html$")
    AREA_PATH_RE = re.compile(r"^/(_?[a-z0-9]+)/m\d+(/a\d+)?(/g\d+)?/?$")
    PREF_TOP_RE = re.compile(r"^/(_?[a-z0-9]+)/$")

    REGION_ROOTS = {
        "/",
        "/hokkaido/",
        "/tohoku/",
        "/nn/",
        "/hokuriku/",
        "/kanto/",
        "/shizuoka/",
        "/tokai/",
        "/kansai/",
        "/chugoku/",
        "/kyushu/",
        "/okinawa/",
    }

    EXCLUDE_SUBSTRINGS = [
        "mailto:",
        "tel:",
        "javascript:",
        "/login/",
        "/register",
        "/identity",
        "/search",
        "/reserve",
        "/favorite",
        "/keep",
        "/entry",
        "/tg_",
        "/pickup",
        "/girl_",
        "/blog",
        "/photo",
        "/js/",
        "/css/",
        "/Images/",
        "/img/",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        def normalize_url(raw_url: str) -> str:
            parsed = urlparse(raw_url)
            path = parsed.path
            while "//" in path:
                path = path.replace("//", "/")
            return urlunparse((parsed.scheme, parsed.netloc, path, "", "", ""))

        def is_same_domain(target_url: str) -> bool:
            return urlparse(target_url).netloc == urlparse(self.BASE).netloc

        def should_exclude(href: str) -> bool:
            h = (href or "").strip()
            if not h:
                return True
            return any(s in h for s in self.EXCLUDE_SUBSTRINGS)

        def extract_links(soup, base_url: str) -> list[str]:
            if soup is None:
                return []

            links = []
            for a in soup.select("a[href]"):
                href = (a.get("href") or "").strip()
                if should_exclude(href):
                    continue

                absolute_url = normalize_url(urljoin(base_url, href))
                if not is_same_domain(absolute_url):
                    continue

                links.append(absolute_url)

            return list(dict.fromkeys(links))

        def canonical_shop_only(target_url: str) -> str:
            parsed = urlparse(target_url)
            if self.SHOP_PATH_RE.match(parsed.path):
                return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))
            return ""

        def is_region_root_path(path: str) -> bool:
            return path in self.REGION_ROOTS

        def is_pref_top_path(path: str) -> bool:
            return bool(self.PREF_TOP_RE.match(path)) and not is_region_root_path(path)

        def is_area_list_path(path: str) -> bool:
            return bool(self.AREA_PATH_RE.match(path))

        def is_pagination_path(path: str) -> bool:
            return bool(self.PAGINATION_RE.search(path))

        def collect_prefecture_tops() -> list[str]:
            self.logger.info("Prefecture discovery started")

            pref_urls = []
            seen = set()

            for seed in self.REGION_SEEDS:
                self.logger.info("Region seed fetch started: %s", seed)

                try:
                    soup = self.get_soup(seed)
                except Exception as exc:
                    self.logger.warning("Region seed fetch failed: %s (%s)", seed, exc)
                    continue

                if soup is None:
                    self.logger.warning("Region seed skipped because soup is None: %s", seed)
                    continue

                for link_url in extract_links(soup, seed):
                    if is_pref_top_path(urlparse(link_url).path) and link_url not in seen:
                        seen.add(link_url)
                        pref_urls.append(link_url)

            if not pref_urls:
                self.logger.info("Fallback discovery from BASE started")

                try:
                    soup = self.get_soup(self.BASE + "/")
                except Exception as exc:
                    self.logger.warning("Fallback BASE fetch failed: %s", exc)
                    soup = None

                if soup is not None:
                    for link_url in extract_links(soup, self.BASE + "/"):
                        if is_pref_top_path(urlparse(link_url).path) and link_url not in seen:
                            seen.add(link_url)
                            pref_urls.append(link_url)

            self.logger.info("Prefecture discovery completed: %s prefecture URLs", len(pref_urls))
            return pref_urls

        normalized_input_url = normalize_url(url)

        if canonical_shop_only(normalized_input_url):
            detail_urls = [normalized_input_url]
            self.logger.info("Direct detail URL mode: %s", normalized_input_url)

        else:
            parsed_input = urlparse(normalized_input_url)

            if parsed_input.path.strip("/"):
                start_urls = [normalized_input_url]
                self.logger.info("Single listing URL mode: %s", normalized_input_url)
            else:
                start_urls = collect_prefecture_tops()
                self.logger.info("Root URL mode: start prefecture URLs=%s", len(start_urls))

            if not start_urls:
                self.logger.warning("No start URLs found: %s", url)
                return

            q = deque(start_urls)
            visited = set()
            found_map = {}

            pages = 0

            while q:
                list_url = q.popleft()

                if list_url in visited:
                    continue

                visited.add(list_url)
                pages += 1

                self.logger.info(
                    "Listing page fetch started: pages=%s queue=%s url=%s",
                    pages,
                    len(q),
                    list_url,
                )

                try:
                    soup = self.get_soup(list_url)
                except Exception as exc:
                    self.logger.warning("Listing page fetch failed: %s (%s)", list_url, exc)
                    continue

                if soup is None:
                    self.logger.warning("Listing page skipped because soup is None: %s", list_url)
                    continue

                links = extract_links(soup, list_url)

                new_details = 0
                for link_url in links:
                    shop_url = canonical_shop_only(link_url)
                    if shop_url and shop_url not in found_map:
                        found_map[shop_url] = list_url
                        new_details += 1

                new_queue = 0
                for link_url in links:
                    path = urlparse(link_url).path

                    if (
                        is_pref_top_path(path)
                        or is_area_list_path(path)
                        or is_pagination_path(path)
                    ):
                        if link_url not in visited and link_url not in q:
                            q.append(link_url)
                            new_queue += 1

                self.logger.info(
                    "Listing page fetch completed: pages=%s new_details=%s total_details=%s new_queue=%s queue=%s visited=%s",
                    pages,
                    new_details,
                    len(found_map),
                    new_queue,
                    len(q),
                    len(visited),
                )

            detail_urls = list(found_map.keys())
            self.logger.info("Listing collection completed: detail_urls=%s", len(detail_urls))

            if not detail_urls:
                self.logger.warning("No detail URLs collected from listing pages: %s", url)
                return

        self.total_items = len(detail_urls)

        for index, detail_url in enumerate(detail_urls, start=1):
            found_on = ""
            if "found_map" in locals():
                found_on = found_map.get(detail_url, "")

            self.logger.info(
                "Detail URL yielded [%s/%s]: %s",
                index,
                self.total_items,
                detail_url,
            )

            yield {
                Schema.URL: detail_url,
                Schema.NAME: "",
                "found_on": found_on,
            }

        self.logger.info("Finished: %s detail URLs yielded", self.total_items)


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)

    crawler = PokeparaTainewScraper()
    crawler.execute("https://www.pokepara-tainew.jp/")