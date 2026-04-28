# -*- coding: utf-8 -*-
"""
Target site: https://girlsbaito.jp/
"""

import sys
import re
import logging
from datetime import datetime
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

root_path = Path(__file__).resolve()
while not (root_path / "src").exists() and root_path != root_path.parent:
    root_path = root_path.parent

sys.path.insert(0, str(root_path))

from src.const.schema import Schema
from src.framework.static import StaticCrawler


class GirlsBaitoDetailCrawler(StaticCrawler):
    DELAY = 3.0
    EXTRA_COLUMNS = ["職種", "最寄駅", "取得日時"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        base_url = "https://girlsbaito.jp"
        archive_base = "https://girlsbaito.jp/kanto/archive"
        seen: set[str] = set()
        detail_urls: list[str] = []

        def clean_text(node) -> str:
            if not node:
                return ""
            return " ".join(node.stripped_strings).strip()

        def is_detail_url(target_url: str) -> bool:
            """
            例:
            https://girlsbaito.jp/16475/single
            /16475/single
            """
            parsed = urlparse(target_url)
            return re.fullmatch(r"/\d+/single/?", parsed.path) is not None

        def find_value_by_label_text(soup, label: str) -> str:
            """
            詳細ページ内で「職 種」「最寄駅」などのラベルの次にある値をざっくり取得する
            """
            text = soup.get_text("\n", strip=True)
            lines = [x.strip() for x in text.split("\n") if x.strip()]

            label_variants = {
                label,
                label.replace(" ", ""),
                label.replace("　", ""),
            }

            for i, line in enumerate(lines):
                normalized = line.replace(" ", "").replace("　", "")
                if normalized in label_variants:
                    if i + 1 < len(lines):
                        return lines[i + 1].strip()

            return ""

        self.logger.info("Archive collection started: %s", base_url)

        page = 1
        total_archive_links = 0

        while True:
            page_url = f"{archive_base}?page={page}&search_mode=detail"
            self.logger.info("Archive page fetch started: page=%s url=%s", page, page_url)

            try:
                soup = self.get_soup(page_url)
            except Exception as exc:
                self.logger.warning("Archive page fetch failed: page=%s url=%s (%s)", page, page_url, exc)
                break

            links = soup.select("a[href]")
            new_count = 0

            for link in links:
                href = (link.get("href") or "").strip()
                if not href:
                    continue

                detail_url = urljoin(base_url, href)

                if not is_detail_url(detail_url):
                    continue

                total_archive_links += 1

                if detail_url in seen:
                    continue

                seen.add(detail_url)
                detail_urls.append(detail_url)
                new_count += 1

            self.logger.info(
                "Archive page fetch completed: page=%s all_links=%s new_details=%s total_details=%s",
                page,
                len(links),
                new_count,
                len(detail_urls),
            )

            if new_count == 0:
                self.logger.info("Archive pagination finished: no new details at page=%s", page)
                break

            page += 1

        if not detail_urls:
            raise RuntimeError(f"No detail URLs collected from archive: {base_url}")

        self.total_items = len(detail_urls)

        self.logger.info(
            "Archive collection completed: scanned_detail_links=%s detail_urls=%s",
            total_archive_links,
            self.total_items,
        )

        for index, detail_url in enumerate(detail_urls, start=1):
            self.logger.info("Detail scrape started [%s/%s]: %s", index, self.total_items, detail_url)

            try:
                detail_soup = self.get_soup(detail_url)
            except Exception as exc:
                self.logger.warning("Detail fetch failed [%s/%s]: %s (%s)", index, self.total_items, detail_url, exc)
                continue

            name = clean_text(detail_soup.select_one("h1")) or clean_text(detail_soup.select_one("h2"))

            job_type = (
                find_value_by_label_text(detail_soup, "職 種")
                or find_value_by_label_text(detail_soup, "職種")
            )

            nearest_station = find_value_by_label_text(detail_soup, "最寄駅")

            addr = (
                find_value_by_label_text(detail_soup, "勤務地")
                or find_value_by_label_text(detail_soup, "勤務先住所")
                or find_value_by_label_text(detail_soup, "住所")
            )

            # 電話番号っぽい文字列を詳細ページ全体から取得
            detail_text = detail_soup.get_text("\n", strip=True)
            tel_match = re.search(r"0\d{1,4}[-ー−]?\d{1,4}[-ー−]?\d{3,4}", detail_text)
            tel = tel_match.group(0) if tel_match else ""

            if not name and not any([job_type, tel, addr, nearest_station]):
                self.logger.warning("Structured data not found [%s/%s]: %s", index, self.total_items, detail_url)
                continue

            item = {
                Schema.URL: detail_url,
                Schema.NAME: name,
                Schema.TEL: tel,
                Schema.ADDR: addr,
                "職種": job_type,
                "最寄駅": nearest_station,
                "取得日時": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

            self.logger.info(
                "Detail scrape completed [%s/%s]: name=%s | job_type=%s | tel=%s | addr=%s",
                index,
                self.total_items,
                name or "-",
                job_type or "-",
                tel or "-",
                addr or "-",
            )

            yield item

        self.logger.info("Detail scraping finished: %s detail URLs processed", self.total_items)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    crawler = GirlsBaitoDetailCrawler()
    crawler.execute("https://girlsbaito.jp/kanto/archive?page=1&search_mode=detail")
