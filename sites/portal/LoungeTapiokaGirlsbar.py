"""
Target site: https://lounge-tapioca.com/
"""
import re
import sys
import xml.etree.ElementTree as ET
from collections import deque
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

root_path = Path(__file__).resolve()
while not (root_path / "src").exists() and root_path != root_path.parent:
    root_path = root_path.parent

sys.path.insert(0, str(root_path))

from src.const.schema import Schema
from src.framework.static import StaticCrawler


class LoungeDetailCrawler(StaticCrawler):
    DELAY = 3.0

    def parse(self, url: str) -> Generator[dict, None, None]:
        base_url = url.rstrip("/") + "/"
        detail_pattern = re.compile(r"^https?://(?:www\.)?lounge-tapioca\.com/\d+/$")

        def clean_text(node) -> str:
            if not node:
                return ""
            return " ".join(node.stripped_strings).strip()

        def normalize_label(text: str) -> str:
            return text.replace("\u3000", "").replace(" ", "").strip()

        def normalize_url(raw_url: str) -> str:
            parsed = urlparse(raw_url.strip())
            path = parsed.path if parsed.path.endswith("/") else f"{parsed.path}/"
            return f"{parsed.scheme}://{parsed.netloc}{path}"

        def fetch_xml_root(xml_url: str):
            response = self.session.get(xml_url, timeout=self.TIMEOUT)
            response.raise_for_status()
            return ET.fromstring(response.content)

        self.logger.info("Sitemap collection started: %s", base_url)

        sitemap_candidates: list[str] = []
        robots_url = urljoin(base_url, "robots.txt")

        try:
            robots_response = self.session.get(robots_url, timeout=self.TIMEOUT)
            robots_response.raise_for_status()

            for line in robots_response.text.splitlines():
                if line.lower().startswith("sitemap:"):
                    sitemap_url = line.split(":", 1)[1].strip()
                    if sitemap_url and sitemap_url not in sitemap_candidates:
                        sitemap_candidates.append(sitemap_url)
                        self.logger.info("robots.txt sitemap found: %s", sitemap_url)
        except Exception as exc:
            self.logger.warning("Failed to read robots.txt: %s (%s)", robots_url, exc)

        for fallback in (
            urljoin(base_url, "sitemap.xml"),
            urljoin(base_url, "wp-sitemap.xml"),
        ):
            if fallback not in sitemap_candidates:
                sitemap_candidates.append(fallback)
                self.logger.info("Fallback sitemap queued: %s", fallback)

        visited_sitemaps: set[str] = set()
        seen_detail_urls: set[str] = set()
        sitemap_queue = deque(sitemap_candidates)
        detail_urls: list[str] = []
        sitemap_file_count = 0
        sitemap_url_count = 0

        while sitemap_queue:
            sitemap_url = sitemap_queue.popleft()
            if sitemap_url in visited_sitemaps:
                continue

            visited_sitemaps.add(sitemap_url)
            self.logger.info("Reading sitemap: %s", sitemap_url)

            try:
                root = fetch_xml_root(sitemap_url)
            except Exception as exc:
                self.logger.warning("Failed to parse sitemap: %s (%s)", sitemap_url, exc)
                continue

            sitemap_file_count += 1
            root_tag = root.tag.rsplit("}", 1)[-1]

            if root_tag == "sitemapindex":
                child_count = 0
                for loc in root.findall(".//{*}sitemap/{*}loc"):
                    child_url = (loc.text or "").strip()
                    if not child_url:
                        continue
                    child_count += 1
                    if child_url not in visited_sitemaps:
                        sitemap_queue.append(child_url)

                self.logger.info(
                    "Sitemap index parsed: %s child sitemaps discovered from %s",
                    child_count,
                    sitemap_url,
                )
                continue

            if root_tag != "urlset":
                self.logger.warning("Unsupported sitemap root <%s>: %s", root_tag, sitemap_url)
                continue

            urlset_count = 0
            new_detail_count = 0

            for loc in root.findall(".//{*}url/{*}loc"):
                page_url = (loc.text or "").strip()
                if not page_url:
                    continue

                urlset_count += 1
                sitemap_url_count += 1

                normalized_url = normalize_url(page_url)
                if not detail_pattern.fullmatch(normalized_url):
                    continue

                if normalized_url in seen_detail_urls:
                    continue

                seen_detail_urls.add(normalized_url)
                detail_urls.append(normalized_url)
                new_detail_count += 1

            self.logger.info(
                "Sitemap urlset parsed: %s URLs scanned, %s new detail URLs kept from %s",
                urlset_count,
                new_detail_count,
                sitemap_url,
            )

        if not detail_urls:
            raise RuntimeError(f"No detail URLs collected from sitemap: {base_url}")

        self.total_items = len(detail_urls)
        self.logger.info(
            "Sitemap collection completed: %s sitemap files, %s URLs scanned, %s detail URLs queued",
            sitemap_file_count,
            sitemap_url_count,
            self.total_items,
        )

        for index, detail_url in enumerate(detail_urls, start=1):
            self.logger.info("Detail scrape started [%s/%s]: %s", index, self.total_items, detail_url)

            try:
                soup = self.get_soup(detail_url)
            except Exception as exc:
                self.logger.warning("Detail fetch failed [%s/%s]: %s (%s)", index, self.total_items, detail_url, exc)
                continue

            name = clean_text(soup.select_one(".shop_detail_header h2"))
            genre = clean_text(soup.select_one(".shop_detail_genre"))

            shop_data_table = None
            for title in soup.select(".top_ranking_title"):
                if "ショップデータ" in clean_text(title):
                    shop_data_table = title.find_next("table", class_="shop_detail_table")
                    break

            rows: dict[str, str] = {}
            homepage = ""

            if shop_data_table:
                for tr in shop_data_table.select("tr"):
                    th = tr.select_one("th")
                    td = tr.select_one("td")
                    if not th or not td:
                        continue

                    label = normalize_label(clean_text(th))
                    value = clean_text(td)
                    if not label:
                        continue

                    rows[label] = value

                    if label == "ホームページ":
                        link = td.select_one("a[href]")
                        homepage = (link.get("href") or "").strip() if link else value

            if not name and not rows:
                self.logger.warning("Structured data not found [%s/%s]: %s", index, self.total_items, detail_url)
                continue

            item = {
                Schema.URL: detail_url,
                Schema.NAME: name,
                Schema.ADDR: rows.get("住所") or rows.get("アクセス・地図住所", ""),
                Schema.TEL: rows.get("電話番号", ""),
                Schema.HP: homepage,
                Schema.HOLIDAY: rows.get("定休日", ""),
                Schema.TIME: rows.get("営業時間", ""),
            }

            if genre:
                item[Schema.CAT_SITE] = genre

            self.logger.info(
                "Detail scrape completed [%s/%s]: name=%s | tel=%s | addr=%s",
                index,
                self.total_items,
                item[Schema.NAME] or "-",
                item[Schema.TEL] or "-",
                item[Schema.ADDR] or "-",
            )

            yield item

        self.logger.info("Detail scraping finished: %s detail URLs processed", self.total_items)


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)

    crawler = LoungeDetailCrawler()
    crawler.execute("https://lounge-tapioca.com/")
