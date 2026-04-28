"""
Target site: https://www.hoskyu.com/
"""
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

root_path = Path(__file__).resolve()
while not (root_path / "src").exists() and root_path != root_path.parent:
    root_path = root_path.parent

sys.path.insert(0, str(root_path))

from src.const.schema import Schema
from src.framework.static import StaticCrawler


class HostKyuzinDotcomScraper(StaticCrawler):
    DELAY = 3.0
    EXTRA_COLUMNS = ["携帯TEL", "アクセス", "職種"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        site_root = "https://www.hoskyu.com/"
        detail_pattern = re.compile(r"^https?://(?:www\.)?hoskyu\.com/shop/[^/?#]+/?$")

        def clean_text(node) -> str:
            if not node:
                return ""
            return " ".join(node.stripped_strings).strip()

        def normalize_url(raw_url: str) -> str:
            parsed = urlparse(raw_url.strip())
            path = parsed.path if parsed.path.endswith("/") else f"{parsed.path}/"
            return f"{parsed.scheme}://{parsed.netloc}{path}"

        def normalize_list_url(raw_url: str) -> str:
            normalized = normalize_url(raw_url)
            parsed = urlparse(normalized)
            path = re.sub(r"/page/\d+/?$", "/", parsed.path)
            return f"{parsed.scheme}://{parsed.netloc}{path}"

        def normalize_label(text: str) -> str:
            return (
                text.replace("\u3000", "")
                .replace(" ", "")
                .replace(":", "")
                .replace("：", "")
                .strip()
            )

        def extract_rows(table) -> dict[str, str]:
            rows: dict[str, str] = {}
            if not table:
                return rows

            for tr in table.select("tr"):
                cells = tr.select("td")
                if len(cells) < 2:
                    continue

                label = normalize_label(clean_text(cells[0]))
                value = clean_text(cells[1])
                if label:
                    rows[label] = value

            return rows

        def find_table_by_heading(soup, heading_text: str):
            for heading in soup.select("h2.job-title-h2"):
                if clean_text(heading) == heading_text:
                    return heading.find_next("table", class_="job-table")
            return None

        def discover_area_urls(home_url: str) -> list[str]:
            self.logger.info("Area discovery started: %s", home_url)
            home_soup = self.get_soup(home_url)

            candidates: list[str] = []
            seen_candidates: set[str] = set()

            for link in home_soup.select("main a[href], .entry-content a[href]"):
                href = (link.get("href") or "").strip()
                if not href:
                    continue

                absolute_url = normalize_list_url(urljoin(home_url, href))
                parsed = urlparse(absolute_url)

                if "hoskyu.com" not in parsed.netloc:
                    continue

                stripped_path = parsed.path.strip("/")
                if not stripped_path or "/" in stripped_path:
                    continue

                if absolute_url == home_url or absolute_url in seen_candidates:
                    continue

                seen_candidates.add(absolute_url)
                candidates.append(absolute_url)

            self.logger.info("Area discovery candidate count: %s", len(candidates))

            area_urls: list[str] = []
            for candidate_url in candidates:
                try:
                    candidate_soup = self.get_soup(candidate_url)
                except Exception as exc:
                    self.logger.warning("Area candidate fetch failed: %s (%s)", candidate_url, exc)
                    continue

                item_count = len(candidate_soup.select(".my-job-item"))
                if item_count == 0:
                    continue

                area_urls.append(candidate_url)
                self.logger.info("Area candidate accepted: %s (items=%s)", candidate_url, item_count)

            if not area_urls:
                raise RuntimeError(f"No area URLs discovered from homepage: {home_url}")

            self.logger.info("Area discovery completed: %s area URLs", len(area_urls))
            return area_urls

        normalized_input_url = normalize_url(url)

        if detail_pattern.fullmatch(normalized_input_url):
            detail_urls = [normalized_input_url]
            self.total_items = 1
            self.logger.info("Direct detail URL mode: %s", normalized_input_url)
        else:
            parsed_input = urlparse(normalized_input_url)

            if parsed_input.path.strip("/"):
                base_urls = [normalize_list_url(normalized_input_url)]
                self.logger.info("Single area URL mode: %s", base_urls[0])
            else:
                base_urls = discover_area_urls(site_root)

            detail_urls = []
            seen_detail_urls: set[str] = set()
            total_listing_pages = 0

            for area_index, base_url in enumerate(base_urls, start=1):
                self.logger.info(
                    "Area crawl started [%s/%s]: %s",
                    area_index,
                    len(base_urls),
                    base_url,
                )

                page = 1
                while True:
                    page_url = base_url if page == 1 else urljoin(base_url, f"page/{page}/")
                    self.logger.info("Listing page fetch started: area=%s page=%s url=%s", base_url, page, page_url)

                    try:
                        soup = self.get_soup(page_url)
                    except Exception as exc:
                        self.logger.warning("Listing page fetch failed: %s (%s)", page_url, exc)
                        break

                    total_listing_pages += 1
                    items = soup.select(".my-job-item")
                    if not items:
                        self.logger.info("Listing pagination finished: area=%s page=%s no items", base_url, page)
                        break

                    new_count = 0

                    for item in items:
                        for link in item.select("a[href]"):
                            href = (link.get("href") or "").strip()
                            if "/shop/" not in href:
                                continue

                            detail_url = normalize_url(urljoin(page_url, href))
                            if not detail_pattern.fullmatch(detail_url):
                                continue

                            if detail_url in seen_detail_urls:
                                continue

                            seen_detail_urls.add(detail_url)
                            detail_urls.append(detail_url)
                            new_count += 1
                            break

                    self.logger.info(
                        "Listing page fetch completed: area=%s page=%s items=%s new_details=%s total_details=%s",
                        base_url,
                        page,
                        len(items),
                        new_count,
                        len(detail_urls),
                    )

                    if new_count == 0:
                        self.logger.info("Listing pagination finished: area=%s page=%s no new detail URLs", base_url, page)
                        break

                    page += 1

            if not detail_urls:
                raise RuntimeError(f"No detail URLs collected from listing pages: {url}")

            self.total_items = len(detail_urls)
            self.logger.info(
                "Listing collection completed: areas=%s listing_pages=%s detail_urls=%s",
                len(base_urls),
                total_listing_pages,
                self.total_items,
            )

        for index, detail_url in enumerate(detail_urls, start=1):
            self.logger.info("Detail scrape started [%s/%s]: %s", index, self.total_items, detail_url)

            try:
                soup = self.get_soup(detail_url)
            except Exception as exc:
                self.logger.warning("Detail fetch failed [%s/%s]: %s (%s)", index, self.total_items, detail_url, exc)
                continue

            recruit_table = find_table_by_heading(soup, "募集概要")
            shop_info_table = find_table_by_heading(soup, "店舗情報")

            recruit_rows = extract_rows(recruit_table)
            shop_rows = extract_rows(shop_info_table)

            name = shop_rows.get("店名", "") or clean_text(soup.select_one("h1"))
            addr = shop_rows.get("住所", "")
            tel = shop_rows.get("電話番号", "")
            mobile_tel = shop_rows.get("電話番号2", "")
            line_value = shop_rows.get("LINEID", "")
            access = shop_rows.get("アクセス", "")
            job_type = recruit_rows.get("職種", "")

            if not line_value and shop_info_table:
                line_link = shop_info_table.select_one("a[href*='line.me'], a[href*='lin.ee']")
                if line_link:
                    line_value = (line_link.get("href") or "").strip()

            if not tel and shop_info_table:
                tel_link = shop_info_table.select_one("a[href^='tel:']")
                if tel_link:
                    tel = (tel_link.get("href") or "").replace("tel:", "").strip()

            if not mobile_tel and shop_info_table:
                tel_links = [
                    a.get("href", "").replace("tel:", "").strip()
                    for a in shop_info_table.select("a[href^='tel:']")
                ]
                tel_links = [value for value in tel_links if value]
                if len(tel_links) >= 2:
                    mobile_tel = tel_links[1]

            if not name and not any([addr, tel, mobile_tel, line_value, access, job_type]):
                self.logger.warning("Structured data not found [%s/%s]: %s", index, self.total_items, detail_url)
                continue

            item = {
                Schema.URL: detail_url,
                Schema.NAME: name,
                Schema.ADDR: addr,
                Schema.TEL: tel,
                Schema.LINE: line_value,
                "携帯TEL": mobile_tel,
                "アクセス": access,
                "職種": job_type,
            }

            self.logger.info(
                "Detail scrape completed [%s/%s]: name=%s | tel=%s | mobile=%s | addr=%s",
                index,
                self.total_items,
                item[Schema.NAME] or "-",
                item[Schema.TEL] or "-",
                item["携帯TEL"] or "-",
                item[Schema.ADDR] or "-",
            )

            yield item

        self.logger.info("Detail scraping finished: %s detail URLs processed", self.total_items)


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)

    crawler = HostKyuzinDotcomScraper()
    crawler.execute("https://www.hoskyu.com/")
