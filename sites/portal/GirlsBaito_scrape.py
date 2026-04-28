"""
Target site: https://girlsbaito.jp/
"""
import sys
from datetime import datetime
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

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
        seen: set[str] = set()
        detail_urls: list[str] = []

        def clean_text(node) -> str:
            if not node:
                return ""
            return " ".join(node.stripped_strings).strip()

        def find_value_by_label(scope, item_selector: str, title_selector: str, data_selector: str, label: str) -> str:
            if not scope:
                return ""

            for item in scope.select(item_selector):
                title = clean_text(item.select_one(title_selector)).replace("\u3000", " ")
                if title == label:
                    return clean_text(item.select_one(data_selector))

            return ""

        self.logger.info("Archive collection started: %s", base_url)

        page = 1
        total_archive_urls = 0

        while True:
            page_url = f"{base_url}/archive?search_mode=detail&page={page}"
            self.logger.info("Archive page fetch started: page=%s url=%s", page, page_url)

            try:
                soup = self.get_soup(page_url)
            except Exception as exc:
                self.logger.warning("Archive page fetch failed: page=%s url=%s (%s)", page, page_url, exc)
                break

            blocks = soup.select(".archive_result2__inner")
            if not blocks:
                self.logger.info("Archive collection finished: no listing blocks on page=%s", page)
                break

            new_count = 0

            for block in blocks:
                link = block.select_one("h2.archive_result2__name a[href]")
                if not link:
                    continue

                href = (link.get("href") or "").strip()
                if not href:
                    continue

                detail_url = urljoin(base_url, href)
                total_archive_urls += 1

                if detail_url in seen:
                    continue

                seen.add(detail_url)
                detail_urls.append(detail_url)
                new_count += 1

            self.logger.info(
                "Archive page fetch completed: page=%s blocks=%s new_details=%s total_details=%s",
                page,
                len(blocks),
                new_count,
                len(detail_urls),
            )

            if new_count == 0 or len(blocks) < 20:
                self.logger.info("Archive pagination finished at page=%s", page)
                break

            page += 1

        if not detail_urls:
            raise RuntimeError(f"No detail URLs collected from archive: {base_url}")

        self.total_items = len(detail_urls)
        self.logger.info(
            "Archive collection completed: scanned_links=%s detail_urls=%s",
            total_archive_urls,
            self.total_items,
        )

        for index, detail_url in enumerate(detail_urls, start=1):
            self.logger.info("Detail scrape started [%s/%s]: %s", index, self.total_items, detail_url)

            try:
                detail_soup = self.get_soup(detail_url)
            except Exception as exc:
                self.logger.warning("Detail fetch failed [%s/%s]: %s (%s)", index, self.total_items, detail_url, exc)
                continue

            name = clean_text(detail_soup.select_one("h1.single_article__name"))
            interview_inner = detail_soup.select_one(".single_article_interview__inner")
            workplace_inner = detail_soup.select_one(".single_article_workplace__inner")

            job_type = find_value_by_label(
                interview_inner,
                ".single_article_interview__item",
                ".single_article_interview__itemTitle",
                ".single_article_interview__itemData",
                "職種",
            )
            tel = find_value_by_label(
                interview_inner,
                ".single_article_interview__item",
                ".single_article_interview__itemTitle",
                ".single_article_interview__itemData",
                "採用連絡先",
            )
            addr = find_value_by_label(
                workplace_inner,
                ".single_article_workplace__item",
                ".single_article_workplace__itemTitle",
                ".single_article_workplace__itemData",
                "勤務先住所",
            )
            nearest_station = find_value_by_label(
                workplace_inner,
                ".single_article_workplace__item",
                ".single_article_workplace__itemTitle",
                ".single_article_workplace__itemData",
                "最寄駅",
            )

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
    import logging

    logging.basicConfig(level=logging.INFO)
    crawler = GirlsBaitoDetailCrawler()
    crawler.execute("https://girlsbaito.jp/archive?search_mode=detail&page=1")
