"""
対象サイト: https://paypaygourmet.yahoo.co.jp/sitemap_index.xml
"""
import re
import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class PaypayGourmetScraper(StaticCrawler):
    """PayPayグルメ スクレイパー"""

    DELAY = 1.0
    SITEMAP_INDEX_URL = "https://paypaygourmet.yahoo.co.jp/sitemap_index.xml"
    EXTRA_COLUMNS = [
        "エリア",
        "FAX",
        "メール",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        index_url = self.SITEMAP_INDEX_URL
        if "sitemap" in url:
            index_url = url

        index_soup = self.get_soup(index_url)
        child_sitemap_urls = [
            loc.get_text(strip=True)
            for loc in index_soup.select("sitemap > loc")
            if loc.get_text(strip=True)
        ]
        if not child_sitemap_urls:
            child_sitemap_urls = [
                loc.get_text(strip=True)
                for loc in index_soup.select("loc")
                if loc.get_text(strip=True)
            ]

        store_url_pattern = re.compile(r"^https://paypaygourmet\.yahoo\.co\.jp/\d+$")
        store_url_set = set()
        for sitemap_url in child_sitemap_urls:
            child_soup = self.get_soup(sitemap_url)
            locs = [
                loc.get_text(strip=True)
                for loc in child_soup.select("url > loc")
                if loc.get_text(strip=True)
            ]
            if not locs:
                locs = [
                    loc.get_text(strip=True)
                    for loc in child_soup.select("loc")
                    if loc.get_text(strip=True)
                ]
            for loc_url in locs:
                if store_url_pattern.match(loc_url):
                    store_url_set.add(loc_url)

        store_urls = sorted(store_url_set)

        self.total_items = len(store_urls)

        for store_url in store_urls:
            try:
                store_soup = self.get_soup(store_url)
            except Exception as exc:
                continue

            name = ""
            telephone = ""
            address = ""
            area = ""
            prefecture = ""
            postal_code = ""
            category = ""

            tel_tag = store_soup.select_one('[itemprop="telephone"]')
            if tel_tag:
                telephone = tel_tag.get_text(strip=True)

            address_tag = store_soup.select_one('[itemprop="address"]')
            if address_tag:
                address = address_tag.get_text(" ", strip=True)

            area_tag = store_soup.select_one('[itemprop="addressLocality"]')
            if area_tag:
                area = area_tag.get("content", "").strip() or area_tag.get_text(strip=True)

            pref_tag = store_soup.select_one('[itemprop="addressRegion"]')
            if pref_tag:
                prefecture = pref_tag.get("content", "").strip() or pref_tag.get_text(strip=True)

            postal_tag = store_soup.select_one('[itemprop="postalCode"]')
            if postal_tag:
                postal_code = postal_tag.get("content", "").strip() or postal_tag.get_text(strip=True)
            if not postal_code and address:
                postal_match = re.search(r"\d{3}-\d{4}", address)
                if postal_match:
                    postal_code = postal_match.group(0)

            category_tag = store_soup.select_one('[itemprop="servesCuisine"]')
            if category_tag:
                category = category_tag.get("content", "").strip() or category_tag.get_text(strip=True)

            h1_tag = store_soup.select_one("h1")
            if h1_tag:
                name = h1_tag.get_text(" ", strip=True)
            if not name:
                title_tag = store_soup.select_one("title")
                if title_tag:
                    name = title_tag.get_text(" ", strip=True).split(" - ")[0].strip()

            page_text = store_soup.get_text(" ", strip=True)
            fax_match = re.search(r"FAX[:：]?\s*(\d{2,4}-\d{2,4}-\d{3,4})", page_text)
            fax = fax_match.group(1) if fax_match else ""

            emails = set()
            homepage = ""
            instagram = ""
            facebook = ""
            x_url = ""
            line_official = ""
            for a_tag in store_soup.select("a[href]"):
                href = a_tag.get("href", "").strip()
                if not href:
                    continue
                if href.startswith("mailto:"):
                    emails.add(href.replace("mailto:", "", 1).strip())
                elif "instagram.com" in href and not instagram:
                    instagram = href
                elif "facebook.com" in href and not facebook:
                    facebook = href
                elif ("twitter.com" in href or "x.com" in href) and not x_url:
                    x_url = href
                elif "line.me" in href and not line_official:
                    line_official = href
                elif (
                    "paypaygourmet.yahoo.co.jp" not in href
                    and href.startswith("http")
                    and not homepage
                ):
                    homepage = href

            yield {
                Schema.NAME: name,
                Schema.ADDR: address,
                Schema.TEL: telephone,
                Schema.URL: store_url,
                "エリア": area,
                Schema.PREF: prefecture,
                Schema.POST_CODE: postal_code,
                Schema.CAT_SITE: category,
                "FAX": fax,
                "メール": ", ".join(sorted(emails)),
                Schema.HP: homepage,
                Schema.INSTA: instagram,
                Schema.FB: facebook,
                Schema.X: x_url,
                Schema.LINE: line_official,
            }


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    PaypayGourmetScraper().execute("https://paypaygourmet.yahoo.co.jp/")
