"""
スクレピングの対象サイト：https://www.beauty-park.jp/
"""

import re
import time
import xml.etree.ElementTree as ET
from typing import Generator
from urllib.parse import urljoin

from src.const.schema import Schema
from src.framework.static import StaticCrawler


class BeautyparkScraper(StaticCrawler):
	"""Beauty Park の店舗URLを取得するクローラー"""

	DELAY = 3.0
	MAX_ITEMS = 15
	MAX_AREA_PAGES = 20

	_STORE_URL_RE = re.compile(r"^https://www\.beauty-park\.jp/shop/\d+/?$")
	_PHONE_RE = re.compile(r"\d{2,4}-\d{2,4}-\d{3,4}")

	def parse(self, url: str) -> Generator[dict, None, None]:
		top_url = url.rstrip("/") + "/"
		area_sitemap_url = urljoin(top_url, "sitemap/area.xml.gz")

		area_urls = self._extract_loc_urls(area_sitemap_url)
		if self.MAX_AREA_PAGES > 0:
			area_urls = area_urls[: self.MAX_AREA_PAGES]

		self.logger.info("エリアページを %d 件クロールします", len(area_urls))

		seen: set[str] = set()
		shop_urls: list[str] = []
		for idx, area_url in enumerate(area_urls, start=1):
			if self.MAX_ITEMS > 0 and len(seen) >= self.MAX_ITEMS:
				break

			time.sleep(self.DELAY)
			soup = self.get_soup(area_url)
			self.logger.info("[%d/%d] エリアページ解析: %s", idx, len(area_urls), area_url)

			for link in soup.select("a[href]"):
				href = (link.get("href") or "").strip()
				if not href:
					continue

				shop_url = urljoin(top_url, href)
				shop_url = shop_url.split("?", 1)[0]
				if not self._STORE_URL_RE.match(shop_url):
					continue

				shop_url = shop_url.rstrip("/") + "/"
				if shop_url in seen:
					continue

				seen.add(shop_url)
				shop_urls.append(shop_url)

				if self.MAX_ITEMS > 0 and len(seen) >= self.MAX_ITEMS:
					break

		self.total_items = len(shop_urls)
		self.logger.info("店舗詳細ページの解析を開始します: %d 件", self.total_items)

		for idx, shop_url in enumerate(shop_urls, start=1):
			time.sleep(self.DELAY)
			try:
				soup = self.get_soup(shop_url)
			except Exception as exc:
				self.logger.warning("アクセスエラーのためスキップ: %s (%s)", shop_url, exc)
				continue

			self.logger.info("[%d/%d] 店舗詳細ページ解析: %s", idx, self.total_items, shop_url)
			fields = self._extract_shop_fields(soup, top_url)

			yield {
				Schema.URL: shop_url,
				Schema.NAME: fields.get("name", ""),
				Schema.TEL: fields.get("tel", ""),
				Schema.ADDR: fields.get("addr", ""),
				Schema.HOLIDAY: fields.get("holiday", ""),
				Schema.TIME: fields.get("time", ""),
				Schema.PAY: fields.get("pay", ""),
				Schema.HP: fields.get("hp", ""),
			}

	def _extract_shop_fields(self, soup, top_url: str) -> dict[str, str]:
		fields = {
			"name": "",
			"tel": "",
			"addr": "",
			"holiday": "",
			"time": "",
			"pay": "",
			"hp": "",
		}

		table = soup.select_one("#Company-table-main")
		if not table:
			return fields

		for row in table.select("tr"):
			th = row.select_one("th")
			td = row.select_one("td")
			if not th or not td:
				continue

			label = th.get_text(" ", strip=True).replace(" ", "")
			text = td.get_text(" ", strip=True)

			if label == "店舗名":
				fields["name"] = text
			elif label == "電話番号":
				phone_match = self._PHONE_RE.search(text)
				fields["tel"] = phone_match.group(0) if phone_match else ""
			elif label == "住所":
				fields["addr"] = text
			elif label == "定休日":
				fields["holiday"] = text
			elif label == "営業時間":
				fields["time"] = text
			elif label in ("支払い方法", "支払方法"):
				fields["pay"] = text
			elif label == "ホームページ":
				link = td.select_one("a[href]")
				if link and (link.get("href") or "").strip():
					fields["hp"] = urljoin(top_url, (link.get("href") or "").strip())
				else:
					fields["hp"] = text

		return fields

	def _extract_loc_urls(self, sitemap_url: str) -> list[str]:
		response = self.session.get(sitemap_url, timeout=self.TIMEOUT)
		response.raise_for_status()

		root = ET.fromstring(response.text)
		urls: list[str] = []
		for elem in root.iter():
			if not str(elem.tag).endswith("loc"):
				continue

			loc_url = (elem.text or "").strip()
			if not loc_url:
				continue

			if "?" in loc_url:
				continue

			urls.append(loc_url)

		return urls