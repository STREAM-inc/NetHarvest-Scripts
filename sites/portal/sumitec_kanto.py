"""
対象サイト: https://sumitec-kanto.com/contractor
"""

import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

# VS Code の「Run Python File」(右上の実行) でも src パッケージを解決できるようにする
root_path = Path(__file__).resolve().parent.parent.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from src.const.schema import Schema
from src.framework.static import StaticCrawler


class SumitecKantoScraper(StaticCrawler):
	"""sumitec-kanto 一覧ページから店舗詳細URLを取得するクローラー"""

	DELAY = 5.0
	MAX_LIST_PAGES = 500

	def parse(self, url: str) -> Generator[dict, None, None]:
		base_url = url.rstrip("/")
		seen_urls: set[str] = set()
		detail_urls: list[str] = []

		max_list_pages = int(os.getenv("NH_MAX_LIST_PAGES", "0")) or self.MAX_LIST_PAGES
		self.logger.info("一覧ページのクロールを開始します (上限: %d ページ)", max_list_pages)

		for page in range(1, max_list_pages + 1):
			page_url = base_url if page == 1 else f"{base_url}/page/{page}"
			self.logger.info("[%d] 一覧ページ取得: %s", page, page_url)
			time.sleep(self.DELAY)
			soup = self.get_soup(page_url)
			before_count = len(detail_urls)

			if soup is None:
				self.logger.info("ページが存在しないため一覧クロールを終了します: page=%d", page)
				break

			for link in soup.select("div.m_sec_w a[href]"):
				href = (link.get("href") or "").strip()
				if not href:
					continue

				detail_url = urljoin(page_url, href)
				if not self._is_detail_url(detail_url):
					continue

				if detail_url in seen_urls:
					continue

				seen_urls.add(detail_url)
				detail_urls.append(detail_url)

			added_count = len(detail_urls) - before_count
			self.logger.info(
				"[%d] このページで %d 件追加 (累計 %d 件)",
				page,
				added_count,
				len(detail_urls),
			)
		else:
			self.logger.warning("最大ページ数 %d に到達したため一覧クロールを終了します", self.MAX_LIST_PAGES)

		self.total_items = len(detail_urls)
		self.logger.info("詳細URLの抽出が完了しました: 合計 %d 件", self.total_items)

		max_details = int(os.getenv("NH_MAX_DETAILS", "0"))
		target_urls = detail_urls[:max_details] if max_details > 0 else detail_urls
		self.total_items = len(target_urls)
		self.logger.info("詳細ページの取得を開始します (%d 件)", self.total_items)

		for idx, detail_url in enumerate(target_urls, start=1):
			self.logger.info("[%d/%d] 詳細ページ取得: %s", idx, self.total_items, detail_url)
			time.sleep(self.DELAY)
			detail_soup = self.get_soup(detail_url)
			if detail_soup is None:
				continue

			name_text = ""
			name_el = detail_soup.select_one("header.m_sec_h h2.h.mainTxt")
			if name_el:
				name_text = name_el.get_text(" ", strip=True)
			name_main, name_kana = self._split_name_and_kana(name_text)

			row_map: dict[str, str] = {}
			for row in detail_soup.select("table.p_table.c_tableBlue tr"):
				th = row.select_one("th")
				td = row.select_one("td")
				if not th or not td:
					continue

				label = th.get_text(" ", strip=True)
				value = td.get_text(" ", strip=True)
				if label == "ホームページ":
					link = td.select_one("a[href]")
					if link and (link.get("href") or "").strip():
						value = (link.get("href") or "").strip()

				row_map[label] = value

			tags = detail_soup.select("div.tags ul li a")
			lob = ",".join(a.get_text(strip=True) for a in tags)

			yield {
				Schema.URL: detail_url,
				Schema.NAME: name_main,
				Schema.NAME_KANA: name_kana,
				Schema.TIME: row_map.get("営業時間", ""),
				Schema.HOLIDAY: row_map.get("定休日", ""),
				Schema.ADDR: row_map.get("住所", ""),
				Schema.TEL: row_map.get("電話番号", ""),
				Schema.HP: row_map.get("ホームページ", ""),
				Schema.LOB: lob,
			}

	def _split_name_and_kana(self, raw_name: str) -> tuple[str, str]:
		text = re.sub(r"\s+", " ", (raw_name or "").replace("\u3000", " ")).strip()
		if not text:
			return "", ""

		paren_match = re.search(r"[\(（]([ァ-ヶー・･\s]+)[\)）]$", text)
		if paren_match:
			kana = re.sub(r"\s+", "", paren_match.group(1)).strip()
			name = re.sub(r"[\(（][ァ-ヶー・･\s]+[\)）]$", "", text).strip()
			return name or text, kana

		tokens = text.split(" ")
		kana_tokens: list[str] = []
		while tokens and re.fullmatch(r"[ァ-ヶー・･]+", tokens[-1]):
			kana_tokens.insert(0, tokens.pop())

		if kana_tokens:
			return " ".join(tokens).strip(), "".join(kana_tokens).strip()

		return text, ""

	def _is_detail_url(self, target_url: str) -> bool:
		path = target_url.split("?", 1)[0]
		if "/contractor/page/" in path:
			return False

		return re.search(r"/contractor/.+/\d+/?$", path) is not None


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    scraper = SumitecKantoScraper()
    scraper.site_name = "sumitec_kanto"
    scraper.site_id = ""
    scraper.execute("https://sumitec-kanto.com/contractor")

    print(f"CSV保存先: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")


if __name__ == "__main__":
    main()