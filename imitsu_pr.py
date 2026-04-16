"""
対象サイト：https://imitsu.jp/ct-pr/search/
ページの送り方：例）https://imitsu.jp/ct-pr/search/?pn=10#title
動的なサイト（Playwright）
"""

import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urlencode, urljoin, urlsplit, urlunsplit

# プロジェクトルートを sys.path に追加（VSCode 実行ボタン対応）
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup
from src.const.schema import Schema
from src.framework.dynamic import DynamicCrawler


class ImitsuPrScraper(DynamicCrawler):
	"""アイミツPR一覧ページクローラー"""

	DELAY = 3.0
	MAX_PAGES = 0
	EXTRA_COLUMNS = [
		"個人事業主対応",
		"下請け対応",
		"創業年",
		"最低受注金額",
		"実績数",
		"公開実績",
		"得意とする案件規模",
		"予算",
		"設立年",
		"売上高",
		"会社概要",
	]

	def prepare(self):
		self.site_name = "Imitsu_pr"

	def parse(self, url: str) -> Generator[dict, None, None]:
		seen_urls: set[str] = set()
		page = 1

		while True:
			if self.MAX_PAGES > 0 and page > self.MAX_PAGES:
				break

			max_pages_str = str(self.MAX_PAGES) if self.MAX_PAGES > 0 else "?"
			print(f"[{page}/{max_pages_str}ページ] 一覧取得中...", flush=True)

			page_url = self._build_page_url(url, page)
			soup = self._get_list_soup(page_url)
			if soup is None:
				print(f"[{page}/{max_pages_str}ページ] 一覧ページ取得失敗のため終了: {page_url}", flush=True)
				self.logger.warning("一覧ページ取得に失敗したため終了: %s", page_url)
				break
			titles = soup.select("h3.service-title")

			if not titles:
				print(f"[{page}/{max_pages_str}ページ] 一覧取得0件のため終了: {page_url}", flush=True)
				self.logger.info("ページに対象要素がないため終了: %s", page_url)
				break

			found_on_page = 0
			total_on_page = len(titles)
			for title in titles:
				link = title.select_one("a[href]")
				if not link:
					continue

				href = (link.get("href") or "").strip()
				if not href:
					continue

				detail_url = urljoin(page_url, href)
				if detail_url in seen_urls:
					continue

				name = link.get_text(" ", strip=True)
				addr = self._extract_address(title)

				seen_urls.add(detail_url)
				found_on_page += 1
				print(f"  [{found_on_page}/{total_on_page}件] 詳細取得中: {name}", flush=True)

				item = {
					Schema.URL: detail_url,
					Schema.NAME: name,
					Schema.ADDR: addr,
					Schema.REP_NM: "",
					Schema.EMP_NUM: "",
					Schema.HP: "",
					"個人事業主対応": "",
					"下請け対応": "",
					"創業年": "",
					"最低受注金額": "",
					"実績数": "",
					"公開実績": "",
					"得意とする案件規模": "",
					"予算": "",
					"設立年": "",
					"売上高": "",
					"会社概要": "",
				}

				detail_soup = self._get_detail_soup(detail_url)
				if detail_soup is not None:
					self._merge_service_info(item, detail_soup)
					self._merge_company_info(item, detail_soup)

				yield {
					Schema.URL: item[Schema.URL],
					Schema.NAME: item[Schema.NAME],
					Schema.ADDR: item[Schema.ADDR],
					Schema.REP_NM: item[Schema.REP_NM],
					Schema.EMP_NUM: item[Schema.EMP_NUM],
					Schema.HP: item[Schema.HP],
					"個人事業主対応": item["個人事業主対応"],
					"下請け対応": item["下請け対応"],
					"創業年": item["創業年"],
					"最低受注金額": item["最低受注金額"],
					"実績数": item["実績数"],
					"公開実績": item["公開実績"],
					"得意とする案件規模": item["得意とする案件規模"],
					"予算": item["予算"],
					"設立年": item["設立年"],
					"売上高": item["売上高"],
					"会社概要": item["会社概要"],
				}

			print(f"[{page}/{max_pages_str}ページ] 完了: {found_on_page}件取得 (累計 {len(seen_urls)}件)", flush=True)
			self.logger.info("一覧解析: page=%d, 取得件数=%d", page, found_on_page)
			if found_on_page == 0:
				print(f"[{page}/{max_pages_str}ページ] 新規URL0件のため終了 (累計 {len(seen_urls)}件)", flush=True)
				break

			page += 1

	def _build_page_url(self, base_url: str, page: int) -> str:
		split = urlsplit(base_url)
		query = {"pn": str(page)}
		new_query = urlencode(query)
		fragment = split.fragment or "title"
		return urlunsplit((split.scheme, split.netloc, split.path, new_query, fragment))

	def _extract_address(self, title) -> str:
		for parent in title.parents:
			addr_el = parent.select_one("div.service-address")
			if addr_el:
				return addr_el.get_text(" ", strip=True)
		return ""

	def _get_list_soup(self, url: str) -> BeautifulSoup | None:
		try:
			self.page.goto(url, wait_until="domcontentloaded", timeout=30000)
			try:
				self.page.wait_for_selector("h3.service-title", timeout=15000)
			except Exception:
				pass
			time.sleep(2.0)
			self._remove_overlays()
			return BeautifulSoup(self.page.content(), "html.parser")
		except Exception as e:
			self.logger.warning("一覧ページ取得エラー: %s (%s)", url, e)
			return None

	def _get_detail_soup(self, url: str) -> BeautifulSoup | None:
		try:
			self.page.goto(url, wait_until="domcontentloaded")
			time.sleep(1.5)
			self._remove_overlays()
			return BeautifulSoup(self.page.content(), "html.parser")
		except Exception as e:
			self.logger.warning("詳細ページ取得エラー: %s (%s)", url, e)
			return None

	def _remove_overlays(self):
		try:
			self.page.evaluate("""
				document.querySelectorAll(
					'[class*="modal"], [class*="overlay"], [class*="popup"], [class*="dialog"]'
				).forEach(el => el.remove());
			""")
		except Exception:
			pass

	def _extract_dtdd_map(self, section) -> dict[str, str]:
		rows: dict[str, str] = {}
		if not section:
			return rows
		for dt in section.select("dt"):
			key = dt.get_text(" ", strip=True)
			if not key:
				continue
			dd = dt.find_next_sibling("dd")
			if not dd:
				continue
			rows[key] = dd.get_text(" ", strip=True)
		return rows

	def _set_if_empty(self, item: dict, key: str, value: str):
		if value and not (item.get(key) or "").strip():
			item[key] = value

	def _merge_service_info(self, item: dict, soup):
		section = soup.select_one("section.service-information-section")
		rows = self._extract_dtdd_map(section)

		mapping = {
			"個人事業主対応": "個人事業主対応",
			"下請け対応": "下請け対応",
			"創業年": "創業年",
			"社員数": Schema.EMP_NUM,
			"最低受注金額": "最低受注金額",
			"実績数": "実績数",
			"公開実績": "公開実績",
			"得意とする案件規模": "得意とする案件規模",
			"予算": "予算",
		}

		for label, target_key in mapping.items():
			self._set_if_empty(item, target_key, rows.get(label, ""))

	def _merge_company_info(self, item: dict, soup):
		section = soup.select_one("section.supplier-information-section")
		rows = self._extract_dtdd_map(section)

		mapping = {
			"会社名": Schema.NAME,
			"設立年": "設立年",
			"代表者名": Schema.REP_NM,
			"従業員数": Schema.EMP_NUM,
			"売上高": "売上高",
			"住所": Schema.ADDR,
			"会社URL": Schema.HP,
			"会社概要": "会社概要",
		}

		for label, target_key in mapping.items():
			self._set_if_empty(item, target_key, rows.get(label, ""))


if __name__ == "__main__":
	scraper = ImitsuPrScraper()
	scraper.execute("https://imitsu.jp/ct-pr/search/")
	print(f"取得件数: {scraper.item_count}")
	print(f"出力ファイル: {scraper.output_filepath}")
