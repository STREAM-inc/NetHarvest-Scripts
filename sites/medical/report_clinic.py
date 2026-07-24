"""
スクレイピング対象サイト: 美容医療の口コミ広場 (https://report.clinic/)

一覧ページ (都道府県指定の /pref_... パス) を ?pageID=N でページ送りし、
各クリニックの詳細ページ (/detail/L_xxxxxxx) を 1 件ずつ取得して yield する
(Pattern B: 詳細取得のたびに即 yield)。

利用規約 (https://report.clinic/info/kiyaku.html) にはスクレイピング/クローリング/
自動アクセスを明示的に禁止する条項は無い。ただし第12条 (情報の私的利用以外の禁止) /
第13条 (無断転載及び再配布の禁止) によりコンテンツの再配布が制限されるため、
著作権リスクのある「文章 (自由記述プロース)」カラム (交通手段・予約案内・駐車場案内・
備考・メディカルローン説明 等) は取得対象から除外し、構造化された事実情報のみを取得する。
"""

import re
from typing import Generator
from urllib.parse import urljoin, urlparse

from src.const.schema import Schema
from src.framework.static import StaticCrawler


class ReportClinicScraper(StaticCrawler):
	"""美容医療の口コミ広場 クローラー (一覧→詳細)"""

	DELAY = 1.0
	MAX_PAGES = 250  # ページ送りの安全上限 (無限ループ防止)

	# 特徴タグ (キーワード列)。長文プロースではないため EXTRA として取得可。
	EXTRA_COLUMNS = ["グループ", "特徴"]

	# /detail/L_1234567 形式のクリニック詳細 URL のみを対象とする
	# (report_list / pickup_menu / access 等のサブページは除外)
	_DETAIL_RE = re.compile(r"^/detail/(L_\d+)/?$")
	_SCORE_RE = re.compile(r"([0-5]\.\d{1,2})")
	_COUNT_RE = re.compile(r"([0-9,]+)\s*件")

	_DAYS = ["月", "火", "水", "木", "金", "土", "日"]
	_DAY_COL = {
		"月": Schema.TIME_MON, "火": Schema.TIME_TUE, "水": Schema.TIME_WED,
		"木": Schema.TIME_THU, "金": Schema.TIME_FRI, "土": Schema.TIME_SAT,
		"日": Schema.TIME_SUN,
	}

	# ---------------------------------------------------------------- main
	def parse(self, url: str) -> Generator[dict, None, None]:
		seen: set[str] = set()

		for page in range(1, self.MAX_PAGES + 1):
			list_url = url if page == 1 else f"{url}?pageID={page}"
			soup = self.get_soup(list_url)
			if soup is None:
				break

			detail_urls = self._extract_detail_urls(soup, url)
			# このページに新規クリニックが 1 件も無ければ終端とみなす
			new_urls = [u for u in detail_urls if u not in seen]
			if not new_urls:
				self.logger.info("ページ %d に新規クリニックなし。巡回を終了します。", page)
				break

			self.logger.info("[ページ %d] クリニック %d 件", page, len(new_urls))

			for detail_url in new_urls:
				seen.add(detail_url)
				dsoup = self.get_soup(detail_url)
				if dsoup is None:
					continue
				item = self._parse_detail(dsoup, detail_url)
				if item:
					yield item  # 詳細を取得するたびに即 yield

	# ------------------------------------------------------------ helpers
	def _extract_detail_urls(self, soup, base_url: str) -> list[str]:
		"""一覧ページからクリニック詳細 URL を出現順・重複なしで抽出する。"""
		out: list[str] = []
		local_seen: set[str] = set()
		for a in soup.select('a[href*="/detail/"]'):
			href = (a.get("href") or "").strip()
			abs_url = urljoin(base_url, href).split("?", 1)[0].split("#", 1)[0]
			path = urlparse(abs_url).path
			if not self._DETAIL_RE.match(path):
				continue
			if abs_url in local_seen:
				continue
			local_seen.add(abs_url)
			out.append(abs_url)
		return out

	def _label_value_el(self, soup, label: str):
		"""p.headline_h4 のラベル文字列に一致する最初の値要素 (次の兄弟) を返す。"""
		for p in soup.select("p.headline_h4"):
			if p.get_text(strip=True) == label:
				return p.find_next_sibling()
		return None

	def _label_text(self, soup, label: str) -> str:
		el = self._label_value_el(soup, label)
		return el.get_text(" ", strip=True) if el else ""

	def _section_links(self, soup, label: str) -> list[str]:
		"""指定ラベル (複数存在しうる) の値要素に含まれるリンク href を集める。"""
		out: list[str] = []
		for p in soup.select("p.headline_h4"):
			if p.get_text(strip=True) == label:
				sib = p.find_next_sibling()
				if sib:
					for a in sib.select("a[href]"):
						href = (a.get("href") or "").strip()
						if href:
							out.append(href)
		return out

	def _parse_detail(self, soup, url: str) -> dict | None:
		h1 = soup.select_one("h1")
		name = h1.get_text(strip=True) if h1 else ""
		if not name:
			return None

		# --- 総合満足度 (口コミ採点) と 口コミ件数 ---
		scores = ""
		node = soup.find(
			lambda t: t.name in ("div", "span", "p", "section")
			and "総合満足度" in t.get_text()
			and len(t.get_text(strip=True)) < 40
		)
		if node:
			m = self._SCORE_RE.search(node.get_text(" ", strip=True))
			scores = m.group(1) if m else ""
		if not scores:
			el = soup.select_one(".rate_star_score")
			if el:
				m = self._SCORE_RE.search(el.get_text(strip=True))
				scores = m.group(1) if m else ""

		rev_cnt = ""
		a_rev = soup.select_one('a[href*="report_list"]')
		if a_rev:
			m = self._COUNT_RE.search(a_rev.get_text(" ", strip=True))
			rev_cnt = m.group(1).replace(",", "") if m else ""

		# --- 住所 / 都道府県 ---
		pref, addr = "", ""
		addr_el = self._label_value_el(soup, "住所")
		if addr_el:
			pref_a = addr_el.select_one('a[href*="/pref_"]')
			full = addr_el.get_text(" ", strip=True)
			full = re.sub(r"\s+", " ", full).strip()
			if pref_a:
				pref = pref_a.get_text(strip=True)
				addr = full[len(pref):].strip() if full.startswith(pref) else full
			else:
				parts = full.split(" ", 1)
				pref = parts[0] if parts else ""
				addr = parts[1] if len(parts) > 1 else ""

		# --- 電話番号 ---
		tel = ""
		tel_el = self._label_value_el(soup, "電話番号")
		if tel_el:
			a_tel = tel_el.select_one('a[href^="tel:"]')
			if a_tel:
				tel = a_tel.get_text(strip=True)
			else:
				m = re.search(r"0\d{1,4}-\d{1,4}-\d{3,4}", tel_el.get_text(" ", strip=True))
				tel = m.group(0) if m else ""

		# --- 営業時間 (曜日別) ---
		time_all, day_times = self._parse_hours(soup)

		# --- 休業日 / 支払い方法 (クレジットカード) ---
		holiday = self._label_text(soup, "休業日")
		payments = self._label_text(soup, "クレジットカード")

		# --- HP / SNS リンク ---
		hp = ""
		for href in self._section_links(soup, "HPのURL"):
			if href.startswith("http"):
				hp = href
				break

		sns = {"insta": "", "x": "", "fb": "", "tiktok": "", "line": ""}
		for href in self._section_links(soup, "ブログ・SNS") + self._section_links(soup, "その他"):
			low = href.lower()
			if "instagram.com" in low and not sns["insta"]:
				sns["insta"] = href
			elif ("twitter.com" in low or "://x.com" in low or low.startswith("https://x.com")) and not sns["x"]:
				sns["x"] = href
			elif "facebook.com" in low and not sns["fb"]:
				sns["fb"] = href
			elif "tiktok.com" in low and not sns["tiktok"]:
				sns["tiktok"] = href
			elif ("line.me" in low or "lin.ee" in low) and not sns["line"]:
				sns["line"] = href

		# --- EXTRA: グループ / 特徴タグ ---
		group = self._label_text(soup, "グループ")
		feat_el = self._label_value_el(soup, "このクリニックの特徴")
		features = ""
		if feat_el:
			tags = [t.get_text(strip=True) for t in feat_el.select("a, span, li")]
			tags = [t for t in tags if t]
			if not tags:
				tags = feat_el.get_text(" ", strip=True).split()
			features = " / ".join(dict.fromkeys(tags))

		item = {
			Schema.URL: url,
			Schema.NAME: name,
			Schema.PREF: pref,
			Schema.ADDR: addr,
			Schema.TEL: tel,
			Schema.PHONE: tel,
			Schema.SCORES: scores,
			Schema.REV_SCR: rev_cnt,
			Schema.HP: hp,
			Schema.INSTA: sns["insta"],
			Schema.X: sns["x"],
			Schema.FB: sns["fb"],
			Schema.TIKTOK: sns["tiktok"],
			Schema.LINE: sns["line"],
			Schema.HOLIDAY: holiday,
			Schema.PAYMENTS: payments,
			Schema.TIME: time_all,
			Schema.CAT_SITE: "美容医療",
			"グループ": group,
			"特徴": features,
		}
		item.update(day_times)
		return item

	def _parse_hours(self, soup) -> tuple[str, dict]:
		"""営業時間テーブル (div.table_business_day) を曜日別に解析する。"""
		day_times: dict[str, str] = {}
		el = self._label_value_el(soup, "営業時間")
		table = None
		if el:
			table = el if getattr(el, "name", "") == "table" else el.find("table")
		if table is None:
			return "", day_times

		rows = [
			[c.get_text(" ", strip=True) for c in tr.select("th, td")]
			for tr in table.select("tr")
		]
		rows = [r for r in rows if r]
		if len(rows) < 2:
			return "", day_times

		header, values = rows[0], rows[1]
		pairs = []
		for i, day in enumerate(header):
			day = day.strip()
			if day not in self._DAY_COL:
				continue
			val = values[i].strip() if i < len(values) else ""
			val = re.sub(r"\s*～\s*", "～", val)
			day_times[self._DAY_COL[day]] = val
			pairs.append(f"{day} {val}")
		return " / ".join(pairs), day_times


if __name__ == "__main__":
	scraper = ReportClinicScraper()
	scraper.execute(
		"https://report.clinic/pref_1_2_3_4_5_6_7_8_9_10_11_12_13_14_15_16_17_18_19_20_21_22_23_24_25_26_27_28_29_30_31_32_33_34_35_36_37_38_39_40_41_42_43_44_45_46_47"
	)
