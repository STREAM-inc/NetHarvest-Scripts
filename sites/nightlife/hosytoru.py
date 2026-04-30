# scripts/sites/portal/hostle.py

from datetime import datetime
from urllib.parse import urljoin

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class HostleScraper(StaticCrawler):
    """ホストル 求人一覧クローラー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["取得日時"]

    BASE_URL = "https://hostle.jp"

    def parse(self, url: str):
        page = 1
        total_set = False

        while True:
            page_url = self._build_page_url(url, page)
            soup = self.get_soup(page_url)

            # 一覧カード
            items = soup.select("section.shop-item.shop-item-grid")
            if not items:
                self.logger.info(f"ページ{page}: 店舗データが見つからなかったため終了します")
                break

            # 総件数が取れる場合は進捗表示に使う
            if not total_set:
                total = self._extract_total_count(soup)
                if total:
                    self.total_items = total
                    total_set = True

            self.logger.info(f"ページ{page}: {len(items)}件の店舗を検出")

            for item in items:
                try:
                    detail_link = item.select_one('a.btn.btn-orange[href*="/detail/"]')
                    if not detail_link:
                        self.logger.warning("詳細リンクが見つからないためスキップ")
                        continue

                    detail_url = urljoin(self.BASE_URL, detail_link.get("href", "").strip())
                    detail_soup = self.get_soup(detail_url)

                    record = self._parse_detail(detail_soup, detail_url)
                    if record:
                        yield record

                except Exception as e:
                    self.logger.warning(f"店舗の解析をスキップしました: {e}")
                    continue

            page += 1

    def _build_page_url(self, base_url: str, page: int) -> str:
        if page == 1:
            return base_url

        separator = "&" if "?" in base_url else "?"
        return f"{base_url}{separator}page={page}"

    def _extract_total_count(self, soup):
        strong = soup.select_one("p.srch_info strong")
        if not strong:
            return None

        text = strong.get_text(strip=True).replace(",", "")
        return int(text) if text.isdigit() else None

    def _parse_detail(self, soup, detail_url: str):
        data_map = self._extract_recruit_table(soup)

        # 名称
        name = ""
        name_el = soup.select_one(".shop_info h3.name")
        if name_el:
            # <span class="kana"> を除いて店舗名だけ取得
            kana = name_el.select_one("span.kana")
            if kana:
                kana.extract()
            name = name_el.get_text(strip=True)

        if not name:
            name = data_map.get("店舗名", "")

        # TEL
        tel = ""
        tel_dl = soup.select("div.shop_contact dl.tel_list dt")
        for dt in tel_dl:
            label = dt.get_text(" ", strip=True)
            if "店舗電話番号" in label:
                dd = dt.find_next_sibling("dd")
                if dd:
                    a = dd.select_one('a[href^="tel:"]')
                    tel = a.get_text(strip=True) if a else dd.get_text(" ", strip=True)
                break

        # 住所
        addr = data_map.get("アクセス", "")
        if addr:
            addr = self._normalize_multiline(addr)

        # 営業時間
        open_time = self._normalize_multiline(data_map.get("営業時間", ""))

        # 定休日
        holiday = self._normalize_multiline(data_map.get("定休日", ""))

        return {
            "取得日時": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.TEL: tel,
            Schema.ADDR: addr,
            Schema.TIME: open_time,
            Schema.HOLIDAY: holiday,
        }

    def _extract_recruit_table(self, soup):
        """
        募集要項テーブルから
        th -> td の対応を辞書化する
        """
        result = {}
        table = soup.select_one("div.shop_recruit table")
        if not table:
            return result

        for tr in table.select("tr"):
            th = tr.select_one("th")
            td = tr.select_one("td")
            if not th or not td:
                continue

            key = th.get_text(" ", strip=True)
            value = td.get_text("\n", strip=True)
            result[key] = value

        return result

    def _normalize_multiline(self, text: str) -> str:
        if not text:
            return ""
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        return " ".join(lines)


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    HostleScraper().execute("https://hostle.jp/list/?addr=")