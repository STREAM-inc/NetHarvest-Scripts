import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


PLACEHOLDERS = {"-", "－", "―", "–", "—", "ー", "なし", "無し"}
PAYMENT_KEYS = (
    "クレジットカード", "電子マネー", "QRコード", "QR", "PayPay",
    "d払い", "楽天ペイ", "au PAY", "Apple Pay", "Google Pay",
    "交通系", "現金",
)


class RePhilippinePubScraper(StaticCrawler):
    """フィリピンパブどっと混む！！ スクレイパー"""

    DELAY = 1.5

    EXTRA_COLUMNS = ["エリア", "最寄駅"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        shop_urls = self._collect_shop_urls(url)
        self.total_items = len(shop_urls)
        self.logger.info(f"対象店舗ページ数: {self.total_items}")

        for shop_url in shop_urls:
            yield from self._scrape_detail(shop_url)

    # サイトマップから店舗ページのURL一覧を作る
    def _collect_shop_urls(self, seed_url: str) -> list[str]:
        parsed = urlparse(seed_url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        index_url = seed_url if parsed.path.endswith(".xml") else urljoin(base + "/", "sitemap_index.xml")

        urls: list[str] = []
        seen: set[str] = set()
        for sm in self._read_sitemap(index_url):
            if "/post-sitemap" not in sm:
                continue
            for page_url in self._read_sitemap(sm):
                if page_url in seen or not self._looks_like_shop_url(page_url, parsed.netloc):
                    continue
                seen.add(page_url)
                urls.append(page_url)
        return urls

    def _read_sitemap(self, sitemap_url: str) -> list[str]:
        try:
            response = self.session.get(sitemap_url, timeout=self.TIMEOUT)
            response.raise_for_status()
            root = ET.fromstring(response.content)
        except Exception as e:
            self.logger.warning(f"サイトマップ取得失敗: {sitemap_url} ({e})")
            return []
        return [el.text.strip() for el in root.iter() if el.tag.endswith("loc") and el.text]

    def _looks_like_shop_url(self, page_url: str, host: str) -> bool:
        parsed = urlparse(page_url)
        path = parsed.path.rstrip("/")
        if parsed.netloc != host or path.count("/") < 2:
            return False
        return not any(t in path for t in ("/category/", "/tag/", "/author/", "/feed/"))

    # 詳細ページからデータを抽出するメソッド
    def _scrape_detail(self, url: str) -> Generator[dict, None, None]:
        try:
            soup = self.get_soup(url)
            if soup is None:
                return

            info_table = self._table_after(soup, anchor_id="shop")
            if info_table is None:
                return  # 店舗情報テーブルがない記事ページはスキップ

            data = {
                Schema.URL: url,
                Schema.NAME: self._shop_title(soup),
            }

            # 店舗情報テーブルから Schema 定数へマッピング
            for tr in info_table.find_all("tr"):
                cells = tr.find_all(["th", "td"])
                if len(cells) < 2:
                    continue
                key = " ".join(cells[0].get_text(" ", strip=True).split())
                val = " ".join(cells[1].get_text(" ", strip=True).split())
                if val in PLACEHOLDERS:
                    val = ""

                if "業態" in key: data[Schema.CAT_SITE] = val
                elif "住所" in key:
                    post_code, pref, addr = self._split_address(val)
                    data[Schema.POST_CODE] = post_code
                    data[Schema.PREF] = pref
                    data[Schema.ADDR] = addr
                elif "電話番号" in key: data[Schema.TEL] = self._first_tel(val)
                elif "営業時間" in key: data[Schema.TIME] = val
                elif "定休日" in key: data[Schema.HOLIDAY] = val
                elif "ウェブサイト" in key: data[Schema.HP] = val
                elif "最寄駅" in key: data["最寄駅"] = val
                elif key == "SNS": data.update(self._extract_sns(cells[1]))

            # 料金表から支払方法を組み立て
            data[Schema.PAYMENTS] = self._build_payments(self._table_after(soup, anchor_id="system"))

            # パンくずから市区町村＋都道府県
            data["エリア"] = self._extract_area(soup)

            # 閉業フラグ（1=閉業, 0=営業中）
            data[Schema.STS_NM] = "1" if self._is_closed(soup) else "0"

            yield data

        except Exception as e:
            self.logger.warning(f"詳細ページのスキップ: {url} ({e})")

    # 店舗名は <meta og:title> の " | " 区切り先頭を採用（<h1>はサイトロゴ画像のため）
    def _shop_title(self, soup) -> str:
        og = soup.find("meta", attrs={"property": "og:title"})
        content = og.get("content") if og else None
        if content:
            return content.split(" | ", 1)[0].strip()
        h2 = soup.find("h2")
        return h2.get_text(strip=True) if h2 else ""

    def _table_after(self, soup, anchor_id: str):
        anchor = soup.find(id=anchor_id)
        return anchor.find_next("table") if anchor else None

    def _split_address(self, address: str) -> tuple[str, str, str]:
        if not address:
            return "", "", ""
        post_code = ""
        m = re.search(r"〒?\s*(\d{3}-\d{4})", address)
        if m:
            post_code = m.group(1)
            address = (address[:m.start()] + address[m.end():]).strip()
        pref = ""
        rest = address
        m = re.match(r"\s*(北海道|東京都|(?:京都|大阪)府|.{2,3}県)", address)
        if m:
            pref = m.group(1)
            rest = address[m.end():].lstrip()
        return post_code, pref, rest

    def _first_tel(self, value: str) -> str:
        if not value:
            return ""
        s = value.translate(str.maketrans("ーｰ－—–", "-----"))
        m = re.search(r"\d[\d-]{8,15}", s)
        return m.group(0) if m else ""

    def _extract_sns(self, cell) -> dict[str, str]:
        sns = {Schema.INSTA: "", Schema.FB: "", Schema.X: "", Schema.LINE: ""}
        for a in cell.find_all("a", href=True):
            href = a["href"].strip()
            low = href.lower()
            if "instagram.com" in low:
                sns[Schema.INSTA] = sns[Schema.INSTA] or href
            elif "facebook.com" in low:
                sns[Schema.FB] = sns[Schema.FB] or href
            elif "x.com" in low or "twitter.com" in low:
                sns[Schema.X] = sns[Schema.X] or href
            elif "line.me" in low:
                sns[Schema.LINE] = sns[Schema.LINE] or href
        return sns

    def _build_payments(self, price_table) -> str:
        if price_table is None:
            return ""
        out: list[str] = []
        for tr in price_table.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            if len(cells) < 2:
                continue
            key = " ".join(cells[0].get_text(" ", strip=True).split())
            matched = next((p for p in PAYMENT_KEYS if p in key), None)
            if not matched:
                continue
            value_cell = cells[1]
            text = " ".join(value_cell.get_text(" ", strip=True).split())
            if value_cell.find("img"):
                tag = matched
            elif text and text not in PLACEHOLDERS:
                tag = f"{matched}:{text}"
            else:
                continue
            if tag not in out:
                out.append(tag)
        return " / ".join(out)

    def _extract_area(self, soup) -> str:
        bc = soup.find(id="bread_crumb")
        if bc is None:
            return ""
        names: list[str] = []
        for li in bc.find_all("li"):
            classes = li.get("class") or []
            if "home" in classes or "last" in classes:
                continue
            for span in li.find_all("span", attrs={"itemprop": "name"}):
                t = span.get_text(strip=True).rstrip(",").rstrip("，").strip()
                if t and t not in names:
                    names.append(t)
        return " ".join(names)

    # 赤色強調表示の「閉業」を検出（例: <p class="has-vivid-red-color ...">閉業</p>）
    def _is_closed(self, soup) -> bool:
        for el in soup.find_all(["p", "span", "div", "strong"]):
            classes = el.get("class") or []
            if not any("red" in c.lower() for c in classes):
                continue
            if "閉業" in el.get_text(strip=True):
                return True
        return False


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    RePhilippinePubScraper().execute("https://philippine-pub.com/")
