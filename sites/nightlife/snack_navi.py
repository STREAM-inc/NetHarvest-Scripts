import re
import sys
import time
from pathlib import Path
from typing import Generator

from bs4 import BeautifulSoup

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler


class SnackNaviCrawler(StaticCrawler):
    """スナックナビ クローラー — スナック店舗求人情報を取得"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["最寄駅", "給与", "仕事内容", "年齢要件", "経験要件", "シフト", "福利厚生"]

    BASE_URL = "https://snacknavi.com"

    def parse(self, url: str) -> Generator:
        """一覧ページと詳細ページから店舗情報を取得"""
        page = 1

        while True:
            list_url = f"{self.BASE_URL}/girl_top.php?page={page}"
            self.logger.info(f"Fetching page {page}: {list_url}")

            try:
                response = self.session.get(list_url, timeout=10)
                response.raise_for_status()
            except Exception as e:
                self.logger.error(f"Failed to fetch {list_url}: {e}")
                break

            soup = BeautifulSoup(response.content, "html.parser")

            # 初回ページで総件数を設定
            if page == 1:
                for elem in soup.find_all(["p", "div", "span"]):
                    match = re.search(r"([\d,]+)件", elem.get_text())
                    if match:
                        self.total_items = int(match.group(1).replace(",", ""))
                        break

            # 一覧ページから店舗リンクを抽出（/rec/0/{id}/ パターン）
            seen = set()
            shop_hrefs = []
            for link in soup.find_all("a", href=True):
                href = link["href"]
                if href.startswith("/rec/0/") and href.endswith("/") and href not in seen:
                    seen.add(href)
                    shop_hrefs.append(href)

            if not shop_hrefs:
                self.logger.info(f"No shop links on page {page}, stopping.")
                break

            self.logger.info(f"Found {len(shop_hrefs)} shops on page {page}")

            for href in shop_hrefs:
                record = self._scrape_detail(self.BASE_URL + href)
                if record:
                    yield record

            page += 1
            if page > 1000:
                self.logger.warning("Reached 1000 pages, stopping crawl")
                break

    def _scrape_detail(self, detail_url: str) -> dict | None:
        """店舗詳細ページから情報を抽出してdictで返す"""
        try:
            response = self.session.get(detail_url, timeout=10)
            response.raise_for_status()
        except Exception as e:
            self.logger.error(f"Failed to fetch {detail_url}: {e}")
            return None

        soup = BeautifulSoup(response.content, "html.parser")
        record = {Schema.URL: detail_url}

        # 店舗名
        name_elem = soup.find("h1") or soup.find(["h2", "title"])
        record[Schema.NAME] = name_elem.get_text(strip=True) if name_elem else None

        # 住所
        for elem in soup.find_all(["p", "div", "dd"]):
            text = elem.get_text(strip=True)
            if ("〒" in text or "東京都" in text) and "営業時間" not in text:
                record[Schema.ADDR] = text.split("\n")[0]
                break

        # 電話番号
        for elem in soup.find_all(["a", "p", "div", "dd"]):
            href = elem.get("href", "")
            if href.startswith("tel:"):
                record[Schema.TEL] = href.replace("tel:", "")
                break
            text = elem.get_text(strip=True)
            stripped = re.sub(r"[\s\-\(\)]", "", text)
            if stripped.isdigit() and len(stripped) >= 10:
                record[Schema.TEL] = text
                break

        # 最寄駅
        for elem in soup.find_all(["p", "div", "li"]):
            text = elem.get_text(strip=True)
            if "駅" in text and len(text) < 100:
                stations = re.findall(r"[ぁ-ん\w一-龥]+駅", text)
                record["最寄駅"] = ", ".join(dict.fromkeys(stations)) if stations else text
                break

        # 営業時間 / 定休日
        for elem in soup.find_all(["p", "div", "dd"]):
            text = elem.get_text(strip=True)
            if Schema.TIME not in record and ("営業時間" in text or ("時" in text and ":" in text)):
                record[Schema.TIME] = text.split("\n")[0]
            if Schema.HOLIDAY not in record and ("定休" in text or "休み" in text):
                record[Schema.HOLIDAY] = text.split("\n")[0]

        # 給与
        for elem in soup.find_all(["p", "div", "dd", "span"]):
            text = elem.get_text(strip=True)
            if "円" in text and any(x in text for x in ["時", "給"]):
                record["給与"] = text.split("\n")[0]
                break

        # 仕事内容
        for elem in soup.find_all(["p", "div", "dd"]):
            text = elem.get_text(strip=True)
            if any(x in text for x in ["スタッフ", "職種", "女性"]) and len(text) < 100:
                record["仕事内容"] = text.split("\n")[0]
                break

        # 年齢要件
        for elem in soup.find_all(["p", "div", "dd"]):
            text = elem.get_text(strip=True)
            if "才" in text and any(x in text for x in ["以上", "OK"]):
                record["年齢要件"] = text.split("\n")[0]
                break

        # 経験要件
        for elem in soup.find_all(["p", "div", "dd"]):
            text = elem.get_text(strip=True)
            if any(x in text for x in ["未経験", "経験者", "経験OK"]):
                record["経験要件"] = text.split("\n")[0]
                break

        # シフト
        for elem in soup.find_all(["p", "div", "dd"]):
            text = elem.get_text(strip=True)
            if "週" in text and "日" in text and "OK" in text.upper():
                record["シフト"] = text.split("\n")[0]
                break

        # 福利厚生
        for elem in soup.find_all(["p", "div", "dd", "li"]):
            text = elem.get_text(strip=True)
            if any(x in text for x in ["日払い", "ボーナス", "福利"]):
                record["福利厚生"] = text
                break

        # Instagram
        for link in soup.find_all("a", href=True):
            if "instagram.com" in link["href"]:
                record[Schema.INSTA] = link["href"]
                break

        self.logger.info(f"Saved: {record.get(Schema.NAME) or detail_url}")
        return record


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    scraper = SnackNaviCrawler()
    scraper.execute("https://snacknavi.com/girl_top.php?page=1")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
