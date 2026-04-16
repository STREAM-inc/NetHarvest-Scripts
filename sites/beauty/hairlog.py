import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class HairlogScraper(StaticCrawler):
    """HAIRLOG ヘアサロン情報スクレイパー"""

    DELAY = 0.5
    EXTRA_COLUMNS = ["名称_フリガナ", "最寄駅", "SNS"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        """サイトマップから店舗URLを収集して詳細ページをスクレイプ"""
        self.logger.info("サイトマップ取得: %s", url)
        shop_urls = self._collect_from_sitemap(url)
        self.total_items = len(shop_urls)
        self.logger.info("店舗URL収集完了: %d 件", len(shop_urls))

        for shop_url in shop_urls:
            item = self._scrape_detail(shop_url)
            if item:
                yield item

    def _collect_from_sitemap(self, sitemap_url: str) -> list[str]:
        """サイトマップXMLから日本語エンコード('%'含む)の店舗URLを収集"""
        urls = []
        try:
            resp = self.session.get(sitemap_url, timeout=self.TIMEOUT)
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
            ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

            # サイトマップインデックスの場合、子サイトマップを再帰的に処理
            children = root.findall("sm:sitemap/sm:loc", ns)
            if children:
                for child_loc in children:
                    child_url = child_loc.text.strip()
                    urls.extend(self._collect_from_sitemap(child_url))
            else:
                for loc in root.findall("sm:url/sm:loc", ns):
                    u = loc.text.strip()
                    if "%" in u:  # 日本語エンコードされた店舗URL
                        urls.append(u)
        except Exception as e:
            self.logger.warning("サイトマップ取得エラー: %s", e)
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        for tr in soup.select("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            label = th.get_text(strip=True)

            if label == "店名":
                kana_span = td.find("span", class_="text-muted")
                kana = kana_span.get_text(strip=True) if kana_span else ""
                lines = [x.strip() for x in td.get_text("\n", strip=True).splitlines() if x.strip()]
                if kana and lines and lines[0] == kana:
                    lines = lines[1:]
                data[Schema.NAME] = lines[-1] if lines else ""
                data["名称_フリガナ"] = kana
            elif label == "電話番号":
                a = td.find("a", href=True)
                data[Schema.TEL] = a.get_text(" ", strip=True).replace(" ", "") if a else td.get_text(" ", strip=True)
            elif "住所" in label:
                direct = [s.strip() for s in td.find_all(string=True, recursive=False) if s.strip()]
                data[Schema.ADDR] = " ".join(direct).strip() if direct else td.get_text("\n", strip=True).splitlines()[0].strip()
            elif label in ("アクセス", "最寄駅"):
                first_a = td.find("a")
                data["最寄駅"] = first_a.get_text(strip=True) if first_a else td.get_text(" ", strip=True)
            elif label == "営業時間":
                data[Schema.TIME] = td.get_text("\n", strip=True)
            elif label == "定休日":
                data[Schema.HOLIDAY] = td.get_text(" ", strip=True)
            elif label in ("公式URL", "URL", "ホームページ", "HP"):
                a = td.find("a", href=True)
                data[Schema.HP] = a.get("href", "").strip() if a else td.get_text(" ", strip=True)
            elif label == "SNS":
                links = [a["href"].strip() for a in td.find_all("a", href=True) if a["href"].strip()]
                data["SNS"] = ", ".join(dict.fromkeys(links))

        if not data.get(Schema.NAME) and not data.get(Schema.ADDR):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    HairlogScraper().execute("https://hairlog.jp/sitemap.xml")
