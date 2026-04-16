import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class IiIeScraper(StaticCrawler):
    """いい部屋ネット 不動産会社情報スクレイパー"""

    DELAY = 0.5
    EXTRA_COLUMNS = ["新築戸建て"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        shop_urls = self._collect_from_sitemap(url)
        self.total_items = len(shop_urls)
        self.logger.info("店舗URL収集完了: %d 件", len(shop_urls))
        for shop_url in shop_urls:
            item = self._scrape_detail(shop_url)
            if item:
                yield item

    def _collect_from_sitemap(self, sitemap_url: str) -> list[str]:
        urls = []
        try:
            resp = self.session.get(sitemap_url, timeout=self.TIMEOUT)
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
            children = [el.text.strip() for el in root.iter() if el.tag.endswith("loc")]

            # sitemapindex か urlset かを判断
            ns_tag = root.tag.lower()
            if "sitemapindex" in ns_tag or any("sitemap" in c for c in [el.tag.lower() for el in root]):
                for child_url in children:
                    try:
                        r = self.session.get(child_url, timeout=self.TIMEOUT)
                        r.raise_for_status()
                        child_root = ET.fromstring(r.content)
                        for loc in child_root.iter():
                            if loc.tag.endswith("loc") and loc.text:
                                u = loc.text.strip()
                                if "/shop_search/" in u or "/shop/" in u:
                                    urls.append(u)
                    except Exception as e:
                        self.logger.warning("子サイトマップ取得エラー: %s", e)
            else:
                for u in children:
                    if "/shop_search/" in u or "/shop/" in u:
                        urls.append(u)
        except Exception as e:
            self.logger.warning("サイトマップ取得エラー: %s", e)
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        # 都道府県（パンくず）
        for a in soup.select("li.breadcrumb__item a.breadcrumb__link"):
            href = a.get("href", "")
            if "shop_search" in href and "pref=" in href:
                data[Schema.PREF] = a.get_text(strip=True)
                break

        # 名称
        name_tag = soup.select_one("h2.shop-about__name-st")
        if name_tag:
            data[Schema.NAME] = name_tag.get_text(strip=True)

        # 郵便番号・住所
        add_tag = soup.select_one("p.shop-about__add-value")
        if add_tag:
            lines = [x.strip() for x in add_tag.get_text("\n", strip=True).splitlines() if x.strip()]
            if lines:
                if lines[0].startswith("〒"):
                    data[Schema.POST_CODE] = lines[0]
                    data[Schema.ADDR] = " ".join(lines[1:]).strip()
                else:
                    data[Schema.ADDR] = " ".join(lines).strip()

        # TEL
        tel_tag = soup.select_one("span.cta__phone-txt")
        if tel_tag:
            data[Schema.TEL] = tel_tag.get_text(strip=True)

        # 新築戸建て
        is_new = any("新築" in ph.get_text() for ph in soup.select("div.item-box__ph"))
        data["新築戸建て"] = "1" if is_new else "0"

        # overview-list（動的カラム → EXTRA_COLUMNSには入れず事業内容等にマッピング）
        for item in soup.select(".overview-list .overview-list__item"):
            label = item.select_one(".overview-list__label")
            value = item.select_one(".overview-list__value")
            if label and value:
                lbl = label.get_text(strip=True)
                val = value.get_text(" ", strip=True)
                if "営業時間" in lbl:
                    data[Schema.TIME] = val
                elif "定休日" in lbl:
                    data[Schema.HOLIDAY] = val
                elif "免許" in lbl or "宅建" in lbl:
                    data.setdefault(Schema.LOB, val)

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    IiIeScraper().execute("https://www.ii-ie2.net/sitemap.xml")
