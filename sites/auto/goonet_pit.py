import gzip
import io
import re
import sys
import time
import urllib.parse as urlparse
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 店舗詳細ページ。例: https://www.goo-net.com/pit/shop/0175611/top
_SHOP_PATTERN = re.compile(r"https://www\.goo-net\.com/pit/shop/\d+/top")
# サイトマップに載っているのは店舗URLではなく一覧ページのURL。
# 例: https://www.goo-net.com/pit/repair/list?area_id=01&jititai_id=011011&cate2=10&p=1
#
# 注意: sitemap_index.xml には店舗一覧(sitemap_shop.xml)の他にブログ一覧
#   (sitemap_blog*.xml の /pit/blog/list?selectBrand=... 等) も含まれる。
#   ブログ一覧は area_id を持たず店舗データも無いが index の先頭側に約1500件並ぶため、
#   /list を無条件に拾うとブログ一覧の巡回だけで時間切れ→0件になっていた。
#   店舗一覧は必ず area_id= を持つので、これを必須条件にしてブログ一覧を除外する。
_LIST_PATTERN = re.compile(r"https://www\.goo-net\.com/pit/[^/]+/list\?[^\"'<>]*\barea_id=")


class GoonetPitScraper(StaticCrawler):
    """グーネットピット 自動車整備店情報スクレイパー"""

    DELAY = 0.5
    EXTRA_COLUMNS = ["法人名", "担当者名", "加盟団体"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # サイトマップの <loc> は店舗URLではなく「一覧ページ」のURLなので、
        # 一覧ページを巡回して店舗詳細URLを取り出す必要がある。
        list_urls = self._collect_list_urls(url)
        self.logger.info("一覧ページ収集完了: %d 件", len(list_urls))

        # 同じ店舗が複数のカテゴリ/エリアの一覧に重複して載るため dedupe しつつ、
        # 発見した店舗から逐次 yield する（時間切れ kill でも CSV が空にならないように）。
        seen: set[str] = set()
        for list_url in list_urls:
            time.sleep(self.DELAY)
            for shop_url in self._iter_shop_urls(list_url):
                if shop_url in seen:
                    continue
                seen.add(shop_url)
                self.total_items = len(seen)
                item = self._scrape_detail(shop_url)
                if item:
                    yield item

    def _fetch_xml(self, url: str) -> bytes | None:
        try:
            r = self.session.get(url, timeout=self.TIMEOUT)
            r.raise_for_status()
            data = r.content
            if url.endswith(".gz"):
                with gzip.GzipFile(fileobj=io.BytesIO(data)) as f:
                    data = f.read()
            return data
        except Exception as e:
            self.logger.warning("サイトマップ取得エラー %s: %s", url, e)
            return None

    def _collect_list_urls(self, index_url: str) -> list[str]:
        """サイトマップインデックスを辿り、店舗一覧ページのURLを収集する。"""
        list_urls: list[str] = []
        seen: set[str] = set()
        data = self._fetch_xml(index_url)
        if not data:
            return list_urls
        root = ET.fromstring(data)
        child_locs = [el.text.strip() for el in root.iter() if el.tag.endswith("loc") and el.text]
        for child_url in child_locs:
            child_data = self._fetch_xml(child_url)
            if not child_data:
                continue
            try:
                child_root = ET.fromstring(child_data)
            except Exception:
                continue
            for loc in child_root.iter():
                if not (loc.tag.endswith("loc") and loc.text):
                    continue
                u = loc.text.strip()
                if _LIST_PATTERN.match(u):
                    # p=N を p=1 に正規化して同じ一覧の別ページを重複処理しない
                    u = re.sub(r"([?&]p=)\d+", r"\g<1>1", u)
                    if u not in seen:
                        seen.add(u)
                        list_urls.append(u)
        return list_urls

    def _iter_shop_urls(self, list_url: str) -> Generator[str, None, None]:
        """1つの一覧ページ（p=1）とそのページネーションを辿り、店舗詳細URLを返す。"""
        soup = self.get_soup(list_url)
        if soup is None:
            return
        yield from self._shop_links(soup)

        max_page = self._max_page(soup, list_url)
        for p in range(2, max_page + 1):
            time.sleep(self.DELAY)
            page_url = re.sub(r"([?&]p=)\d+", lambda m: m.group(1) + str(p), list_url)
            page_soup = self.get_soup(page_url)
            if page_soup is None:
                break
            links = self._shop_links(page_soup)
            if not links:
                break
            yield from links

    @staticmethod
    def _shop_links(soup) -> list[str]:
        """一覧ページ内の店舗詳細リンクを抽出する。"""
        out: list[str] = []
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            href = urlparse.urljoin("https://www.goo-net.com/", a["href"].strip())
            if _SHOP_PATTERN.match(href) and href not in seen:
                seen.add(href)
                out.append(href)
        return out

    @staticmethod
    def _max_page(soup, list_url: str) -> int:
        """同一条件（area/jititai/cate2）のページネーションリンクから最大ページ番号を求める。"""
        base_q = urlparse.parse_qs(urlparse.urlparse(list_url).query)
        key = (base_q.get("area_id"), base_q.get("jititai_id"), base_q.get("cate2"))
        max_p = 1
        for a in soup.find_all("a", href=True):
            parsed = urlparse.urlparse(a["href"])
            if not parsed.path.endswith("/list"):
                continue
            q = urlparse.parse_qs(parsed.query)
            if (q.get("area_id"), q.get("jititai_id"), q.get("cate2")) != key:
                continue
            for pv in q.get("p", []):
                if pv.isdigit():
                    max_p = max(max_p, int(pv))
        return max_p

    @staticmethod
    def _extract_name(soup) -> str | None:
        """店名は h1/title の【...】内に入っている。"""
        for el in (soup.select_one("h1"), soup.title):
            if not el:
                continue
            text = el.get_text(strip=True)
            m = re.search(r"【(.+?)】", text)
            if m:
                return m.group(1).strip()
            if el.name == "h1" and text:
                return text
        return None

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        # 固定フィールド: dl.top_info
        for dl in soup.select("dl.top_info"):
            dt = dl.find("dt")
            dd = dl.find("dd")
            if not dt or not dd:
                continue
            key = dt.get_text(strip=True)
            val = re.sub(r"\s+", " ", dd.get_text(separator=" ", strip=True)).strip()
            if "住所" in key:
                m = re.match(r"(〒\d{3}-\d{4})\s*(.+)", val)
                if m:
                    data[Schema.POST_CODE] = m.group(1)
                    data[Schema.ADDR] = m.group(2)
                else:
                    data[Schema.ADDR] = val
            elif "営業時間" in key:
                data[Schema.TIME] = val
            elif "定休日" in key:
                data[Schema.HOLIDAY] = val

        # 動的フィールド: div.info_bottom table td (key：value, 全角コロン)
        info = soup.select_one("div.info_bottom")
        if info:
            for td in info.select("table tbody tr td"):
                text = re.sub(r"\s+", " ", td.get_text(separator=" ", strip=True)).strip()
                if "：" not in text:
                    continue
                key, val = text.split("：", 1)
                key = key.strip()
                val = val.strip()
                a = td.find("a", href=True)
                if a and a["href"].strip():
                    val = a["href"].strip()
                if "店名" in key or "名称" in key:
                    data.setdefault(Schema.NAME, val)
                elif "法人名" in key:
                    data.setdefault("法人名", val)
                elif "担当者" in key:
                    data.setdefault("担当者名", val)
                elif "電話" in key or "TEL" in key:
                    data.setdefault(Schema.TEL, val)
                elif "FAX" in key:
                    pass
                elif "加盟" in key:
                    data.setdefault("加盟団体", val)
                elif "ホームページ" in key or "HP" in key:
                    data.setdefault(Schema.HP, val)

        # 店名が info_bottom に無い場合は h1/title の【...】から取得
        if not data.get(Schema.NAME):
            name = self._extract_name(soup)
            if name:
                data[Schema.NAME] = name

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    GoonetPitScraper().execute("https://www.goo-net.com/pit/sitemap_index.xml")
