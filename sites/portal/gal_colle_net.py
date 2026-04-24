# scripts/sites/portal/gal_colle_net.py
"""
ギャルコレネット — 愛媛県ナイト系店舗情報スクレイパー

取得対象:
    - 愛媛県（松山・今治・宇和島・新居浜）のキャバクラ・ラウンジ・ホストクラブ等
    - 全カテゴリー (sel_shopcate[]=1-10) + sitemap.xml の和集合で全店舗URLを収集

取得フロー:
    1. gc_search.php?sel_shopcate[]=X (カテゴリー1-10) で現在掲載中の全店舗を収集
    2. sitemap.xml からも補完収集 (旧店舗分)
    3. 重複排除後、各詳細ページからデータ取得

実行方法:
    python scripts/sites/portal/gal_colle_net.py
    python bin/run_flow.py --site-id gal_colle_net
"""

import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://gal-colle.net"
SITEMAP_URL = "https://gal-colle.net/sitemap.xml"
DEFAULT_PREF = "愛媛県"

# 全カテゴリーID (1=クラブ&キャバ, 2=ラウンジ, 3=ガールズバー, 4=セクキャバ,
#                5=ホスト, 6=居酒屋, 7=バー, 8=美容, 9=バラエティ, 10=カフェ)
ALL_CATEGORIES = list(range(1, 11))

_PREF_RE = re.compile(r"^(愛媛県|[^\s]{2,4}[都道府県])")


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _is_valid_url(href: str) -> bool:
    return bool(href) and href != "-" and href.startswith("http")


class GalColleNetScraper(StaticCrawler):
    """ギャルコレネット 愛媛県ナイト系店舗情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["店舗名カナ", "平均予算", "VIPルーム", "カラオケ", "ダーツ", "システム"]

    def parse(self, url: str):
        # sitemap.xml から全店舗 URL を収集
        shop_urls = self._collect_shop_urls()
        self.total_items = len(shop_urls)
        self.logger.info("店舗URL収集完了: %d 件", len(shop_urls))

        for shop_url in shop_urls:
            try:
                item = self._scrape_detail(shop_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("詳細ページ取得失敗: %s (%s)", shop_url, e)
                continue

    def _collect_shop_urls(self) -> list[str]:
        seen_ids: set[str] = set()
        urls: list[str] = []

        def _add(shopno: str) -> None:
            if shopno not in seen_ids:
                seen_ids.add(shopno)
                urls.append(f"{BASE_URL}/gc_shop.php?SHOPNO={shopno}")

        # 1. 全カテゴリーの検索ページから現在掲載中の店舗を収集
        import re as _re
        for cat in ALL_CATEGORIES:
            cat_url = f"{BASE_URL}/gc_search.php?sel_shopcate[]={cat}"
            try:
                soup = self.get_soup(cat_url)
                for shopno in _re.findall(r"gc_shop\.php\?SHOPNO=(\d+)", str(soup)):
                    _add(shopno)
                self.logger.debug("カテゴリー%d: %d店舗収集", cat, len(seen_ids))
            except Exception as e:
                self.logger.warning("カテゴリー%d 取得失敗: %s", cat, e)

        cat_count = len(seen_ids)
        self.logger.info("カテゴリー検索完了: %d 件", cat_count)

        # 2. sitemap.xml から補完収集 (カテゴリー検索で取得できない旧店舗)
        try:
            sitemap_soup = self.get_soup(SITEMAP_URL)
            for loc in sitemap_soup.find_all("loc"):
                href = loc.get_text(strip=True)
                m = _re.search(r"gc_shop\.php\?SHOPNO=(\d+)", href)
                if m:
                    _add(m.group(1))
            self.logger.info(
                "sitemap補完: %d 件追加 (合計 %d 件)",
                len(seen_ids) - cat_count,
                len(seen_ids),
            )
        except Exception as e:
            self.logger.warning("sitemap取得失敗: %s", e)

        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        data = {Schema.URL: url}

        # ── カテゴリ・店舗名カナ (h2.title) ────────────────────────────
        h2 = soup.select_one("h2.title")
        if h2:
            span = h2.select_one("span")
            if span:
                data[Schema.CAT_SITE] = span.get_text(strip=True)
                span.decompose()
            small = h2.select_one("small")
            if small:
                data["店舗名カナ"] = small.get_text(strip=True)

        # ── 店舗情報テーブル (div.col-xs-12.cell) ──────────────────────
        cells = soup.select("div.col-xs-12.cell")
        for cell in cells:
            label_el = cell.select_one("div.htd")
            value_el = cell.select_one("div.dtd")
            if not label_el or not value_el:
                continue
            label = label_el.get_text(strip=True)
            # iframeを除いて最初のテキストノードを取得
            for tag in value_el.select("iframe, script"):
                tag.decompose()
            # aタグがある場合はhrefも確認
            a_tag = value_el.select_one("a[href]")
            raw_text = _clean(value_el.get_text())

            if label == "店舗名":
                data[Schema.NAME] = raw_text

            elif label == "住所":
                addr = raw_text
                m = _PREF_RE.match(addr)
                if m:
                    data[Schema.PREF] = m.group(1)
                    data[Schema.ADDR] = addr[m.end():].strip()
                else:
                    data[Schema.PREF] = DEFAULT_PREF
                    data[Schema.ADDR] = addr

            elif label == "TEL":
                # "xxx-xxx-xxxx\nギャルコレネットを..." の形式
                tel = raw_text.split("\n")[0].strip() if "\n" in raw_text else raw_text
                # get_textでは改行がスペースになる場合があるのでsplitで対応
                tel = re.split(r"ギャルコレ", raw_text)[0].strip()
                data[Schema.TEL] = tel

            elif label == "営業時間":
                data[Schema.TIME] = raw_text

            elif label == "URL":
                if a_tag and _is_valid_url(a_tag.get("href", "")):
                    data[Schema.HP] = a_tag["href"]

            elif label == "BLOG":
                if a_tag and _is_valid_url(a_tag.get("href", "")):
                    href = a_tag["href"]
                    if "facebook" in href.lower():
                        data.setdefault(Schema.FB, href)
                    else:
                        data.setdefault(Schema.HP, href)

            elif label == "Facebook":
                if a_tag and _is_valid_url(a_tag.get("href", "")):
                    data[Schema.FB] = a_tag["href"]

            elif label == "Instagram":
                if a_tag and _is_valid_url(a_tag.get("href", "")):
                    data[Schema.INSTA] = a_tag["href"]

            elif label == "定休日":
                if raw_text and raw_text != "-":
                    data[Schema.HOLIDAY] = raw_text

            elif label == "クレジットカード":
                if raw_text and raw_text != "-":
                    data[Schema.PAY] = raw_text

            elif label == "平均予算":
                if raw_text and raw_text != "-":
                    data["平均予算"] = raw_text

            elif label == "VIPルーム":
                data["VIPルーム"] = raw_text

            elif label == "カラオケ":
                data["カラオケ"] = raw_text

            elif label == "ダーツ":
                data["ダーツ"] = raw_text

            elif label == "システム":
                if raw_text and raw_text != "-":
                    data["システム"] = raw_text[:300]

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = GalColleNetScraper()
    scraper.execute(BASE_URL + "/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
