"""
ナイトグラム — キャバクラ店舗情報ポータル クローラー

取得対象:
    - nightgram.com 掲載の全キャバクラ・ガールズバー等の店舗情報 (197件)
    - 店名 / 店名カナ / 都道府県 / 住所 / TEL / 営業時間 / 定休日 / HP / 業種 / エリア

取得フロー:
    1. sitemap.xml から全店舗 URL を収集 (urllib, Playwright 不要)
    2. 各店舗詳細ページ (/shop/{id}) を Playwright で取得
    3. 店舗情報テーブル (th="店名" を含む最後のテーブル) を解析 → 即 yield

実行方法:
    python scripts/sites/nightlife/nightgram.py
    docker compose exec worker python /app/bin/run_flow.py --site-id nightgram
"""

import re
import sys
import urllib.request
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema

_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


class NightgramScraper(DynamicCrawler):
    """ナイトグラム スクレイパー"""

    DELAY = 2.0
    EXTRA_COLUMNS = ["エリア"]

    def parse(self, url: str):
        base = url.rstrip("/")
        sitemap_url = f"{base}/sitemap.xml"

        try:
            req = urllib.request.Request(
                sitemap_url,
                headers={"User-Agent": self.USER_AGENT},
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                sitemap_content = resp.read().decode("utf-8")
        except Exception as e:
            self.logger.error("sitemap取得失敗: %s — %s", sitemap_url, e)
            return

        # /shop/{id} のURL (cast_recruit / staff_recruit は除外)
        shop_urls = re.findall(
            r"<loc>(https://nightgram\.com/shop/[^/<]+)</loc>",
            sitemap_content,
        )
        self.total_items = len(shop_urls)
        self.logger.info("総店舗数: %d", self.total_items)

        for shop_url in shop_urls:
            item = self._scrape_detail(shop_url)
            if item:
                yield item

    def _scrape_detail(self, url: str) -> dict | None:
        try:
            soup = self.get_soup(url)
            if soup is None:
                return None

            # 店舗情報テーブル: th="店名" を含むテーブルを探す
            info_table = None
            for table in soup.find_all("table"):
                first_th = table.find("th")
                if first_th and first_th.get_text(strip=True) == "店名":
                    info_table = table
                    break

            if info_table is None:
                self.logger.warning("店舗情報テーブルなし: %s", url)
                return None

            # th → td のマッピングを構築
            rows: dict = {}
            for tr in info_table.find_all("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if th and td:
                    rows[th.get_text(strip=True)] = td

            # 店名 / 店名カナ (td 内に <br> で区切られている)
            name = ""
            name_kana = ""
            name_td = rows.get("店名")
            if name_td:
                texts = list(name_td.strings)
                name = texts[0].strip() if texts else ""
                name_kana = texts[1].strip() if len(texts) > 1 else ""

            if not name:
                return None

            # 住所 → 都道府県 + 住所
            pref = ""
            addr = ""
            addr_td = rows.get("住所")
            if addr_td:
                addr_full = addr_td.get_text(strip=True)
                m = _PREF_RE.match(addr_full)
                if m:
                    pref = m.group(1)
                    addr = addr_full[m.end():].strip()
                else:
                    addr = addr_full

            # 電話番号
            tel = ""
            tel_td = rows.get("電話番号")
            if tel_td:
                tel = tel_td.get_text(strip=True)

            # 営業時間 ("OPEN.19:30～LAST" → "19:30～LAST")
            time_str = ""
            time_td = rows.get("営業時間")
            if time_td:
                time_str = re.sub(r"^OPEN\s*[.．]\s*", "", time_td.get_text(strip=True))

            # 定休日 ("CLOSE.月曜日" → "月曜日")
            holiday = ""
            holiday_td = rows.get("店休日")
            if holiday_td:
                holiday = re.sub(r"^CLOSE\s*[.．]\s*", "", holiday_td.get_text(strip=True))

            # 業種
            cat_site = ""
            biz_td = rows.get("業種")
            if biz_td:
                cat_site = biz_td.get_text(strip=True)

            # エリア
            area = ""
            area_td = rows.get("エリア")
            if area_td:
                area = area_td.get_text(strip=True)

            # HP (公式ホームページ リンク)
            hp = ""
            hp_link = soup.find("a", string="公式ホームページ")
            if hp_link:
                hp = hp_link.get("href", "")

            return {
                Schema.NAME: name,
                Schema.NAME_KANA: name_kana,
                Schema.PREF: pref,
                Schema.ADDR: addr,
                Schema.TEL: tel,
                Schema.TIME: time_str,
                Schema.HOLIDAY: holiday,
                Schema.HP: hp,
                Schema.CAT_SITE: cat_site,
                Schema.URL: url,
                "エリア": area,
            }

        except Exception as e:
            self.logger.warning("詳細取得失敗: %s — %s", url, e)
            return None


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = NightgramScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://nightgram.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
