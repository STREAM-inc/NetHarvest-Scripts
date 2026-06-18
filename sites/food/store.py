"""
ほっともっと — 全国のほっともっとお弁当店舗情報

取得対象:
    - 全国約2,400件の「ほっともっと（Hotto Motto）」弁当チェーン店舗情報

取得フロー:
    1. https://store.hottomotto.com/b/hottomotto/attr/ で一覧取得 (?start=N でオフセットページング)
    2. 各詳細ページ /b/hottomotto/info/{ID}/ で住所・TEL・営業時間・座標を取得
    3. 1件ごとに即 yield

実行方法:
    # ローカルテスト
    python scripts/sites/food/store.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id store
"""

import re
import sys
import urllib.parse
from collections import Counter
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|"
    r"三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_DETAIL_URL_RE = re.compile(r"/b/hottomotto/info/\d+/")
_POST_CODE_RE = re.compile(r"〒(\d{3}-\d{4})")
_TEL_RE = re.compile(r"(\d{2,4}-\d{3,4}-\d{4})")
_TIME_RE = re.compile(r"\d{1,2}:\d{2}-\d{1,2}:\d{2}")
_LAT_RE = re.compile(r"'lat'\s*:\s*'([\d.\-]+)'")
_LON_RE = re.compile(r"'lon'\s*:\s*'([\d.\-]+)'")
_MAPS_RE = re.compile(r"/@([\-\d.]+),([\-\d.]+),")


class StoreScraper(StaticCrawler):
    """ほっともっと 店舗スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["駐車場", "緯度", "経度"]

    def parse(self, url: str):
        page_size = 1
        start = 0
        total_set = False

        while True:
            page_url = f"{url}?start={start}" if start > 0 else url
            soup = self.get_soup(page_url)
            if soup is None:
                break

            if not total_set:
                src = str(soup)
                m = re.search(r"(\d{3,5})件の店舗", src)
                if m:
                    self.total_items = int(m.group(1))
                total_set = True

            links = soup.find_all("a", href=_DETAIL_URL_RE)
            if not links:
                break

            seen = set()
            for link in links:
                href = link.get("href", "")
                detail_url = urllib.parse.urljoin(url, href)
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                parking = "あり" if link.find("img", alt="駐車場あり") else ""
                record = self._scrape_detail(detail_url, parking)
                if record:
                    yield record

            start += page_size

    def _scrape_detail(self, url: str, parking: str = "") -> dict | None:
        try:
            soup = self.get_soup(url)
            if soup is None:
                return None
            src = str(soup)

            # 店舗名
            name_el = soup.select_one("h1 span.ttl-main") or soup.select_one("h1")
            name = name_el.get_text(strip=True) if name_el else ""
            if not name:
                return None

            # 郵便番号・都道府県・住所
            post_code = pref = addr = ""
            addr_div = soup.select_one("div.description-list-address")
            if addr_div:
                ps = addr_div.select("dd > p")
                if ps:
                    post_code = ps[0].get_text(strip=True).lstrip("〒")
                if len(ps) >= 2:
                    full_addr = ps[1].get_text(strip=True)
                    m = _PREF_RE.match(full_addr)
                    if m:
                        pref = m.group(1)
                        addr = full_addr[m.end():]
                    else:
                        addr = full_addr
            else:
                # フォールバック: テキスト全体から正規表現で抽出
                page_text = soup.get_text(" ")
                pc_m = _POST_CODE_RE.search(page_text)
                if pc_m:
                    post_code = pc_m.group(1)
                    tel_m = _TEL_RE.search(page_text)
                    end_pos = (
                        tel_m.start()
                        if tel_m and tel_m.start() > pc_m.end()
                        else pc_m.end() + 100
                    )
                    addr_block = re.sub(r"\s+", "", page_text[pc_m.end():end_pos])
                    pm = _PREF_RE.search(addr_block)
                    if pm:
                        pref = pm.group(1)
                        addr = addr_block[pm.end():]
                    else:
                        addr = addr_block

            # 電話番号
            tel_el = soup.select_one("dd.tel-wrap a.phone-text") or soup.select_one(
                "a[href^='tel:']"
            )
            if tel_el:
                tel = re.sub(r"\D", "", tel_el.get_text(strip=True))
                # ハイフン付きに整形 (Pipeline が自動正規化するためそのままでも可)
                tel = tel_el.get_text(strip=True)
            else:
                tel_m = _TEL_RE.search(soup.get_text())
                tel = tel_m.group(1) if tel_m else ""

            # 営業時間
            time_str = ""
            dt_time = soup.find("dt", string="営業時間")
            if dt_time:
                dd_time = dt_time.find_next_sibling("dd")
                if dd_time:
                    spans = [
                        s for s in dd_time.children
                        if hasattr(s, "name") and s.name == "span"
                    ]
                    parts = []
                    for span in spans:
                        text = span.get_text(strip=True)
                        text = re.sub(r"^([月火水木金土日])", r"\1 ", text)
                        if text:
                            parts.append(text)
                    time_str = " / ".join(parts)
            if not time_str:
                times = _TIME_RE.findall(soup.get_text())
                if times:
                    time_str = Counter(times).most_common(1)[0][0]

            # 支払い方法
            pay_imgs = soup.select("ul.detail-icon-pays li img")
            pay_str = "、".join(img["alt"] for img in pay_imgs if img.get("alt"))

            # 緯度・経度 (window.mapConfig から優先取得、なければ Google Maps リンクから)
            lat = lon = ""
            m_lat = _LAT_RE.search(src)
            m_lon = _LON_RE.search(src)
            if m_lat and m_lon:
                lat, lon = m_lat.group(1), m_lon.group(1)
            else:
                maps_a = soup.find("a", href=re.compile(r"google\.co\.jp/maps/@"))
                if maps_a:
                    m = _MAPS_RE.search(maps_a.get("href", ""))
                    if m:
                        lat, lon = m.group(1), m.group(2)

            return {
                Schema.NAME: name,
                Schema.URL: url,
                Schema.POST_CODE: post_code,
                Schema.PREF: pref,
                Schema.ADDR: addr,
                Schema.TEL: tel,
                Schema.TIME: time_str,
                Schema.PAYMENTS: pay_str,
                "駐車場": parking,
                "緯度": lat,
                "経度": lon,
            }
        except Exception as e:
            self.logger.error("Error scraping %s: %s", url, e)
            return None


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = StoreScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://store.hottomotto.com/b/hottomotto/attr/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")