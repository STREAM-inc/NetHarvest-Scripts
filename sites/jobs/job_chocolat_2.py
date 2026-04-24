"""
ジョブショコラ (job-chocolat.jp) — キャバクラボーイ求人・黒服バイト・夜職求人ポータル

取得対象:
    - 全国の夜職求人店舗情報 (sitemap_shop.xml に約13,000件)

取得フロー:
    1. https://job-chocolat.jp/sitemap_shop.xml から全店舗詳細URLを列挙
    2. 各店舗詳細ページ (/{pref}/a_{area}/shop/{id}/) を解析して
       名称・業種・住所・電話・営業時間・定休日・アクセス・SNS を取得

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/job_chocolat_2.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id job_chocolat_2
"""

import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


BASE_URL = "https://job-chocolat.jp"
SHOP_SITEMAP = f"{BASE_URL}/sitemap_shop.xml"
SHOP_URL_RE = re.compile(r"^https://job-chocolat\.jp/([a-z]+)/[a-z0-9_]+/shop/\d+/?$")
TEL_RE = re.compile(r"0\d{1,4}-\d{1,4}-\d{3,4}")

PREF_CODE_MAP = {
    "hokkaido": "北海道", "aomori": "青森県", "iwate": "岩手県", "miyagi": "宮城県",
    "akita": "秋田県", "yamagata": "山形県", "fukushima": "福島県",
    "ibaraki": "茨城県", "tochigi": "栃木県", "gunma": "群馬県",
    "saitama": "埼玉県", "chiba": "千葉県", "tokyo": "東京都", "kanagawa": "神奈川県",
    "niigata": "新潟県", "toyama": "富山県", "ishikawa": "石川県", "fukui": "福井県",
    "yamanashi": "山梨県", "nagano": "長野県", "gifu": "岐阜県", "shizuoka": "静岡県",
    "aichi": "愛知県", "mie": "三重県", "shiga": "滋賀県", "kyoto": "京都府",
    "osaka": "大阪府", "hyogo": "兵庫県", "nara": "奈良県", "wakayama": "和歌山県",
    "tottori": "鳥取県", "shimane": "島根県", "okayama": "岡山県", "hiroshima": "広島県",
    "yamaguchi": "山口県", "tokushima": "徳島県", "kagawa": "香川県", "ehime": "愛媛県",
    "kochi": "高知県", "fukuoka": "福岡県", "saga": "佐賀県", "nagasaki": "長崎県",
    "kumamoto": "熊本県", "oita": "大分県", "miyazaki": "宮崎県",
    "kagoshima": "鹿児島県", "okinawa": "沖縄県",
}


def _clean(text) -> str:
    if text is None:
        return ""
    s = str(text)
    s = re.sub(r"[\r\n\t]+", " ", s)
    s = re.sub(r"\s{2,}", " ", s)
    return s.strip()


class JobChocolat2Scraper(StaticCrawler):
    """ジョブショコラ 夜職求人スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["アクセス", "女性向け求人"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        shop_urls = self._collect_shop_urls()
        self.total_items = len(shop_urls)
        self.logger.info("sitemap から店舗URLを収集: %d件", len(shop_urls))

        for shop_url in shop_urls:
            try:
                item = self._scrape_detail(shop_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("詳細取得失敗 %s: %s", shop_url, e)
                continue

    def _collect_shop_urls(self) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        try:
            r = self.session.get(SHOP_SITEMAP, timeout=self.TIMEOUT)
            r.raise_for_status()
            root = ET.fromstring(r.content)
            for el in root.iter():
                if not (el.tag.endswith("loc") and el.text):
                    continue
                loc = el.text.strip()
                if SHOP_URL_RE.match(loc) and loc not in seen:
                    seen.add(loc)
                    urls.append(loc)
        except Exception as e:
            self.logger.error("sitemap_shop.xml の取得に失敗: %s", e)
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}

        m = SHOP_URL_RE.match(url)
        if m and m.group(1) in PREF_CODE_MAP:
            data[Schema.PREF] = PREF_CODE_MAP[m.group(1)]

        shop_info_table = None
        for table in soup.select("table.infoTable"):
            if "店鋪名" in table.get_text() or "店舗名" in table.get_text():
                shop_info_table = table
                break

        if shop_info_table:
            for tr in shop_info_table.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                key = _clean(th.get_text())
                val = _clean(td.get_text(" "))
                if key in ("店鋪名", "店舗名"):
                    data[Schema.NAME] = val
                elif "業" in key and "種" in key:
                    data[Schema.CAT_SITE] = val
                elif "営業時間" in key:
                    data[Schema.TIME] = val
                elif "定休日" in key:
                    data[Schema.HOLIDAY] = val
                elif "アクセス" in key:
                    data["アクセス"] = val
                elif "女性向け" in key:
                    data["女性向け求人"] = val

        app_table = soup.select_one("table.appTable")
        if app_table:
            for tr in app_table.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                key = _clean(th.get_text())
                if "住" in key and "所" in key:
                    for tag in td.select("a, .btn, button, .map-btn"):
                        tag.decompose()
                    addr = _clean(td.get_text(" "))
                    addr = addr.replace("Google MAPを開く", "").replace("GoogleMAPを開く", "").strip()
                    pref = data.get(Schema.PREF, "")
                    if pref and addr.startswith(pref):
                        data[Schema.ADDR] = addr[len(pref):].strip()
                    else:
                        data[Schema.ADDR] = addr
                elif "電話" in key or key.upper() == "TEL":
                    tel_match = TEL_RE.search(td.get_text(" "))
                    if tel_match:
                        data[Schema.TEL] = tel_match.group(0)

        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            if "instagram.com" in href and Schema.INSTA not in data:
                data[Schema.INSTA] = href
            elif "tiktok.com" in href and Schema.TIKTOK not in data:
                data[Schema.TIKTOK] = href
            elif ("twitter.com" in href or "x.com" in href) and Schema.X not in data:
                data[Schema.X] = href
            elif "facebook.com" in href and Schema.FB not in data:
                data[Schema.FB] = href

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = JobChocolat2Scraper()
    scraper.execute("https://job-chocolat.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
