"""
キャバイト求人 (www.cabaito.jp) — セクキャバ/いちゃキャバ/おっパブの掲載店舗スクレイパー

取得対象:
    - 関東/関西/東海/九州/広島岡山/北海道の21都道府県ページに掲載された全店舗
    - 店名 / 業種(セクキャバ・いちゃキャバ 等) / 都道府県 / 住所 / 電話
    - 給与 / 衣装 / 最寄り駅 / 勤務時間 / 勤務日 / 応募資格 / その他待遇 / 謝礼金額

取得フロー:
    1. 都道府県ごとの一覧 /{pref_slug}/?page=N を巡回 (1ページ約20件)
    2. 各ページから shop-detail_{id}.html リンクを収集し、shop_id で dedup
    3. 詳細ページから table(店舗名/住所/担当TEL/衣装) + dl(給与/勤務時間/勤務日/応募資格/その他待遇) を抽出

実行方法:
    python scripts/sites/nightlife/cabaito.py
    python bin/run_flow.py --site-id cabaito
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


BASE_URL = "https://www.cabaito.jp"

# (地方名, URL slug, 都道府県名) — 都道府県単位で重複なく全国を網羅する
PREFECTURES: list[tuple[str, str, str]] = [
    ("関東",   "tokyo",        "東京都"),
    ("関東",   "kanagawa",     "神奈川県"),
    ("関東",   "saitamaken",   "埼玉県"),
    ("関東",   "chiba",        "千葉県"),
    ("関東",   "gunma",        "群馬県"),
    ("関東",   "tochigi",      "栃木県"),
    ("関東",   "ibaragi",      "茨城県"),
    ("関西",   "osaka",        "大阪府"),
    ("関西",   "kyoto",        "京都府"),
    ("関西",   "hyogo",        "兵庫県"),
    ("東海",   "nagoya",       "愛知県"),
    ("東海",   "aichiother",   "愛知県"),
    ("九州",   "fukuoka",      "福岡県"),
    ("九州",   "okinawa",      "沖縄県"),
    ("九州",   "kumamoto",     "熊本県"),
    ("九州",   "kagoshima",    "鹿児島県"),
    ("九州",   "miyazaki",     "宮崎県"),
    ("九州",   "oita",         "大分県"),
    ("広島岡山", "hiroshimaken", "広島県"),
    ("広島岡山", "okayamaken",   "岡山県"),
    ("北海道",  "sapporo",      "北海道"),
]

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_REWARD_PATTERN = re.compile(r"謝礼金[^\d]{0,40}(\d{1,3}(?:,\d{3})*)\s*円")
_SHOP_ID_PATTERN = re.compile(r"/shop-detail_(\d+)\.html")
_NAME_SUFFIX_PATTERN = re.compile(r"の(?:[^の]{1,10}?)求人情報$")


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"[ \t　]+", " ", text.replace("\xa0", " ")).strip()


def _multiline(text: str) -> str:
    """改行は保ったまま各行の余分な空白を整える。"""
    if not text:
        return ""
    s = text.replace("\r", "\n").replace("\xa0", " ")
    lines = [re.sub(r"[ \t　]+", " ", l).strip() for l in s.split("\n")]
    cleaned: list[str] = []
    prev_empty = False
    for l in lines:
        if l == "":
            if not prev_empty and cleaned:
                cleaned.append("")
            prev_empty = True
        else:
            cleaned.append(l)
            prev_empty = False
    return "\n".join(cleaned).strip()


def _split_pref(addr: str) -> tuple[str, str]:
    addr = _clean(addr)
    if not addr:
        return "", ""
    m = _PREF_PATTERN.match(addr)
    if m:
        return m.group(1), addr[m.end():].strip()
    return "", addr


class CabaitoScraper(StaticCrawler):
    """キャバイト求人 (www.cabaito.jp) 掲載店舗スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "エリア",
        "地方",
        "業種",
        "最寄り駅",
        "給与",
        "衣装",
        "応募資格",
        "勤務日",
        "その他待遇",
        "体験入店に必要なもの",
        "レポート謝礼金額",
    ]

    def parse(self, url: str):
        shop_items: list[tuple[str, str, str, str]] = []
        seen: set[str] = set()

        for region, pref_slug, pref_ja in PREFECTURES:
            base = f"{BASE_URL}/{pref_slug}/"
            self.logger.info("一覧収集: %s (%s)", pref_ja, base)
            for detail_url in self._collect_from_listing(base):
                m = _SHOP_ID_PATTERN.search(detail_url)
                if not m:
                    continue
                sid = m.group(1)
                if sid in seen:
                    continue
                seen.add(sid)
                shop_items.append((detail_url, region, pref_ja, pref_slug))

        self.total_items = len(shop_items)
        self.logger.info("収集した店舗数: %d", self.total_items)

        for detail_url, region, pref_ja, pref_slug in shop_items:
            try:
                item = self._scrape_detail(detail_url, region, pref_ja, pref_slug)
            except Exception:
                self.logger.exception("詳細取得失敗: %s", detail_url)
                continue
            if item:
                yield item

    def _collect_from_listing(self, base_url: str) -> list[str]:
        urls: list[str] = []
        page = 1
        while page <= 50:
            page_url = base_url if page == 1 else f"{base_url}?page={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                break
            page_urls = self._extract_shop_urls(soup)
            if not page_urls:
                break
            urls.extend(page_urls)
            if not self._has_next_page(soup, page):
                break
            page += 1
        return urls

    def _extract_shop_urls(self, soup) -> list[str]:
        out: list[str] = []
        for a in soup.select('a[href*="shop-detail_"]'):
            href = a.get("href") or ""
            if not href:
                continue
            abs_url = urljoin(BASE_URL, href)
            if "cabaito.jp" in abs_url and "/shop-detail_" in abs_url:
                out.append(abs_url)
        return list(dict.fromkeys(out))

    def _has_next_page(self, soup, current_page: int) -> bool:
        pager = soup.select_one("ul.pagerList")
        if not pager:
            return False
        nums: list[int] = []
        for el in pager.select("a, b"):
            txt = el.get_text(strip=True)
            if txt.isdigit():
                nums.append(int(txt))
        return bool(nums) and current_page < max(nums)

    def _scrape_detail(self, url: str, region: str, pref_ja: str, pref_slug: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        table_info: dict[str, str] = {}
        name = ""
        business_type = ""

        for tr in soup.select("table tr"):
            th = tr.select_one("th")
            td = tr.select_one("td")
            if not th or not td:
                continue
            k = _clean(th.get_text(" ", strip=True))
            if not k:
                continue
            v = _multiline(td.get_text("\n", strip=True))
            if k == "店舗名" and not name:
                raw = _clean(v.replace("\n", " "))
                if "/" in raw:
                    parts = [p.strip() for p in raw.split("/", 1)]
                    name = parts[0]
                    business_type = parts[1] if len(parts) > 1 else ""
                else:
                    name = raw
                continue
            if k not in table_info:
                table_info[k] = v

        if not name:
            h1 = soup.select_one("h1")
            if h1:
                raw = _clean(h1.get_text(" ", strip=True))
                name = _NAME_SUFFIX_PATTERN.sub("", raw)

        dl_info: dict[str, str] = {}
        for dl in soup.select("dl"):
            dts = dl.select("dt")
            dds = dl.select("dd")
            for i, dt in enumerate(dts):
                k = _clean(dt.get_text(" ", strip=True))
                if not k or i >= len(dds):
                    continue
                v = _multiline(dds[i].get_text("\n", strip=True))
                if k not in dl_info:
                    dl_info[k] = v

        tel = table_info.get("担当TEL", "")
        if not tel:
            tel_a = soup.select_one('a[href^="tel:"]')
            if tel_a:
                tel = tel_a.get("href", "").replace("tel:", "").strip()

        address_raw = table_info.get("住所", "")
        pref_from_addr, addr_rest = _split_pref(address_raw)
        pref_final = pref_from_addr or pref_ja

        reward = ""
        body_text = soup.get_text(" ", strip=True)
        m = _REWARD_PATTERN.search(body_text)
        if m:
            reward = f"{m.group(1)}円"

        area_slug = ""
        path_parts = [p for p in urlparse(url).path.split("/") if p]
        if len(path_parts) >= 2:
            area_slug = path_parts[0]

        pay = dl_info.get("給与") or table_info.get("給与", "")
        hours = dl_info.get("勤務時間") or table_info.get("営業時間", "")
        holiday = dl_info.get("勤務日", "")
        costume = table_info.get("衣装", "")
        nearest = table_info.get("最寄り駅", "")
        qualification = dl_info.get("応募資格", "")
        benefits = dl_info.get("その他待遇", "")
        required_items = dl_info.get("体験入店に必要なもの", "")

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: pref_final,
            Schema.ADDR: addr_rest,
            Schema.TEL: tel,
            Schema.CAT_SITE: business_type,
            Schema.TIME: hours,
            Schema.HOLIDAY: holiday,
            "エリア": area_slug,
            "地方": region,
            "業種": business_type,
            "最寄り駅": nearest,
            "給与": pay,
            "衣装": costume,
            "応募資格": qualification,
            "勤務日": holiday,
            "その他待遇": benefits,
            "体験入店に必要なもの": required_items,
            "レポート謝礼金額": reward,
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = CabaitoScraper()
    scraper.execute(BASE_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
