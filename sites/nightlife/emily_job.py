"""
体入エミリー (emily-job.jp) — ナイトワーク求人サイト掲載店舗情報スクレイパー

取得対象:
    - キャバクラ / ガールズバー / クラブ / コンカフェ / ラウンジ / スナック 掲載店舗

取得フロー:
    1. 6ジャンル × 47都道府県 の一覧ページを巡回 (/{genre}/{pref}/?page=N)
    2. 各一覧ページから店舗詳細リンクを収集 (a.shopInfo__name)
    3. 各店舗詳細ページから店舗情報を抽出 (.businessInfo__item など)

実行方法:
    python scripts/sites/nightlife/emily_job.py
    python bin/run_flow.py --site-id emily_job
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


GENRES = {
    "cabaretclub": "キャバクラ",
    "club":        "クラブ",
    "concafe":     "コンカフェ",
    "girlsbar":    "ガールズバー",
    "lounge":      "ラウンジ",
    "snack":       "スナック",
}

PREFECTURES = [
    "hokkaido", "aomori", "iwate", "miyagi", "akita", "yamagata", "fukushima",
    "ibaraki", "tochigi", "gunma", "saitama", "chiba", "tokyo", "kanagawa",
    "niigata", "toyama", "ishikawa", "fukui", "yamanashi", "nagano",
    "gifu", "shizuoka", "aichi", "mie",
    "shiga", "kyoto", "osaka", "hyogo", "nara", "wakayama",
    "tottori", "shimane", "okayama", "hiroshima", "yamaguchi",
    "tokushima", "kagawa", "ehime", "kochi",
    "fukuoka", "saga", "nagasaki", "kumamoto", "oita", "miyazaki", "kagoshima", "okinawa",
]

BASE_URL = "https://emily-job.jp"

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

_POST_PATTERN = re.compile(r"〒?\s*(\d{3}-?\d{4})")


def _extract_social_links(soup):
    """詳細ページ上の店舗SNSリンクを分類して返す。"""
    social = {"instagram": None, "tiktok": None, "x": None, "facebook": None, "line": None, "youtube": None}
    # 店舗情報セクション配下のSNSリンク (公式Emilyアカウントを除外するためセクション限定)
    section = soup.select_one(".shopInformation") or soup
    for a in section.select(".snsList a[href], .businessInfo__item a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        low = href.lower()
        if "instagram.com" in low and not social["instagram"]:
            social["instagram"] = href
        elif "tiktok.com" in low and not social["tiktok"]:
            social["tiktok"] = href
        elif ("twitter.com" in low or "x.com" in low) and not social["x"]:
            social["x"] = href
        elif "facebook.com" in low and not social["facebook"]:
            social["facebook"] = href
        elif "line.me" in low and not social["line"]:
            social["line"] = href
        elif "youtube.com" in low and not social["youtube"]:
            social["youtube"] = href
    return social


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def _split_address(raw: str) -> tuple[str, str, str]:
    """住所テキストから (郵便番号, 都道府県, 住所以降) を返す。"""
    if not raw:
        return "", "", ""
    post = ""
    m = _POST_PATTERN.search(raw)
    if m:
        post = m.group(1)
        if "-" not in post:
            post = f"{post[:3]}-{post[3:]}"
    # 郵便番号と駅情報を除いた住所本体を抽出
    text = _POST_PATTERN.sub("", raw).strip()
    # 「xxx駅から徒歩...」の類を除去
    text = re.split(r"[ 　]+(?=[^\s]*駅[^\s]*から)", text)[0].strip()
    pref = ""
    addr = text
    pm = _PREF_PATTERN.match(text)
    if pm:
        pref = pm.group(1)
        addr = text[pm.end():].strip()
    return post, pref, addr


class EmilyJobScraper(StaticCrawler):
    """体入エミリー (emily-job.jp) の店舗情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "カナ名称",
        "エリア",
        "最寄駅",
        "職種",
        "雇用形態",
        "応募資格",
        "体入時給",
        "入店時給",
        "給与",
        "交通費",
        "勤務日",
        "待遇",
        "1日の平均来客数",
        "キャッチコピー",
        "ジャンル",
        "TikTokアカウント店舗",
    ]

    def parse(self, url: str):
        shop_links: list[tuple[str, str]] = []  # (detail_url, genre_ja)
        seen: set[str] = set()

        for genre_code, genre_ja in GENRES.items():
            for pref in PREFECTURES:
                for detail_url in self._collect_from_listing(genre_code, pref):
                    if detail_url in seen:
                        continue
                    seen.add(detail_url)
                    shop_links.append((detail_url, genre_ja))

        self.total_items = len(shop_links)
        self.logger.info("収集した店舗数: %d", self.total_items)

        for detail_url, genre_ja in shop_links:
            item = self._scrape_detail(detail_url, genre_ja)
            if item:
                yield item

    def _collect_from_listing(self, genre_code: str, pref: str) -> list[str]:
        """/{genre}/{pref}/?page=N を全ページ巡回して店舗詳細URLを返す。"""
        base = f"{BASE_URL}/{genre_code}/{pref}/"
        first = self.get_soup(base)
        if first is None:
            return []

        # 一覧が空ならスキップ
        if not first.select("a.shopInfo__name"):
            return []

        total_pages = self._get_total_pages(first)
        urls = self._extract_shop_urls(first)
        for page in range(2, total_pages + 1):
            soup = self.get_soup(f"{base}?page={page}")
            if soup is None:
                continue
            urls.extend(self._extract_shop_urls(soup))

        return list(dict.fromkeys(urls))

    def _extract_shop_urls(self, soup) -> list[str]:
        return [a.get("href") for a in soup.select("a.shopInfo__name") if a.get("href")]

    def _get_total_pages(self, soup) -> int:
        nums = []
        for a in soup.select(".pagination .pager--num"):
            txt = a.get_text(strip=True)
            if txt.isdigit():
                nums.append(int(txt))
        return max(nums) if nums else 1

    def _scrape_detail(self, url: str, genre_ja: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        name = _clean(soup.select_one(".shopName").get_text(" ", strip=True)) if soup.select_one(".shopName") else ""
        kana = _clean(soup.select_one(".kataName").get_text(" ", strip=True)) if soup.select_one(".kataName") else ""
        location = _clean(soup.select_one(".shopLocation").get_text(" ", strip=True)) if soup.select_one(".shopLocation") else ""

        # .businessInfo__item からラベル-内容を辞書化
        info: dict[str, str] = {}
        for li in soup.select(".businessInfo__item"):
            label_el = li.select_one(".label")
            content_el = li.select_one(".content")
            if not label_el or not content_el:
                continue
            k = _clean(label_el.get_text(" ", strip=True))
            v = _clean(content_el.get_text(" ", strip=True))
            if k and k not in info:
                info[k] = v

        # TEL: 店舗情報セクション内の tel: リンクを優先
        tel = ""
        section = soup.select_one(".shopInformation") or soup
        tel_a = section.select_one('a[href^="tel:"]') or soup.select_one('a[href^="tel:"]')
        if tel_a:
            tel = tel_a.get("href", "").replace("tel:", "").strip()

        # HP: businessInfo の 公式HP 行内の最初のアンカー
        hp = ""
        for li in soup.select(".businessInfo__item"):
            label_el = li.select_one(".label")
            if label_el and _clean(label_el.get_text()) == "公式HP":
                a = li.select_one(".content a[href]")
                if a:
                    hp = a.get("href", "").strip()
                break

        # SNS
        social = _extract_social_links(soup)

        # 住所の分解
        post, pref, addr_rest = _split_address(info.get("住所", ""))

        # キャッチコピー
        sticker = soup.select_one(".sticker__content")
        catchphrase = _clean(sticker.get_text(" ", strip=True)) if sticker else ""

        # 業種 (CAT_SITE): businessInfo の "業種" を優先、なければジャンル名
        cat_site = info.get("業種") or genre_ja

        item = {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.NAME_KANA: kana,
            Schema.PREF: pref,
            Schema.POST_CODE: post,
            Schema.ADDR: addr_rest,
            Schema.TEL: tel,
            Schema.LOB: info.get("職種", ""),
            Schema.CAT_SITE: cat_site,
            Schema.HP: hp,
            Schema.TIME: info.get("勤務時間", ""),
            Schema.HOLIDAY: info.get("休日", "") or info.get("勤務日", ""),
            Schema.INSTA: social["instagram"] or "",
            Schema.TIKTOK: social["tiktok"] or "",
            Schema.X: social["x"] or "",
            Schema.FB: social["facebook"] or "",
            Schema.LINE: social["line"] or "",
            # EXTRA_COLUMNS
            "カナ名称": kana,
            "エリア": info.get("エリア", "") or location,
            "最寄駅": info.get("最寄駅", ""),
            "職種": info.get("職種", ""),
            "雇用形態": info.get("雇用形態", ""),
            "応募資格": info.get("応募資格", ""),
            "体入時給": info.get("体入時給", ""),
            "入店時給": info.get("入店時給", ""),
            "給与": info.get("給与", ""),
            "交通費": info.get("交通費", ""),
            "勤務日": info.get("勤務日", ""),
            "待遇": info.get("待遇", ""),
            "1日の平均来客数": info.get("1日の平均来客数", ""),
            "キャッチコピー": catchphrase,
            "ジャンル": genre_ja,
            "TikTokアカウント店舗": social["tiktok"] or "",
        }
        return item


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = EmilyJobScraper()
    scraper.execute(BASE_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
