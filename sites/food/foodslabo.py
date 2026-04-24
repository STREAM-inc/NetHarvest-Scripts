"""
foodslabo (フーズラボ) — 飲食店求人情報スクレイパー

取得対象:
    - https://foods-labo.com/offers の全求人 (約 35,044 件 / 約 1,500 ページ)

取得フロー:
    一覧 (?page=N) → 詳細 (/offers/{id}) の 2 段構成。
    ページネーションは `?page=N` を 1 から回し、article が 0 件になったら終了。

実行方法:
    python scripts/sites/food/foodslabo.py
    python bin/run_flow.py --site-id foodslabo
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


BASE_URL = "https://foods-labo.com"
START_URL = "https://foods-labo.com/offers"

# 想定総ページ数の上限 (防御的な打ち切り。article=0 なら早期終了する)
MAX_PAGES = 2000
# 連続で article=0 のページが何回続いたら打ち切るか
EMPTY_PAGE_BREAK = 3

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


def _clean(text) -> str:
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text).replace("　", " ")).strip()


class FoodsLaboScraper(StaticCrawler):
    """foodslabo (フーズラボ) 飲食店求人スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "求人ID",
        "求人タイトル",
        "ブランド名",
        "募集職種",
        "雇用形態",
        "月収",
        "アクセス",
        "最寄り駅",
        "試用期間",
        "交通費",
        "賞与",
        "社会保険",
        "福利厚生",
        "転勤",
        "応募資格",
        "選考フロー",
        "面接地",
        "お店の特徴",
        "受動喫煙",
        "会社情報",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_urls: set[str] = set()
        empty_streak = 0

        for page in range(1, MAX_PAGES + 1):
            list_url = f"{START_URL}?page={page}"
            soup = self.get_soup(list_url)
            if soup is None:
                empty_streak += 1
                if empty_streak >= EMPTY_PAGE_BREAK:
                    break
                continue

            cards = self._parse_listing_cards(soup)
            if not cards:
                empty_streak += 1
                if empty_streak >= EMPTY_PAGE_BREAK:
                    self.logger.info("article=0 が %d ページ連続。終了します (page=%d)", EMPTY_PAGE_BREAK, page)
                    break
                continue
            empty_streak = 0

            # 初回に総件数を取得して進捗表示用に反映
            if page == 1 and not self.total_items:
                total = self._extract_total_count(soup)
                if total:
                    self.total_items = total

            for card in cards:
                detail_url = card["url"]
                if detail_url in seen_urls:
                    continue
                seen_urls.add(detail_url)

                item = self._scrape_detail(detail_url, card)
                if item and item.get(Schema.NAME):
                    yield item

    def _parse_listing_cards(self, soup) -> list[dict]:
        cards = []
        for art in soup.select("article.c-article-7"):
            a = art.select_one("a.c-article-7__container[href]")
            if not a:
                continue
            href = a.get("href", "").strip()
            if not href:
                continue
            full = href if href.startswith("http") else urljoin(BASE_URL, href)

            category = _clean(art.select_one("h2.c-article-7__category").get_text() if art.select_one("h2.c-article-7__category") else "")
            title = _clean(art.select_one("h3.c-article-7__ttl").get_text() if art.select_one("h3.c-article-7__ttl") else "")
            shop = _clean(art.select_one("p.c-article-7__shop").get_text() if art.select_one("p.c-article-7__shop") else "")

            salary_dd = art.select_one("dl.c-article-7__salary dd.c-article-7__salary-body")
            salary = _clean(salary_dd.get_text() if salary_dd else "")

            loc_head = art.select_one("dl.c-article-7__location-container dt.c-article-7__location-head")
            loc_body = art.select_one("dl.c-article-7__location-container dd.c-article-7__location-body")
            pref_from_list = _clean(loc_head.get_text() if loc_head else "")
            city_from_list = _clean(loc_body.get_text() if loc_body else "")

            cards.append({
                "url": full,
                "category": category,
                "title": title,
                "shop": shop,
                "salary": salary,
                "pref": pref_from_list,
                "city": city_from_list,
            })
        return cards

    def _extract_total_count(self, soup) -> int | None:
        text = soup.get_text(" ", strip=True)
        m = re.search(r"([0-9,]{3,})\s*件", text)
        if m:
            try:
                return int(m.group(1).replace(",", ""))
            except ValueError:
                return None
        return None

    def _scrape_detail(self, url: str, card: dict) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            # 詳細取得失敗時は一覧情報だけでも拾う
            data = {
                Schema.URL: url,
                Schema.NAME: card.get("shop") or "",
                Schema.PREF: card.get("pref") or "",
                Schema.ADDR: card.get("city") or "",
                Schema.CAT_SITE: card.get("category") or "",
                "求人タイトル": card.get("title") or "",
                "月収": card.get("salary") or "",
            }
            cat = card.get("category") or ""
            first_cat = cat.split("|")[0].split(",")[0].strip() if cat else ""
            if first_cat:
                data[Schema.CAT_LV1] = first_cat
            m = re.search(r"/offers/(\d+)", url)
            if m:
                data["求人ID"] = m.group(1)
            return data if data.get(Schema.NAME) else None

        data: dict = {Schema.URL: url}

        # 求人ID
        m = re.search(r"/offers/(\d+)", url)
        if m:
            data["求人ID"] = m.group(1)

        # 店舗名: h1 の " | " 以前を採用。fallback は一覧の shop
        h1 = soup.select_one("h1")
        shop_name = card.get("shop") or ""
        if h1:
            h1_text = _clean(h1.get_text())
            if "|" in h1_text:
                shop_name = h1_text.split("|", 1)[0].strip() or shop_name
            else:
                shop_name = h1_text or shop_name
        if shop_name:
            data[Schema.NAME] = shop_name

        # 業種カテゴリ (一覧由来)
        cat = card.get("category") or ""
        if cat:
            data[Schema.CAT_SITE] = cat
            # 先頭の大分類 (例: "和食, 居酒屋 | 店長・店長候補" → "和食")
            first = cat.split("|")[0].split(",")[0].strip()
            if first:
                data[Schema.CAT_LV1] = first

        # 求人タイトル
        title = card.get("title") or ""
        ttl_el = soup.select_one(".c-article-5__ttl, .c-article-7__ttl")
        if ttl_el:
            title = _clean(ttl_el.get_text()) or title
        if title:
            data["求人タイトル"] = title

        # c-table-4__dl に集約されているキー-値を全部取る
        kv: dict[str, str] = {}
        for dl in soup.select("dl.c-table-4__dl"):
            dt = dl.find("dt")
            dd = dl.find("dd")
            if not dt or not dd:
                continue
            key = _clean(dt.get_text())
            val = _clean(dd.get_text())
            if key and key not in kv:
                kv[key] = val

        # 勤務地 → PREF / ADDR
        workplace = kv.get("勤務地", "")
        if workplace:
            pm = _PREF_PATTERN.match(workplace)
            if pm:
                data[Schema.PREF] = pm.group(1)
                data[Schema.ADDR] = workplace[pm.end():].strip()
            else:
                # 一覧の値にフォールバック
                if card.get("pref"):
                    data[Schema.PREF] = card["pref"]
                data[Schema.ADDR] = workplace
        else:
            if card.get("pref"):
                data[Schema.PREF] = card["pref"]
            if card.get("city"):
                data[Schema.ADDR] = card["city"]

        # Schema フィールド割り当て
        if kv.get("休日"):
            data[Schema.HOLIDAY] = kv["休日"]
        if kv.get("勤務時間"):
            data[Schema.TIME] = kv["勤務時間"]
        if kv.get("お店の特徴"):
            # お店の特徴はスペース区切り → pipe 区切りに正規化
            features = [f for f in re.split(r"\s+", kv["お店の特徴"]) if f]
            data[Schema.LOB] = "|".join(features) if features else kv["お店の特徴"]

        # EXTRA_COLUMNS
        extra_map = {
            "募集職種": "募集職種",
            "雇用形態": "雇用形態",
            "アクセス": "アクセス",
            "最寄り駅": "最寄り駅",
            "試用期間": "試用期間",
            "交通費": "交通費",
            "賞与": "賞与",
            "社会保険": "社会保険",
            "福利厚生": "福利厚生",
            "転勤": "転勤",
            "応募資格": "応募資格",
            "選考フロー": "選考フロー",
            "面接地": "面接地",
            "お店の特徴": "お店の特徴",
            "会社情報": "会社情報",
        }
        for src_key, dst_key in extra_map.items():
            if kv.get(src_key):
                data[dst_key] = kv[src_key]

        # 月収: 一覧の "30~40" を優先、なければ 給与 先頭行
        if card.get("salary"):
            data["月収"] = card["salary"]
        elif kv.get("給与"):
            # 給与文字列から "月収/XX~YY万円" や "月収 XX~YY" を拾う
            sm = re.search(r"月収[\s/]*([\d,]+~?[\d,]*)", kv["給与"])
            if sm:
                data["月収"] = sm.group(1)

        # 受動喫煙 (その他 に入る)
        other = kv.get("その他", "")
        if other:
            sm = re.search(r"受動喫煙[^■]*", other)
            if sm:
                data["受動喫煙"] = _clean(sm.group(0))
            else:
                data["受動喫煙"] = other

        # ブランド名: 会社情報 "◯◯について知る" から抽出 (2番目が店舗ブランド)
        ci = kv.get("会社情報", "")
        if ci:
            brand_matches = re.findall(r"([^\s]+?)\s*について知る", ci)
            if len(brand_matches) >= 2:
                data["ブランド名"] = brand_matches[1]
            elif brand_matches:
                data["ブランド名"] = brand_matches[0]

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = FoodsLaboScraper()
    scraper.execute(START_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
