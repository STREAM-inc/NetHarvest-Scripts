"""
ヨルナビ — スナック・ラウンジ求人ポータル（yorumachi.jp）

取得対象:
    - 全国7エリア（東京・神奈川・大阪・兵庫・京都・奈良・和歌山）の店舗求人情報

取得フロー:
    各エリアURL → ページネーション（?page=N） → 詳細ページ

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/yorumachi.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id yorumachi
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

BASE_URL = "https://yorumachi.jp"
AREA_PATHS = [
    "/tokyo/",
    "/kanagawa/",
    "/osaka/",
    "/hyogo/",
    "/kyoto/",
    "/nara/",
    "/wakayama/",
]

_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県"
    r"|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県"
    r"|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県"
    r"|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_BENEFIT_KEYWORDS = {"払い", "OK", "ok", "歓迎", "なし", "あり", "可", "チカ", "研修", "優遇"}
_COMPANY_ENTITY_RE = re.compile(r"株式会社|有限会社|合同会社|合資会社|NPO法人|一般社団法人|公益社団法人|協同組合")
_SHOP_TYPE_RE = re.compile(r"スナック|バー|クラブ|ラウンジ|キャバクラ|パブ|ホスト|カラオケ|ガールズ|ナイト")
_PERSON_NAME_RE = re.compile(r"^[一-龥ぁ-んァ-ヶ　\s]{2,10}$")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class YorumachiScraper(StaticCrawler):
    """ヨルナビ スナック・ラウンジ求人スクレイパー（yorumachi.jp）"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["職種", "給与", "最寄駅", "待遇", "運営会社"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        detail_urls = self._collect_detail_urls()
        self.total_items = len(detail_urls)
        self.logger.info("詳細URL収集完了: %d 件", len(detail_urls))
        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("詳細取得失敗 %s: %s", detail_url, e)

    def _collect_detail_urls(self) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        for area in AREA_PATHS:
            area_url = BASE_URL + area
            page = 1
            while True:
                page_url = area_url if page == 1 else f"{area_url}?page={page}"
                soup = self.get_soup(page_url)
                if soup is None:
                    break
                found_on_page = False
                for a in soup.select("a[href]"):
                    href = a.get("href", "").strip()
                    if "/job/" in href and href.endswith(".html"):
                        full = href if href.startswith("http") else urljoin(BASE_URL, href)
                        if full not in seen:
                            seen.add(full)
                            urls.append(full)
                            found_on_page = True
                if not found_on_page:
                    break
                page += 1
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data = {Schema.URL: url}

        # 店舗名（h1 全文を雇用主判定の比較用に保持）
        shop_name_full = ""
        h1 = soup.select_one("h1")
        if h1:
            shop_name_full = h1.get_text(strip=True)
            name = re.split(r"[（\(]", shop_name_full)[0].strip()
            if name:
                data[Schema.NAME] = name

        # dt/dd ペアからフィールドを取得
        for dt in soup.select("dt"):
            key = dt.get_text(strip=True)
            dd = dt.find_next_sibling("dd")
            if not dd:
                continue
            val = _clean(dd.get_text(" ", strip=True))
            if not val:
                continue

            if "業種" in key:
                data[Schema.CAT_SITE] = val
            elif "住所" in key or "勤務地" in key:
                addr_val = _clean(" ".join(
                    t.get_text(" ", strip=True)
                    for t in dd.children
                    if getattr(t, "name", None) != "a"
                ))
                if not addr_val:
                    addr_val = val
                pref_m = _PREF_RE.search(addr_val)
                if pref_m:
                    data[Schema.PREF] = pref_m.group(1)
                    data[Schema.ADDR] = addr_val[addr_val.index(pref_m.group(1)) + len(pref_m.group(1)):].strip()
                else:
                    data[Schema.ADDR] = addr_val
            elif "営業時間" in key:
                data[Schema.TIME] = val
            elif "雇用主" in key:
                employer_type = self._classify_employer(val, shop_name_full)
                if employer_type == "person":
                    data[Schema.REP_NM] = val
                elif employer_type == "company":
                    data["運営会社"] = val
            elif "職種" in key and "職種" not in data:
                data["職種"] = val
            elif "給与" in key and "給与" not in data:
                data["給与"] = val
            elif "最寄駅" in key:
                data["最寄駅"] = val

        # 電話番号
        tel_a = soup.select_one("a[href^='tel:']")
        if tel_a:
            data[Schema.TEL] = tel_a.get_text(strip=True)

        # 待遇タグ（日払いOKなどのタグリストを検出）
        for ul in soup.select("ul"):
            items = [li.get_text(strip=True) for li in ul.select("li") if li.get_text(strip=True)]
            if items and any(kw in item for item in items for kw in _BENEFIT_KEYWORDS):
                data["待遇"] = " / ".join(items)
                break

        if not data.get(Schema.NAME):
            return None
        return data

    @staticmethod
    def _classify_employer(employer: str, shop_name_full: str) -> str:
        """雇用主の値を分類する。
        Returns: "person" | "company" | "shop"
        """
        # 法人格キーワードがあれば会社名
        if _COMPANY_ENTITY_RE.search(employer):
            return "company"
        # 業種キーワードがあれば店舗名
        if _SHOP_TYPE_RE.search(employer):
            return "shop"
        # 店舗名（h1全文）との部分一致（英語店舗名対応）
        if shop_name_full:
            e = employer.lower()
            s = shop_name_full.lower()
            if e in s or s in e:
                return "shop"
        # 漢字・ひらがな・カタカナのみで構成される短い文字列を個人名と判定
        if _PERSON_NAME_RE.match(employer):
            return "person"
        # それ以外（英語店舗名など）は店舗名扱い
        return "shop"


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = YorumachiScraper()
    scraper.execute("https://yorumachi.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
