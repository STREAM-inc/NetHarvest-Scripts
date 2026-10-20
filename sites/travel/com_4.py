"""
一休.com宿泊施設 — www.ikyu.com の宿泊施設情報スクレイパー (東北6県＋新潟県)

対象サイト: https://www.ikyu.com/

取得対象 (備考による絞り込み):
    青森・岩手・宮城・秋田・山形・福島・新潟 の宿泊施設
    (施設名/都道府県/住所/TEL/施設タイプ/客室数/宿泊料金目安/温泉の有無/施設ページURL)

取得フロー:
    1. 列挙: ルート URL から派生した都道府県別一覧 `/{地方}/{エリアID}/` →
       `/{地方}/{エリアID}/p{N}/` (20件/ページ)。施設リンクは `/{8桁ID}/`。
       リンクが 0 件になったページで打ち切る (範囲外ページも 200 で空一覧を返す)。
    2. 詳細: `/{8桁ID}/` を静的取得。
       - JSON-LD (@type=Hotel): name / telephone / address / priceRange
         (priceRange = 2名1室・1泊の最安プラン料金。人数条件を併記して出力)
       - __NUXT_DATA__ (devalue 形式): type (HOTEL/RESORT_HOTEL/INN/BUSINESS…) /
         roomCount / baths[].springGround (温泉)
       - 施設タイプの日本語名はパンくず 2 番目 ("全国 旅館" 等)。HOTEL はパンくずに
         種別が付かないため enum → 日本語の対応表で補う。
       - 温泉: JSON-LD の amenityFeature Onsen は欠落・false 誤りがある
         (例: 八甲田ホテルは Onsen=false だが大浴場 springGround=true)。
         baths[].springGround を優先し、JSON-LD Onsen は補助に使う。
    3. 一覧には他エリアのおすすめ施設が混ざり得るため、詳細の都道府県
       (JSON-LD addressRegion) で対象 7 県に絞り込む。

    ⚠ アクセス上の注意 (2026-10 調査):
      Chrome UA は全パス 403 (Fastly)。Safari UA なら 200 (com_2 と同じ挙動)。
      利用規約 (/terms/service/) 第10条にスクレイピング明示禁止条項なし。
      robots.txt は /search?・/*aca*acr 等を Disallow。本クローラーは
      許可されている都道府県一覧 (/{地方}/{ID}/pN/) と施設ページのみ使う。

実行方法:
    python scripts/sites/travel/com_4.py
    docker compose exec worker python /app/bin/run_flow.py --site-id com_4
"""

import json
import logging
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4
from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 施設詳細のパスは 8 桁 ID (例: /00000378/)
_DETAIL_PATH_RE = re.compile(r"^/(\d{8})/(?:[?#]|$)")
_ROOM_RE = re.compile(r"(\d[\d,]*)\s*室")
# JSON-LD priceRange: "2名で60,588円～" (一覧の既定条件 ppc=2 / rc=1 / 1泊 での最安プラン)
_PRICE_RE = re.compile(r"(\d+)\s*名で\s*([\d,]+)\s*円")


class IkyuAccommodationScraper(StaticCrawler):
    """一休.com宿泊施設 スクレイパー (東北6県＋新潟県)"""

    DELAY = 3.0
    TIMEOUT = 30

    # Fastly WAF は UA を見ている。Chrome UA / 既定 UA は 403、Safari UA のみ 200。
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/17.5 Safari/605.1.15"
    )

    EXTRA_COLUMNS = ["施設タイプ", "客室数", "宿泊料金目安", "温泉の有無"]

    # 対象都道府県 → 都道府県別一覧のパス (ルート URL からの相対)
    TARGET_PREFS = {
        "青森県": "/tohoku/120210/",
        "岩手県": "/tohoku/120220/",
        "宮城県": "/tohoku/120230/",
        "秋田県": "/tohoku/120240/",
        "山形県": "/tohoku/120250/",
        "福島県": "/tohoku/120260/",
        "新潟県": "/koshinetsu/210551/",
    }
    MAX_PAGES = 100  # 暴走ガード

    # __NUXT_DATA__ の type enum → 日本語 (パンくずに種別が無い場合の補完用)
    TYPE_LABELS = {
        "HOTEL": "ホテル",
        "RESORT_HOTEL": "リゾートホテル",
        "INN": "旅館",
        "BUSINESS": "ビジネスホテル",
    }

    # ------------------------------------------------------------------ #
    # enumeration
    # ------------------------------------------------------------------ #
    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        for pref, path in self.TARGET_PREFS.items():
            list_url = urljoin(url, path)
            for page in range(1, self.MAX_PAGES + 1):
                page_url = list_url if page == 1 else urljoin(list_url, f"p{page}/")
                soup = self.get_soup(page_url)
                if soup is None:
                    break
                ids = self._extract_ids(soup)
                if not ids:
                    break
                for acc_id in ids:
                    if acc_id in seen:
                        continue
                    seen.add(acc_id)
                    item = self._parse_detail(urljoin(url, f"/{acc_id}/"))
                    if item:
                        yield item

    @staticmethod
    def _extract_ids(soup: bs4.BeautifulSoup) -> list[str]:
        ids: list[str] = []
        for a in soup.select("a[href]"):
            m = _DETAIL_PATH_RE.match(a["href"])
            if m and m.group(1) not in ids:
                ids.append(m.group(1))
        return ids

    # ------------------------------------------------------------------ #
    # detail
    # ------------------------------------------------------------------ #
    def _parse_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        ld = self._hotel_jsonld(soup)
        addr = ld.get("address") or {}
        pref = (addr.get("addressRegion") or "").strip()
        if pref not in self.TARGET_PREFS:
            logger.debug("対象外の都道府県のためスキップ: %s (%s)", detail_url, pref)
            return None

        acc = self._nuxt_accommodation(soup)

        name = (ld.get("name") or acc.get("name") or "").strip()
        if not name:
            h1 = soup.find("h1")
            name = h1.get_text(strip=True) if h1 else ""

        room = acc.get("roomCount")
        if room in (None, ""):
            th = soup.find("th", string=re.compile(r"^\s*客室数\s*$"))
            td = th.find_next_sibling("td") if th else None
            m = _ROOM_RE.search(td.get_text()) if td else None
            room = m.group(1).replace(",", "") if m else ""

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: (addr.get("postalCode") or "").strip(),
            Schema.ADDR: (addr.get("addressLocality") or "").strip(),
            Schema.TEL: (ld.get("telephone") or "").strip(),
            Schema.URL: detail_url,
            "施設タイプ": self._facility_type(soup, acc.get("type")),
            "客室数": str(room) if room not in (None, "") else "",
            "宿泊料金目安": self._price(ld.get("priceRange")),
            "温泉の有無": self._onsen(ld, acc.get("baths")),
        }

    @staticmethod
    def _hotel_jsonld(soup: bs4.BeautifulSoup) -> dict:
        for tag in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(tag.string or "")
            except json.JSONDecodeError:
                continue
            for obj in data if isinstance(data, list) else [data]:
                if isinstance(obj, dict) and obj.get("@type") in ("Hotel", "LodgingBusiness", "Resort"):
                    return obj
        return {}

    @staticmethod
    def _nuxt_accommodation(soup: bs4.BeautifulSoup) -> dict:
        """__NUXT_DATA__ (devalue 形式のフラット配列) から施設オブジェクトを復元する。"""
        tag = soup.find("script", id="__NUXT_DATA__")
        if not tag or not tag.string:
            return {}
        try:
            arr = json.loads(tag.string)
        except json.JSONDecodeError:
            return {}

        def resolve(v, depth=0):
            if depth > 6:
                return None
            if isinstance(v, int) and not isinstance(v, bool):
                if not 0 <= v < len(arr):
                    return None
                v = arr[v]
            if isinstance(v, dict):
                return {k: resolve(x, depth + 1) for k, x in v.items()}
            if isinstance(v, list):
                if v and v[0] in ("Reactive", "ShallowReactive", "Ref", "ShallowRef"):
                    return resolve(v[1], depth)
                return [resolve(x, depth + 1) for x in v]
            return v

        for obj in arr:
            if isinstance(obj, dict) and "accommodationId" in obj and "roomCount" in obj:
                return {k: resolve(obj[k]) for k in ("name", "type", "roomCount", "baths") if k in obj}
        return {}

    def _facility_type(self, soup: bs4.BeautifulSoup, type_code) -> str:
        # パンくず 2 番目: "全国 旅館" / "全国 リゾートホテル" (HOTEL は "全国" のみ)
        crumb = soup.select_one('[itemtype*="BreadcrumbList"] meta[itemprop="position"][content="2"]')
        if crumb:
            li = crumb.parent
            name_meta = li.find("meta", itemprop="name") if li else None
            label = (name_meta.get("content") or "") if name_meta else ""
            label = re.sub(r"^全国\s*", "", label).strip()
            if label:
                return label
        if isinstance(type_code, str) and type_code:
            return self.TYPE_LABELS.get(type_code, type_code)
        return ""

    @staticmethod
    def _price(price_range) -> str:
        """最安プラン料金を「N円～（2名1室・1泊あたり合計）」形式に整形する。"""
        raw = (price_range or "").strip() if isinstance(price_range, str) else ""
        m = _PRICE_RE.search(raw)
        if not m:
            return raw
        return f"{m.group(2)}円～（{m.group(1)}名1室・1泊あたり合計）"

    @staticmethod
    def _onsen(ld: dict, baths) -> str:
        if isinstance(baths, list) and any(isinstance(b, dict) and b.get("springGround") for b in baths):
            return "あり"
        ld_onsen = None
        for feat in ld.get("amenityFeature") or []:
            if isinstance(feat, dict) and feat.get("name") == "Onsen":
                ld_onsen = feat.get("value")
        if ld_onsen is True:
            return "あり"
        if ld_onsen is False or isinstance(baths, list):
            return "なし"
        return ""


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = IkyuAccommodationScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.ikyu.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
