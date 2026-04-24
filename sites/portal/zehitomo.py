"""
ゼヒトモ — プロ(事業者)マッチングポータル

取得対象:
    - トップページ (https://www.zehitomo.com/) に掲載される
      カテゴリ別「○○のプロ」セクションの代表プロ一覧。
      各プロのプロフィールページから JSON-LD・各種セクションの情報を取得。

取得フロー:
    1. トップページから a.HomeContentRedesign_pro-card__* を全件抽出
       (一覧上の表示: 代表者名 / 評価 / エリア / 一言PR抜粋 / セクション見出し)
    2. 各プロのプロフィールページ /profile/{slug}/pro へ遷移
       JSON-LD (LocalBusiness) から 法人名・郵便番号・都道府県・住所・評価情報、
       breadcrumb から カテゴリ階層、本文 h3 から 一言PR / 企業情報 / 経歴・資格 /
       お仕事で心がけていること を抽出。

実行方法:
    python scripts/sites/portal/zehitomo.py
    python bin/run_flow.py --site-id zehitomo
"""

import json
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

BASE_URL = "https://www.zehitomo.com"

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_RATING_PATTERN = re.compile(r"([\d.]+)\s*\(?(\d+)?件?\)?")
_POST_CODE_RAW = re.compile(r"^(\d{3})(\d{4})$")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).replace("　", " ")).strip()


def _format_postal(raw: str) -> str:
    if not raw:
        return ""
    raw = raw.strip()
    m = _POST_CODE_RAW.match(raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    return raw


class ZehitomoScraper(StaticCrawler):
    """ゼヒトモ (zehitomo.com) プロ情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "rating",
        "review_count",
        "area_text",
        "desc_short",
        "one_line_pr",
        "company_intro",
        "career_qualifications",
        "work_principles",
        "avatar_image_url",
        "instagram_url",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)
        cards = soup.select('a[class*="HomeContentRedesign_pro-card"]')

        # Deduplicate by profile URL while keeping section heading per first occurrence
        seen: dict[str, dict] = {}
        for card in cards:
            href = card.get("href", "")
            if not href or not href.startswith("/profile/"):
                continue
            detail_url = urljoin(BASE_URL, href)
            if detail_url in seen:
                continue
            section_heading = self._find_section_heading(card)
            seen[detail_url] = {
                "card": card,
                "section_heading": section_heading,
            }

        self.total_items = len(seen)
        self.logger.info(f"トップページから {self.total_items} プロを検出")

        for detail_url, ctx in seen.items():
            try:
                item = self._scrape_detail(detail_url, ctx["card"], ctx["section_heading"])
                if item:
                    yield item
            except Exception as e:
                self.logger.error(f"詳細ページの解析に失敗 {detail_url}: {e}")
                continue

    def _find_section_heading(self, card) -> str:
        """カード祖先から「○○のプロ」セクション見出しを探す"""
        cur = card.parent
        while cur is not None:
            h3 = cur.find("h3")
            if h3:
                text = _clean(h3.get_text())
                if text.endswith("のプロ"):
                    # Strip trailing "のプロ" → カテゴリ名
                    return text[:-3]
            cur = cur.parent
        return ""

    def _scrape_detail(self, url: str, card, section_heading: str) -> dict | None:
        # List card extracted info
        rep_name = _clean(self._select_text(card, '[class*="HomeContentRedesign_pro-name"]'))
        rating_text = _clean(self._select_text(card, '[class*="HomeContentRedesign_pro-rating"]'))
        area_text = _clean(self._select_text(card, '[class*="HomeContentRedesign_pro-area"]'))
        desc_short = _clean(self._select_text(card, '[class*="HomeContentRedesign_pro-desc"]'))
        avatar_img = card.select_one('[class*="HomeContentRedesign_pro-avatar"] img')
        avatar_url = avatar_img.get("src", "") if avatar_img else ""

        rating = ""
        review_count = ""
        m = _RATING_PATTERN.search(rating_text.replace("★", ""))
        if m:
            rating = m.group(1) or ""
            review_count = m.group(2) or ""

        # Detail page
        soup = self.get_soup(url)

        local_business, breadcrumb_list = self._parse_jsonld(soup)

        # Company name from h1, fallback to JSON-LD name (strip trailing area)
        company_name = _clean(self._select_text(soup, "h1"))
        if not company_name and local_business:
            ld_name = local_business.get("name", "")
            company_name = _clean(ld_name.split("|")[0])

        # Address
        post_code = ""
        pref = ""
        addr = ""
        if local_business:
            addr_obj = local_business.get("address") or {}
            post_code = _format_postal(addr_obj.get("postalCode", ""))
            pref = _clean(addr_obj.get("addressRegion", ""))
            locality = _clean(addr_obj.get("addressLocality", ""))
            addr = f"{pref}{locality}".strip()

        # Fallback: parse from area_text
        if not pref and area_text:
            m = _PREF_PATTERN.match(area_text)
            if m:
                pref = m.group(1)
                if not addr:
                    addr = area_text

        # Description (LOB) from JSON-LD
        lob = ""
        if local_business:
            lob = _clean(local_business.get("description", ""))

        # Categories from breadcrumb (skip first "Zehitomo" and last item which is the company)
        cats = []
        if breadcrumb_list:
            items = breadcrumb_list.get("itemListElement", []) or []
            names = [
                _clean((it.get("item") or {}).get("name", ""))
                for it in items
            ]
            # Drop "Zehitomo" head + company tail
            if names and names[0].lower() == "zehitomo":
                names = names[1:]
            if names:
                # Drop final entry if it equals the company name
                if names[-1] == company_name:
                    names = names[:-1]
            # Filter out prefecture/city slots (Schema has dedicated PREF/ADDR)
            cats = [n for n in names if n and n != pref and not n.endswith(("市", "区", "町", "村"))]

        cat_lv1 = cats[0] if len(cats) > 0 else ""
        cat_lv2 = cats[1] if len(cats) > 1 else ""
        cat_lv3 = cats[2] if len(cats) > 2 else ""
        cat_nm = cats[3] if len(cats) > 3 else ""

        # h3-section bodies on detail page
        one_line_pr = self._extract_section_body(soup, "一言PR")
        company_intro = self._extract_section_body(soup, "企業情報・自己紹介")
        career_quals = self._extract_section_body(soup, "経歴・資格")
        work_principles = self._extract_section_body(soup, "お仕事で心がけていること")

        # Instagram link (external, not zehitomo's own)
        insta_url = ""
        for a in soup.select('a[href*="instagram.com"]'):
            href = a.get("href", "")
            if "zehitomo" not in href.lower():
                insta_url = href
                break

        return {
            Schema.NAME: company_name,
            Schema.REP_NM: rep_name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.LOB: lob,
            Schema.CAT_LV1: cat_lv1,
            Schema.CAT_LV2: cat_lv2,
            Schema.CAT_LV3: cat_lv3,
            Schema.CAT_NM: cat_nm,
            Schema.CAT_SITE: section_heading,
            Schema.INSTA: insta_url,
            Schema.URL: url,
            "rating": rating,
            "review_count": review_count,
            "area_text": area_text,
            "desc_short": desc_short,
            "one_line_pr": one_line_pr,
            "company_intro": company_intro,
            "career_qualifications": career_quals,
            "work_principles": work_principles,
            "avatar_image_url": avatar_url,
            "instagram_url": insta_url,
        }

    @staticmethod
    def _select_text(node, selector: str) -> str:
        el = node.select_one(selector)
        return el.get_text(separator=" ", strip=True) if el else ""

    @staticmethod
    def _parse_jsonld(soup):
        """Returns (local_business_dict_or_None, breadcrumb_dict_or_None)"""
        lb = None
        bc = None
        for s in soup.select('script[type="application/ld+json"]'):
            try:
                data = json.loads(s.string or s.get_text() or "")
            except (json.JSONDecodeError, TypeError):
                continue
            t = data.get("@type") if isinstance(data, dict) else None
            if t == "LocalBusiness" and lb is None:
                lb = data
            elif t == "BreadcrumbList" and bc is None:
                bc = data
        return lb, bc

    @staticmethod
    def _extract_section_body(soup, heading_text: str) -> str:
        """h3 見出しの直後にある本文テキストを取得"""
        for h3 in soup.find_all("h3"):
            if _clean(h3.get_text()) == heading_text:
                # The next sibling block usually holds the body text
                sibling = h3.find_next_sibling()
                if sibling:
                    return _clean(sibling.get_text(separator=" "))
                # Fallback: parent next text
                parent = h3.parent
                if parent:
                    text = _clean(parent.get_text(separator=" "))
                    return text.replace(heading_text, "", 1).strip()
        return ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = ZehitomoScraper()
    scraper.execute(BASE_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
