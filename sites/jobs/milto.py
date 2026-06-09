"""
version 1.0.0 niwai 
milto（ミルト）— 自社採用ページ構築プラットフォーム 求人スクレイパー

対象サイト: https://mil-to.com/
取得対象:
    - sitemap_detail.xml に掲載された全求人詳細ページ
      (https://mil-to.com/{client_slug}/job/{job_id}/)

取得フロー:
    sitemap_detail.xml から求人詳細URL一覧を取得
    → 各詳細ページ (.job-info-box .info-content) から th/td 相当の項目を抽出
    → 必要に応じてクライアント採用ページから HP / TEL を補完

実行方法:
    python scripts/sites/jobs/milto.py
    python bin/run_flow.py --site-id milto
"""

import re
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Generator
from urllib.parse import urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE_URL = "https://mil-to.com"
SITEMAP_DETAIL_URL = f"{BASE_URL}/sitemap_xml/sitemap_detail.xml"

_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_RE = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_TEL_RE = re.compile(
    r"(?:代表電話|TEL|Tel|電話)[^\d]{0,10}([\d\-()（）\s]{8,20})"
)
_JOB_URL_RE = re.compile(
    r"^https://mil-to\.com/(?P<slug>[^/]+)/job/(?P<job_id>\d+)/?$"
)
_SKIP_HP_HOSTS = (
    "mil-to.com",
    "google.",
    "kyujin-ascom.com",
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "youtube.com",
    "bootstrapcdn.com",
    "maxcdn.bootstrapcdn.com",
)


def _normalize_key(key: str) -> str:
    return re.sub(r"\s+", "", key)


class MiltoScraper(StaticCrawler):
    """milto（ミルト）求人 スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "店舗名",
        "雇用形態",
        "勤務地",
        "最寄駅",
        "勤務時間",
    ]

    def prepare(self):
        """トップページでセッションを温める。"""
        self.get_soup(BASE_URL)
        time.sleep(self.DELAY)

    def parse(self, url: str) -> Generator[dict, None, None]:
        self._company_cache: dict[str, dict[str, str]] = {}
        detail_urls = self._load_detail_urls()
        self.total_items = len(detail_urls)
        self.logger.info("求人詳細URL: %d 件", self.total_items)

        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as exc:
                self.logger.warning("詳細ページ取得失敗: %s (%s)", detail_url, exc)

    def _load_detail_urls(self) -> list[str]:
        """sitemap_detail.xml から求人詳細URL一覧を取得する。"""
        self.logger.info("sitemap取得: %s", SITEMAP_DETAIL_URL)
        time.sleep(self.DELAY)
        resp = self.session.get(SITEMAP_DETAIL_URL, timeout=120)
        resp.raise_for_status()

        root = ET.fromstring(resp.content)
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        urls: list[str] = []
        for url_el in root.findall("sm:url", ns):
            loc = url_el.find("sm:loc", ns)
            if loc is not None and loc.text:
                urls.append(loc.text.strip())
        return urls

    def _extract_pairs(self, soup: bs4.BeautifulSoup) -> dict[str, str]:
        """詳細ページの .info-content から項目辞書を構築する。"""
        pairs: dict[str, str] = {}
        for block in soup.select(".job-info-box .info-content"):
            title_el = block.select_one(".title")
            detail_el = block.select_one(".detail")
            if not title_el or not detail_el:
                continue

            key = title_el.get_text(strip=True)
            norm_key = _normalize_key(key)

            if norm_key in {"特徴", "メリット"}:
                merits = [
                    span.get_text(strip=True)
                    for span in detail_el.select(".merit-icon-inn dd span")
                ]
                value = ", ".join(m for m in merits if m)
            else:
                value = detail_el.get_text("\n", strip=True)
                value = re.sub(r"\n{2,}", "\n", value).strip()

            if not value:
                continue

            if key in pairs:
                pairs[key] = f"{pairs[key]}\n{value}"
            else:
                pairs[key] = value
        return pairs

    def _pick_value(self, pairs: dict[str, str], *keys: str) -> str:
        for key in keys:
            if key in pairs and pairs[key]:
                return pairs[key]
            norm = _normalize_key(key)
            for pair_key, pair_val in pairs.items():
                if _normalize_key(pair_key) == norm and pair_val:
                    return pair_val
        return ""

    def _split_address(self, address_raw: str) -> tuple[str, str, str]:
        """住所文字列から郵便番号・都道府県・市区町村以降を分離する。"""
        if not address_raw:
            return "", "", ""

        addr = address_raw.replace("\n", " ")
        addr = re.sub(r"\s+", " ", addr).strip()
        post_code = ""
        m_post = _POST_RE.search(addr)
        if m_post:
            post_code = m_post.group(1)
            addr = _POST_RE.sub("", addr, count=1).strip()

        pref = ""
        m_pref = _PREF_RE.search(addr)
        if m_pref:
            pref = m_pref.group(1)
            addr_body = addr[m_pref.end() :].strip()
        else:
            addr_body = addr
        return post_code, pref, addr_body

    def _parse_job_url(self, detail_url: str) -> tuple[str, str]:
        m = _JOB_URL_RE.match(detail_url)
        if not m:
            path = urlparse(detail_url).path.strip("/").split("/")
            if len(path) >= 3 and path[1] == "job":
                return path[0], path[2]
            return "", ""
        return m.group("slug"), m.group("job_id")

    def _clean_work_location(self, raw: str) -> str:
        """勤務地テキストから重複行や MAP などのノイズを除去する。"""
        if not raw:
            return ""
        lines = [ln.strip() for ln in raw.split("\n") if ln.strip()]
        lines = [ln for ln in lines if ln.upper() != "MAP"]
        deduped: list[str] = []
        for line in lines:
            if not deduped or deduped[-1] != line:
                deduped.append(line)
        for line in deduped:
            if _PREF_RE.search(line):
                return line
        return deduped[0] if deduped else ""

    def _get_company_meta(self, slug: str) -> dict[str, str]:
        """クライアント採用ページから HP / TEL を1回だけ取得してキャッシュする。"""
        if not slug:
            return {}
        if slug in self._company_cache:
            return self._company_cache[slug]

        meta: dict[str, str] = {}
        company_url = f"{BASE_URL}/{slug}/"
        soup = self.get_soup(company_url)
        if soup is not None:
            for a in soup.select("a[href^='http']"):
                href = a.get("href", "").strip()
                if not href:
                    continue
                if any(host in href for host in _SKIP_HP_HOSTS):
                    continue
                meta[Schema.HP] = href
                break

            text = soup.get_text("\n", strip=True)
            m_tel = _TEL_RE.search(text)
            if m_tel:
                tel = re.sub(r"[\s()（）]", "", m_tel.group(1)).strip("-")
                meta[Schema.TEL] = tel

        self._company_cache[slug] = meta
        return meta

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        pairs = self._extract_pairs(soup)
        slug, _job_id = self._parse_job_url(detail_url)

        company_el = soup.select_one(".company-name-box")
        digest_el = soup.select_one(".job-digest-box")

        store_name = company_el.get_text(strip=True) if company_el else ""
        job_type = digest_el.get_text(strip=True) if digest_el else ""

        work_location_raw = self._pick_value(pairs, "勤務地")
        work_location = self._clean_work_location(work_location_raw)
        post_code, pref, addr = self._split_address(work_location)

        rep_nm = self._pick_value(pairs, "代表者名")

        item: dict = {
            Schema.URL: detail_url,
            Schema.CAT_SITE: job_type or "milto求人",
            Schema.NAME: store_name,
            "店舗名": store_name,
            "雇用形態": self._pick_value(pairs, "雇用形態"),
            "勤務地": work_location or work_location_raw,
            "最寄駅": self._pick_value(pairs, "勤務地・最寄駅"),
            "勤務時間": self._pick_value(pairs, "勤務時間"),
        }

        if post_code:
            item[Schema.POST_CODE] = post_code
        if pref:
            item[Schema.PREF] = pref
        if addr:
            item[Schema.ADDR] = addr
        if rep_nm:
            item[Schema.REP_NM] = rep_nm

        company_meta = self._get_company_meta(slug)
        for key, value in company_meta.items():
            if value and key not in item:
                item[key] = value

        if not item.get(Schema.NAME):
            return None

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = MiltoScraper()
    scraper.execute(BASE_URL)

    print("\n" + "=" * 60)
    print("実行結果サマリ")
    print("=" * 60)
    print(f"  出力ファイル:     {scraper.output_filepath}")
    print(f"  取得件数:         {scraper.item_count}")
    print(f"  観測カラム数:     {len(scraper.observed_columns)}")
    print(f"  観測カラム:       {scraper.observed_columns}")
    print("=" * 60)
