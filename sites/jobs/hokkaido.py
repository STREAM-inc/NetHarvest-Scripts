"""
対象サイト: https://hokkaido.jobantenna.jp/
ジョブアンテナ北海道

- トップは SPA の殻だが、求人詳細 (/at/{企業ID}/offer/{求人ID}/) は SSR 済みで
  JSON-LD(JobPosting) と会社概要 dl が静的 HTML に含まれる → StaticCrawler で取得可能。
- 企業ページ (/at/{企業ID}/) は会社概要がクライアント描画のため使わず、
  sitemap.xml の求人 URL を企業 ID で集約し、企業ごとに最新求人 1 件の詳細から企業情報を取得する。
- sitemap は新しい順に並ぶので、企業内の先頭 URL = 最新求人。
"""
import json
import re
import urllib.parse
import warnings
from typing import Generator

import bs4
from bs4 import XMLParsedAsHTMLWarning

from src.const.schema import Schema
from src.framework.static import StaticCrawler

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)  # sitemap.xml を get_soup で読むため

_OFFER_RE = re.compile(r"/at/(\d+)/offer/(\d+)/?$")
_PREFS = (
    "北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    "新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|"
    "奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|"
    "熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
_PREF_RE = re.compile(rf"^({_PREFS})")


class HokkaidoScraper(StaticCrawler):
    """ジョブアンテナ北海道 スクレイパー (企業単位)"""
    DELAY = 3.0  # 待機時間（秒）
    EXTRA_COLUMNS = ["求人詳細URL", "掲載日", "掲載求人数"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 一覧の代わりに sitemap で全求人を列挙し、企業 ID ごとに集約する
        sitemap = self.get_soup(urllib.parse.urljoin(url, "sitemap.xml"))
        if sitemap is None:
            return
        companies: dict[str, list[str]] = {}
        for loc in sitemap.find_all("loc"):
            href = loc.get_text(strip=True)
            m = _OFFER_RE.search(href)
            if m:
                companies.setdefault(m.group(1), []).append(href)
        self.logger.info("企業数: %d / 求人数: %d", len(companies), sum(len(v) for v in companies.values()))

        for company_id, offers in companies.items():
            soup = self.get_soup(offers[0])
            if soup is None:
                continue
            item = self._parse_offer(soup, offers)
            if item:
                yield item

    def _parse_offer(self, soup: bs4.BeautifulSoup, offers: list[str]) -> dict | None:
        ld = self._job_posting(soup)
        org = ld.get("hiringOrganization") or {}
        name = org.get("name") or (ld.get("identifier") or {}).get("name", "")
        if not name:
            return None

        pref, addr = self._split_addr(self._dd_text(soup, "所在地"))
        industry = ld.get("industry") or []
        if isinstance(industry, str):
            industry = [industry]
        posted = (ld.get("datePosted") or "")[:10]

        return {
            Schema.URL: offers[0],
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: self._tel(soup),
            Schema.HP: org.get("url", ""),
            Schema.CAT_SITE: "、".join(dict.fromkeys(i for i in industry if i)),
            Schema.LOB: self._dd_text(soup, "事業内容", sep="\n"),
            Schema.OPEN_DATE: self._dd_text(soup, "設立"),
            Schema.CAP: self._dd_text(soup, "資本金"),
            Schema.EMP_NUM: self._dd_text(soup, "従業員数", sep=""),
            "求人詳細URL": "\n".join(offers),
            "掲載日": posted,
            "掲載求人数": str(len(offers)),
        }

    @staticmethod
    def _job_posting(soup: bs4.BeautifulSoup) -> dict:
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
            except json.JSONDecodeError:
                continue
            for d in data if isinstance(data, list) else [data]:
                if isinstance(d, dict) and d.get("@type") == "JobPosting":
                    return d
        return {}

    @staticmethod
    def _dd_text(soup: bs4.BeautifulSoup, label: str, sep: str = " ") -> str:
        """会社概要 dl (dt.c-offerDetailAboutUsShort-profile-term) のラベルに対応する値。"""
        for dt in soup.select("dt.c-offerDetailAboutUsShort-profile-term"):
            if dt.get_text(strip=True) != label:
                continue
            dd = dt.find_next_sibling("dd")
            if dd is None:
                return ""
            for btn in dd.find_all(["button", "a"]):
                btn.decompose()  # 「地図」ボタン等
            if sep == "\n":
                return dd.get_text("\n", strip=False).strip()
            return sep.join(dd.stripped_strings)
        return ""

    @staticmethod
    def _split_addr(text: str) -> tuple[str, str]:
        text = text.strip()
        m = _PREF_RE.match(text)
        if not m:
            return "", text
        return m.group(1), text[m.end():].replace(" ", "").strip()

    @staticmethod
    def _tel(soup: bs4.BeautifulSoup) -> str:
        for p in soup.select("p.OfferDetailPage-contact-information-title"):
            if p.get_text(strip=True) != "電話番号":
                continue
            val = p.find_next_sibling()
            if val is not None:
                m = re.search(r"0\d{1,4}-?\d{1,4}-?\d{3,4}", val.get_text(" ", strip=True))
                return m.group(0) if m else ""
        return ""


if __name__ == "__main__":
    scraper = HokkaidoScraper()
    scraper.execute("https://hokkaido.jobantenna.jp/")
