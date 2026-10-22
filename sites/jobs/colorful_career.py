"""
カラフルキャリアドライバー (colorful-career.jp) — ドライバー求人クローラー

対象サイト: https://colorful-career.jp/search?keyword=

取得フロー:
    1. 検索一覧 (sites.yml の url) を /search/page/{N}?keyword= で巡回 (10件/ページ、約27,500件)
    2. 求人カード (article.c-job-card__card) から詳細 /jobs/{id} を取得し、1件ごとに即 yield
       - 「募集概要」「企業情報」セクションの h3.p-job-detail__info-label → info-value を読む
       - JSON-LD (JobPosting) から掲載日を補完
    3. カードが0件になったページで終了

備考:
    - 求人単位で出力 (同一企業の複数求人はそのまま別行)
    - TEL / 代表者 / 資本金 / HP / FAX / メール / 法人番号 はサイト上に存在しない
    - 仕事内容 / 応募条件 / 歓迎条件 / 給与について は自由記述のため除外 (著作権リスク)

実行方法:
    python scripts/sites/jobs/colorful_career.py
"""

import json
import logging
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse, urlunparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

_CARD_SELECTOR = "article.c-job-card__card a.c-job-card__link[href*='/jobs/']"
_JOB_ID_PATTERN = re.compile(r"/jobs/(\d+)")
_TOTAL_PATTERN = re.compile(r"([\d,]+)\s*件")
_POST_CODE_PATTERN = re.compile(r"〒?\s*(\d{3})-?(\d{4})")
_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|"
    r"東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|"
    r"香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
# 住所ブロックから除外する補助テキスト
_ADDR_NOISE = ("Google Map", "※")


class ColorfulCareerScraper(StaticCrawler):
    """カラフルキャリアドライバー スクレイパー"""

    DELAY = 3.0
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
    # 一覧ページ上限 (無限ループ防止のセーフティ。2026-10 時点で約2,753ページ)
    MAX_PAGES = 5000

    EXTRA_COLUMNS = [
        "求人ID",
        "求人タイトル",
        "掲載日",
        "エリア",
        "雇用形態",
        "給与",
        "福利厚生",
        "勤務時間",
        "休日",
        "勤務地",
        "車種・サイズ",
        "特定条件",
    ]

    def prepare(self):
        self._seen_jobs: set[str] = set()

    def parse(self, url: str) -> Generator[dict, None, None]:
        for page in range(1, self.MAX_PAGES + 1):
            list_url = url if page == 1 else self._page_url(url, page)
            soup = self.get_soup(list_url)
            if soup is None:
                break

            if page == 1:
                m = _TOTAL_PATTERN.search(soup.get_text(" ", strip=True))
                if m:
                    self.total_items = int(m.group(1).replace(",", ""))
                    logger.info("総件数: %d", self.total_items)

            job_urls = [urljoin(list_url, a["href"]) for a in soup.select(_CARD_SELECTOR)]
            if not job_urls:
                logger.info("page=%d に求人カードが無いため終了", page)
                break

            for job_url in job_urls:
                m = _JOB_ID_PATTERN.search(job_url)
                job_id = m.group(1) if m else job_url
                if job_id in self._seen_jobs:
                    continue
                self._seen_jobs.add(job_id)
                item = self._scrape_detail(job_url, job_id)
                if item:
                    yield item

    # ------------------------------------------------------------------ helpers

    @staticmethod
    def _page_url(url: str, page: int) -> str:
        """/search?keyword= → /search/page/{N}?keyword="""
        p = urlparse(url)
        path = f"{p.path.rstrip('/')}/page/{page}"
        return urlunparse(p._replace(path=path))

    def _scrape_detail(self, job_url: str, job_id: str) -> dict | None:
        soup = self.get_soup(job_url)
        if soup is None:
            return None

        sections = self._read_sections(soup)
        job = sections.get("募集概要", {})
        company = sections.get("企業情報", {})

        name = self._text(company.get("企業名"))
        if not name:
            card_company = soup.select_one(".c-job-card__company")
            name = card_company.get_text(strip=True) if card_company else ""
        if not name:
            logger.warning("企業名が取得できません: %s", job_url)
            return None

        post_code, pref, addr = self._split_address(company.get("勤務地"))
        work_post, work_pref, work_rest = self._split_address(job.get("勤務地"))
        work_location = " ".join(
            x for x in (f"〒{work_post}" if work_post else "", work_pref + work_rest) if x
        )
        area = self._area(job.get("勤務地"))

        title_el = soup.select_one("h1")
        return {
            Schema.URL: job_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: "",
            Schema.CAT_SITE: self._badges(job.get("職種")),
            Schema.OPEN_DATE: self._text(company.get("設立年月日")),
            Schema.EMP_NUM: self._text(company.get("従業員数")),
            "求人ID": job_id,
            "求人タイトル": title_el.get_text(strip=True) if title_el else "",
            "掲載日": self._date_posted(soup),
            "エリア": area,
            "雇用形態": self._badges(job.get("雇用形態")),
            "給与": self._text(job.get("給与")),
            "福利厚生": self._badges(job.get("福利厚生")),
            "勤務時間": self._text(job.get("勤務時間")),
            "休日": self._text(job.get("休日")),
            "勤務地": work_location,
            "車種・サイズ": self._badges(job.get("車種・サイズ")),
            "特定条件": self._badges(job.get("特定条件")),
        }

    @staticmethod
    def _read_sections(soup) -> dict[str, dict]:
        """{セクション見出し: {ラベル: info-value 要素}} (同名ラベルは先勝ち)"""
        result: dict[str, dict] = {}
        for sec in soup.select("section.p-job-detail__section"):
            h2 = sec.select_one("h2.p-job-detail__section-title")
            if not h2:
                continue
            fields = result.setdefault(h2.get_text(strip=True), {})
            for item in sec.select(".p-job-detail__info-item"):
                label = item.select_one("h3.p-job-detail__info-label")
                value = item.select_one(".p-job-detail__info-value")
                if label and value:
                    fields.setdefault(label.get_text(strip=True), value)
        return result

    @staticmethod
    def _text(el) -> str:
        if el is None:
            return ""
        lines = [t.strip() for t in el.get_text("\n").split("\n")]
        return " / ".join(t for t in lines if t)

    @staticmethod
    def _badges(el) -> str:
        if el is None:
            return ""
        badges = [b.get_text(strip=True) for b in el.select(".p-job-detail__info-badge")]
        if badges:
            return "/".join(b for b in badges if b)
        return el.get_text(" ", strip=True)

    @staticmethod
    def _address_text(el) -> str:
        if el is None:
            return ""
        parts = []
        for t in el.stripped_strings:
            if any(t.startswith(n) for n in _ADDR_NOISE):
                continue
            parts.append(t)
        return "".join(parts).replace("　", "").strip()

    @classmethod
    def _split_address(cls, el) -> tuple[str, str, str]:
        """→ (郵便番号, 都道府県, 市区町村以降)"""
        text = cls._address_text(el)
        post_code = ""
        m = _POST_CODE_PATTERN.search(text)
        if m:
            post_code = f"{m.group(1)}-{m.group(2)}"
            text = text[m.end():].strip()
        pref = ""
        m = _PREF_PATTERN.match(text)
        if m:
            pref = m.group(1)
            text = text[m.end():].strip()
        return post_code, pref, text

    @staticmethod
    def _area(el) -> str:
        """募集概要の勤務地 span 構造 (〒/都道府県/市区町村/番地) から 都道府県+市区町村"""
        if el is None:
            return ""
        spans = [s.get_text(strip=True) for s in el.select("p > span")]
        spans = [s for s in spans if s and not s.startswith("〒")]
        return "".join(spans[:2])

    @staticmethod
    def _date_posted(soup) -> str:
        for sc in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(sc.string or "")
            except ValueError:
                continue
            nodes = data.get("@graph", [data]) if isinstance(data, dict) else data
            for node in nodes:
                if isinstance(node, dict) and node.get("@type") == "JobPosting":
                    return node.get("datePosted", "") or ""
        return ""


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = ColorfulCareerScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://colorful-career.jp/search?keyword=")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
