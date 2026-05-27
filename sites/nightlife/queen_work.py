"""
クイーン体入 (queenwork.jp) — ナイトワーク系体験入店求人サイト掲載店舗スクレイパー

取得対象:
    - キャバクラ / ガールズバー / コンカフェ / スナック / クラブ 等の掲載全店舗

取得フロー:
    1. sitemap.xml から全店舗URL (/job{id}/) を収集 (約85件)
    2. 各詳細ページから <b>ラベル</b><span>値</span> パターンで店舗情報を抽出

実行方法:
    python scripts/sites/nightlife/queen_work.py
    python bin/run_flow.py --site-id queen_work
"""

import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


BASE_URL = "https://queenwork.jp"
SITEMAP_URL = f"{BASE_URL}/sitemap.xml"

_JOB_URL_RE = re.compile(r"https://queenwork\.jp/job\d+/$")

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


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"[ \t　]+", " ", text.replace("\xa0", " ")).strip()


def _multiline(text: str) -> str:
    if not text:
        return ""
    lines = [_clean(line) for line in text.replace("\r", "\n").split("\n")]
    result: list[str] = []
    prev_empty = False
    for line in lines:
        if line == "":
            if not prev_empty and result:
                result.append("")
            prev_empty = True
        else:
            result.append(line)
            prev_empty = False
    return "\n".join(result).strip()


def _get_field(soup, label: str) -> str:
    """<b>label</b> と同じ親要素内の <span> テキストを返す。"""
    for b in soup.find_all("b"):
        if _clean(b.get_text()) == label:
            parent = b.parent
            if parent:
                span = parent.find("span")
                if span:
                    return _multiline(span.get_text("\n", strip=False))
    return ""


class QueenWorkScraper(StaticCrawler):
    """クイーン体入 (queenwork.jp) 掲載店舗スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "エリア",
        "最寄り駅",
        "給与",
        "職種",
        "待遇",
        "雇用形態",
        "応募方法",
        "受付時間",
        "担当者名",
    ]

    def parse(self, url: str):
        resp = self.session.get(SITEMAP_URL, timeout=self.TIMEOUT)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        job_urls = [
            el.text.strip()
            for el in root.iter()
            if el.tag.endswith("loc") and el.text and _JOB_URL_RE.match(el.text.strip())
        ]
        self.total_items = len(job_urls)
        self.logger.info("sitemap から %d 件収集", len(job_urls))

        for job_url in job_urls:
            try:
                item = self._scrape_detail(job_url)
                if item:
                    yield item
            except Exception:
                self.logger.exception("詳細取得失敗: %s", job_url)
                continue

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        name = _clean(_get_field(soup, "店舗名"))
        if not name:
            h1 = soup.select_one("h1")
            if h1:
                raw = _clean(h1.get_text(" ", strip=True))
                name = re.sub(r"（[^）]+）の公式求人情報$", "", raw).strip()
        if not name:
            return None

        # 住所: <span class="address"> から郵便番号・都道府県・住所を分離
        postal, pref, addr = "", "", ""
        for b in soup.find_all("b"):
            if _clean(b.get_text()) == "住所":
                addr_span = b.parent.find("span") if b.parent else None
                if addr_span:
                    for a_tag in addr_span.find_all("a"):
                        a_tag.decompose()
                    raw_addr = _clean(addr_span.get_text(" ", strip=True))
                    m_post = re.search(r"(\d{3}-\d{4})", raw_addr)
                    if m_post:
                        postal = m_post.group(1)
                        raw_addr = raw_addr.replace(postal, "").strip()
                    m_pref = _PREF_PATTERN.match(raw_addr)
                    if m_pref:
                        pref = m_pref.group(1)
                        addr = raw_addr[m_pref.end():].strip()
                    else:
                        addr = raw_addr
                break

        return {
            Schema.URL:      url,
            Schema.NAME:     name,
            Schema.POST_CODE: postal,
            Schema.PREF:     pref,
            Schema.ADDR:     addr,
            Schema.TEL:      _clean(_get_field(soup, "電話番号")),
            Schema.CAT_SITE: _clean(_get_field(soup, "業種")),
            Schema.TIME:     _get_field(soup, "勤務時間"),
            Schema.HOLIDAY:  _clean(_get_field(soup, "休日")),
            "エリア":        _clean(_get_field(soup, "エリア")),
            "最寄り駅":      _get_field(soup, "最寄駅"),
            "給与":          _get_field(soup, "給与"),
            "職種":          _clean(_get_field(soup, "職種")),
            "待遇":          _get_field(soup, "待遇"),
            "雇用形態":      _clean(_get_field(soup, "雇用形態")),
            "応募方法":      _get_field(soup, "応募方法"),
            "受付時間":      _clean(_get_field(soup, "受付時間")),
            "担当者名":      _clean(_get_field(soup, "担当")),
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = QueenWorkScraper()
    scraper.execute(BASE_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
