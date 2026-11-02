"""
求人かごしま (kyu-kago.com) — 鹿児島県の求人情報クローラー

対象サイト: https://www.kyu-kago.com/n/

取得フロー:
    1. sites.yml の url (新着求人 /n/) を起点に、ページャの「次へ」(/n/p1, /n/p2 …) を辿って
       求人詳細 URL (/detail/{id}) を収集する。
       ※ /n/ は「新着求人」のみ (約190件) で、掲載全件 (約577件) は含まれない。
    2. 続けて url から導出した検索結果 /s/?page=N (0 始まり, 20件/ページ) を空ページまで巡回し、
       /n/ に無かった残りの求人を補完する (詳細 ID で重複排除)。
    3. 求人詳細を 1 件取得するごとに即 yield する。
       - 募集内容 / 企業情報テーブルの th/td から 法人名・店舗名・勤務地・HP・業務内容 等
       - 「電話番号を表示」ボタンの番号は a.tel_num span に静的に埋め込まれている (応募用 050 番号)
       - 掲載期間は「掲載期間：YYYY/MM/DD～YYYY/MM/DD」テキスト

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/kyu_kago.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id kyu_kago
"""

import logging
import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

import bs4

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

_DETAIL_PATTERN = re.compile(r"/detail/(\d+)")
# 一覧の求人カード (サイドバー等のリンクを除外するため .recruit_list 内に限定)
_CARD_SELECTOR = ".recruit_list .recruit_list_head_main a[href*='/detail/']"
_PERIOD_PATTERN = re.compile(r"掲載期間[：:]\s*(\S+?～\S+)")
_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|"
    r"東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|"
    r"香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_TEL_PATTERN = re.compile(r"0\d[\d\-]{8,12}")
_MAX_SEARCH_PAGES = 200  # /s/?page=N の安全上限


class KyuKagoScraper(StaticCrawler):
    """求人かごしま スクレイパー"""

    DELAY = 3.0
    EXTRA_COLUMNS = ["店舗名/事業所名", "募集職種", "雇用形態", "掲載期間", "会社所在地", "求人詳細URL"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()

        # 1. 起点 url (/n/) のページャを辿る
        page_url = url
        visited_pages: set[str] = set()
        while page_url and page_url not in visited_pages:
            visited_pages.add(page_url)
            soup = self.get_soup(page_url)
            if soup is None:
                break
            links = self._detail_links(soup, page_url)
            if not links:
                break
            yield from self._crawl_details(links, seen)
            next_a = soup.select_one(".page_pager li.next a[href]")
            page_url = urljoin(page_url, next_a["href"]) if next_a else None

        # 2. 検索結果 /s/?page=N で残り (新着以外) を補完
        search_url = urljoin(url, "../s/")
        for page in range(_MAX_SEARCH_PAGES):
            list_url = search_url if page == 0 else f"{search_url}?page={page}"
            soup = self.get_soup(list_url)
            if soup is None:
                break
            links = self._detail_links(soup, list_url)
            if not links:
                break
            yield from self._crawl_details(links, seen)

    def _detail_links(self, soup: bs4.BeautifulSoup, base_url: str) -> list[str]:
        links = []
        for a in soup.select(_CARD_SELECTOR):
            href = urljoin(base_url, a["href"])
            if _DETAIL_PATTERN.search(href) and href not in links:
                links.append(href)
        return links

    def _crawl_details(self, links: list[str], seen: set[str]) -> Generator[dict, None, None]:
        for link in links:
            job_id = _DETAIL_PATTERN.search(link).group(1)
            if job_id in seen:
                continue
            seen.add(job_id)
            item = self._parse_detail(link)
            if item:
                yield item

    def _parse_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        fields: dict[str, str] = {}
        for tr in soup.select("tr"):
            th, td = tr.find("th"), tr.find("td")
            if not th or not td:
                continue
            label = th.get_text(strip=True)
            if label and label not in fields:
                fields[label] = self._td_text(td)

        shop = fields.get("店舗名／事業所名", "")
        name = fields.get("法人名", "") or shop
        if not name:
            logger.warning("名称が取得できません: %s", url)
            return None

        pref, addr = self._split_pref(fields.get("勤務地", ""))

        tel = ""
        tel_span = soup.select_one("a.tel_num span")
        if tel_span:
            m = _TEL_PATTERN.search(tel_span.get_text(strip=True))
            tel = m.group(0) if m else ""

        # 募集職種行が無い求人もある → タイトル下の職種表示 (p.kind 先頭 span) で補完
        job_type = fields.get("募集職種", "")
        if not job_type:
            kind_span = soup.select_one(".recruit_detail_info p.kind span")
            job_type = kind_span.get_text(strip=True) if kind_span else ""

        period = ""
        period_node = soup.find(string=_PERIOD_PATTERN)
        if period_node:
            period = _PERIOD_PATTERN.search(str(period_node)).group(1)

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: fields.get("ホームページ", "") or fields.get("WEBサイト", ""),
            Schema.LOB: fields.get("業務内容", "") or fields.get("事業内容", ""),
            Schema.REP_NM: fields.get("代表者名", ""),
            Schema.CAP: fields.get("資本金", ""),
            Schema.OPEN_DATE: fields.get("設立", ""),
            "店舗名/事業所名": shop,
            "募集職種": job_type,
            "雇用形態": fields.get("雇用形態", ""),
            "掲載期間": period,
            "会社所在地": fields.get("会社所在地", ""),
            "求人詳細URL": url,
        }

    @staticmethod
    def _td_text(td: bs4.Tag) -> str:
        """td のテキスト (地図リンク・コメントを除外し、改行は空白に畳む)。"""
        for a in td.find_all("a", string="地図を表示"):
            a.decompose()
        lines = [s.strip() for s in td.get_text("\n").splitlines() if s.strip()]
        return " ".join(lines)

    @staticmethod
    def _split_pref(location: str) -> tuple[str, str]:
        """「鹿児島県 鹿児島市 高麗町43-30」→ ("鹿児島県", "鹿児島市高麗町43-30")。"""
        # 県・市区町村・町域の間の半角スペースを詰める (建物名前の全角スペースは残す)
        location = re.sub(r" +", "", location.strip())
        m = _PREF_PATTERN.match(location)
        if not m:
            return "", location
        return m.group(1), location[m.end():]


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = KyuKagoScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.kyu-kago.com/n/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
