"""
官報決算データベース (catr.jp) — 官報記載の決算公告データ取得

取得対象:
    - 建設業 (industries/4) の企業情報・決算データ (売上高・純利益・利益剰余金・総資産 等)

取得フロー:
    1. {url}/industries/4?limit=20&offset={N}&order=desc&sort=total_assets でページネーション
    2. 各企業詳細ページ (/companies/{hash}/{id}) から財務情報・企業情報を取得

実行方法:
    # ローカルテスト
    python scripts/sites/corporate/catr.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id catr
"""

import re
import sys
import urllib.parse
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_RE = re.compile(
    r"^(北海道|東京都|大阪府|京都府|神奈川県|愛知県|兵庫県|埼玉県|千葉県|福岡県"
    r"|静岡県|茨城県|広島県|長野県|新潟県|宮城県|栃木県|岐阜県|群馬県|岡山県"
    r"|福島県|三重県|熊本県|鹿児島県|山口県|愛媛県|長崎県|奈良県|青森県|岩手県"
    r"|大分県|石川県|山形県|富山県|秋田県|香川県|和歌山県|宮崎県|高知県|福井県"
    r"|島根県|徳島県|鳥取県|佐賀県|沖縄県|滋賀県|山梨県)"
)


class CatrCrawler(StaticCrawler):
    """官報決算データベース スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["売上高", "純利益", "利益剰余金", "総資産", "決算末日"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        base = url.rstrip("/")
        industry_url = f"{base}/industries/4"
        limit = 20
        offset = 0

        while True:
            list_url = (
                f"{industry_url}?limit={limit}&offset={offset}"
                f"&order=desc&sort=total_assets"
            )
            soup = self.get_soup(list_url)

            if offset == 0:
                cur = soup.select_one(".kanpou-industry-pagination__current")
                if cur:
                    m = re.search(r"(\d+)件", cur.get_text())
                    if m:
                        self.total_items = int(m.group(1))

            rows = soup.select("tr.company-list-row")
            if not rows:
                break

            for row in rows:
                link = row.select_one("a[href]")
                if not link:
                    continue
                company_url = urllib.parse.urljoin(url, link["href"])
                item = self._scrape_company(company_url)
                if item:
                    yield item

            # "次の20件" が <a> (有効) の場合のみ継続
            pagination_links = soup.select("a.kanpou-industry-pagination__link")
            next_offset = offset + limit
            has_next = any(
                re.search(r"offset=(\d+)", a.get("href", ""))
                and int(re.search(r"offset=(\d+)", a.get("href", "")).group(1)) == next_offset
                for a in pagination_links
            )
            if not has_next:
                break
            offset = next_offset

    def _scrape_company(self, url: str) -> dict | None:
        try:
            soup = self.get_soup(url)

            h1 = soup.select_one("h1")
            name = re.sub(r"の情報$", "", h1.get_text(strip=True)) if h1 else ""

            info: dict[str, str] = {}
            profile = soup.select_one(".company-profile-shell")
            if profile:
                dl = profile.select_one("dl")
                if dl:
                    for dt, dd in zip(dl.select("dt"), dl.select("dd")):
                        label = dt.get_text(strip=True)
                        span = dd.select_one(".company-profile-hero__fact-value")
                        if span:
                            info[label] = span.get_text(strip=True)
                            continue
                        main_link = dd.select_one("a.company-profile-hero__main-link")
                        if main_link:
                            info[label] = main_link.get("href", "")
                            continue
                        tag = dd.select_one("a[class*='tag']")
                        info[label] = tag.get_text(strip=True) if tag else ""

            address_raw = info.get("住所", "")
            pref = ""
            addr = address_raw
            m = _PREF_RE.match(address_raw)
            if m:
                pref = m.group(1)
                addr = address_raw[m.end():].strip()

            # 企業自身の直近決算 (table.table-striped の最初のデータ行)
            s: dict[str, str] = {}
            table = soup.select_one("table.table-striped")
            if table:
                data_rows = [r for r in table.select("tr") if not r.select_one("th")]
                if data_rows:
                    tds = data_rows[0].select("td")
                    if len(tds) >= 5:
                        s["決算末日"] = tds[0].get_text(strip=True)
                        raw_uriage = tds[1].get_text(strip=True)
                        s["売上高"] = "" if raw_uriage == "-" else raw_uriage
                        s["純利益"] = tds[2].get_text(strip=True)
                        s["利益剰余金"] = tds[3].get_text(strip=True)
                        s["総資産"] = tds[4].get_text(strip=True)

            return {
                Schema.NAME: name,
                Schema.PREF: pref,
                Schema.ADDR: addr,
                Schema.REP_NM: info.get("代表", ""),
                Schema.HP: info.get("会社URL", ""),
                Schema.CAT_SITE: info.get("業種", ""),
                Schema.URL: url,
                "売上高": s.get("売上高", ""),
                "純利益": s.get("純利益", ""),
                "利益剰余金": s.get("利益剰余金", ""),
                "総資産": s.get("総資産", ""),
                "決算末日": s.get("決算末日", ""),
            }
        except Exception as e:
            self.logger.warning("company scrape failed: %s %s", url, e)
            return None


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = CatrCrawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://catr.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
