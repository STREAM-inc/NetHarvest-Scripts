# scripts/sites/jobs/tsunoru.py
"""
TSUNORU既卒第二新卒 — 既卒・第二新卒向け就活サイトの企業情報スクレイパー

取得対象:
    - 一覧ページ (/prev/search/) の企業カード: 企業名、業種、募集職種、勤務地等
    - 詳細ページ (/prev/company/{id}/) の会社概要テーブル:
      住所、TEL、代表者名、設立年月日、資本金、従業員数、HP URL、売上高

取得フロー:
    /prev/ (引数 url) → /prev/search/ (一覧) → ページネーション ?p=N →
    各社の /prev/company/{id}/ → 会社概要テーブルを解析

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/tsunoru.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id tsunoru
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup
from src.framework.dynamic import DynamicCrawler
from src.const.schema import Schema


_PREF_RE = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県"
    r"|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県"
    r"|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県"
    r"|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県"
    r"|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_REP_PREFIX_RE = re.compile(
    r"^(代表取締役(?:社長|会長|CEO)?|取締役(?:社長)?|社長|会長|CEO)\s+"
)


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


class TsunoruScraper(DynamicCrawler):
    """TSUNORU既卒第二新卒 企業情報スクレイパー"""

    DELAY = 2.0
    EXTRA_COLUMNS = ["主な募集職種", "主な勤務地", "採用の特徴", "選考のポイント", "売上高"]

    def get_soup(self, url: str) -> BeautifulSoup:
        self.page.goto(url, wait_until="domcontentloaded")
        return BeautifulSoup(self.page.content(), "html.parser")

    def parse(self, url: str):
        # url = https://job.tsunoru.jp/prev/
        search_url = urljoin(url, "search/")
        page_num = 0

        while True:
            page_url = search_url if page_num == 0 else f"{search_url}?p={page_num}"
            soup = self.get_soup(page_url)

            if page_num == 0:
                count_el = soup.select_one("span.num_hit")
                if count_el is None:
                    # Fallback: look for "155件中" pattern
                    body_text = soup.get_text()
                    m = re.search(r"(\d+)\s*件中", body_text)
                    if m:
                        self.total_items = int(m.group(1))
                else:
                    try:
                        self.total_items = int(_clean(count_el.get_text()))
                    except ValueError:
                        pass

            items = soup.select("div.corp_detail")
            if not items:
                break

            for item in items:
                try:
                    link_el = item.select_one("h3.tit_entry a")
                    if not link_el:
                        continue
                    company_name = _clean(link_el.get_text())
                    detail_href = link_el.get("href", "")
                    detail_url = urljoin(page_url, detail_href)

                    # Listing-level structured fields
                    cat_site = ""
                    extra: dict = {k: "" for k in self.EXTRA_COLUMNS}
                    for li in item.select("div.data ul li"):
                        span = li.select_one("span")
                        p = li.select_one("p")
                        if not span or not p:
                            continue
                        key = _clean(span.get_text())
                        val = _clean(p.get_text())
                        if key == "業種":
                            cat_site = val
                        elif key in ("主な募集職種", "主な勤務地", "採用の特徴", "選考のポイント"):
                            extra[key] = val

                    # Detail page
                    detail = self._scrape_detail(detail_url)

                    yield {
                        Schema.NAME: company_name,
                        Schema.URL: detail_url,
                        Schema.CAT_SITE: cat_site,
                        Schema.PREF: detail.get(Schema.PREF, ""),
                        Schema.POST_CODE: detail.get(Schema.POST_CODE, ""),
                        Schema.ADDR: detail.get(Schema.ADDR, ""),
                        Schema.TEL: detail.get(Schema.TEL, ""),
                        Schema.REP_NM: detail.get(Schema.REP_NM, ""),
                        Schema.CAP: detail.get(Schema.CAP, ""),
                        Schema.EMP_NUM: detail.get(Schema.EMP_NUM, ""),
                        Schema.OPEN_DATE: detail.get(Schema.OPEN_DATE, ""),
                        Schema.HP: detail.get(Schema.HP, ""),
                        "主な募集職種": extra.get("主な募集職種", ""),
                        "主な勤務地": extra.get("主な勤務地", ""),
                        "採用の特徴": extra.get("採用の特徴", ""),
                        "選考のポイント": extra.get("選考のポイント", ""),
                        "売上高": detail.get("売上高", ""),
                    }
                except Exception as e:
                    self.logger.warning("item error: %s", e)
                    continue

            page_num += 1

    def _scrape_detail(self, url: str) -> dict:
        result: dict = {}
        try:
            soup = self.get_soup(url)
        except Exception as e:
            self.logger.warning("detail fetch error %s: %s", url, e)
            return result

        table = soup.select_one("table.tbstyle01")
        if not table:
            return result

        for row in table.select("tr"):
            tds = row.select("td")
            if len(tds) == 2:
                # colspan=2 header + value
                key = _clean(tds[0].get_text())
                val = _clean(tds[1].get_text(separator=" "))
            elif len(tds) == 3:
                # rowspan header | sub-header | value
                key = _clean(tds[1].get_text())
                val = _clean(tds[2].get_text(separator=" "))
            else:
                continue

            if not key or not val:
                continue

            if key == "郵便番号":
                result[Schema.POST_CODE] = val
            elif key == "所在地":
                m = _PREF_RE.match(val)
                if m:
                    result[Schema.PREF] = m.group(1)
                result[Schema.ADDR] = val
            elif key == "電話番号":
                result[Schema.TEL] = val
            elif key == "代表者":
                rep = _REP_PREFIX_RE.sub("", val).strip()
                result[Schema.REP_NM] = rep or val
            elif key == "設立／創業":
                result[Schema.OPEN_DATE] = val
            elif key == "資本金":
                result[Schema.CAP] = val
            elif key == "従業員":
                # "89人\n（2024年04月）" → "89人"
                result[Schema.EMP_NUM] = val.split("（")[0].strip()
            elif key == "売上高":
                result["売上高"] = val

        # Homepage URL (first external link in div#page_link)
        hp_div = soup.select_one("div#page_link")
        if hp_div:
            for a in hp_div.select("a[href]"):
                href = a.get("href", "")
                # Skip internal tsunoru links and recruit/worker sub-pages
                if "tsunoru.jp" not in href and href.startswith("http"):
                    result[Schema.HP] = href
                    break

        return result


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = TsunoruScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://job.tsunoru.jp/prev/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
