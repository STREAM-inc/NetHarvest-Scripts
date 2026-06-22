"""
Agreキャリア (アグレキャリア) — 沖縄の転職・正社員求人 (webagre.com/career)

取得対象:
    - 沖縄県の転職・正社員求人（全件）。求人カードの求人タイトル・雇用形態・勤務地と、
      詳細ページの企業情報（会社名・住所・代表者・設立・事業内容・勤務時間・休日・電話番号）を取得する。

取得フロー:
    一覧ページ (career/list?page=N) → 求人カード → 詳細ページ (career/view/web/{id})
    会社名・住所・代表者等は詳細ページの #company_name / #address 等から取得。
    電話番号は詳細ページの JobPosting JSON-LD の description 内
    「【電話番号】」ブロックから抽出する（可視 DOM には電話番号ラベルが無いため）。
    求人タイトル・雇用形態・勤務地は一覧カードのバッジから取得。
    detail を 1 件取得するごとに即 yield する (Pattern B / 早期 yield)。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/webagre.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id webagre
"""

import json
import re
import sys
import urllib.parse
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_POST_CODE_RE = re.compile(r"〒\s*([\d０-９][\d０-９\-－]+)")
# JobPosting JSON-LD の description 内「【電話番号】 …」ブロックから電話番号を抽出する。
# 区切りの罫線（－－－…）または次の【…】ラベルまでを電話番号ブロックとみなす。
_TEL_BLOCK_RE = re.compile(r"電話番号】(.*?)(?:[-－─]{4,}|【|$)", re.S)
_TEL_NUM_RE = re.compile(r"0[\d０-９][\d０-９\-－]{7,12}")


class WebagreCrawler(StaticCrawler):
    """Agreキャリア（沖縄の転職・正社員求人）スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["求人タイトル", "雇用形態", "勤務地"]

    def parse(self, url: str):
        # 引数 url ("https://webagre.com/career") を唯一のルートとし、一覧 URL を派生させる。
        list_url = urllib.parse.urljoin(url.rstrip("/") + "/", "list")
        page = 1
        while True:
            soup = self.get_soup(f"{list_url}?page={page}")
            if soup is None:
                break
            cards = soup.select('div.card[id^="jobCard-"]')
            if not cards:
                break
            if page == 1:
                count_el = soup.select_one(".search-count")
                if count_el:
                    try:
                        self.total_items = int(
                            count_el.get_text(strip=True).replace(",", "")
                        )
                    except ValueError:
                        pass
            for card in cards:
                try:
                    link = card.select_one('h2 a[href*="/career/view/web/"]')
                    if not link or not link.get("href"):
                        continue
                    detail_url = urllib.parse.urljoin(url, link["href"])
                    job_title = link.get_text(strip=True)
                    emp_type = ""
                    area = ""
                    for badge in card.select("div.border.rounded"):
                        icon = badge.select_one("i")
                        label = badge.select_one("div.col.text-truncate-1")
                        if icon and label:
                            icon_classes = " ".join(icon.get("class", []))
                            if "fa-location-dot" in icon_classes:
                                area = label.get_text(strip=True)
                            elif "fa-address-card" in icon_classes:
                                emp_type = label.get_text(strip=True)
                    item = self._scrape_detail(detail_url)
                    if item:
                        item["求人タイトル"] = job_title
                        item["雇用形態"] = emp_type
                        item["勤務地"] = area
                        yield item  # detail 取得ごとに即 yield
                except Exception as e:
                    self.logger.warning(f"page {page}: card skip — {e}")
                    continue
            page += 1

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        def get_section(section_id: str) -> str:
            el = soup.select_one(f"#{section_id}")
            return el.get_text(" ", strip=True) if el else ""

        # --- 住所: 郵便番号・都道府県・残りの住所に分解 ---
        addr_raw = get_section("address")
        post_code = ""
        pc_m = _POST_CODE_RE.search(addr_raw)
        if pc_m:
            post_code = pc_m.group(1)
        addr_clean = _POST_CODE_RE.sub("", addr_raw).strip()
        pref = ""
        addr = addr_clean
        m = _PREF_PATTERN.match(addr_clean)
        if m:
            pref = m.group(1)
            addr = addr_clean[m.end():].strip()

        # --- 代表者: 「役職：氏名」形式なら役職と氏名に分割 ---
        rep_raw = get_section("represent")
        pos_nm = ""
        rep_nm = rep_raw
        if rep_raw:
            rm = re.split(r"[：:]", rep_raw, maxsplit=1)
            if len(rm) == 2:
                pos_nm = rm[0].strip()
                rep_nm = rm[1].strip()

        # --- 電話番号: JobPosting JSON-LD の description から抽出 ---
        tel = self._extract_tel(soup)

        return {
            Schema.URL: url,
            Schema.NAME: get_section("company_name"),
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.REP_NM: rep_nm,
            Schema.POS_NM: pos_nm,
            Schema.OPEN_DATE: get_section("set_up"),
            Schema.LOB: get_section("business_details"),
            Schema.TIME: get_section("working_hours"),
            Schema.HOLIDAY: get_section("day_off"),
        }

    @staticmethod
    def _extract_tel(soup) -> str:
        for sc in soup.find_all("script", type="application/ld+json"):
            if not sc.string:
                continue
            try:
                data = json.loads(sc.string)
            except (ValueError, TypeError):
                continue
            if isinstance(data, dict) and data.get("@type") == "JobPosting":
                desc = data.get("description", "") or ""
                mb = _TEL_BLOCK_RE.search(desc)
                if mb:
                    mn = _TEL_NUM_RE.search(mb.group(1))
                    if mn:
                        return mn.group(0).strip()
        return ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = WebagreCrawler()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://webagre.com/career")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
