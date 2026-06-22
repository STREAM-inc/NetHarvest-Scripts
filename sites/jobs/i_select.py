"""
iセレクト — 鳥取県を中心とした求人情報サイト

取得対象:
    - 全求人の企業・施設情報と求人詳細（約 2,575 件 / 258 ページ）

取得フロー:
    一覧ページ (/search → /search?page=N) を巡回し、各求人カード (.box_kensaku) から
    詳細ページ (/recruits/detail/{id}) のURLとエリアを取得。詳細ページの
    表 (table.tbl_syosai) と見出し (h3.h3_style) から全フィールドを取得して
    1件ずつ即 yield する (Pattern B)。

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/i_select.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id i_select
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 住所先頭の都道府県を抽出 (本サイトの住所は市区町村始まりが多く未マッチが多いが best-effort)
_PREF_PATTERN = re.compile(
    r"^(北海道|(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|新潟|富山|"
    r"石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|和歌山|鳥取|島根|岡山|広島|山口|"
    r"徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県|東京都|(?:大阪|京都)府)"
)


class ISelectScraper(StaticCrawler):
    """iセレクト スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "求人タイトル", "雇用形態", "給与", "採用人数", "待遇", "エリア", "特徴タグ",
    ]

    def parse(self, url: str):
        page = 1
        while True:
            # 🔒 ルートは引数 url のみを起点にする (SSOT = sites.yml の url)
            page_url = url if page == 1 else f"{url}?page={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                break

            if page == 1:
                # 「該当件数 2575件」から総件数を取得
                body_text = soup.get_text(" ", strip=True)
                m = re.search(r"該当件数\s*([\d,]+)\s*件", body_text)
                if m:
                    self.total_items = int(m.group(1).replace(",", ""))

            boxes = soup.select(".box_kensaku")
            if not boxes:
                break

            for box in boxes:
                try:
                    detail_a = box.select_one('a[href*="/recruits/detail/"]')
                    if not detail_a:
                        continue
                    detail_url = urljoin(page_url, detail_a["href"])

                    # 一覧カードのエリア (例: 鳥取市) を拾って詳細へ引き渡す
                    area = ""
                    for dt in box.select("dl.syousai dt"):
                        if dt.get_text(strip=True) == "エリア":
                            dd = dt.find_next_sibling("dd")
                            if dd:
                                area = dd.get_text(" ", strip=True)
                            break

                    result = self._scrape_detail(detail_url, area)
                    if result:
                        yield result
                except Exception as e:
                    self.logger.warning("スキップ: %s", e)

            page += 1

    def _scrape_detail(self, url: str, area: str = "") -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        # 詳細表 (table.tbl_syosai) の th→td 全ペアを辞書化
        fields: dict[str, str] = {}
        for tr in soup.select("table.tbl_syosai tr"):
            th = tr.find("th")
            td = tr.find("td")
            if th and td:
                key = th.get_text(strip=True)
                if key and key not in fields:
                    fields[key] = td.get_text(" ", strip=True)

        # 企業・施設名
        name_el = soup.select_one("h3.h3_style")
        name = name_el.get_text(strip=True) if name_el else ""

        # 求人タイトル
        ttl_el = soup.select_one(".ttl")
        title = ttl_el.get_text(strip=True) if ttl_el else ""

        # 雇用形態 (NEW バッジを除いた tab)。表に無い場合のフォールバック
        emp_type = fields.get("雇用形態", "")
        if not emp_type:
            for li in soup.select(".tab li"):
                t = li.get_text(strip=True)
                if t and "NEW" not in t.upper() and "ＮＥＷ" not in t:
                    emp_type = t
                    break

        # 特徴タグ
        tags = [li.get_text(strip=True) for li in soup.select(".tag li") if li.get_text(strip=True)]

        # 勤務地 → 住所 (「（地図）」を除去) と都道府県
        address_raw = re.sub(r"（地図）", "", fields.get("勤務地", "")).strip()
        pref, addr_short = "", address_raw
        m = _PREF_PATTERN.match(address_raw)
        if m:
            pref = m.group(1)
            addr_short = address_raw[m.end():].strip()

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr_short,
            Schema.TEL: fields.get("電話番号", ""),
            Schema.TIME: fields.get("勤務時間", ""),
            Schema.HOLIDAY: fields.get("休日", ""),
            Schema.CAT_SITE: fields.get("職種", ""),
            "求人タイトル": title,
            "雇用形態": emp_type,
            "給与": fields.get("給与", ""),
            "採用人数": fields.get("採用人数", ""),
            "待遇": fields.get("待遇", ""),
            "エリア": area,
            "特徴タグ": "、".join(tags),
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = ISelectScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("http://www.i-select.jp/search")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
