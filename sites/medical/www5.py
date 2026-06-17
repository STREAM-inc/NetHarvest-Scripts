"""
認定補聴器専門店認定システム【認定補聴器専門店】 — 全国認定補聴器専門店一覧

取得対象:
    - 全国認定補聴器専門店（47都道府県、推定約2800〜3200件）

取得フロー:
    map.php → prefecture.php?p=01〜47 → 各都道府県ページのテーブル行を解析。
    1ページに都道府県の全店舗が掲載（JS によるフィルターは無視して全行取得）。
    テーブルは2行1店舗: row1 に店舗名・住所・設置者、row2 に認定番号・店舗運営責任者。

実行方法:
    # ローカルテスト
    python scripts/sites/medical/www5.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id www5
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import NavigableString
from src.framework.static import StaticCrawler
from src.const.schema import Schema


_PREF_RE = re.compile(
    r"^(北海道|東京都|大阪府|京都府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|"
    r"新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|"
    r"滋賀|兵庫|奈良|和歌山|鳥取|島根|岡山|広島|山口|"
    r"徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県)"
)


class Www5TechnoAids(StaticCrawler):
    """認定補聴器専門店認定システム【認定補聴器専門店】スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["認定番号", "認定年度", "設置者名", "店舗運営責任者", "FAX"]

    def parse(self, url: str):
        for p in range(1, 48):
            pref_url = urljoin(url, f"prefecture.php?p={p:02d}")
            soup = self.get_soup(pref_url)
            if soup is None:
                continue

            rows = soup.select("#shop_list tr")

            i = 0
            while i < len(rows):
                row1 = rows[i]
                shop_td = row1.select_one("td.shop")
                if shop_td is None:
                    i += 1
                    continue

                row2 = rows[i + 1] if i + 1 < len(rows) else None
                try:
                    item = self._parse_shop(row1, row2, pref_url)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.warning(f"parse error p={p:02d} i={i}: {e}")

                i += 2

    def _parse_shop(self, row1, row2, page_url: str) -> dict | None:
        shop_td = row1.select_one("td.shop")
        addr_td = row1.select_one("td.address")
        if not shop_td or not addr_td:
            return None

        # Shop name: first direct NavigableString child (exclude .homepage span text)
        shop_name = ""
        for node in shop_td.children:
            if isinstance(node, NavigableString):
                text = str(node).strip()
                if text:
                    shop_name = text
                    break

        # Homepage URL
        hp_a = shop_td.select_one(".homepage a")
        hp = hp_a.get("href", "").strip() if hp_a else ""

        # Address field parsing
        lines = [
            l.strip()
            for l in addr_td.get_text(separator="\n").split("\n")
            if l.strip()
        ]
        post_code = ""
        addr_lines = []
        tel = ""
        fax = ""
        for line in lines:
            if line.startswith("〒"):
                post_code = line[1:].strip()
            elif re.match(r"^TEL\s+", line):
                tel = line[4:].strip()
            elif re.match(r"^FAX\s+", line):
                fax = line[4:].strip()
            else:
                addr_lines.append(line)

        pref = ""
        addr = ""
        if addr_lines:
            m = _PREF_RE.match(addr_lines[0])
            if m:
                pref = m.group(1)
                rest = addr_lines[0][m.end():]
                # Some entries double-prefix the prefecture (e.g. "北海道北海道...")
                m2 = _PREF_RE.match(rest)
                if m2:
                    rest = rest[m2.end():]
                parts = ([rest] if rest else []) + addr_lines[1:]
            else:
                parts = addr_lines
            addr = " ".join(parts)

        # Row1 person: 設置者名(法人名) + 代表者名
        setter_name = ""
        rep_name = ""
        person1_td = row1.select_one("td.person")
        if person1_td:
            p_lines = [
                l.strip()
                for l in person1_td.get_text(separator="\n").split("\n")
                if l.strip()
            ]
            if len(p_lines) >= 2:
                setter_name = p_lines[0]
                rep_name = p_lines[1]
            elif p_lines:
                rep_name = p_lines[0]

        # Row1 no: 認定年度
        year_td = row1.select_one("td.no")
        cert_year = year_td.get_text(strip=True) if year_td else ""

        # Row2: 認定番号・店舗運営責任者
        cert_no = ""
        manager = ""
        if row2 is not None:
            no_td = row2.select_one("td.no")
            if no_td:
                cert_no = no_td.get_text(strip=True)
            person2_td = row2.select_one("td.person")
            if person2_td:
                manager = person2_td.get_text(strip=True)

        return {
            Schema.NAME: shop_name,
            Schema.HP: hp,
            Schema.POST_CODE: post_code,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.REP_NM: rep_name,
            Schema.URL: page_url,
            "認定番号": cert_no,
            "認定年度": cert_year,
            "設置者名": setter_name,
            "店舗運営責任者": manager,
            "FAX": fax,
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Www5TechnoAids()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://www5.techno-aids.or.jp/shop/map.php")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
