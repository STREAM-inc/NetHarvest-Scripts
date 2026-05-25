"""
日本スイミングクラブ協会関東支部 (sckanto.net) — 加盟クラブ一覧

取得対象:
    - 関東支部（東京/神奈川/埼玉/千葉/茨城/栃木/群馬/山梨）の加盟クラブ
    - 約 194 件

取得フロー:
    1. /club/view.php?area={tokyoarea,kanagawaarea,chibaarea} の 3 ページを巡回
    2. 各ページ内の h3 (都道府県見出し) と div.kyousanBox を順に走査して
       各クラブの所属都道府県を確定
    3. div.kyousanBox から h4 (名称) と table.kyousanTable
       (所在地/電話/URL) を抽出

実行方法:
    # ローカルテスト
    python scripts/sites/service/sckanto.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id sckanto
"""

import re
import sys
from pathlib import Path
from typing import Generator

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


_AREA_PARAMS = ["tokyoarea", "kanagawaarea", "chibaarea"]
_AREA_PREF_MAP = {
    "tokyoarea": "東京都",
    "kanagawaarea": "神奈川県",
    "chibaarea": "千葉県",
}
_BASE = "https://sckanto.net"

_POSTAL_RE = re.compile(r"〒\s*(\d{3}-?\d{4})\s*(.*)$")
_PREF_RE = re.compile(
    r"^(東京都|神奈川県|埼玉県|千葉県|茨城県|栃木県|群馬県|山梨県)"
)


class SckantoCrawler(StaticCrawler):
    """日本スイミングクラブ協会関東支部 加盟クラブ スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["都道府県"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        clubs = []
        for area in _AREA_PARAMS:
            area_url = f"{_BASE}/club/view.php?area={area}"
            soup = self.get_soup(area_url)
            if soup is None:
                self.logger.warning("ページ取得失敗: %s", area_url)
                continue

            contents = soup.select_one("#contents") or soup
            current_pref = ""
            for el in contents.find_all(["h3", "div"]):
                if el.name == "h3":
                    text = el.get_text(strip=True)
                    if "の加盟クラブ" in text:
                        current_pref = text.replace("の加盟クラブ", "").strip()
                elif el.name == "div" and "kyousanBox" in (el.get("class") or []):
                    clubs.append((area, area_url, current_pref, el))

        self.total_items = len(clubs)
        self.logger.info("加盟クラブ総数: %d", self.total_items)

        for area, area_url, pref, box in clubs:
            try:
                item = self._parse_box(box, area, area_url, pref)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("クラブパース失敗 (%s): %s", pref, e)
                continue

    def _parse_box(self, box, area: str, area_url: str, pref: str) -> dict | None:
        h4 = box.select_one("h4")
        name = h4.get_text(strip=True) if h4 else ""
        if not name:
            return None

        post_code = ""
        addr = ""
        tel = ""
        hp = ""

        for row in box.select("table tr"):
            th = row.select_one("th")
            td = row.select_one("td")
            if not th or not td:
                continue
            label = th.get_text(strip=True).replace("　", "")
            if label == "所在地":
                raw = td.get_text(" ", strip=True)
                post_code, addr = self._split_address(raw, pref)
            elif label == "電話":
                tel = td.get_text(strip=True)
            elif label == "URL":
                a = td.select_one("a[href]")
                if a:
                    hp = a.get("href", "").strip()
                else:
                    hp = td.get_text(strip=True)

        return {
            Schema.URL: area_url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HP: hp,
            Schema.CAT_SITE: "加盟クラブ",
            "都道府県": _AREA_PREF_MAP.get(area, ""),
        }

    @staticmethod
    def _split_address(raw: str, pref: str) -> tuple[str, str]:
        m = _POSTAL_RE.match(raw)
        if m:
            post = m.group(1)
            rest = m.group(2).strip()
        else:
            post = ""
            rest = raw.strip()

        if pref and rest.startswith(pref):
            rest = rest[len(pref):].strip()
        else:
            pm = _PREF_RE.match(rest)
            if pm:
                rest = rest[pm.end():].strip()
        return post, rest


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = SckantoCrawler()
    scraper.execute("https://sckanto.net/club/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
