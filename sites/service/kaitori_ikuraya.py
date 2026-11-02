"""
買取専門いくらや 店舗一覧 (kaitori-ikuraya.jp)

取得対象:
    関東 (/store/kanto/) と中部 (/store/tyubu/) の地区別店舗一覧ページに載る全店舗

取得フロー:
    1. 引数 url (関東一覧) と、url から派生させた中部一覧 (../tyubu/) を順に取得
    2. section.prefectureListUnit (h3=都道府県[＞エリア]) 配下の
       details.prefectureListUnitUnit を1店舗として抽出
       - 名称: summary / 住所・定休日・営業時間: dl dt/dd
       - TEL: a.prefectureListUnitUnit__phone の tel: リンク
       - 店舗詳細URL(HP) / 地図URL: ul.prefectureListUnitUnit__buttons
    ページ送りなし・詳細ページ遷移なし。/shop_area_data.json は取得しない。

実行方法:
    python scripts/sites/service/kaitori_ikuraya.py
    python bin/run_flow.py --site-id kaitori_ikuraya
"""

import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 引数 url (関東一覧) からの相対パス。関東 → 中部 の順に巡回する
_REGION_PATHS = ["./", "../tyubu/"]

_PREF_RE = re.compile(r"^(東京都|北海道|(?:京都|大阪)府|.{2,3}県)")


def _text(el) -> str:
    return el.get_text(" ", strip=True) if el else ""


def _normalize_tel(raw: str) -> str:
    # 一部店舗は半角カナ長音「ｰ」や全角ハイフンで区切られている
    tel = re.sub(r"[ｰー－―‐−]", "-", raw.strip())
    return tel.replace(" ", "")


class KaitoriIkurayaScraper(StaticCrawler):
    """買取専門いくらや 店舗一覧スクレイパー"""

    DELAY = 1.0  # 備考指定: リクエスト間隔1秒
    EXTRA_COLUMNS = ["エリア", "地図URL"]

    def parse(self, url: str):
        seen = set()
        for path in _REGION_PATHS:
            list_url = urljoin(url, path)
            soup = self.get_soup(list_url)
            if soup is None:
                logger.warning(f"一覧取得失敗: {list_url}")
                continue

            for section in soup.select("section.prefectureListUnit"):
                area = _text(section.select_one("h3.prefectureListUnit__title"))
                area_pref = area.split("＞")[0].strip()

                for store in section.select("details.prefectureListUnitUnit"):
                    name = _text(store.select_one("summary"))
                    if not name:
                        continue

                    info = {}
                    for row in store.select("dl.prefectureListUnitUnit__information > div"):
                        dt, dd = row.find("dt"), row.find("dd")
                        if dt and dd:
                            info[_text(dt)] = _text(dd)

                    full_addr = info.get("住所", "")
                    m = _PREF_RE.match(full_addr)
                    pref = m.group(1) if m else area_pref
                    addr = full_addr[len(pref):].strip() if full_addr.startswith(pref) else full_addr

                    tel_a = store.select_one('a.prefectureListUnitUnit__phone[href^="tel:"]')
                    tel = _normalize_tel(tel_a["href"][4:]) if tel_a else ""

                    hp = ""
                    map_url = ""
                    for a in store.select("ul.prefectureListUnitUnit__buttons a[href]"):
                        href = a["href"].strip()
                        if "Map" in (a.get("class") or []) or "地図" in _text(a):
                            map_url = href
                        elif "/store/" in href:
                            hp = urljoin(list_url, href)

                    key = hp or (name, full_addr)
                    if key in seen:
                        continue
                    seen.add(key)

                    yield {
                        Schema.URL: list_url,
                        Schema.NAME: name,
                        Schema.PREF: pref,
                        Schema.ADDR: addr,
                        Schema.TEL: tel,
                        Schema.TIME: info.get("営業時間", ""),
                        Schema.HOLIDAY: info.get("定休日", ""),
                        Schema.HP: hp,
                        "エリア": area,
                        "地図URL": map_url,
                    }


if __name__ == "__main__":
    scraper = KaitoriIkurayaScraper()
    scraper.execute("https://www.kaitori-ikuraya.jp/store/kanto/")

    print("\n" + "=" * 40)
    print("📊 実行結果サマリ")
    print("=" * 40)
    print(f"  出力ファイル:   {scraper.output_filepath}")
    print(f"  取得件数:       {scraper.item_count}")
    print(f"  観測カラム数:   {len(scraper.observed_columns)}")
    print("=" * 40)
