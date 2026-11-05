"""
買取大吉 (kaitori_daikichi_2) — エリア別店舗一覧スクレイパー

対象サイト: https://www.kaitori-daikichi.jp/store/kanto/

取得対象:
    買取大吉 公式サイトのエリア別店舗一覧 (/store/<area>/)。
    調査時点 (2026-11) で 関東 882 店舗 + 中部 437 店舗。
    店舗名 / 都道府県 / 住所 / TEL / 定休日 / 営業時間 / 店舗詳細URL / エリア を取得する。

取得フロー:
    1. 引数 url (= /store/kanto/) を起点に、_AREAS の各エリア一覧 URL を
       urljoin(url, "../<area>/") で派生させる (他エリアは _AREAS に追加するだけで巡回可能)。
    2. 各一覧ページの details.pre-ListUnitUnit (1 ブロック = 1 店舗) から
       summary=店舗名、dt「住所/定休日/営業時間」に対応する dd、tel: リンク、
       「店舗詳細を見る」リンク (/store/<スラッグ>/) を取得し即 yield する。
    ページ送り無し・詳細ページ巡回不要 (一覧だけで全項目が揃う)。

注意:
    - 都道府県は住所先頭から導出 (住所が空/都道府県省略の店舗は空)。
    - 同一店舗ブロックが一覧に重複掲載されている箇所がある (調査時 1 件) ため、
      店舗詳細URL + 名称 + 住所 + TEL が完全一致する行は 1 行にまとめる。
    - 利用規約 (/privacy/) にスクレイピング禁止条項は無い。

実行方法:
    python scripts/sites/service/kaitori_daikichi_2.py
    docker compose exec worker python /app/bin/run_flow.py --site-id kaitori_daikichi_2
"""

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


# 巡回するエリア (/store/<area>/)。他エリアを足す場合はここに追加する
_AREAS = ["kanto", "tyubu"]

_PREFS = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県",
    "岐阜県", "静岡県", "愛知県", "三重県",
    "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]
_PREF_PATTERN = re.compile("^(" + "|".join(_PREFS) + ")")
_AREA_PATTERN = re.compile(r"/store/([\w\-]+)/?$")

_COL_AREA = "エリア"


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


class KaitoriDaikichi2(StaticCrawler):
    """買取大吉 エリア別店舗一覧 スクレイパー"""

    DELAY = 3.0

    EXTRA_COLUMNS = [_COL_AREA]

    def parse(self, url: str) -> Generator[dict, None, None]:
        base_area = self._area_of(url)
        areas = [base_area] + [a for a in _AREAS if a != base_area] if base_area else list(_AREAS)

        seen = set()
        for area in areas:
            list_url = url if area == base_area else urljoin(url, f"../{area}/")
            soup = self.get_soup(list_url)
            if soup is None:
                self.logger.warning("一覧取得失敗: %s", list_url)
                continue

            for unit in soup.select("details.pre-ListUnitUnit"):
                item = self._parse_unit(unit, list_url, area)
                if not item:
                    continue
                key = (item[Schema.HP], item[Schema.NAME], item[Schema.ADDR], item[Schema.TEL])
                if key in seen:
                    continue
                seen.add(key)
                yield item

    @staticmethod
    def _area_of(url: str) -> str:
        m = _AREA_PATTERN.search(url.split("?")[0])
        return m.group(1) if m else ""

    def _parse_unit(self, unit, list_url: str, area: str) -> dict:
        summary = unit.select_one("summary")
        name = _clean(summary.get_text(" ", strip=True)) if summary else ""
        if not name:
            return {}

        info = {}
        for dt in unit.select("dl.pre-ListUnitUnit_Information dt"):
            dd = dt.find_next_sibling("dd")
            if dd:
                info[_clean(dt.get_text())] = _clean(dd.get_text(" ", strip=True))

        addr = info.get("住所", "")
        m = _PREF_PATTERN.match(addr)
        pref = m.group(1) if m else ""

        tel_a = unit.select_one('a[href^="tel:"]')
        tel = _clean(tel_a.get_text(" ", strip=True)) if tel_a else ""
        if tel_a and not tel:
            tel = tel_a["href"][len("tel:"):].strip()

        hp = ""
        for a in unit.select("ul.pre-ListUnitUnit_Btns a[href]"):
            href = urljoin(list_url, a["href"].strip())
            if "/store/" in href and "Map" not in (a.get("class") or []):
                hp = href
                break

        return {
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.HOLIDAY: info.get("定休日", ""),
            Schema.TIME: info.get("営業時間", ""),
            Schema.HP: hp,
            _COL_AREA: area,
        }


if __name__ == "__main__":
    scraper = KaitoriDaikichi2()
    scraper.execute("https://www.kaitori-daikichi.jp/store/kanto/")
