"""
アップステージ — 男性高収入求人サイトスクレイパー

取得対象:
    - 全国 8 エリア (北海道・東北/北陸・甲信越/北関東/関東/東海/関西/中国・四国/九州・沖縄)
      の /jobresult/ に掲載されている全求人

取得フロー:
    1. 各エリアの一覧ページ (?stline=N) を 20件刻みで巡回
    2. 各 article から詳細ページURL (/jobdetail/?id=XXX) を収集
    3. 詳細ページの dt/dd 構造から NAME/住所/TEL/LINE/HP 等を抽出

実行方法:
    # ローカルテスト
    python scripts/sites/jobs/up_stage.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id up_stage
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

BASE_URL = "https://www.up-stage.info"

REGIONS = [
    "hokaido", "hokuriku", "kitakanto", "kanto",
    "toukai", "kansai", "tyugoku", "kyusyu",
]

PAGE_SIZE = 20

_PREF_PATTERN = re.compile(
    r"(北海道|東京都|(?:大阪|京都)府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|新潟|富山|"
    r"石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|和歌山|鳥取|島根|岡山|広島|山口|"
    r"徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県)"
)

_POST_CODE_PATTERN = re.compile(r"〒?\s*(\d{3}-?\d{4})")
_TEL_PATTERN = re.compile(r"TEL[：:]?\s*([\d\-()]+)")
_HP_PATTERN = re.compile(r"https?://[^\s<]+")
_NAME_SUFFIX = re.compile(r"の男性求人募集\s*$")
_COUNT_PATTERN = re.compile(r"([\d,]+)\s*件\s*の求人があります")
_LEADING_CIRCLE_NUM = re.compile(r"^[①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬⑭⑮⑯⑰⑱⑲⑳]+")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class UpStageScraper(StaticCrawler):
    """アップステージ 男性高収入求人サイトスクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "キャッチコピー",
        "職種",
        "給与",
        "勤務地",
        "最寄駅",
        "特徴",
        "社会保険",
        "受動喫煙対策",
        "採用担当者から",
        "応募方法",
        "面接地",
        "情報最終更新日",
        "掲載終了予定日",
        "エリア",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen_ids: set[str] = set()
        self.total_items = 0
        region_entries: list[tuple[str, object]] = []

        for region in REGIONS:
            first_url = f"{BASE_URL}/{region}/jobresult/"
            soup = self.get_soup(first_url)
            if soup is None:
                self.logger.warning("エリア初期ページ取得失敗: %s", region)
                continue
            m = _COUNT_PATTERN.search(soup.get_text())
            count = int(m.group(1).replace(",", "")) if m else 0
            self.total_items += count
            self.logger.info("[%s] 総件数: %d", region, count)
            region_entries.append((region, soup))

        for region, first_soup in region_entries:
            yield from self._scrape_region(region, first_soup, seen_ids)

    def _scrape_region(self, region: str, first_soup, seen_ids: set[str]) -> Generator[dict, None, None]:
        offset = 0
        soup = first_soup
        while True:
            articles = soup.select("article")
            if not articles:
                self.logger.info("[%s] stline=%d でアイテム無し。エリア完了。", region, offset)
                break

            detail_tasks: list[tuple[str, str]] = []
            for art in articles:
                a = art.select_one("a[href*='/jobdetail/']")
                if not a:
                    continue
                href = (a.get("href") or "").strip()
                if not href:
                    continue
                detail_url = urljoin(BASE_URL, href)
                m = re.search(r"id=(\d+)", detail_url)
                if not m:
                    continue
                job_id = m.group(1)
                if job_id in seen_ids:
                    continue
                seen_ids.add(job_id)
                catch_el = art.select_one("p.catchPhrase")
                catch_txt = _clean(catch_el.get_text(" ")) if catch_el else ""
                detail_tasks.append((detail_url, catch_txt))

            for detail_url, catch_txt in detail_tasks:
                try:
                    item = self._scrape_detail(detail_url, catch_txt, region)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.warning("詳細取得失敗: %s — %s", detail_url, e)
                    continue

            offset += PAGE_SIZE
            next_url = f"{BASE_URL}/{region}/jobresult/?stline={offset}"
            soup = self.get_soup(next_url)
            if soup is None:
                self.logger.warning("[%s] stline=%d 取得失敗。次のエリアへ。", region, offset)
                break

    def _scrape_detail(self, url: str, catch_txt: str, region: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        kv: dict[str, str] = {}
        for dl in soup.select("dl"):
            dts = dl.select("dt")
            dds = dl.select("dd")
            for i, dt in enumerate(dts):
                header = _clean(dt.get_text(" "))
                if not header or header in kv:
                    continue
                if i < len(dds):
                    kv[header] = _clean(dds[i].get_text(" "))

        data: dict = {Schema.URL: url}

        h1 = soup.select_one("h1")
        if h1:
            name = _NAME_SUFFIX.sub("", _clean(h1.get_text(" ")))
            if name:
                data[Schema.NAME] = name

        contact = kv.get("連絡先", "")
        if contact:
            pm = _POST_CODE_PATTERN.search(contact)
            if pm:
                pc = pm.group(1)
                if "-" not in pc and len(pc) == 7:
                    pc = f"{pc[:3]}-{pc[3:]}"
                data[Schema.POST_CODE] = pc
            tm = _TEL_PATTERN.search(contact)
            if tm:
                data[Schema.TEL] = tm.group(1).strip()
            hm = _HP_PATTERN.search(contact)
            if hm:
                data[Schema.HP] = hm.group(0).strip().rstrip(".,)")
            addr_full = self._extract_address(contact)
            if addr_full:
                pref_m = _PREF_PATTERN.match(addr_full)
                if pref_m:
                    data[Schema.PREF] = pref_m.group(1)
                    data[Schema.ADDR] = addr_full[pref_m.end():].strip()
                else:
                    data[Schema.ADDR] = addr_full

        if Schema.ADDR not in data:
            workplace = kv.get("勤務地", "")
            addr = _LEADING_CIRCLE_NUM.sub("", workplace).strip()
            if addr:
                pref_m = _PREF_PATTERN.match(addr)
                if pref_m:
                    data[Schema.PREF] = pref_m.group(1)
                    data[Schema.ADDR] = addr[pref_m.end():].strip()
                else:
                    data[Schema.ADDR] = addr

        if kv.get("ジャンル"):
            data[Schema.CAT_SITE] = kv["ジャンル"]

        lob_parts: list[str] = []
        if kv.get("ジャンル"):
            lob_parts.append(kv["ジャンル"])
        if kv.get("職種"):
            lob_parts.append(kv["職種"])
        if lob_parts:
            data[Schema.LOB] = " / ".join(lob_parts)[:500]

        line_id = kv.get("LINE ID", "").strip()
        if line_id:
            data[Schema.LINE] = line_id
        else:
            line_a = soup.find("a", href=re.compile(r"line\.me/"))
            if line_a:
                data[Schema.LINE] = (line_a.get("href") or "").strip()

        for col in (
            "職種", "給与", "勤務地", "最寄駅", "特徴",
            "社会保険", "受動喫煙対策", "採用担当者から",
            "応募方法", "面接地",
            "情報最終更新日", "掲載終了予定日",
        ):
            if kv.get(col):
                data[col] = kv[col]
        if catch_txt:
            data["キャッチコピー"] = catch_txt
        data["エリア"] = region

        if not data.get(Schema.NAME):
            return None
        return data

    @staticmethod
    def _extract_address(contact: str) -> str:
        text = re.sub(r"^.*?〒?\s*\d{3}-?\d{4}\s*", "", contact)
        text = re.split(r"TEL[：:]|https?://", text)[0]
        return _clean(text)


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = UpStageScraper()
    scraper.execute(BASE_URL)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
