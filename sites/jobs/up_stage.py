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
# H1 例: "店名の<wbr>男性求人募集"。<wbr> が get_text で空白化するため、
# 「の」直後や各語間に空白が入っても末尾の定型句を除去できるようにする。
_NAME_SUFFIX = re.compile(r"\s*の?\s*男性\s*求人\s*募集\s*$")
_COUNT_PATTERN = re.compile(r"([\d,]+)\s*件\s*の求人があります")
# 勤務地/連絡先の先頭に付く丸数字(①②…)
_LEADING_CIRCLE_NUM = re.compile(r"^[①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬⑭⑮⑯⑰⑱⑲⑳]+")
# 複数勤務地を区切る丸数字(途中に出現する①②…)
_CIRCLE_NUM = re.compile(r"[①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬⑭⑮⑯⑰⑱⑲⑳]")
# 郵便番号トークン(住所文字列から除去する用)
_POST_TOKEN = re.compile(r"〒?\s*\d{3}-?\d{4}\s*")


def _clean(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


class UpStageScraper(StaticCrawler):
    """アップステージ 男性高収入求人サイトスクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "職種",
        "給与",
        "勤務地",
        "最寄駅",
        "特徴",
        "社会保険",
        "受動喫煙対策",
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

            detail_urls: list[str] = []
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
                detail_urls.append(detail_url)

            for detail_url in detail_urls:
                try:
                    item = self._scrape_detail(detail_url, region)
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

    def _scrape_detail(self, url: str, region: str) -> dict | None:
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

        # 住所は「勤務地」が最もクリーン(店名・TEL を含まない)なので優先し、
        # 無ければ「連絡先」から復元する。いずれも _extract_pref_addr で正規化。
        pref, addr = self._extract_pref_addr(kv.get("勤務地", "") or contact)
        if pref:
            data[Schema.PREF] = pref
        if addr:
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
            "社会保険", "受動喫煙対策",
            "応募方法", "面接地",
            "情報最終更新日", "掲載終了予定日",
        ):
            if kv.get(col):
                data[col] = kv[col]
        data["エリア"] = region

        if not data.get(Schema.NAME):
            return None
        return data

    @classmethod
    def _extract_pref_addr(cls, raw: str) -> tuple[str, str]:
        """生の勤務地/連絡先文字列から (都道府県, 市区町村以降) を抽出する。

        当サイトの住所欄には以下のノイズが混入するため、順に除去する:
            - 店名・職種が住所の前に連結される (例: "店名 職種 大阪府…")
            - 「都道府県+市区」が二重連結される (例: "大阪府豊中市大阪府豊中市東寺内町…")
            - 先頭/途中の丸数字 (①②…) による複数勤務地の列挙
            - 〒郵便番号・TEL・URL・※注記・【面接地】等の付随情報
        """
        s = _clean(raw)
        if not s:
            return "", ""
        # 【勤務地】… があれば実際の勤務地はその直後に書かれている
        if "【勤務地】" in s:
            s = s.split("【勤務地】", 1)[1]
        # 注記・面接地・連絡先など、住所より後ろの情報を切り落とす
        s = re.split(r"※|【|TEL[：:]|https?://", s)[0]
        # 先頭の丸数字を除去 → 途中の丸数字以降(2件目以降の勤務地)は捨て先頭1件のみ採用
        s = _LEADING_CIRCLE_NUM.sub("", s).strip()
        s = _CIRCLE_NUM.split(s)[0]
        # 郵便番号トークンを除去
        s = _clean(_POST_TOKEN.sub(" ", s))
        return cls._split_pref(s)

    @staticmethod
    def _split_pref(s: str) -> tuple[str, str]:
        """都道府県名を起点に (都道府県, 市区町村以降) へ分割する。

        都道府県より前の文字列(店名など)は捨てる。
        「都道府県+市区」が二重連結されている場合は重複分を除去する。
        """
        s = _clean(s)
        m1 = _PREF_PATTERN.search(s)
        if not m1:
            return "", s
        m2 = _PREF_PATTERN.search(s, m1.end())
        if m2:
            dup = s[m1.start():m2.start()]
            # 例: "大阪府豊中市" + "大阪府豊中市東寺内町…" → 後半(完全な住所)を採用
            if dup and s[m2.start():].startswith(dup):
                s = s[m2.start():]
            else:
                s = s[m1.start():]
        else:
            s = s[m1.start():]
        pref_m = _PREF_PATTERN.match(s)
        return pref_m.group(1), s[pref_m.end():].strip()


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
