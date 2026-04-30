"""
ビズリサーチ — 事業者限定の代理店募集・フランチャイズ募集サイト

取得対象:
    - /search?seed=1&page=N で全件ページネーション
    - 各 /prod/{id} 詳細から 会社情報・ビジネス概要 を取得

実行方法:
    python scripts/sites/agency_franchise/bizresearch.py
    python bin/run_flow.py --site-id bizresearch
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

BASE_URL = "https://bizresearch.net"
LIST_URL = BASE_URL + "/search?seed=1&page={page}"

PREFS = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
]

_POST_RE = re.compile(r"〒?\s*(\d{3})-?(\d{4})")
_TOTAL_RE = re.compile(r"検索結果[：:]?\s*(\d+)\s*件")


class BizresearchScraper(StaticCrawler):
    """ビズリサーチ スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "募集タイトル",
        "サービス名",
        "キャッチコピー",
        "カテゴリ",
        "募集エリア",
        "サポート体制",
        "初期費用",
        "加盟金",
        "保証金",
        "マージン率",
        "顧客の特徴",
        "市場性",
        "競合",
        "強み",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        page = 1
        seen_total = False
        while True:
            list_url = LIST_URL.format(page=page)
            soup = self.get_soup(list_url)
            if soup is None:
                break

            if not seen_total:
                disp = soup.select_one(".dispNum")
                if disp:
                    m = _TOTAL_RE.search(disp.get_text(" ", strip=True))
                    if m:
                        self.total_items = int(m.group(1))
                seen_total = True

            cards = soup.select(".box_list01")
            if not cards:
                break

            for card in cards:
                list_data = self._parse_list_card(card)
                detail_url = list_data.pop("_detail_url", None)
                if not detail_url:
                    continue
                try:
                    detail_data = self._scrape_detail(detail_url)
                except Exception as e:
                    self.logger.warning("詳細取得失敗: %s — %s", detail_url, e)
                    detail_data = None
                if not detail_data:
                    continue
                merged = {**list_data, **detail_data}
                yield merged

            if not soup.select_one(f'.pagination a[href*="page={page + 1}"]'):
                break
            page += 1

    def _parse_list_card(self, card) -> dict:
        data: dict = {}
        link = card.select_one('a[href*="/prod/"]')
        if link and link.get("href"):
            data["_detail_url"] = link["href"]
        h2 = card.select_one("h2")
        if h2:
            data["募集タイトル"] = h2.get_text(strip=True)
        p = card.select_one(".box_list01_wrap + p, p")
        if p:
            txt = p.get_text(strip=True)
            if txt:
                data["キャッチコピー"] = txt
        cats = [
            s.get_text(strip=True)
            for s in card.select(".cat_wt")
            if s.get_text(strip=True)
        ]
        if cats:
            data["カテゴリ"] = " / ".join(dict.fromkeys(cats))
        return data

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}

        h1 = soup.select_one("h1")
        if h1:
            data["募集タイトル"] = h1.get_text(strip=True)

        green = soup.select_one(".lead .cat_green") or soup.select_one(".cat_green")
        if green:
            data["サービス名"] = green.get_text(strip=True)

        areas = [
            s.get_text(strip=True)
            for s in soup.select(".area-summary .cat_blue")
            if s.get_text(strip=True)
        ]
        if areas:
            data["募集エリア"] = " / ".join(dict.fromkeys(areas))

        for table in soup.select("table.main"):
            for tr in table.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if not th or not td:
                    continue
                key = th.get_text(strip=True)
                val = td.get_text("\n", strip=True)
                val = re.sub(r"\n{2,}", "\n", val).strip()
                if not key or not val:
                    continue
                self._assign_field(data, key, val)

        if not data.get(Schema.NAME):
            return None
        return data

    def _assign_field(self, data: dict, key: str, val: str) -> None:
        if "会社名" in key:
            data[Schema.NAME] = val
        elif "所在地" in key:
            self._parse_address(data, val)
        elif "設立" in key:
            data[Schema.OPEN_DATE] = val
        elif "従業員" in key:
            data[Schema.EMP_NUM] = val.split("\n", 1)[0].strip()
        elif "事業内容" in key:
            data[Schema.LOB] = val
        elif key in (
            "サポート体制",
            "初期費用",
            "加盟金",
            "保証金",
            "マージン率",
            "顧客の特徴",
            "市場性",
            "競合",
            "強み",
        ):
            data[key] = val

    def _parse_address(self, data: dict, val: str) -> None:
        flat = re.sub(r"\s+", " ", val).strip()
        m = _POST_RE.search(flat)
        if m:
            data[Schema.POST_CODE] = f"〒{m.group(1)}-{m.group(2)}"
            flat = (flat[: m.start()] + flat[m.end():]).strip()
        data[Schema.ADDR] = flat
        for p in PREFS:
            if flat.startswith(p) or p in flat[:10]:
                data[Schema.PREF] = p
                break


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    scraper = BizresearchScraper()
    scraper.execute(BASE_URL + "/")
    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
