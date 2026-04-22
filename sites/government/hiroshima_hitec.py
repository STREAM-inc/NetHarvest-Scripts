# scripts/sites/government/hiroshima_hitec.py
"""
ひろしまの企業情報 (hitec.city.hiroshima.jp) — 広島市企業データベーススクレイパー

取得対象:
    - 製造業 (E, 242件)
    - 情報通信業 (G, 61件)
    - 卸売業 (I, 25件)
    - デザイン業 (L, 40件)
    合計 約368件

取得フロー:
    ichiran.php?gyo=X&p=N (X∈{E,G,I,L}) で各カテゴリを全ページ巡回
        → 各企業の詳細ページ (EJ/GJ/IJ/LJ prefix) を取得

実行方法:
    # ローカルテスト
    python scripts/sites/government/hiroshima_hitec.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id hiroshima_hitec
"""

import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

BASE = "https://www.hitec.city.hiroshima.jp"

_CATEGORIES = [
    ("E", "EJ"),  # 製造業
    ("G", "GJ"),  # 情報通信業
    ("I", "IJ"),  # 卸売業
    ("L", "LJ"),  # デザイン業
]

_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_TOTAL_RE = re.compile(r"\[\s*(\d+)\s*件")


class HiroshimaHitecScraper(StaticCrawler):
    """ひろしまの企業情報 企業スクレイパー"""

    DELAY = 1.0
    EXTRA_COLUMNS = [
        "代表者フリガナ",
        "売上高",
        "FAX番号",
        "E-Mail",
        "主要製品",
        "主要取引先",
        "保有技術",
        "主要設備",
        "最終更新日",
    ]

    def parse(self, url: str):
        # 全カテゴリから詳細URLを収集
        detail_urls: list[str] = []
        totals: dict[str, int] = {}
        seen: set[str] = set()

        for gyo, prefix in _CATEGORIES:
            urls, total = self._collect_urls(gyo, prefix)
            totals[gyo] = total
            for u in urls:
                if u not in seen:
                    seen.add(u)
                    detail_urls.append(u)

        self.total_items = len(detail_urls)
        self.logger.info(
            "詳細URL収集完了: 合計 %d 件 (内訳 E=%d G=%d I=%d L=%d)",
            len(detail_urls), totals.get("E", 0), totals.get("G", 0),
            totals.get("I", 0), totals.get("L", 0),
        )

        for du in detail_urls:
            try:
                item = self._scrape_detail(du)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("詳細ページ取得失敗: %s (%s)", du, e)

    def _collect_urls(self, gyo: str, prefix: str) -> tuple[list[str], int]:
        """1カテゴリ分の詳細URLを全ページから収集"""
        collected: list[str] = []
        page = 1
        total = 0
        max_page: int | None = None

        while True:
            list_url = f"{BASE}/ichiran.php?gyo={gyo}&p={page}"
            soup = self.get_soup(list_url)

            if page == 1:
                text = soup.get_text(" ", strip=True)
                m = _TOTAL_RE.search(text)
                if m:
                    total = int(m.group(1))
                # 最終ページ番号を取得
                nums = []
                for a in soup.select(f'a[href*="ichiran.php?gyo={gyo}&p="]'):
                    pm = re.search(r"p=(\d+)", a.get("href", ""))
                    if pm:
                        nums.append(int(pm.group(1)))
                if nums:
                    max_page = max(nums)

            anchors = soup.select(f'a[href*="{prefix}/{prefix.lower()[0]}"]')
            # より厳密にパスで絞り込む
            page_urls = []
            for a in anchors:
                href = a.get("href", "")
                if re.search(rf"{prefix}/[a-z]+\d+\.html", href):
                    full = href if href.startswith("http") else f"{BASE}/{href.lstrip('/')}"
                    if full not in page_urls:
                        page_urls.append(full)

            if not page_urls:
                break
            collected.extend(u for u in page_urls if u not in collected)

            if max_page and page >= max_page:
                break
            page += 1

        return collected, total

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)

        # 会社名とフリガナ (h4 内)
        h4 = soup.select_one("h4")
        if not h4:
            return None
        name_el = h4.select_one("p.shamei")
        if not name_el:
            return None
        name = name_el.get_text(strip=True)
        if not name:
            return None

        item: dict = {Schema.URL: url, Schema.NAME: name}

        kana_el = h4.select_one("p.kana")
        if kana_el:
            kana = kana_el.get_text(strip=True)
            if kana:
                item[Schema.NAME_KANA] = kana

        gyo_img = h4.select_one("span.gyo img[alt]")
        if gyo_img:
            item[Schema.CAT_SITE] = gyo_img.get("alt", "").strip()

        # 最終更新日
        upd = soup.select_one("div.u-right.u-mb8")
        if upd:
            t = upd.get_text(strip=True).replace("最終更新日：", "").strip()
            if t:
                item["最終更新日"] = t

        # dl.gaiyoList のパース
        gaiyo = soup.select_one("dl.gaiyoList")
        if gaiyo:
            self._parse_gaiyo(gaiyo, item)

        # h5 セクション: 会社PR, 主要製品, 主要取引先, 保有技術, 主要設備
        self._parse_sections(soup, item)

        return item

    def _parse_gaiyo(self, gaiyo, item: dict) -> None:
        current_dt = None
        for child in gaiyo.find_all(["dt", "dd"], recursive=False):
            if child.name == "dt":
                current_dt = child.get_text(strip=True)
            elif child.name == "dd" and current_dt:
                value = " ".join(child.stripped_strings)
                value = re.sub(r"\s+", " ", value).strip()
                # 「地図を見る」などのノイズを除去
                value = re.sub(r"\s*地図を見る\s*$", "", value).strip()
                self._apply_gaiyo_field(current_dt, value, child, item)
                current_dt = None

    def _apply_gaiyo_field(self, label: str, value: str, dd, item: dict) -> None:
        if label == "代表者名":
            item[Schema.REP_NM] = value
        elif label == "フリガナ":
            item["代表者フリガナ"] = value
        elif label == "設立年":
            item[Schema.OPEN_DATE] = value
        elif label == "資本金":
            item[Schema.CAP] = value
        elif label == "売上高":
            item["売上高"] = value
        elif label == "従業員数":
            item[Schema.EMP_NUM] = value
        elif label == "郵便番号":
            item[Schema.POST_CODE] = value
        elif label == "会社住所":
            pm = _PREF_RE.search(value)
            if pm:
                item[Schema.PREF] = pm.group(1)
                item[Schema.ADDR] = value[pm.end():].strip()
            else:
                # 「広島市～」のみの場合は広島県を補完
                item[Schema.PREF] = "広島県"
                item[Schema.ADDR] = value
        elif label == "電話番号":
            item[Schema.TEL] = value
        elif label == "FAX番号":
            item["FAX番号"] = value
        elif label == "E-Mail":
            item["E-Mail"] = value
        elif label == "URL":
            a = dd.select_one("a[href]")
            if a:
                item[Schema.HP] = a["href"].strip()
            elif value.startswith("http"):
                item[Schema.HP] = value

    def _parse_sections(self, soup, item: dict) -> None:
        """<h5>ラベル</h5> に続くブロックからテキストを抽出"""
        section_map = {
            "会社PR": "_pr",
            "主要製品": "主要製品",
            "主要取引先": "主要取引先",
            "保有技術": "保有技術",
            "主要設備": "主要設備",
        }

        for h5 in soup.select("h5"):
            label = h5.get_text(strip=True)
            key = section_map.get(label)
            if not key:
                continue

            # 次の兄弟要素(div または dl)からテキストを抽出
            nxt = h5.find_next_sibling()
            if not nxt:
                continue

            if nxt.name == "dl":
                parts = []
                for dd in nxt.find_all("dd"):
                    t = " ".join(dd.stripped_strings)
                    t = re.sub(r"\s+", " ", t).strip()
                    if t:
                        parts.append(t)
                text = " / ".join(parts)
            else:
                text = " ".join(nxt.stripped_strings)
                text = re.sub(r"\s+", " ", text).strip()

            if not text:
                continue

            if key == "_pr":
                # 会社PR は事業内容として Schema.LOB に入れる
                item[Schema.LOB] = text[:800]
            else:
                item[key] = text[:500]


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = HiroshimaHitecScraper()
    scraper.execute(BASE)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
