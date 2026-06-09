"""
体入ルート (icondolllounge.jp + tainew.com) — キャバクラ/ナイトワーク体験入店求人スクレイパー

取得対象:
    - icondolllounge.jp の全掲載店舗（約1,136件 / 76ページ）
    - 各店舗の tainew.com 詳細ページから住所・TEL・SNS等を取得

取得フロー:
    1. icondolllounge.jp/lists?page=N を巡回し、各店舗カードから基本情報と tainew.com URL を収集
    2. tainew.com/shop/view/{id}/ から詳細情報を取得
    3. 一覧データ + 詳細データを結合して出力

実行方法:
    python scripts/sites/nightlife/tainyu_route.py
    python bin/run_flow.py --site-id tainyu_route
"""

import re
import sys
from pathlib import Path
from urllib.parse import urlparse, urlunparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


LIST_BASE = "https://icondolllounge.jp/lists"

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


def _clean(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"[ \t　\xa0]+", " ", text).strip()


def _strip_reading(name: str) -> str:
    """'CLUB GEMME(クラブ ジェム)' → 'CLUB GEMME' のように読み仮名の括弧を除去する"""
    return re.sub(r"[（(][^）)]*[ぁ-んァ-ヶー][^）)]*[）)]", "", name).strip()


def _split_pref(addr: str) -> tuple[str, str]:
    addr = _clean(addr)
    if not addr:
        return "", ""
    m = _PREF_PATTERN.match(addr)
    if m:
        return m.group(1), addr[m.end():].strip()
    return "", addr


def _strip_query(url: str) -> str:
    p = urlparse(url)
    return urlunparse(p._replace(query="", fragment=""))


class TainyuRouteScraper(StaticCrawler):
    """体入ルート (icondolllounge.jp → tainew.com) スクレイパー"""

    DELAY = 2.0
    EXTRA_COLUMNS = [
        "キャッチコピー",
        "時給",
        "エリア",
        "キーワード",
        "設備",
        "在籍キャスト",
        "キャストの系統",
        "服装",
        "職種",
        "応募資格",
        "待遇",
        "最寄駅",
        "アクセス",
        "担当",
        "応募方法",
        "受付時間",
    ]

    def parse(self, url: str):
        page = 1
        soup = self.get_soup(f"{LIST_BASE}?page=1")
        if soup is None:
            return

        count_el = soup.select_one(".count")
        if count_el:
            m = re.search(r"([\d,]+)件中", count_el.get_text())
            if m:
                self.total_items = int(m.group(1).replace(",", ""))

        while True:
            cards = [j for j in soup.select("div.job") if "button" not in j.get("class", [])]
            if not cards:
                break

            for card in cards:
                tainew_url = self._get_tainew_url(card)
                if not tainew_url:
                    continue
                list_data = self._extract_list_data(card)
                try:
                    detail_data = self._scrape_detail(tainew_url)
                except Exception:
                    self.logger.exception("詳細取得失敗: %s", tainew_url)
                    detail_data = {Schema.URL: tainew_url}
                yield {**list_data, **detail_data}

            next_page = page + 1
            if not soup.select_one(f'a[href*="page={next_page}"]'):
                break
            page = next_page
            soup = self.get_soup(f"{LIST_BASE}?page={page}")
            if soup is None:
                break

    # ------------------------------------------------------------------ #
    # 一覧ページ
    # ------------------------------------------------------------------ #

    def _get_tainew_url(self, card) -> str:
        a = card.select_one('a[href*="tainew.com"]')
        return _strip_query(a.get("href", "")) if a else ""

    def _extract_list_data(self, card) -> dict:
        h2 = card.select_one("h2")
        h3 = card.select_one("h3")

        longs_no_badge = [
            d for d in card.select(".job-detail.job-detail-long")
            if not d.select_one(".job-detail-badge")
        ]
        wage = ""
        if longs_no_badge:
            p = longs_no_badge[0].select_one("p")
            wage = _clean(p.get_text(strip=True)) if p else ""

        return {
            Schema.NAME: _strip_reading(_clean(h2.get_text(strip=True))) if h2 else "",
            Schema.CAT_SITE: self._badge_field(card, "業種"),
            "キャッチコピー": _clean(h3.get_text(strip=True)) if h3 else "",
            "時給": wage,
            "エリア": self._badge_field(card, "エリア"),
            "キーワード": self._badge_field(card, "キーワード"),
        }

    def _badge_field(self, item, label: str) -> str:
        for detail in item.select(".job-detail"):
            badge = detail.select_one(".job-detail-badge")
            if badge and badge.get_text(strip=True) == label:
                el = detail.select_one("a") or detail.select_one("p")
                return _clean(el.get_text(strip=True)) if el else ""
        return ""

    # ------------------------------------------------------------------ #
    # tainew.com 詳細ページ
    # ------------------------------------------------------------------ #

    def _scrape_detail(self, url: str) -> dict:
        soup = self.get_soup(url)
        if soup is None:
            return {Schema.URL: url}

        info = self._parse_info_map(soup)
        pref, addr = _split_pref(info.get("住所", ""))
        sns = self._parse_sns(soup)

        return {
            Schema.URL: url,
            Schema.PREF: pref,
            Schema.ADDR: f"{pref}{addr}" if pref else addr,
            Schema.TEL: info.get("TEL", ""),
            Schema.TIME: info.get("時間", ""),
            Schema.HOLIDAY: info.get("休日", ""),
            Schema.HP: self._parse_hp(soup),
            Schema.INSTA: sns.get("insta", ""),
            Schema.X: sns.get("x", ""),
            Schema.LINE: sns.get("line", ""),
            Schema.TIKTOK: sns.get("tiktok", ""),
            Schema.FB: sns.get("fb", ""),
            "設備": info.get("設備", ""),
            "在籍キャスト": info.get("在籍キャスト", ""),
            "キャストの系統": info.get("キャストの系統", ""),
            "服装": info.get("働く時の服装", ""),
            "職種": info.get("職種", ""),
            "応募資格": info.get("資格", ""),
            "待遇": info.get("待遇", ""),
            "最寄駅": self._parse_stations(soup),
            "アクセス": info.get("アクセス", ""),
            "担当": info.get("担当", ""),
            "応募方法": info.get("応募方法", ""),
            "受付時間": info.get("受付時間", ""),
        }

    def _parse_info_map(self, soup) -> dict[str, str]:
        info: dict[str, str] = {}
        for row in soup.select(".p-simple-row-info"):
            h3 = row.select_one(".simple-row-info-title")
            data = row.select_one(".simple-row-info-data")
            if not h3 or not data:
                continue
            label = h3.get_text(strip=True)
            if not label or label in info:
                continue
            map_span = data.select_one(".map-open-txt")
            if map_span:
                map_span.decompose()
            val = _clean(data.get_text(" ", strip=True))
            if val:
                info[label] = val
        return info

    def _parse_hp(self, soup) -> str:
        for row in soup.select(".p-simple-row-info"):
            h3 = row.select_one(".simple-row-info-title")
            if not h3 or h3.get_text(strip=True) != "店舗URL":
                continue
            a = row.select_one("a[href]")
            return a.get("href", "") if a else ""
        return ""

    def _parse_stations(self, soup) -> str:
        for row in soup.select(".p-simple-row-info"):
            h3 = row.select_one(".simple-row-info-title")
            if not h3 or h3.get_text(strip=True) != "最寄駅":
                continue
            data = row.select_one(".simple-row-info-data")
            if not data:
                break
            text = _clean(data.get_text(" ", strip=True))
            # "線名 - 駅名" 形式に整える
            text = re.sub(r"\s*-\s*", "-", text)
            text = re.sub(r"\s+", " / ", text)
            return text
        return ""

    def _parse_sns(self, soup) -> dict[str, str]:
        result = {"insta": "", "x": "", "line": "", "tiktok": "", "fb": ""}
        for row in soup.select(".p-simple-row-info"):
            h3 = row.select_one(".simple-row-info-title")
            if not h3 or h3.get_text(strip=True) != "SNS":
                continue
            for a in row.select("a[href]"):
                href = a.get("href", "")
                if "instagram.com" in href:
                    result["insta"] = href
                elif ("twitter.com" in href or "x.com" in href) and "share" not in href and "intent" not in href:
                    result["x"] = href
                elif "line.me" in href or "lin.ee" in href:
                    result["line"] = href
                elif "tiktok.com" in href:
                    result["tiktok"] = href
                elif "facebook.com" in href:
                    result["fb"] = href
        return result


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = TainyuRouteScraper()
    scraper.execute(LIST_BASE)

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
