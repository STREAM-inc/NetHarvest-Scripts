"""
食べログ — レストラン・飲食店情報スクレイパー

取得対象:
    - 食べログの全国一覧から各店舗の基本情報・詳細情報
    - エントリ: https://tabelog.com/rstLst/{page}/  (page=1..60, 20件/ページ)
    - 食べログの全国検索は最大60ページでキャップされるため、取得上限は約 1,200 件

取得フロー:
    一覧ページ (/rstLst/{page}/) を順にクロールし、各 .list-rst の data-detail-url から
    詳細ページ URL を収集 → 詳細ページから店舗情報テーブルをパースして全カラムを抽出

実行方法:
    # ローカルテスト
    python scripts/sites/food/tabelog.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id tabelog
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

_LIST_URL_TEMPLATE = "https://tabelog.com/rstLst/{page}/"
_MAX_PAGES = 60  # 食べログの全国検索は最大60ページでキャップ
_STATION_PATTERN = re.compile(r"([^\s、。　]+駅から\s*\d+(?:\.\d+)?\s*[km]+)")
_HOLIDAY_PATTERN = re.compile(r"定休日[ :：]*([^■]+)")
_BUDGET_PATTERN = re.compile(r"￥[\d,]+[～〜](?:￥[\d,]+)?")


class TabelogScraper(StaticCrawler):
    """食べログ スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "サイト定義ジャンル",
        "予約可否",
        "交通手段",
        "最寄駅情報",
        "予算_夜",
        "予算_昼",
        "予算_口コミ集計",
        "席数",
        "最大予約可能人数",
        "個室",
        "貸切",
        "禁煙・喫煙",
        "駐車場",
        "空間・設備",
        "コース",
        "ドリンク",
        "料理",
        "利用シーン",
        "サービス",
        "お子様連れ",
        "備考",
        "評価点",
        "口コミ数",
        "保存人数",
    ]

    def parse(self, url: str) -> Generator[dict, None, None]:
        shop_urls = self._collect_shop_urls()
        self.total_items = len(shop_urls)
        self.logger.info("詳細取得対象: %d 件", self.total_items)

        for shop_url in shop_urls:
            try:
                item = self._scrape_detail(shop_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("詳細取得エラー: %s (%s)", shop_url, e)
                continue

    def _collect_shop_urls(self) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        for page in range(1, _MAX_PAGES + 1):
            list_url = _LIST_URL_TEMPLATE.format(page=page)
            soup = self.get_soup(list_url)
            if soup is None:
                self.logger.warning("一覧取得失敗: page=%d", page)
                break
            items = soup.select(".list-rst")
            if not items:
                self.logger.info("page=%d: 0件 → 終了", page)
                break
            for item in items:
                detail_url = item.get("data-detail-url") or ""
                if not detail_url:
                    a = item.select_one("h3.list-rst__rst-name a")
                    if a and a.get("href"):
                        detail_url = a["href"].strip()
                if detail_url and detail_url not in seen:
                    seen.add(detail_url)
                    urls.append(detail_url)
            self.logger.info("page=%d: 累計 %d 件", page, len(urls))
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}

        name_el = soup.select_one("h2.display-name")
        if name_el:
            data[Schema.NAME] = name_el.get_text(strip=True)

        # 全 th-td ペアを辞書化
        fields: dict = {}
        for table in soup.select("table.rstinfo-table__table"):
            for tr in table.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if th and td:
                    key = re.sub(r"\s+", "", th.get_text())
                    if key not in fields:
                        fields[key] = td

        def _text(key: str) -> str:
            td = fields.get(key)
            if td is None:
                return ""
            txt = td.get_text(" ", strip=True)
            txt = txt.replace("大きな地図を見る", "").replace("周辺のお店を探す", "").strip()
            return re.sub(r"\s+", " ", txt)

        # 店名カナ (fields["店名"] の "（...）" 内)
        shop_name_td = fields.get("店名")
        if shop_name_td:
            inner = shop_name_td.get_text(" ", strip=True)
            m = re.search(r"（([^）]+)）", inner)
            if m:
                kana = m.group(1).strip()
                # 「【旧店名】xxx」は除外
                if not kana.startswith("【"):
                    data[Schema.NAME_KANA] = kana

        # 住所 → 都道府県抽出
        addr_p = soup.select_one(".rstinfo-table__address")
        if addr_p:
            addr_text = addr_p.get_text(" ", strip=True)
            addr_text = addr_text.replace("大きな地図を見る", "").replace("周辺のお店を探す", "").strip()
            addr_text = re.sub(r"\s+", " ", addr_text)
        else:
            addr_text = _text("住所")
        if addr_text:
            m = _PREF_PATTERN.match(addr_text)
            if m:
                data[Schema.PREF] = m.group(1)
                data[Schema.ADDR] = addr_text[m.end():].strip()
            else:
                data[Schema.ADDR] = addr_text

        tel = _text("電話番号")
        if tel:
            data[Schema.TEL] = tel

        genre = _text("ジャンル")
        if genre:
            data[Schema.CAT_SITE] = genre
            data["サイト定義ジャンル"] = genre

        hp_td = fields.get("ホームページ")
        if hp_td:
            a = hp_td.find("a", href=True)
            if a:
                data[Schema.HP] = a["href"].strip()
            else:
                hp_txt = hp_td.get_text(strip=True)
                if hp_txt:
                    data[Schema.HP] = hp_txt

        hours = _text("営業時間")
        if hours:
            data[Schema.TIME] = hours
            hm = _HOLIDAY_PATTERN.search(hours)
            if hm:
                data[Schema.HOLIDAY] = hm.group(1).strip()

        pay = _text("支払い方法")
        if pay:
            data[Schema.PAY] = pay

        open_date = _text("オープン日")
        if open_date:
            data[Schema.OPEN_DATE] = open_date

        # 公式アカウント (Facebook / X / Instagram / LINE / TikTok)
        official_td = fields.get("公式アカウント")
        if official_td:
            for a in official_td.find_all("a", href=True):
                href = a["href"].strip()
                if "facebook.com" in href and not data.get(Schema.FB):
                    data[Schema.FB] = href
                elif ("twitter.com" in href or "x.com" in href) and not data.get(Schema.X):
                    data[Schema.X] = href
                elif "instagram.com" in href and not data.get(Schema.INSTA):
                    data[Schema.INSTA] = href
                elif ("line.me" in href or "lin.ee" in href) and not data.get(Schema.LINE):
                    data[Schema.LINE] = href
                elif "tiktok.com" in href and not data.get(Schema.TIKTOK):
                    data[Schema.TIKTOK] = href

        # EXTRA_COLUMNS
        simple_extras = {
            "予約可否": "予約可否",
            "交通手段": "交通手段",
            "席数": "席数",
            "最大予約可能人数": "最大予約可能人数",
            "個室": "個室",
            "貸切": "貸切",
            "禁煙・喫煙": "禁煙・喫煙",
            "駐車場": "駐車場",
            "空間・設備": "空間・設備",
            "コース": "コース",
            "ドリンク": "ドリンク",
            "料理": "料理",
            "利用シーン": "利用シーン",
            "サービス": "サービス",
            "お子様連れ": "お子様連れ",
            "備考": "備考",
        }
        for src_key, out_key in simple_extras.items():
            val = _text(src_key)
            if val:
                data[out_key] = val

        access = _text("交通手段")
        if access:
            sm = _STATION_PATTERN.search(access)
            if sm:
                data["最寄駅情報"] = sm.group(1)

        budget_td = fields.get("予算")
        if budget_td:
            btxt = budget_td.get_text(" ", strip=True)
            parts = _BUDGET_PATTERN.findall(btxt)
            if parts:
                data["予算_夜"] = parts[0]
                if len(parts) > 1:
                    data["予算_昼"] = parts[1]

        budget_review_td = fields.get("予算（口コミ集計）")
        if budget_review_td:
            btxt = budget_review_td.get_text(" ", strip=True).replace("利用金額分布を見る", "").strip()
            data["予算_口コミ集計"] = re.sub(r"\s+", " ", btxt)

        rating_el = soup.select_one(
            ".rdheader-rating__score-val, .rdheader-rating__score-val-text, .rating-val"
        )
        if rating_el:
            rv = rating_el.get_text(strip=True)
            if re.match(r"^\d+\.\d+$", rv):
                data["評価点"] = rv

        review_el = soup.select_one(".rdheader-rating__review-target em, .rvw-count-num")
        if review_el:
            rc = re.sub(r"[^0-9]", "", review_el.get_text())
            if rc:
                data["口コミ数"] = rc

        save_el = soup.select_one(".rdheader-counts__hozon-target em, .save-count-num")
        if save_el:
            sv = re.sub(r"[^0-9]", "", save_el.get_text())
            if sv:
                data["保存人数"] = sv

        if not data.get(Schema.NAME):
            return None
        return data


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = TabelogScraper()
    scraper.execute("https://tabelog.com/rstLst/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
