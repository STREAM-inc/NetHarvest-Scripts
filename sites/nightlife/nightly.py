"""
ナイトリー — 夜のお仕事求人サイト クローラー

取得対象:
    - 夜職（スナック・キャバクラ・コンカフェ等）求人情報

取得フロー:
    /job/ (一覧) → ページネーション /job/page/{N}/ → 各店舗詳細 /shops/shop{ID}/job/

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/nightly.py

    # Prefect Flow 経由
    python bin/run_flow.py --site-id nightly
"""

import re
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

_PREF_RE = re.compile(
    r"^(北海道|東京都|大阪府|京都府|"
    r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|"
    r"新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|"
    r"和歌山|鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|"
    r"長崎|熊本|大分|宮崎|鹿児島|沖縄)県)"
)
_NAME_KANA_RE = re.compile(r"[（(]([^）)]+)[）)]")
_TEL_NOISE_RE = re.compile(r"[「」（()）\s].+")
_TOTAL_RE = re.compile(r"全(\d+)件")


class NightlyCrawler(StaticCrawler):
    """ナイトリー 求人クローラー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["最寄駅", "募集職種", "体験入店時給", "本入店時給", "在籍年齢"]

    def parse(self, url: str):
        page = 1
        while True:
            page_url = url if page == 1 else f"{url}page/{page}/"
            soup = self.get_soup(page_url)
            articles = soup.select("article.shop_flame")
            if not articles:
                break

            if page == 1:
                total_el = soup.select_one("p.display_number")
                if total_el:
                    m = _TOTAL_RE.search(total_el.get_text())
                    if m:
                        self.total_items = int(m.group(1))

            for article in articles:
                detail_link = article.select_one("a.shop_link")
                if not detail_link:
                    continue
                detail_url = detail_link.get("href", "")

                tel_link = article.select_one("a.phone")
                tel_fallback = ""
                if tel_link:
                    tel_fallback = tel_link.get("href", "").replace("tel:", "").strip()

                try:
                    item = self._scrape_detail(detail_url, tel_fallback)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.error(f"Detail error {detail_url}: {e}")
                    continue

            page += 1

    def _scrape_detail(self, url: str, tel_fallback: str = "") -> dict | None:
        soup = self.get_soup(url)

        # th → td テキストマッピング
        fields: dict[str, str] = {}
        for tr in soup.find_all("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if th and td and th.get_text(strip=True) != "SNS":
                fields[th.get_text(strip=True)] = td.get_text(strip=True)

        # 店名・読み仮名
        shop_name_raw = fields.get("店名", "")
        m = _NAME_KANA_RE.search(shop_name_raw)
        name_kana = m.group(1) if m else ""
        name = _NAME_KANA_RE.sub("", shop_name_raw).strip()

        # 住所・都道府県分割
        addr_full = fields.get("住所", "")
        pref = ""
        addr = addr_full
        m = _PREF_RE.match(addr_full)
        if m:
            pref = m.group(1)
            addr = addr_full[m.end():].strip()

        # 電話番号（末尾の案内文を除去）
        tel = fields.get("電話番号", tel_fallback)
        tel = _TEL_NOISE_RE.sub("", tel).strip()

        # SNS リンク
        line_url = x_url = insta_url = ""
        sns_ul = soup.select_one("ul.user_sns")
        if sns_ul:
            for a in sns_ul.select("a[href]"):
                cls = " ".join(a.get("class", []))
                href = a.get("href", "")
                if "fl_li2" in cls or "line.me" in href or "lin.ee" in href:
                    line_url = href
                elif "fl_tw2" in cls or "x.com" in href or "twitter.com" in href:
                    x_url = href
                elif "fl_in2" in cls or "instagram.com" in href:
                    insta_url = href

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.NAME_KANA: name_kana,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.CAT_SITE: fields.get("業種", ""),
            Schema.TIME: fields.get("営業時間", ""),
            Schema.HOLIDAY: fields.get("定休日", ""),
            Schema.LINE: line_url,
            Schema.X: x_url,
            Schema.INSTA: insta_url,
            "最寄駅": fields.get("最寄駅", ""),
            "募集職種": fields.get("募集職種", ""),
            "体験入店時給": fields.get("時給（体験入店）", ""),
            "本入店時給": fields.get("時給（本入店）", ""),
            "在籍年齢": fields.get("在籍年齢", ""),
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = NightlyCrawler()
    scraper.execute("https://nightly.jp/job/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
