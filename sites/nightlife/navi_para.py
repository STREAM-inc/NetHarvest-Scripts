"""
ナビパラネット (navi-para.net) — ナイト系店舗情報

取得対象:
    - 掲載店舗 (キャバクラ・ガールズバー・バー・スナック等)

取得フロー:
    一覧ページ (/shoplist/) で全店舗の詳細リンク (/shop/shoptop/nc/{code}/) と
    ジャンルを収集 → 各店舗の詳細ページ (td.item ラベル + 値 td のテーブル) を解析。
    詳細を 1 件取得するごとに即 yield する (Pattern B)。

備考 (呼び出し指示) の全フィールドを取得:
    店名 / 所在地 / TEL / 営業時間 / 定休日 / LADY'S数 / LADY'S衣装 / 予約 / HP

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/navi_para.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id navi_para
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

_PREF_RE = re.compile(
    r"(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県"
    r"|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県"
    r"|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県"
    r"|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 一覧の a.link title 属性: "店名[ジャンル/エリア]の店舗情報はこちらから"
_TITLE_RE = re.compile(r"\[([^/\]]+)/([^\]]+)\]")
_CODE_RE = re.compile(r"/shop/shoptop/nc/(\d+)/")


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _norm_value(text: str) -> str:
    """`-` や空白のみのプレースホルダを空文字に正規化する。"""
    v = _clean(text)
    return "" if v in {"-", "ー", "―", "－", "なし"} else v


class NaviParaScraper(StaticCrawler):
    """ナビパラネット ナイト系店舗スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["LADY'S数", "LADY'S衣装", "予約"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        soup = self.get_soup(url)
        if soup is None:
            return

        # 一覧ページから (詳細URL, ジャンル) を重複除去しつつ収集
        shops: list[tuple[str, str]] = []
        seen: set[str] = set()
        for box in soup.select("div.shopbox"):
            a = box.find("a", href=_CODE_RE)
            if not a:
                continue
            href = a.get("href", "")
            m = _CODE_RE.search(href)
            if not m:
                continue
            code = m.group(1)
            if code in seen:
                continue
            seen.add(code)
            detail_url = urljoin(url, href)
            genre = ""
            tm = _TITLE_RE.search(a.get("title", ""))
            if tm:
                genre = tm.group(1).strip()
            shops.append((detail_url, genre))

        self.total_items = len(shops)

        for detail_url, genre in shops:
            try:
                item = self._scrape_detail(detail_url, genre)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("詳細取得失敗 %s: %s", detail_url, e)

    def _scrape_detail(self, url: str, genre: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}
        if genre:
            data[Schema.CAT_SITE] = genre

        label_td = soup.find("td", class_="item")
        if not label_td:
            return None
        table = label_td.find_parent("table")
        if not table:
            return None

        for tr in table.find_all("tr"):
            item_td = tr.find("td", class_="item")
            if not item_td:
                continue
            tds = tr.find_all("td")
            idx = tds.index(item_td)
            if idx + 1 >= len(tds):
                continue
            value_td = tds[idx + 1]
            key = re.sub(r"\s+", "", item_td.get_text())
            val = _norm_value(value_td.get_text(" ", strip=True))

            if key == "店名":
                data[Schema.NAME] = val
            elif key == "所在地":
                addr = val
                pref_m = _PREF_RE.search(addr)
                if pref_m:
                    data[Schema.PREF] = pref_m.group(0)
                    data[Schema.ADDR] = addr[pref_m.end():].strip()
                else:
                    data[Schema.ADDR] = addr
            elif key == "TEL":
                data[Schema.TEL] = val
            elif key == "営業時間":
                data[Schema.TIME] = val
            elif key == "定休日":
                data[Schema.HOLIDAY] = val
            elif key == "LADY'S数":
                data["LADY'S数"] = val
            elif key == "LADY'S衣装":
                data["LADY'S衣装"] = val
            elif key == "予約":
                data["予約"] = val
            elif key == "HP":
                a = value_td.find("a", href=True)
                data[Schema.HP] = a["href"] if a else val

        return data if data.get(Schema.NAME) else None


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = NaviParaScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://navi-para.net/shoplist/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
