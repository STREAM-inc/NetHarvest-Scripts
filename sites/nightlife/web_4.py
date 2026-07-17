"""
すすきのタウン情報WEB (nomi-dokoro) — 札幌・すすきのの飲食・ナイト店舗ディレクトリ

取得対象:
    - すすきの「飲み処」カテゴリの店舗情報 (店名 / TEL / 営業時間 / 定休日 / 住所 / ジャンル / 公式サイト)

取得フロー:
    1. 引数 url (index.php) を起点に一覧ページ shop.php を導出
    2. shop.php?page=N を 1 ページずつ巡回 (div.page-shop-parts、10件/ページ、全9ページ・約81件)
    3. 一覧ブロックから店名・TEL・営業時間・定休日・住所・ジャンルを取得
    4. 各店の詳細ページ (shop-detail.php?sid=N) を開き公式サイト(HP)を補完し、1件ずつ即 yield

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/web_4.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id web_4
"""

import logging
import sys
import urllib.parse
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler

logger = logging.getLogger(__name__)

# 一覧ブロックの dt ラベル → 処理の対応。ラベルは「営業時間 :」等 (末尾の " :" は除去して照合)
_LABEL_TIME = "営業時間"
_LABEL_HOLIDAY = "定休日"
_LABEL_ADDR = "住所"
_LABEL_GENRE = "ジャンル"


class SusukinoNomidokoro(StaticCrawler):
    """すすきのタウン情報WEB (飲み処) スクレイパー"""

    DELAY = 1.5
    # サイト固有の構造化カラム。ジャンル(ニュークラブ/BAR等)は CAT_SITE に格納するため EXTRA は無し。
    # 料金システム表は自由記述の価格プロースのため著作権リスク回避で取得しない。
    EXTRA_COLUMNS = []

    def parse(self, url: str):
        # url = 正規ルート (index.php)。一覧・詳細 URL はここから派生させる (SSOT)。
        list_base = urllib.parse.urljoin(url, "shop.php")

        page = 1
        while True:
            list_url = f"{list_base}?page={page}"
            soup = self.get_soup(list_url)
            if soup is None:
                logger.warning("一覧ページ取得失敗: %s", list_url)
                break

            parts = soup.select("div.page-shop-parts")
            if not parts:
                # これ以上ページが無い
                break

            for part in parts:
                try:
                    item = self._parse_list_block(part, url)
                    if item:
                        yield item
                except Exception as e:  # 個別アイテムのエラーは握って続行
                    logger.warning("店舗ブロック解析エラー (page=%s): %s", page, e)
                    continue

            page += 1

    def _parse_list_block(self, part, root_url: str) -> dict | None:
        """一覧の 1 ブロックを解析し、詳細ページから HP を補完して返す。"""
        # 店名: dd.pspi-name、無ければ h4 内 img の alt
        name_el = part.select_one("dd.pspi-name")
        name = name_el.get_text(strip=True) if name_el else ""
        if not name:
            img = part.select_one("h4 img[alt]")
            name = img.get("alt", "").strip() if img else ""
        if not name:
            return None

        tel_el = part.select_one("dd.pspi-tel")
        tel = tel_el.get_text(strip=True) if tel_el else ""

        time_val = holiday = addr = genre = ""
        for dl in part.select("dl"):
            dt = dl.select_one("dt")
            dd = dl.select_one("dd")
            if not dt or not dd:
                continue
            label = dt.get_text(strip=True).rstrip("：: ").strip()
            value = dd.get_text(strip=True)
            if label == _LABEL_TIME:
                time_val = value
            elif label == _LABEL_HOLIDAY:
                holiday = value
            elif label == _LABEL_ADDR:
                addr = value
            elif label == _LABEL_GENRE:
                genre = value

        # 詳細ページ URL (公式サイト補完用)
        detail_url = root_url
        hp = ""
        link = part.select_one("a.fl[href]")
        if link:
            detail_url = urllib.parse.urljoin(root_url, link["href"])
            hp = self._fetch_hp(detail_url)

        return {
            Schema.NAME: name,
            # すすきの(札幌)地域限定ディレクトリのため都道府県は北海道で確定
            Schema.PREF: "北海道",
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.TIME: time_val,
            Schema.HOLIDAY: holiday,
            Schema.CAT_SITE: genre,
            Schema.HP: hp,
            Schema.URL: detail_url,
        }

    def _fetch_hp(self, detail_url: str) -> str:
        """詳細ページの OFFICIAL SITE ボタンから公式サイト URL を取得する。"""
        soup = self.get_soup(detail_url)
        if soup is None:
            return ""
        for a in soup.select("a.ghostBtn[href]"):
            if "OFFICIAL" in a.get_text(strip=True).upper():
                return a["href"].strip()
        return ""


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = SusukinoNomidokoro()
    # 🔒 sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://www.susukino.gr.jp/nomi-dokoro/index.php")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
