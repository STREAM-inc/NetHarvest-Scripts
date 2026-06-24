"""
便利屋お助けナビ — 便利屋(お困りごと解決業者)ディレクトリ

取得対象:
    - 全国の便利屋の店舗情報(店舗名・代表者名・地域・電話番号・各種URL)

取得フロー:
    WordPress REST API (wp-json/wp/v2/helperinfo) で投稿一覧を取得し、
    各投稿の詳細ページ (/helperinfo/{slug}) の div.left03 から構造化フィールドを抽出する。
    一覧 → 詳細を 1 件ずつ取得即 yield (Pattern B)。

実行方法:
    # ローカルテスト
    python scripts/sites/service/helper.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id helper
"""

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class Helper(StaticCrawler):
    """便利屋お助けナビ スクレイパー"""

    DELAY = 1.5
    PER_PAGE = 50  # API ページサイズ (read timeout 回避のため小さめ)
    EXTRA_COLUMNS = ["SNS"]

    def parse(self, url: str):
        # 引数 url を唯一のルート(SSOT)として API / 詳細 URL を派生させる
        api_base = urljoin(url, "wp-json/wp/v2/helperinfo")

        page = 1
        while True:
            list_url = f"{api_base}?per_page={self.PER_PAGE}&page={page}"
            records = self._get_json(list_url)
            if not records:
                break

            if page == 1:
                # 総件数で進捗表示を有効化 (X-WP-Total 相当を概算)
                self.total_items = len(records) if len(records) < self.PER_PAGE else None

            for rec in records:
                try:
                    item = self._scrape_detail(rec)
                    if item:
                        yield item
                except Exception as e:  # 個別アイテムのエラーはログして継続
                    self.logger.warning("詳細取得失敗 id=%s: %s", rec.get("id"), e)
                    continue

            if len(records) < self.PER_PAGE:
                break
            page += 1

    # ------------------------------------------------------------------
    # 詳細ページ
    # ------------------------------------------------------------------
    def _scrape_detail(self, rec: dict) -> dict | None:
        detail_url = rec.get("link")
        if not detail_url:
            return None
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        fields = self._extract_fields(soup)

        item = {
            Schema.URL: detail_url,
            Schema.NAME: fields.get("店舗名", {}).get("text", ""),
            Schema.REP_NM: fields.get("代表者名", {}).get("text", ""),
            Schema.PREF: fields.get("地域", {}).get("text", ""),
        }

        # 電話番号: tel: リンク優先、なければ本文から数字抽出
        tel_f = fields.get("電話番号", {})
        item[Schema.TEL] = self._extract_tel(tel_f)

        # ウェブサイト欄: SNS/LINE なら該当カラム、それ以外は HP
        web_url = fields.get("ウェブサイト", {}).get("href") or ""
        self._assign_url(item, web_url, default_key=Schema.HP)

        # SNS欄: 既知 SNS なら該当カラム、未知ドメインは EXTRA "SNS" に格納
        sns_url = fields.get("SNS", {}).get("href") or ""
        self._assign_url(item, sns_url, default_key="SNS")

        return item

    # ------------------------------------------------------------------
    # ヘルパー
    # ------------------------------------------------------------------
    def _extract_fields(self, soup) -> dict:
        """div.left03 内の label(div.kasen) → value(div.naiyou01/.naiyou) を辞書化"""
        result = {}
        box = soup.select_one("div.left03")
        if not box:
            return result
        for label_el in box.find_all("div", class_="kasen"):
            label = re.sub(r"\s+", "", label_el.get_text(strip=True))
            nxt = label_el
            for _ in range(4):
                nxt = nxt.find_next("div")
                if nxt is None:
                    break
                cls = nxt.get("class") or []
                if "naiyou01" in cls or "naiyou" in cls:
                    a = nxt.find("a", href=True)
                    result[label] = {
                        "text": nxt.get_text(" ", strip=True),
                        "href": a["href"] if a else None,
                    }
                    break
        return result

    @staticmethod
    def _extract_tel(tel_field: dict) -> str:
        href = tel_field.get("href") or ""
        if href.startswith("tel:"):
            return href[len("tel:"):].strip()
        text = tel_field.get("text") or ""
        m = re.search(r"[\d\-()]{8,}", text)
        return m.group(0).strip() if m else ""

    @staticmethod
    def _assign_url(item: dict, url: str, default_key):
        """URL を SNS 種別で分類して該当カラムへ格納。未知は default_key へ。"""
        if not url:
            return
        u = url.lower()
        if "instagram.com" in u:
            item[Schema.INSTA] = url
        elif "twitter.com" in u or "//x.com" in u or "/x.com" in u:
            item[Schema.X] = url
        elif "facebook.com" in u:
            item[Schema.FB] = url
        elif "tiktok.com" in u:
            item[Schema.TIKTOK] = url
        elif "line.me" in u or "lin.ee" in u:
            item[Schema.LINE] = url
        else:
            # 既に値があれば上書きしない
            if not item.get(default_key):
                item[default_key] = url

    def _get_json(self, api_url: str):
        """session.get で JSON を取得 (ソフトタイムアウトのラップ対象)"""
        try:
            resp = self.session.get(api_url, timeout=self.TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if self.CONTINUE_ON_ERROR:
                self.logger.warning("API取得失敗 (スキップ): %s — %s", api_url, e)
                return None
            raise


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = Helper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://helper.kokoroegao.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
