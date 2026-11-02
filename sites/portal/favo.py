"""
Favo石川版 — 石川県(＋富山・福井等)の地域情報誌 Web 版 店舗情報

対象サイト: https://ishikawa.favo-web.jp/store/

取得フロー:
    WordPress REST API (/wp-json/wp/v2/store?per_page=50&page=N) で全店舗(約492件)の
    詳細 URL・掲載エリア・掲載カテゴリを列挙し、各詳細ページの div.store_data
    (「店舗情報」欄の dl > dt/dd) から住所・TEL・営業時間等を取得する。
    一覧 → 詳細を 1 件ずつ取得即 yield (Pattern B)。エリアでの絞り込みはしない。

備考:
    - メールアドレスはサイト上にほぼ掲載されていない(店舗データ自体に項目が無い)。
      mailto リンクがある場合のみ取得する。
    - 店舗本文・見出し等の自由記述は著作権リスクのため取得しない。

実行方法:
    python scripts/sites/portal/favo.py
"""

import html
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_RE = re.compile(r"^(東京都|北海道|(?:京都|大阪)府|.{2,3}県)")


class FavoScraper(StaticCrawler):
    """Favo石川版 スクレイパー"""

    DELAY = 3.0
    PER_PAGE = 50  # API ページサイズ (read timeout 回避のため小さめ)
    EXTRA_COLUMNS = ["掲載エリア", "予約サイト", "駐車場", "予算", "お店のポイント"]

    def parse(self, url: str):
        # 引数 url を唯一のルート(SSOT)として API URL を派生させる
        api_base = urljoin(url, "/wp-json/wp/v2/")
        area_map = self._term_map(api_base + "store_area")
        cate_map = self._term_map(api_base + "store_cate")

        page = 1
        while True:
            list_url = (
                f"{api_base}store?per_page={self.PER_PAGE}&page={page}"
                "&_fields=id,link,title,store_area,store_cate"
            )
            resp = self._get_api(list_url)
            if resp is None:
                break
            records = resp.json()
            if not records:
                break
            if page == 1:
                total = resp.headers.get("X-WP-Total")
                self.total_items = int(total) if total and total.isdigit() else None

            for rec in records:
                item = self._scrape_detail(rec, area_map, cate_map)
                if item:
                    yield item

            total_pages = resp.headers.get("X-WP-TotalPages")
            if total_pages and total_pages.isdigit() and page >= int(total_pages):
                break
            if len(records) < self.PER_PAGE:
                break
            page += 1

    # ------------------------------------------------------------------
    # 詳細ページ
    # ------------------------------------------------------------------
    def _scrape_detail(self, rec: dict, area_map: dict, cate_map: dict) -> dict | None:
        detail_url = rec.get("link")
        if not detail_url:
            return None
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        fields = {}
        box = soup.select_one("div.store_data")
        if box:
            for dl in box.select("dl"):
                dt, dd = dl.find("dt"), dl.find("dd")
                if not dt or not dd:
                    continue
                label = dt.get_text(strip=True)
                if label == "お店のポイント":
                    value = "/".join(s.get_text(strip=True) for s in dd.select(".shop_point"))
                else:
                    value = re.sub(r"\s+", " ", dd.get_text(" ", strip=True))
                fields.setdefault(label, value)

        # パンくず: HOME > 石川県の店舗一覧 > {エリア} > {カテゴリ}
        crumbs = [a.get_text(strip=True) for a in soup.select("[class*=bread] a")]
        areas = [area_map[i] for i in rec.get("store_area") or [] if i in area_map]
        cates = [cate_map[i] for i in rec.get("store_cate") or [] if i in cate_map]
        if not areas and len(crumbs) >= 3:
            areas = [crumbs[2]]
        if not cates and len(crumbs) >= 4:
            cates = [crumbs[3]]

        name = fields.get("店名") or self._html_text(rec.get("title", {}).get("rendered", ""))
        if not name:
            h1 = soup.select_one("h1")
            name = h1.get_text(strip=True) if h1 else ""

        addr = fields.get("住所", "")
        m = _PREF_RE.match(addr)

        tel = fields.get("電話番号", "")
        if not tel:
            a = soup.select_one(".store_btn a[href^='tel:']")
            tel = a["href"][4:].strip() if a else ""

        mail = soup.select_one(".store_single_main a[href^='mailto:']")

        item = {
            Schema.URL: detail_url,
            Schema.NAME: name,
            Schema.PREF: m.group(1) if m else "",
            Schema.ADDR: addr,
            Schema.TEL: tel,
            Schema.TIME: fields.get("営業時間", ""),
            Schema.HOLIDAY: fields.get("定休日", ""),
            Schema.EMAIL: mail["href"][7:].split("?")[0].strip() if mail else "",
            Schema.CAT_SITE: "/".join(cates),
            Schema.HP: "",
            Schema.INSTA: "",
            Schema.LINE: "",
            Schema.X: "",
            Schema.FB: "",
            Schema.TIKTOK: "",
            "掲載エリア": "/".join(areas),
            "予約サイト": "",
            "駐車場": fields.get("駐車場", ""),
            "予算": fields.get("予算", ""),
            "お店のポイント": fields.get("お店のポイント", ""),
        }

        for a in soup.select(".store_btn a[href]"):
            label = a.get_text(strip=True)
            href = a["href"].strip()
            if label == "公式サイト" and not item[Schema.HP]:
                item[Schema.HP] = href
            elif label == "予約サイト" and not item["予約サイト"]:
                item["予約サイト"] = href

        for a in soup.select(".store_sns a[href]"):
            self._assign_sns(item, a["href"].strip())

        return item

    # ------------------------------------------------------------------
    # ヘルパー
    # ------------------------------------------------------------------
    def _get_api(self, api_url: str):
        """session.get で API を取得 (ソフトタイムアウトのラップ対象)"""
        try:
            resp = self.session.get(api_url, timeout=self.TIMEOUT)
            if resp.status_code == 400:  # 範囲外ページ (rest_post_invalid_page_number)
                return None
            resp.raise_for_status()
            return resp
        except Exception as e:
            if self.CONTINUE_ON_ERROR:
                self.logger.warning("API取得失敗 (スキップ): %s — %s", api_url, e)
                return None
            raise

    def _term_map(self, api_url: str) -> dict:
        """タクソノミー (store_area / store_cate) の id → 名称"""
        resp = self._get_api(f"{api_url}?per_page=100&_fields=id,name")
        if resp is None:
            return {}
        return {t["id"]: self._html_text(t["name"]) for t in resp.json()}

    @staticmethod
    def _html_text(s: str) -> str:
        return html.unescape(s or "").strip()

    @staticmethod
    def _assign_sns(item: dict, url: str):
        u = url.lower()
        if "instagram.com" in u:
            key = Schema.INSTA
        elif "line.me" in u or "lin.ee" in u:
            key = Schema.LINE
        elif "twitter.com" in u or "//x.com" in u:
            key = Schema.X
        elif "facebook.com" in u:
            key = Schema.FB
        elif "tiktok.com" in u:
            key = Schema.TIKTOK
        else:
            key = Schema.HP
        if not item.get(key):
            item[key] = url


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = FavoScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://ishikawa.favo-web.jp/store/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
