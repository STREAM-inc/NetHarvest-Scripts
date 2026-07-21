"""
ニシタチナイトマップ — 宮崎市の繁華街「ニシタチ」の飲食店・居酒屋・バー・スナック店舗情報

取得対象:
    - 店舗一覧 (/map/) から各店舗詳細ページ (/map/{genre}/{id}/) を辿り、
      店名・ジャンル・営業時間・定休日・電話番号・席数/喫煙情報を取得する

取得フロー:
    一覧ページ (WordPress, wp-pagenavi) を 1 ページ目 = url、2 ページ目以降 =
    /map/page/{n}/ で全ページ巡回し、各カード (a.shoplist_unit) の詳細リンクへ
    遷移して single01 ブロックの各フィールドを取得する。
    detail は 1 件取得ごとに即 yield する (Pattern B)。

    ※ サイト全体が宮崎市ニシタチ地区の店舗のみを掲載しているため PREF は「宮崎県」固定。
      住所テキストは HTML 本文には無いが、Google マップ埋め込み iframe の pb パラメータ
      内 `!2z<base64>` を base64 デコードすると取得できる場合がある。ただしこの値は
      運営者がマップ登録時に「住所」で検索したか「店名」で検索したかで内容が変わり、
      住所 (例 "〒880-0001 宮崎県宮崎市橘通西３丁目７−８") のこともあれば店名だけの
      こともある。そのため郵便番号か "宮崎県"/"宮崎市" を含むものだけを ADDR に採用する。
      店舗紹介文 (.single01_detail) は自由記述プロースのため著作権リスクを避けて除外。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/nishitachi_museum.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id nishitachi_museum
"""

import base64
import re
import sys
import urllib.parse
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class NishitachiMuseum(StaticCrawler):
    """ニシタチナイトマップ スクレイパー"""

    DELAY = 1.5
    # 席数・喫煙情報 (例: "26席／全席禁煙") は構造化された短いラベルのため EXTRA に採用。
    # 店舗紹介文 (.single01_detail) は自由記述プロースのため著作権リスクで除外。
    EXTRA_COLUMNS = ["席数・喫煙"]

    def parse(self, url: str):
        # url = sites.yml の正規 URL (https://nishitachi-museum.com/map/) を唯一の起点とする。
        page = 1
        while True:
            # 1 ページ目は url そのもの。/map/page/1/ は 0 件になるため使わない。
            list_url = url if page == 1 else urllib.parse.urljoin(url, f"page/{page}/")
            soup = self.get_soup(list_url)
            if soup is None:
                break

            cards = soup.select("a.shoplist_unit[href]")
            if not cards:
                break

            # 詳細リンクを収集 (/map/{genre}/{id}/ 形式のみ)。
            detail_urls = []
            for a in cards:
                href = a.get("href", "")
                if re.search(r"/map/[a-z]+/\d+/?$", href):
                    detail_urls.append(urllib.parse.urljoin(url, href))

            for detail_url in detail_urls:
                try:
                    item = self._scrape_detail(detail_url)
                except Exception as e:  # noqa: BLE001
                    self.logger.warning("詳細ページ取得失敗: %s — %s", detail_url, e)
                    continue
                if item:
                    yield item

            # 次ページの存在確認 (wp-pagenavi / rel=next)。無ければ終了。
            has_next = bool(
                soup.select_one(f'a[href*="page/{page + 1}/"]')
                or soup.select_one(f'link[rel="next"][href*="page/{page + 1}/"]')
            )
            if not has_next:
                break
            page += 1

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        name_el = soup.select_one("h1.single01_ttl")
        if not name_el:
            return None
        name = name_el.get_text(strip=True)
        if not name:
            return None

        genre_el = soup.select_one('div[class*="single01_genre"] span')
        etc_el = soup.select_one(".single01_etc")
        time_el = soup.select_one(".single01_time")
        tel_el = soup.select_one(".single01_tel")

        # 営業時間欄は "18:00～27:00 [ 日曜休 ]" 形式。角括弧内が定休日。
        time_raw = time_el.get_text(" ", strip=True) if time_el else ""
        biz_time, holiday = time_raw, ""
        if "[" in time_raw:
            head, _, tail = time_raw.partition("[")
            biz_time = head.strip()
            holiday = tail.rstrip("]").strip()

        addr = self._addr_from_map_embed(soup)

        # 住所を県名から始まるように調整する．
        if addr and not addr.startswith("宮崎県"):
            addr = "宮崎県" + addr

        return {
            Schema.NAME: name,
            Schema.URL: detail_url,
            Schema.PREF: "宮崎県",  # サイト全体が宮崎市ニシタチ地区の店舗のみ
            Schema.ADDR: addr,
            Schema.TEL: tel_el.get_text(strip=True) if tel_el else "",
            Schema.CAT_SITE: genre_el.get_text(strip=True) if genre_el else "",
            Schema.TIME: biz_time,
            Schema.HOLIDAY: holiday,
            "席数・喫煙": etc_el.get_text(" ", strip=True) if etc_el else "",
        }

    # Google マップ埋め込みの pb 内 !2z<base64> が住所らしければ ADDR に採用。
    _POSTAL_RE = re.compile(r"〒?\d{3}-?\d{4}")

    def _addr_from_map_embed(self, soup) -> str:
        iframe = soup.select_one('iframe[src*="google.com/maps/embed"]')
        if not iframe:
            return ""
        src = iframe.get("src", "")
        m = re.search(r"!2z([A-Za-z0-9_-]+)", src)
        if not m:
            return ""
        seg = m.group(1)
        try:
            text = base64.urlsafe_b64decode(seg + "=" * (-len(seg) % 4)).decode("utf-8")
        except Exception:  # noqa: BLE001
            return ""
        text = text.strip()
        # !2z は店名のことも住所のこともある。住所と判定できるものだけ採用。
        if not (self._POSTAL_RE.search(text) or "宮崎県" in text or "宮崎市" in text):
            return ""
        # 先頭の郵便番号 (例 "〒880-0001 ") を除いた住所部分を返す。
        return self._POSTAL_RE.sub("", text, count=1).strip()


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = NishitachiMuseum()
    # 🔒 sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://nishitachi-museum.com/map/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
