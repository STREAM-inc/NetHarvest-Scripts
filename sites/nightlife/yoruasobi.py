"""
おきなわ夜アソビ (yoruasobi.com) — 沖縄のナイトビジネス店舗情報

取得対象:
    - 沖縄県内のキャバクラ/クラブ/スナック/ソープ/メンズエステ等の店舗基本情報
      (店名・カナ・住所・TEL・営業時間・定休日・業種・HP/Instagram)

取得フロー:
    1. ルート (top ページ) を取得
    2. ルートおよび各エリア一覧 (area.php) / 業種一覧 (shop_list.php) から
       店舗詳細リンク (shop.php?ID=N) を収集
    3. 各詳細ページ shop.php?ID=N を訪問し、基本情報テーブルを解析して即 yield

備考:
    - 沖縄県専門サイトのため PREF は「沖縄県」固定。
    - 利用規約(kiyaku.html)にスクレイピングの明示禁止は無いが、記事・写真等の
      無断転載を禁じる著作権表記があるため、PRコメント等の自由記述(プロース)は
      一切取得しない。取得するのは住所・電話等の構造化された事実情報のみ。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/yoruasobi.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id yoruasobi
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

# 基本情報テーブルで拾うラベル → 内部キーの対応
_LABEL_MAP = {
    "店名": "name",
    "業種": "genre",
    "住所": "addr",
    "TEL": "tel",
    "電話番号": "tel",
    "URL": "url_link",
    "営業時間": "time",
    "休日": "holiday",
    "定休日": "holiday",
}

_KANA_RE = re.compile(r"[\[［]([^\]］]+)[\]］]")


class Yoruasobi(StaticCrawler):
    """おきなわ夜アソビ スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["地域"]  # h1 先頭の市区表記 (エリア分類)。短い構造化ラベルのみ

    def parse(self, url: str):
        top = self.get_soup(url)
        if top is None:
            return

        # --- 一覧ページ (area.php / shop_list.php) を収集 (ルート起点で派生) ---
        listing_urls = [url]
        seen_listing = {url}
        for a in top.select("a[href]"):
            href = a.get("href", "")
            if not re.search(r"(?:area|shop_list)\.php", href):
                continue
            if "smp" in href or "mobile" in href:  # モバイル/スマホ版の重複を除外
                continue
            lu = urljoin(url, href)
            if lu not in seen_listing:
                seen_listing.add(lu)
                listing_urls.append(lu)

        # --- 各一覧から店舗詳細リンクを収集し、取得即 yield (Pattern B / 早期 yield) ---
        seen_detail = set()
        for i, lu in enumerate(listing_urls):
            soup = top if lu == url else self.get_soup(lu)
            if soup is None:
                continue
            for a in soup.select('a[href*="shop.php?ID="]'):
                href = a.get("href", "")
                if "news" in href:  # shop_news.php を念のため除外
                    continue
                durl = urljoin(url, href)
                if durl in seen_detail:
                    continue
                seen_detail.add(durl)
                try:
                    item = self._scrape_detail(durl)
                except Exception as exc:  # noqa: BLE001 個別店舗の失敗はスキップ
                    self.logger.warning("detail failed %s: %s", durl, exc)
                    continue
                if item:
                    yield item

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        h1 = soup.find("h1")
        h1txt = h1.get_text(" ", strip=True) if h1 else ""
        h1txt = re.sub(r"\s+", " ", h1txt).strip()

        # カナ (h1 の [...] 内)
        kana = ""
        m = _KANA_RE.search(h1txt)
        if m:
            kana = re.sub(r"\s+", " ", m.group(1)).strip()
        h1_nobr = _KANA_RE.sub("", h1txt).strip()
        tokens = h1_nobr.split()
        area = tokens[0] if tokens else ""

        # 基本情報テーブル (ラベル td / 値 td) の抽出
        info: dict[str, str] = {}
        url_link = ""
        for table in soup.find_all("table"):
            for tr in table.find_all("tr"):
                cells = tr.find_all(["th", "td"])
                if len(cells) < 2:
                    continue
                label = cells[0].get_text(strip=True)
                key = _LABEL_MAP.get(label)
                if not key:
                    continue
                value = cells[1].get_text(" ", strip=True)
                if key == "url_link":
                    a = cells[1].find("a", href=True)
                    if a:
                        url_link = a["href"].strip()
                    continue
                if key not in info:  # 最初の出現を優先
                    info[key] = value

        genre = info.get("genre", "")

        # 店名: テーブル優先、無ければ h1 から エリア + 業種 を除去
        name = info.get("name", "").strip()
        if not name and h1_nobr:
            rem = h1_nobr[len(area):].strip() if area else h1_nobr
            if genre and rem.startswith(genre):
                rem = rem[len(genre):].strip()
            name = rem
        if not name:
            return None

        # HP / Instagram の振り分け (URL 行のリンク + ページ内 Instagram アンカー)
        hp = ""
        insta = ""
        if url_link:
            if "instagram" in url_link.lower():
                insta = url_link
            else:
                hp = url_link
        if not insta:
            a = soup.find("a", href=re.compile(r"instagram\.com/(?!.*sharer)", re.I))
            if a:
                insta = a["href"].strip()
        
        if not info.get("addr").startswith("沖縄県"):
            info.update({"addr":"沖縄県" + info.get("addr")})

        return {
            Schema.NAME: name,
            Schema.NAME_KANA: kana,
            Schema.PREF: "沖縄県",
            Schema.ADDR: info.get("addr", ""),
            Schema.TEL: info.get("tel", ""),
            Schema.TIME: info.get("time", ""),
            Schema.HOLIDAY: info.get("holiday", ""),
            Schema.CAT_SITE: genre,
            Schema.HP: hp,
            Schema.INSTA: insta,
            Schema.URL: url,
            "地域": area,
        }


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = Yoruasobi()
    # 🔒 sites.yml に登録する url と完全一致 (SSOT = sites.yml)
    scraper.execute("https://www.yoruasobi.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
