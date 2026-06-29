# scripts/sites/service/torecamap.py
"""
トレカの地図 (torecamap.co.jp) — 全国カードショップ情報スクレイパー

取得対象:
    - 全国のトレーディングカードショップ (https://torecamap.co.jp/shops/)
    - 店舗名 / 住所 / 都道府県 / 営業時間(曜日別) / 定休日 / 支払い方法 /
      TEL / メール / 公式サイト(HP) / SNS / 公式ECサイト /
      デュエルスペース / 駐車場 / 買取 / 取扱タイトル

取得フロー (一覧 → 詳細, Pattern B):
    1. /shops/?page=N を順に巡回し、各ページ20件の詳細URLを収集
    2. 詳細URLごとに即座に詳細ページを取得して 1 件ずつ yield
       (途中で停止しても無駄な通信が起きない)
    3. 詳細リンクが 0 件になったページで終了

備考対応:
    - 「全国で取得」: 都道府県で絞らず /shops/ 全件を巡回する (フィルタ無し)
    - 「支払い方法を必ず取得」: Schema.PAYMENTS に確実にマッピング

クローラータイプ:
    - Static (サーバーサイドレンダリング済み HTML にデータが含まれるため requests で取得可能)

実行方法:
    # ローカルテスト
    python scripts/sites/service/torecamap.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id torecamap
"""

import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 詳細ページURLのパターン: /shops/{pref}/r{region}/s{station}/{shop_id}
_DETAIL_RE = re.compile(r"/shops/[a-z]+/r\d+/s\d+/\d+")

# 住所先頭の都道府県を抽出する
_PREF_RE = re.compile(r"^(北海道|東京都|京都府|大阪府|.{2,3}県)")

# 曜日ラベル → Schema 定数
_DAY_MAP = {
    "月曜日": Schema.TIME_MON,
    "火曜日": Schema.TIME_TUE,
    "水曜日": Schema.TIME_WED,
    "木曜日": Schema.TIME_THU,
    "金曜日": Schema.TIME_FRI,
    "土曜日": Schema.TIME_SAT,
    "日曜日": Schema.TIME_SUN,
}


class TorecamapScraper(StaticCrawler):
    """トレカの地図 (torecamap.co.jp) スクレイパー"""

    DELAY = 1.5

    # Schema に存在しないサイト固有カラム (いずれも短い構造化情報 / 文章プロースは含めない)
    EXTRA_COLUMNS = [
        "デュエルスペース",   # 例: "あり (50席)"
        "駐車場",            # 例: "なし"
        "買取",              # 例: "店頭買取"
        "公式ECサイト",       # EC サイト URL
        "営業時間(祝日)",      # 祝日の営業時間
    ]

    def parse(self, url: str):
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        # 一覧ページ (全国)。url が一覧でなければ /shops/ にフォールバック
        list_base = url if "/shops" in parsed.path else urljoin(base, "/shops/")
        list_base = list_base.split("?")[0].rstrip("/") + "/"

        page = 1
        while True:
            list_url = f"{list_base}?page={page}"
            soup = self.get_soup(list_url)
            if not soup:
                break

            # 詳細ページへのリンクを持つ一覧カードを収集
            cards = soup.select('a[class*="CardShop_card-shop"]')
            if not cards:
                # クラス名 (ハッシュ付き) が変わった場合のフォールバック
                cards = [a for a in soup.find_all("a", href=True)
                         if _DETAIL_RE.search(a.get("href", ""))]

            entries = []
            seen = set()
            for c in cards:
                href = c.get("href", "")
                if not _DETAIL_RE.search(href):
                    continue
                detail_url = urljoin(base, href)
                if detail_url in seen:
                    continue
                seen.add(detail_url)
                # 一覧カードのタグ = 取扱タイトル (例: FAB, ポケカ) — 短いラベル
                tags = [t.get_text(strip=True)
                        for t in c.select('div[class*="Tag_tag"]')]
                entries.append((detail_url, tags))

            if not entries:
                break

            # 初回ページで総件数を取得し進捗表示を有効化
            if page == 1 and self.total_items is None:
                cnt_el = soup.select_one('[class*="ShopsResultSection_count"]')
                if cnt_el:
                    m = re.search(r"([0-9,]+)\s*店舗", cnt_el.get_text())
                    if m:
                        self.total_items = int(m.group(1).replace(",", ""))

            for detail_url, tags in entries:
                try:
                    item = self._scrape_detail(detail_url, tags)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.warning("詳細ページ取得失敗: %s (%s)", detail_url, e)
                    continue

            page += 1
            time.sleep(self.DELAY)

    def _scrape_detail(self, url: str, tags: list[str]) -> dict | None:
        soup = self.get_soup(url)
        if not soup:
            return None

        # 基本情報セクションの label → info 要素を辞書化
        info: dict[str, object] = {}
        for blk in soup.select('div[class*="ShopInfo_shop-info"]'):
            lab = blk.select_one('div[class*="ShopInfo_label"]')
            val = blk.select_one('div[class*="ShopInfo_info"]')
            if lab and val:
                info[lab.get_text(strip=True)] = val

        item: dict[str, str] = {Schema.URL: url}

        # --- 店舗名 ---
        name_el = info.get("店舗名")
        item[Schema.NAME] = name_el.get_text(strip=True) if name_el else ""

        # --- 住所 / 都道府県 ---
        addr_el = info.get("住所")
        if addr_el:
            # GoogleMap リンクのテキストを除いた住所本文を取得
            span = addr_el.select_one("span")
            raw_addr = (span.get_text(strip=True) if span
                        else addr_el.get_text(strip=True))
            raw_addr = re.sub(r"GoogleMap\s*$", "", raw_addr).strip()
            m = _PREF_RE.match(raw_addr)
            if m:
                item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = raw_addr[m.end():].strip()
            else:
                item[Schema.ADDR] = raw_addr

        # --- 営業時間 (曜日別) / 祝日 / 定休日 ---
        hours_el = info.get("営業時間")
        if hours_el:
            day_summary = []
            for row in hours_el.select('div[class*="business-hours-row"]'):
                spans = row.find_all("span")
                if len(spans) < 2:
                    continue
                day = spans[0].get_text(strip=True)
                tval = spans[1].get_text(strip=True)
                if day in _DAY_MAP:
                    item[_DAY_MAP[day]] = tval
                    day_summary.append(f"{day} {tval}")
                elif day == "祝日":
                    item["営業時間(祝日)"] = tval
                elif day == "定休日":
                    item[Schema.HOLIDAY] = tval
            if day_summary:
                item[Schema.TIME] = " / ".join(day_summary)

        # --- 支払い方法 (備考: 必ず取得) ---
        pay_el = info.get("支払い方法")
        if pay_el:
            item[Schema.PAYMENTS] = pay_el.get_text(strip=True)

        # --- 公式サイト (HP) ---
        hp_el = info.get("公式サイト")
        if hp_el:
            a = hp_el.find("a", href=True)
            item[Schema.HP] = a["href"] if a else hp_el.get_text(strip=True)

        # --- 公式ECサイト (EXTRA) ---
        ec_el = info.get("公式ECサイト")
        if ec_el:
            a = ec_el.find("a", href=True)
            item["公式ECサイト"] = a["href"] if a else ec_el.get_text(strip=True)

        # --- 公式アカウント (SNS) → ホスト名で振り分け ---
        acc_el = info.get("公式アカウント")
        if acc_el:
            for a in acc_el.find_all("a", href=True):
                href = a["href"]
                low = href.lower()
                if "x.com" in low or "twitter.com" in low:
                    item[Schema.X] = href
                elif "instagram.com" in low:
                    item[Schema.INSTA] = href
                elif "line.me" in low or "lin.ee" in low:
                    item[Schema.LINE] = href
                elif "facebook.com" in low:
                    item[Schema.FB] = href
                elif "tiktok.com" in low:
                    item[Schema.TIKTOK] = href

        # --- お問い合わせ (TEL / メール) ---
        contact_el = info.get("お問い合わせ")
        if contact_el:
            ctext = contact_el.get_text(" ", strip=True)
            mphone = re.search(r"0\d{1,4}[-(]?\d{1,4}[-)]?\d{3,4}", ctext)
            if mphone:
                item[Schema.TEL] = mphone.group(0)
            memail = re.search(r"[\w.+-]+@[\w.-]+\.\w+", ctext)
            if memail:
                item[Schema.EMAIL] = memail.group(0)

        # --- デュエルスペース / 駐車場 / 買取 (EXTRA, 短い構造化情報) ---
        for label in ("デュエルスペース", "駐車場", "買取"):
            el = info.get(label)
            if el:
                item[label] = el.get_text(" ", strip=True)

        # --- 取扱タイトル (一覧カードのタグ) → サイト定義業種・ジャンル ---
        if tags:
            item[Schema.CAT_SITE] = " / ".join(tags)

        return item


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = TorecamapScraper()
    scraper.execute("https://torecamap.co.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
