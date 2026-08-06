"""
日本ホテル協会 加盟ホテル一覧 (j_hotel) — 全国の加盟ホテル情報スクレイパー

取得対象:
    - 日本ホテル協会 (j-hotel.or.jp) の会員ホテル一覧 (/memberlist/) に
      掲載されている全国の加盟ホテル (約 222 件)。
    - ホテル名 / 都道府県 / 郵便番号 / 住所 / TEL / 公式サイト URL / 詳細ページ URL

取得フロー:
    1. 会員ホテル一覧 (引数 url = /memberlist/) から各ホテルの詳細ページ
       (/hotel/{ID}/ 形式) のリンクを列挙し、詳細ページ URL 基準で重複排除する。
       ※ 都道府県順/五十音順/地図表示の 3 経路があるが同一母集団のため、
         一覧ページのリンク収集だけで全件をカバーできる (ページネーション無し)。
    2. 列挙した詳細ページを 1 件取得するたびに即 yield する
       (途中中断に強い Pattern B / 早期 yield)。

注意:
    - ルート URL は引数 `url` を唯一の起点 (SSOT) とし、配下 URL はすべて
      urljoin(url, ...) で派生させる。別 URL はハードコードしない。
    - 詳細ページの「ホテル概要」は自由記述の PR 文 (著作権リスク) のため取得しない。
    - 利用規約 (/policy/) はスクレイピングを明示的には禁止しておらず、
      一般的な著作権表示のみ (2026-08 確認)。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/j_hotel.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id j_hotel
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


# 詳細ページ URL (/hotel/{ID}/) を判定するパターン
_DETAIL_HREF = re.compile(r"^/hotel/(\d+)/?$")

# address テキスト先頭の郵便番号 (〒 省略・ハイフン有/無 いずれも許容)
_POST_PATTERN = re.compile(r"〒?\s*(\d{3}-?\d{4})")

# 住所先頭から都道府県を切り出すためのパターン
_PREF_PATTERN = re.compile(
    r"(北海道|東京都|大阪府|京都府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)


class JHotel(StaticCrawler):
    """日本ホテル協会 加盟ホテル情報スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = []  # Schema で全項目をカバー (サイト固有の構造化項目は無し)

    def parse(self, url: str):
        # 1. 会員ホテル一覧から詳細ページ URL を列挙 (詳細 URL 基準で重複排除)
        soup = self.get_soup(url)
        if soup is None:
            return

        detail_urls = []
        seen = set()
        for a in soup.find_all("a", href=True):
            if not _DETAIL_HREF.match(a["href"]):
                continue
            du = urljoin(url, a["href"])
            if du not in seen:
                seen.add(du)
                detail_urls.append(du)

        self.total_items = len(detail_urls)

        # 2. 詳細ページを 1 件取得するたびに即 yield (早期 yield / 途中中断に強い)
        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as e:  # 個別ホテルの失敗は握りつぶして継続
                self.logger.warning("詳細取得失敗 %s — %s", detail_url, e)
                continue

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        box = soup.select_one("#hotelDetail")
        if box is None:
            return None

        item = {Schema.URL: detail_url}

        # ホテル名 (H1)
        h1 = box.select_one("#ttlWrap h1")
        if h1:
            name = h1.get_text(strip=True)
            if name:
                item[Schema.NAME] = name

        # 住所 (address = "{郵便番号} {都道府県+住所}")
        addr_el = box.select_one("#ttlWrap address")
        if addr_el:
            addr_raw = addr_el.get_text(" ", strip=True)
            mp = _POST_PATTERN.match(addr_raw)
            if mp:
                item[Schema.POST_CODE] = mp.group(1)
                addr = addr_raw[mp.end():].strip()
            else:
                addr = addr_raw.strip()
            mpref = _PREF_PATTERN.match(addr)
            if mpref:
                item[Schema.PREF] = mpref.group(1)
            if addr:
                item[Schema.ADDR] = addr

        # 公式サイト URL
        hp = box.select_one("#info .btn a[href]")
        if hp and hp.get("href"):
            item[Schema.HP] = hp["href"].strip()

        # TEL (#info .note 内の "TEL：..." 段落)
        note = box.select_one("#info .note")
        if note:
            for p in note.find_all("p"):
                txt = p.get_text(strip=True)
                if "TEL" in txt or "電話" in txt:
                    tel = re.sub(r"^\s*(?:TEL|電話)\s*[：:]?\s*", "", txt)
                    # (0142)89-3333 形式のカッコを区切りに正規化
                    tel = tel.replace("(", "").replace(")", "-").strip()
                    if tel:
                        item[Schema.TEL] = tel
                    break

        # NAME が取れなければ無効なページとして捨てる
        if not item.get(Schema.NAME):
            return None

        return item


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = JHotel()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    parse() は引数 url を唯一の起点とし、配下 URL は urljoin で派生させる。
    scraper.execute("https://www.j-hotel.or.jp/memberlist/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
