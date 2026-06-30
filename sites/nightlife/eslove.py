"""
エステラブ (eslove.jp) — メンズエステ店舗ポータル

取得対象:
    - 全国のメンズエステ店舗の基本情報
      (店舗名 / 電話番号 / クレジットカード / 営業時間 / 公式サイト(HP) /
       公式アカウント(SNS・LINE) / 住所)

取得フロー:
    1. ルート URL から sitemap_shop (/sitemap_shop) を取得する。
       サイトマップインデックスなら子サイトマップ 1 つ = 一覧ページ 1 つ、
       単一 urlset ならそのファイル自身を一覧ページとして扱う。
    2. 一覧ページを 1 つ取得するごとに、そこに含まれる詳細ページ
       (/shop/{id}) を取得して即 yield する (Pattern B)

備考 (取得方針) で指定されたフィールドのみを対象とする。
店舗紹介文などの自由記述プロースは著作権リスクのため取得しない。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/eslove.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id eslove
"""

import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 47 都道府県 (住所先頭マッチ用)
_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_TEL_PATTERN = re.compile(r"0\d{1,4}[-\(\)\s]?\d{1,4}[-\(\)\s]?\d{3,4}")
_LINE_ID_PATTERN = re.compile(r"%40([\w.\-]+)")


class Eslove(StaticCrawler):
    """エステラブ スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["クレジットカード", "全店舗住所"]

    def parse(self, url: str):
        # ルート url から sitemap_shop を派生 (SSOT = 引数 url)
        sitemap_url = urljoin(url, "/sitemap_shop")
        soup = self.get_soup(sitemap_url)
        if soup is None:
            logger.error("sitemap_shop の取得に失敗: %s", sitemap_url)
            return

        # sitemap_shop が「サイトマップインデックス」(子サイトマップを列挙) の場合は
        # 子サイトマップ 1 つ = 一覧ページ 1 つ として扱う。
        # 単一の urlset の場合は、その 1 ファイルを唯一の一覧ページとして扱う。
        all_locs = [
            loc.get_text(strip=True)
            for loc in soup.select("loc")
            if loc.get_text(strip=True)
        ]
        sub_sitemaps = [
            loc for loc in all_locs
            if "/shop/" not in loc and (loc.lower().endswith(".xml") or "sitemap" in loc.lower())
        ]
        listing_urls = sub_sitemaps if sub_sitemaps else [sitemap_url]
        logger.info("一覧ページ数: %d", len(listing_urls))

        seen: set[str] = set()
        # 一覧ページを 1 つ取得するごとに、そこに含まれる詳細ページを取得して即 yield する。
        for listing_url in listing_urls:
            listing_soup = soup if listing_url == sitemap_url else self.get_soup(listing_url)
            if listing_soup is None:
                logger.warning("一覧ページ取得失敗: %s", listing_url)
                continue
            shop_urls = [
                loc.get_text(strip=True)
                for loc in listing_soup.select("loc")
                if "/shop/" in loc.get_text()
            ]
            logger.info("一覧ページ %s: %d 店舗", listing_url, len(shop_urls))

            for shop_url in shop_urls:
                if shop_url in seen:  # 一覧をまたいだ重複を排除 (出現順維持)
                    continue
                seen.add(shop_url)
                try:
                    item = self._scrape_detail(shop_url)
                    if item:
                        yield item
                except Exception as e:  # 個別店舗の失敗は握りつぶして継続
                    logger.warning("詳細取得失敗 %s: %s", shop_url, e)
                    continue

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None
        tables = soup.select("table.shopOverview__table")
        if not tables:
            return None

        # --- 全テーブルの「ラベル->td」マップ (基本情報用) ---
        # 表の並び順や基本情報テーブルが先頭でないケースに備えて全テーブルを走査する。
        # ラベルの空白ゆれ (例: "LINE　お問い合わせ") に強くするため空白は1個に正規化する。
        # 「住所」「アクセス」は店舗ごとに複数回現れるため、ここでは使わず後段で個別収集する。
        info: dict[str, "object"] = {}
        for table in tables:
            for tr in table.select("tr"):
                th = tr.select_one("th")
                td = tr.select_one("td")
                if not (th and td):
                    continue
                label = re.sub(r"\s+", " ", th.get_text(" ", strip=True)).strip()
                # 基本情報ラベルは一意。住所/アクセス等の繰り返しラベルで上書きしない。
                info.setdefault(label, td)

        item = {Schema.URL: detail_url}

        # 店舗名 (なければ h1)
        name = ""
        if "店舗名" in info:
            name = info["店舗名"].get_text(" ", strip=True)
        if not name:
            h1 = soup.select_one("h1")
            if h1:
                name = h1.get_text(strip=True)
        name = re.sub(r"^【公式】\s*", "", name).strip()
        item[Schema.NAME] = name

        # 電話番号 (電話予約) — tooltip を除去
        if "電話予約" in info:
            tel_td = info["電話予約"]
            tel_p = tel_td.select_one("p.tel") or tel_td
            for sp in tel_p.select("span.tooltip"):
                sp.decompose()
            m = _TEL_PATTERN.search(tel_p.get_text(" ", strip=True))
            if m:
                item[Schema.TEL] = m.group(0)

        # 営業時間
        if "営業時間" in info:
            item[Schema.TIME] = info["営業時間"].get_text(" ", strip=True)

        # 公式サイト (HP) — ブログではなくオフィシャルサイト
        if "オフィシャルサイト" in info:
            a = info["オフィシャルサイト"].select_one("a[href]")
            if a:
                item[Schema.HP] = a["href"]

        # クレジットカード — ブランド名 (img alt) を列挙
        if "クレジットカード" in info:
            brands = [
                img.get("alt", "").strip()
                for img in info["クレジットカード"].select("img")
                if img.get("alt", "").strip()
            ]
            item["クレジットカード"] = " / ".join(brands)

        # 公式アカウント (SNS) — URL でプラットフォーム判定
        if "公式アカウント" in info:
            for a in info["公式アカウント"].select("a[href]"):
                href = a["href"]
                low = href.lower()
                if "instagram.com" in low:
                    item.setdefault(Schema.INSTA, href)
                elif "twitter.com" in low or "//x.com" in low or "/x.com/" in low:
                    item.setdefault(Schema.X, href)
                elif "facebook.com" in low:
                    item.setdefault(Schema.FB, href)
                elif "tiktok.com" in low:
                    item.setdefault(Schema.TIKTOK, href)

        # LINE (公式アカウント扱い) — LINE ID を優先
        if "LINE お問い合わせ" in info:
            line_td = info["LINE お問い合わせ"]
            line_id_el = line_td.select_one(".line-id")
            if line_id_el and line_id_el.get_text(strip=True):
                item[Schema.LINE] = line_id_el.get_text(strip=True)
            else:
                a = line_td.select_one('a[href*="line.me"]')
                if a:
                    m = _LINE_ID_PATTERN.search(a["href"])
                    if m:
                        item[Schema.LINE] = m.group(1)

        # 住所 — 店舗別テーブルの「住所」行を全て収集
        addresses = []
        for table in tables:
            for tr in table.select("tr"):
                th = tr.select_one("th")
                td = tr.select_one("td")
                if not (th and td) or th.get_text(strip=True) != "住所":
                    continue
                # 地図リンク・注記を除去してクリーンな住所だけ取り出す
                td_copy = td
                for junk in td.select("a.mapLink, .remarks, .mapLink"):
                    junk.extract()
                addr = re.sub(r"\s+", " ", td_copy.get_text(" ", strip=True)).strip()
                if addr:
                    addresses.append(addr)

        if addresses:
            first = addresses[0]
            m = _PREF_PATTERN.match(first)
            if m:
                item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = first[m.end():].strip()
            else:
                item[Schema.ADDR] = first
            if len(addresses) > 1:
                item["全店舗住所"] = " / ".join(addresses)

        return item


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = Eslove()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://eslove.jp")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
