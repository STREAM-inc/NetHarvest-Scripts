"""
夜遊びショコラ (town-night.jp) — 全国47都道府県のナイト系店舗情報

取得対象:
    - 全47都道府県の店舗詳細 (/{都道府県}/a_{エリア}/shop/{id}/)
    - 対象カテゴリ: キャバクラ系 / ガールズバー / ラウンジ系 / スナック / クラブ系 /
      ホスト / コンカフェ / パブ  (性風俗カテゴリは除外)
      ※ 2026-11 時点でサイトの業種は biz_1〜16 (キャバクラ, ガールズバー, ニュークラブ, クラブ,
        スナック, ラウンジ, ガールズラウンジ, 昼キャバ・朝キャバ, ショークラブ, パブクラブ,
        熟女キャバクラ, 姉キャバ・半熟キャバ, 会員制ラウンジ, コンカフェ) で、性風俗業種は無い。
        将来追加された場合に備え、業種名でホワイトリスト/ブラックリスト判定する。

取得フロー:
    起点 url (sites.yml の正規 URL) から urljoin で各都道府県一覧 /{pref}/ を導出 →
    一覧 /{pref}/pageN/ (50件/ページ、範囲外は 404) →
    各店舗詳細ページ (dl.shopData-dl / section.f-shopPrice / section.f-payment を解析)

優先順:
    東京 → 大阪 → 愛知 → 福岡 → 北海道 → その他 (JIS 順)。
    都道府県単位で順次処理するため、途中で止まっても主要都市分が先に出力される。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/town_night.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id town_night
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

# JIS 順の都道府県スラッグ (サイトの URL パス)
_PREF_SLUGS_JIS = [
    "hokkaido", "aomori", "iwate", "miyagi", "akita", "yamagata", "fukushima",
    "ibaraki", "tochigi", "gunma", "saitama", "chiba", "tokyo", "kanagawa",
    "niigata", "toyama", "ishikawa", "fukui", "yamanashi", "nagano", "gifu",
    "shizuoka", "aichi", "mie", "shiga", "kyoto", "osaka", "hyogo", "nara",
    "wakayama", "tottori", "shimane", "okayama", "hiroshima", "yamaguchi",
    "tokushima", "kagawa", "ehime", "kochi", "fukuoka", "saga", "nagasaki",
    "kumamoto", "oita", "miyazaki", "kagoshima", "okinawa",
]
# 優先取得順: 東京 → 大阪 → 愛知 → 福岡 → 北海道 → その他
_PRIORITY = ["tokyo", "osaka", "aichi", "fukuoka", "hokkaido"]
_PREF_ORDER = _PRIORITY + [p for p in _PREF_SLUGS_JIS if p not in _PRIORITY]

# 対象業種 (部分一致)。キャバクラ/ガールズバー/ラウンジ/スナック/クラブ/ホスト/コンカフェ/パブ
_ALLOWED_BIZ_RE = re.compile(r"キャバ|ガールズバー|ラウンジ|スナック|クラブ|ホスト|コンカフェ|カフェ|パブ")
# 性風俗系業種 (含まれていれば除外。許可語より優先)
_EXCLUDED_BIZ_RE = re.compile(
    r"風俗|ヘルス|ソープ|デリヘル|ホテヘル|ピンサロ|セクキャバ|いちゃキャバ|おっぱい|ハプニング"
    r"|エステ|M性感|性感|オナクラ|手コキ|イメクラ|ストリップ"
)

_SHOP_PATH_RE = re.compile(r"/[a-z]+/a_\d+/shop/\d+/$")
_BIZ_IN_LIST_RE = re.compile(r"'item_category4':'([^']*)'")


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _is_target_biz(biz: str) -> bool:
    if not biz:
        return True  # 判定不能時は詳細側で再判定
    if _EXCLUDED_BIZ_RE.search(biz):
        return False
    return bool(_ALLOWED_BIZ_RE.search(biz))


class TownNightScraper(StaticCrawler):
    """夜遊びショコラ 全国ナイト系店舗スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS = ["エリア", "最寄駅", "料金目安", "料金システム", "タグ"]

    def parse(self, url: str) -> Generator[dict, None, None]:
        seen: set[str] = set()
        for pref in _PREF_ORDER:
            pref_url = urljoin(url, f"/{pref}/")
            yield from self._crawl_pref(pref_url, seen)

    def _crawl_pref(self, pref_url: str, seen: set[str]) -> Generator[dict, None, None]:
        soup = self.get_soup(pref_url)
        if soup is None:
            self.logger.warning("都道府県一覧の取得失敗: %s", pref_url)
            return

        count_el = soup.select_one("p.castAndReviewCount")
        if count_el:
            m = re.search(r"店舗：(\d+)件", count_el.get_text())
            if m:
                # 都道府県トップを取得するたびに累計店舗数を加算 (ETA 表示用)
                self.total_items = (self.total_items or 0) + int(m.group(1))

        page = 1
        while True:
            links = soup.select("li.rankingShop span.shop-name a[href]")
            if not links:
                break
            for link_tag in links:
                detail_url = urljoin(pref_url, link_tag.get("href", ""))
                if not _SHOP_PATH_RE.search(detail_url) or detail_url in seen:
                    continue
                seen.add(detail_url)
                m = _BIZ_IN_LIST_RE.search(link_tag.get("onmousedown", "") or "")
                if m and not _is_target_biz(m.group(1)):
                    self.logger.info("対象外業種のためスキップ (%s): %s", m.group(1), detail_url)
                    continue
                try:
                    item = self._scrape_detail(detail_url)
                    if item:
                        yield item
                except Exception as e:
                    self.logger.warning("詳細取得失敗 %s: %s", detail_url, e)

            page += 1
            next_url = urljoin(pref_url, f"page{page}/")
            has_next = soup.find("link", rel="next") or soup.select_one(f'a[href="{next_url}"]')
            if not has_next:
                break
            soup = self.get_soup(next_url)
            if soup is None:
                break

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        data: dict = {Schema.URL: url}

        dl = soup.select_one("dl.shopData-dl")
        if not dl:
            return None

        for div in dl.select("div.shopData-item"):
            dt_el = div.select_one("dt.shopData-dt")
            dd_el = div.select_one("dd.shopData-dd")
            if not dt_el or not dd_el:
                continue

            key = re.sub(r"\s+", "", dt_el.get_text())
            val = _clean(dd_el.get_text(" "))

            if key == "店名":
                data[Schema.NAME] = val
            elif key == "ジャンル":
                # 例: "国分町/コンカフェ" → エリア=国分町, 業種=コンカフェ
                area, sep, biz = val.partition("/")
                if sep:
                    data["エリア"] = area.strip()
                    data[Schema.CAT_SITE] = biz.strip()
                else:
                    data[Schema.CAT_SITE] = val
            elif key == "営業時間":
                data[Schema.TIME] = val
            elif key == "定休日":
                data[Schema.HOLIDAY] = val
            elif key == "電話番号":
                data[Schema.TEL] = val
            elif key == "住所":
                p = dd_el.select_one("p.shopData-add-txt")
                addr_raw = _clean(p.get_text(" ")) if p else val
                pref_m = _PREF_RE.search(addr_raw)
                if pref_m:
                    data[Schema.PREF] = pref_m.group(0)
                    data[Schema.ADDR] = addr_raw[pref_m.end():].strip()
                else:
                    data[Schema.ADDR] = addr_raw
            elif key == "アクセス":
                data["最寄駅"] = val
            elif key == "料金目安":
                data["料金目安"] = val
            elif key == "タグ":
                tags = [a.get_text(strip=True) for a in dd_el.select("a")]
                data["タグ"] = " ".join(tags)
            elif key in ("ホームページ", "HP", "公式HP", "公式サイト", "URL"):
                a = dd_el.select_one("a[href^=http]")
                data[Schema.HP] = a.get("href", "") if a else val

        # 性風俗カテゴリは取得しない (一覧で判定できなかった場合の再判定)
        if not _is_target_biz(data.get(Schema.CAT_SITE, "")):
            self.logger.info("対象外業種のためスキップ (%s): %s", data.get(Schema.CAT_SITE), url)
            return None

        # エリア補完 (パンくず: "国分町キャバクラ" 形式ではなく dataLayer の shop_area を優先)
        if not data.get("エリア"):
            m = re.search(r"'shop_area':'([^']*)'", str(soup))
            if m:
                data["エリア"] = m.group(1)

        # 料金システム: section.f-shopPrice のテーブル群 (caption + th/td)
        price_sec = soup.select_one("section.f-shopPrice")
        if price_sec:
            parts: list[str] = []
            for table in price_sec.select("table.shopPrice-table"):
                cap = table.select_one("caption")
                rows: list[str] = []
                for tr in table.select("tr"):
                    th = tr.select_one("th")
                    td = tr.select_one("td")
                    th_t = _clean(th.get_text(" ")) if th else ""
                    td_t = _clean(td.get_text(" ")) if td else ""
                    if th_t and td_t:
                        rows.append(f"{th_t}:{td_t}")
                    elif th_t or td_t:
                        rows.append(th_t or td_t)
                if rows:
                    head = f"【{_clean(cap.get_text(' '))}】" if cap else ""
                    parts.append(head + " / ".join(rows))
            if parts:
                data["料金システム"] = " ".join(parts)

        # 支払い方法: section.f-payment のアイコン alt
        pay_sec = soup.select_one("section.f-payment")
        if pay_sec:
            pays = [img.get("alt", "").strip() for img in pay_sec.select("li img[alt]")]
            pays = [p for p in pays if p]
            if pays:
                data[Schema.PAYMENTS] = " / ".join(dict.fromkeys(pays))

        # HP: 「公式」「ホームページ」「HP」と明示されたリンクのみ採用 (シェア用アイコン等は除外)
        if not data.get(Schema.HP):
            for a in soup.select("a[href^=http]"):
                href = a.get("href", "")
                if re.search(r"town-night\.jp|chocolat\.(work|jp)|job-chocolat|cloudfront|google\.", href):
                    continue
                if re.search(r"公式(サイト|HP|ホームページ)|ホームページ|^HP$|オフィシャル", a.get_text(strip=True)):
                    data[Schema.HP] = href
                    break

        line_a = soup.find("a", href=re.compile(r"https?://(lin\.ee|line\.me/(R/)?ti/p|line\.naver\.jp/ti/p)/"))
        if line_a:
            data[Schema.LINE] = line_a.get("href", "")

        # 店舗固有 SNS (シェア用の汎用トップ URL は除外)
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if not data.get(Schema.INSTA) and re.match(r"https?://(www\.)?instagram\.com/[^/?#]+", href):
                data[Schema.INSTA] = href
            elif not data.get(Schema.TIKTOK) and re.match(r"https?://(www\.)?tiktok\.com/@[^/?#]+", href):
                data[Schema.TIKTOK] = href.split("?")[0]
            elif not data.get(Schema.X) and re.match(r"https?://(www\.)?(twitter|x)\.com/(?!intent|share)[A-Za-z0-9_]+/?$", href):
                data[Schema.X] = href

        return data if data.get(Schema.NAME) else None


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    scraper = TownNightScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    コンテナ実行・テスト実行も同じ url を parse() に渡すので、ここだけ変えるとローカルの挙動がズレる。
    scraper.execute("https://town-night.jp/miyagi/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
