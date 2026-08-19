"""
楽天トラベル【国内宿泊施設】 (travel) — 全国の宿泊施設情報スクレイパー

取得対象:
    - 楽天トラベル (travel.rakuten.co.jp) に掲載されている国内の宿泊施設
      (ホテル・旅館・民宿等、約 3 万件超)。
    - 施設名 / 郵便番号 / 都道府県 / 住所 / TEL / FAX / エリア / 施設タイプ /
      チェックイン・アウト / 総部屋数 / 館内設備 / 部屋設備 / 支払い方法 /
      風呂・泉質 / 口コミ点数・件数 / 最安値 など

取得フロー:
    1. 引数 url (= サイトトップ) を取得し、そこから派生させた
       /yado/japan.html (地図から探す) の都道府県リンク (/yado/{pref}/map.html) で
       47 都道府県コードを列挙する。
       ※ japan.html が取得できない場合はトップページのエリアマップにある
         地方ページ (/hokkaido/ 等) を辿って都道府県コードを集めるフォールバックを持つ。
    2. 都道府県別の宿一覧 (/yado/{pref}/) を 1 ページ 30 件で巡回する。
       2 ページ目以降はページ内の「次へ」リンク
       (a.pagination__control-btn--next → search.travel.rakuten.co.jp/ds/yado/{pref}/pN)
       を辿る。ページ内のカード (.htl-list-card) から施設 ID・口コミ・最安値を取得。
    3. カード 1 件ごとに施設の基本情報ページ
       (/HOTEL/{id}/{id}_std.html = 設備・アメニティ・基本情報) を取得し、
       住所 / TEL / FAX / 設備等をマージして即 yield する
       (早期 yield / Pattern B。全件収集してからの一括 yield はしない)。

注意:
    - ルート URL は引数 `url` を唯一の起点 (SSOT) とし、配下 URL はすべて
      urljoin(url, ...) で派生させる。別 URL はハードコードしない。
      ※ 2 ページ目以降のみサイト側が別ホスト (search.travel.rakuten.co.jp) の
        リンクを返すため、ページ内リンクをそのまま辿る。
    - 施設 ID 基準で重複排除する (一覧には PR 枠として他エリアの施設も混ざる)。
    - robots.txt が Disallow している施設 ID (/HOTEL/5/ 等) はスキップする。
    - 施設紹介文 (キャッチコピー) / 駐車場説明 / 条件・注意事項 / キャンセルポリシー等の
      自由記述 (プロース) は著作権リスクのため取得しない。
    - 法人番号・代表者・資本金・売上・従業員数・設立日・メール・SNS・公式HP は
      楽天トラベル上に掲載が無いため取得できない (空欄)。
    - 利用規約 (https://travel.rakuten.co.jp/info/agreement.html) にスクレイピング・
      クローリングを禁止する条項は無い (2026-08 確認)。

実行方法:
    # ローカルテスト
    python scripts/sites/travel/travel.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id travel
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


# /yado/{pref}/map.html から都道府県コードを取り出すパターン
_PREF_MAP_HREF = re.compile(r"/yado/([a-z]+)/map(?:_s)?\.html")
# /yado/{pref}/ 形式 (都道府県別 宿一覧) のパターン
_PREF_DIR_HREF = re.compile(r"/yado/([a-z]+)/(?:\?|$)")
# 施設ページ (/HOTEL/{id}/) の ID
_HOTEL_ID = re.compile(r"/HOTEL/(\d+)/")
# 住所先頭の郵便番号
_POST_PATTERN = re.compile(r"〒?\s*(\d{3}-\d{4})")
# 住所から都道府県を切り出すパターン
_PREF_PATTERN = re.compile(
    r"^(北海道|東京都|大阪府|京都府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
# 口コミ件数 "（369件）"
_REV_COUNT = re.compile(r"([\d,]+)\s*件")

# robots.txt (https://travel.rakuten.co.jp/robots.txt) が Disallow している施設 ID
_ROBOTS_DENY_IDS = {
    "5", "6", "1879", "31571", "39642", "107681",
    "146329", "146330", "146332", "146333", "146334", "146335",
    "146336", "146337", "146338", "146339", "146340",
    "196945", "109213",
}

# 「宿の注目ポイント」から施設タイプとみなすラベル (長い順に判定)
_FACILITY_TYPES = [
    "ビジネスホテル", "シティホテル", "リゾートホテル", "カプセルホテル",
    "ラブホテル", "温泉旅館", "旅館", "民宿", "ペンション", "コテージ",
    "ロッジ", "ゲストハウス", "ホステル", "貸別荘", "公共の宿",
    "オーベルジュ", "民泊", "ホテル",
]

# 詳細ページの dt ラベルのうち取得対象 (プロース項目は意図的に除外)
_LABEL_TO_EXTRA = {
    "チェックイン": "チェックイン",
    "チェックアウト": "チェックアウト",
    "FAX": "FAX",
    "交通アクセス": "交通アクセス",
    "総部屋数": "総部屋数",
    "館内設備": "館内設備",
    "部屋設備・備品": "部屋設備・備品",
    "食事場所": "食事場所",
    "バリアフリー対応": "バリアフリー対応",
    "特典": "特典",
}

# ページネーションの安全上限 (1 都道府県あたり。30 件/ページ)
_MAX_PAGES = 500


class RakutenTravelYadoScraper(StaticCrawler):
    """楽天トラベル 国内宿泊施設スクレイパー"""

    DELAY = 0.7
    EXTRA_COLUMNS = [
        "施設ID",
        "施設ページURL",
        "エリア",
        "エリア詳細",
        "FAX",
        "チェックイン",
        "チェックアウト",
        "総部屋数",
        "交通アクセス",
        "館内設備",
        "部屋設備・備品",
        "食事場所",
        "風呂の種類",
        "泉質",
        "効能",
        "バリアフリー対応",
        "宿の注目ポイント",
        "特典",
        "最安値",
    ]

    # ------------------------------------------------------------------ parse

    def parse(self, url: str) -> Generator[dict, None, None]:
        top = self.get_soup(url)
        if top is None:
            self.logger.error("トップページを取得できませんでした: %s", url)
            return

        pref_codes = self._collect_pref_codes(url, top)
        if not pref_codes:
            self.logger.error("都道府県コードを取得できませんでした")
            return
        self.logger.info("都道府県コード収集完了: %d 件", len(pref_codes))

        seen_ids: set[str] = set()
        for pref in pref_codes:
            list_url = urljoin(url, f"/yado/{pref}/")
            yield from self._crawl_pref(url, list_url, pref, seen_ids)

    # ------------------------------------------------------- 都道府県コード列挙

    def _collect_pref_codes(self, url: str, top_soup) -> list[str]:
        """/yado/japan.html (地図から探す) から 47 都道府県コードを列挙する。"""
        codes: list[str] = []

        japan_soup = self.get_soup(urljoin(url, "/yado/japan.html"))
        if japan_soup is not None:
            codes = self._extract_pref_codes(japan_soup)

        if len(codes) >= 40:
            return codes

        # フォールバック: トップページのエリアマップから地方ページを辿る
        self.logger.warning("japan.html から %d 件しか取れず、地方ページを辿ります", len(codes))
        found = list(codes)
        for region_url in self._region_urls(url, top_soup):
            region_soup = self.get_soup(region_url)
            if region_soup is None:
                continue
            for code in self._extract_pref_codes(region_soup):
                if code not in found:
                    found.append(code)
        return found

    def _extract_pref_codes(self, soup) -> list[str]:
        codes: list[str] = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            m = _PREF_MAP_HREF.search(href) or _PREF_DIR_HREF.search(href)
            if m and m.group(1) not in codes:
                codes.append(m.group(1))
        return codes

    def _region_urls(self, url: str, top_soup) -> list[str]:
        """トップページのエリアマップにある地方ページ URL (/hokkaido/ 等) を集める。"""
        urls: list[str] = []
        for a in top_soup.find_all("a", href=True):
            href = a["href"]
            if "l-id=topC_map_area" not in href:
                continue
            full = urljoin(url, href)
            if full not in urls:
                urls.append(full)
        return urls

    # ------------------------------------------------------------ 一覧ページ巡回

    def _crawl_pref(
        self, root_url: str, list_url: str, pref: str, seen_ids: set[str]
    ) -> Generator[dict, None, None]:
        page_url = list_url
        for page_no in range(1, _MAX_PAGES + 1):
            soup = self.get_soup(page_url)
            if soup is None:
                return

            if page_no == 1:
                total_el = soup.select_one(".pagination__info-text--total")
                if total_el:
                    self.logger.info(
                        "%s: 総件数 %s 件", pref, total_el.get_text(strip=True)
                    )

            cards = soup.select(".htl-list-card")
            if not cards:
                self.logger.info("%s: カード無しで終了 (page=%d)", pref, page_no)
                return

            for card in cards:
                hotel_id = self._card_hotel_id(card)
                if not hotel_id or hotel_id in seen_ids:
                    continue
                seen_ids.add(hotel_id)
                if hotel_id in _ROBOTS_DENY_IDS:
                    self.logger.info("robots.txt により除外: %s", hotel_id)
                    continue

                try:
                    item = self._scrape_detail(root_url, hotel_id, card)
                except Exception as e:  # 個別施設の失敗は継続
                    self.logger.warning("詳細取得失敗 id=%s — %s", hotel_id, e)
                    continue
                if item:
                    yield item

            next_a = soup.select_one("a.pagination__control-btn--next[href]")
            if not next_a:
                return
            page_url = urljoin(page_url, next_a["href"])

    def _card_hotel_id(self, card) -> str:
        hid = card.get("data-map-modal-hotel-no", "").strip()
        if hid:
            return hid
        li_id = card.get("id", "")
        m = re.match(r"(\d+)_list", li_id)
        if m:
            return m.group(1)
        link = card.select_one(".hotel-list__title-text a[href]")
        if link:
            m = _HOTEL_ID.search(link["href"])
            if m:
                return m.group(1)
        return ""

    # -------------------------------------------------------------- 詳細ページ

    def _scrape_detail(self, root_url: str, hotel_id: str, card) -> dict | None:
        detail_url = urljoin(root_url, f"/HOTEL/{hotel_id}/{hotel_id}_std.html")
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        item: dict = {
            Schema.URL: detail_url,
            "施設ID": hotel_id,
            "施設ページURL": urljoin(root_url, f"/HOTEL/{hotel_id}/{hotel_id}.html"),
        }

        # 施設名: ページ内 JS の hotelBasicInfo が最も素の名称
        item[Schema.NAME] = self._hotel_name(soup, card)

        # ラベル (dt) → 値 (dd) の辞書を構築
        labels = self._label_map(soup)

        # 住所 / 郵便番号 / 都道府県
        addr_raw = labels.get("住所", "")
        if addr_raw:
            addr_raw = re.split(r"地図を見る", addr_raw)[0].strip()
            mp = _POST_PATTERN.match(addr_raw)
            if mp:
                item[Schema.POST_CODE] = mp.group(1)
                addr = addr_raw[mp.end():].strip()
            else:
                addr = addr_raw
            addr = re.sub(r"\s+", " ", addr).strip()
            if addr:
                item[Schema.ADDR] = addr
            mpref = _PREF_PATTERN.match(addr)
            if mpref:
                item[Schema.PREF] = mpref.group(1)

        if labels.get("TEL"):
            item[Schema.TEL] = labels["TEL"]

        # 単純マッピング (構造化項目のみ)
        for label, column in _LABEL_TO_EXTRA.items():
            value = labels.get(label, "")
            if value:
                item[column] = value

        # 支払い方法 (クレジットカード)
        if labels.get("ご利用可能なクレジットカード"):
            item[Schema.PAYMENTS] = labels["ご利用可能なクレジットカード"]

        # 風呂 ("[種類] ... [泉質] ... [効能] ...")
        bath = labels.get("風呂", "")
        if bath:
            item["風呂の種類"] = self._bath_section(bath, "種類")
            item["泉質"] = self._bath_section(bath, "泉質")
            item["効能"] = self._bath_section(bath, "効能")

        # エリア (パンくず: 全国 > 都道府県 > エリア > エリア詳細)
        crumbs = {
            a.get("data-locate", ""): a.get_text(strip=True)
            for a in soup.select("a[data-locate^=breadcrumb-]")
        }
        item["エリア"] = crumbs.get("breadcrumb-small", "")
        item["エリア詳細"] = crumbs.get("breadcrumb-detail", "")
        if not item.get(Schema.PREF):
            item[Schema.PREF] = crumbs.get("breadcrumb-middle", "")

        # 宿の注目ポイント + そこから施設タイプを判定
        features = [
            sp.get_text(strip=True)
            for sp in soup.select(".htl-detail__features--content > span:not(.icon)")
            if sp.get_text(strip=True)
        ]
        item["宿の注目ポイント"] = "、".join(features)
        item[Schema.CAT_SITE] = self._facility_type(features)

        # 口コミ・最安値 (一覧カード側にのみ掲載)
        item.update(self._card_fields(card))

        if not item.get(Schema.NAME):
            return None
        return item

    def _hotel_name(self, soup, card) -> str:
        for script in soup.find_all("script"):
            text = script.string or ""
            m = re.search(r'hotelName\s*:\s*"(.*?)"', text)
            if m and m.group(1).strip():
                return m.group(1).strip()
        link = card.select_one(".hotel-list__title-text a")
        if link:
            return link.get_text(strip=True)
        h1 = soup.find("h1")
        if h1:
            return re.sub(r"\s*設備・アメニティ・基本情報\s*$", "", h1.get_text(strip=True))
        return ""

    def _label_map(self, soup) -> dict[str, str]:
        """詳細ページの dl (dt/dd) をラベル→値の辞書にする。"""
        labels: dict[str, str] = {}
        for dl in soup.find_all("dl"):
            dt = dl.find("dt")
            dd = dl.find("dd")
            if not dt or not dd:
                continue
            label = dt.get_text(strip=True)
            if not label or label in labels:
                continue
            items = [li.get_text(" ", strip=True) for li in dd.find_all("li")]
            items = [t for t in items if t]
            if items:
                value = "、".join(items)
            else:
                value = re.sub(r"\s+", " ", dd.get_text(" ", strip=True))
            labels[label] = value.strip()
        return labels

    def _bath_section(self, bath: str, section: str) -> str:
        m = re.search(rf"\[{section}\]\s*(.*?)(?=\[|$)", bath)
        if not m:
            return ""
        return "、".join(t for t in re.split(r"[\s、]+", m.group(1).strip()) if t)

    def _card_fields(self, card) -> dict:
        out: dict = {}
        score_el = card.select_one(".cstmrEvl strong")
        if score_el:
            out[Schema.SCORES] = score_el.get_text(strip=True)
        evl = card.select_one(".cstmrEvl")
        if evl:
            m = _REV_COUNT.search(evl.get_text(" ", strip=True))
            if m:
                out[Schema.REV_SCR] = m.group(1).replace(",", "")
        price_el = card.select_one(".htlLowprice strong")
        if price_el:
            out["最安値"] = price_el.get_text(strip=True).replace(",", "")
        return out

    def _facility_type(self, features: list[str]) -> str:
        for feature in features:
            for t in _FACILITY_TYPES:
                if t in feature:
                    return t
        return ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = RakutenTravelYadoScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    #    parse() は引数 url を唯一の起点とし、配下 URL は urljoin で派生させる。
    scraper.execute("https://travel.rakuten.co.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
