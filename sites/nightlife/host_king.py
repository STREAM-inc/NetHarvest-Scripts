"""
ホスキン (HOST KING / hos-kin.com) — 大阪のホストクラブ情報・ホスト求人紹介ポータル スクレイパー

取得対象:
    大阪 (ミナミ/キタ 等) のホストクラブ店舗情報
    (店舗名・カナ・都道府県・住所・サイト内エリア表記・TEL・定休日・公式サイト URL・
     X(Twitter)/Instagram/Facebook/YouTube・最寄駅・営業部・系列グループ)

取得フロー:
    1. 一覧ページ (引数 url = https://hos-kin.com/hostclub) を取得する。
       一覧は `section.list-area` がエリア見出し (h4 例:「宗右衛門町のホストクラブ(大阪)」)
       ごとに区切られ、その中の `li` が 1 店舗カード。カードから詳細 URL (/shop/{ID})、
       サイト内エリア表記 (span.shop-area 例:「ミナミ/宗右衛門町/ホストクラブ」)、
       住所 (span.shop-address)、TEL (span.shop-tel) を取得する。
    2. 補助列挙として `/shop_search` (url から派生) も読み、一覧に出ない店舗を拾って
       URL で重複除去する。
    3. 店舗 1 件ごとに詳細ページ (/shop/{ID}) を取得し、サイドバー `aside#shop-info` から
       店名・カナ・TEL・住所・最寄駅・定休日・営業部を、`aside.side-group` から系列グループを
       取得して **即 yield** する (全件バッファせず 1 件ずつ返す)。

備考:
    - ページネーションは存在しない (一覧は 1 ページ完結)。
    - `sitemap.xml` には約 96 件の /shop/{ID} が載っているが、大半が 404 の古い残骸のため
      列挙元には使わない (無駄な 404 リクエストを避ける)。稼働店舗は一覧ページ側が正。
      2026-08 時点の稼働店舗は 5 件 (26 KING / 60 KiJiMUNA / 77 ATOM / 101 Perfect /
      449 club GO)。少件数はセレクタ不良ではなくサイト側の掲載在庫。
    - 一覧・補助列挙にはテンプレート由来の `/shop/0` (404) が混ざるため ID 0 は除外する。
      `/shop_search` にも 404 の残骸 (例: 221) が混ざるので詳細取得失敗は件数に数えて継続する。
    - 掲載は大阪府内が中心。府外掲載を取りこぼさないよう都道府県フィルターは掛けず、
      住所先頭から都道府県を判定する。
    - 系列グループはサイト側の名称表示 (div.group-name の span) が空で /group/detail/{ID} も
      404 のため、**グループ名はサイト上に存在しない**。取得できるグループ ID と系列店舗名を
      補助カラムに入れ、名称は空文字のままにする (値を捏造しない)。
    - 店舗紹介文 (div.comment) と料金システム表の長文自由記述 (プロース) は
      著作権リスクを避けて取得しない。
    - 利用規約 (/terms) にスクレイピング/クローリングの明示禁止は無く、robots.txt も Allow: /。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/host_king.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id host_king
"""

import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import bs4

from src.framework.static import StaticCrawler
from src.const.schema import Schema

logger = logging.getLogger(__name__)

# 47 都道府県の先頭一致パターン (住所の先頭から都道府県を切り出す)
_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

# 詳細ページ URL (/shop/{ID}) — 一覧・補助列挙から店舗リンクだけを抜き出す
_SHOP_HREF_PATTERN = re.compile(r"/shop/(\d+)/?$")

# ul.spec 内の TEL 行 ("TEL:06-6212-1005")
_TEL_PATTERN = re.compile(r"TEL[:：]\s*([\d\-()]+)")

# 営業部アイコン (ul.open li.part1 / li.part2) → 表示名
_PART_LABELS = {"part1": "1部", "part2": "2部"}


class HostKingScraper(StaticCrawler):
    """ホスキン (hos-kin.com) 大阪ホストクラブ店舗情報スクレイパー"""

    DELAY = 1.5

    # 構造化された短い値・URL のみ。長文プロース (店舗紹介文/料金説明) は含めない
    EXTRA_COLUMNS = [
        "エリア",
        "エリア詳細",
        "一覧見出し",
        "最寄駅",
        "営業部",
        "YouTubeチャンネル",
        "系列グループ名",
        "系列グループID",
        "系列店舗",
    ]

    def parse(self, url: str):
        """一覧 → 詳細を巡回し、1 店舗ごとに即 yield する。

        Args:
            url (str): 一覧ページ URL (sites.yml の url = SSOT)。この url を唯一の
                起点として、補助列挙先・詳細ページ URL はすべてここから派生させる。

        Yields:
            dict: Schema / EXTRA_COLUMNS に沿った店舗 1 件分のデータ
        """
        cards = self._collect_cards(url)
        self.total_items = len(cards)
        logger.info("店舗リンクを %d 件検出しました", len(cards))

        for detail_url, card in cards.items():
            try:
                item = self._parse_detail(detail_url, card)
            except Exception as exc:  # noqa: BLE001
                # 404 の残骸 (/shop_search 由来) や 1 店舗の失敗で全体を止めない
                self.error_count += 1
                logger.warning("店舗の解析に失敗 (スキップ): %s — %s", detail_url, exc)
                continue

            if item:
                yield item

    # ------------------------------------------------------------------
    # 一覧側
    # ------------------------------------------------------------------
    def _collect_cards(self, url: str) -> dict:
        """一覧ページと補助列挙ページから店舗カード情報を集める (URL で重複除去)。

        Returns:
            dict: {詳細URL: {"heading": str, "area": str, "address": str, "tel": str}}
        """
        cards: dict[str, dict] = {}

        # 主列挙: 引数 url (一覧ページ)
        self._harvest_cards(url, cards)

        # 補助列挙: /shop_search (url から派生)。一覧に出ない店舗の取りこぼしを防ぐ
        search_url = urljoin(url, "/shop_search")
        if search_url.rstrip("/") != url.rstrip("/"):
            self._harvest_cards(search_url, cards)

        return cards

    def _harvest_cards(self, list_url: str, cards: dict) -> None:
        """一覧系ページ 1 枚から店舗カードを抽出し cards に追記する (既出はスキップ)。"""
        soup = self.get_soup(list_url)
        if soup is None:
            logger.warning("一覧ページを取得できませんでした: %s", list_url)
            return

        for section in soup.select("section.list-area"):
            heading_el = section.select_one("h4, h3, h2, h1")
            heading = heading_el.get_text(strip=True) if heading_el else ""

            for li in section.select("li"):
                link = li.select_one("span.shop-name a[href]") or li.select_one("a[href]")
                if not link:
                    continue
                detail_url = self._to_detail_url(list_url, link.get("href", ""))
                if not detail_url or detail_url in cards:
                    continue

                area_el = li.select_one("span.shop-area")
                addr_el = li.select_one("span.shop-address")
                tel_el = li.select_one("span.shop-tel")
                cards[detail_url] = {
                    "heading": heading,
                    "area": area_el.get_text(strip=True) if area_el else "",
                    "address": addr_el.get_text(strip=True) if addr_el else "",
                    "tel": tel_el.get_text(strip=True) if tel_el else "",
                }

    def _to_detail_url(self, base_url: str, href: str) -> str | None:
        """href が店舗詳細 (/shop/{ID}) なら絶対 URL にして返す。それ以外は None。

        テンプレート由来のプレースホルダ `/shop/0` は 404 なので除外する。
        """
        if not href:
            return None
        absolute = urljoin(base_url, href)
        # クエリ/フラグメント付きや /shop/{ID}/host のような下層は対象外
        m = _SHOP_HREF_PATTERN.search(absolute)
        if not m or m.group(1) == "0":
            return None
        return absolute

    # ------------------------------------------------------------------
    # 詳細側
    # ------------------------------------------------------------------
    def _parse_detail(self, detail_url: str, card: dict) -> dict | None:
        """詳細ページ 1 枚をパースして 1 件分の dict を返す。"""
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        info = soup.select_one("aside#shop-info .shop-info") or soup

        item = {Schema.URL: detail_url}

        # --- 店名 / カナ -------------------------------------------------
        name, kana = self._parse_name(info, soup)
        if not name:
            logger.warning("店舗名を取得できませんでした: %s", detail_url)
            return None
        item[Schema.NAME] = name
        item[Schema.NAME_KANA] = kana

        # --- ul.spec (TEL / 住所 / 最寄駅 / 定休日) -----------------------
        tel, address, station, holiday = self._parse_spec(info)

        # TEL は詳細優先、無ければ一覧カードの値を使う
        item[Schema.TEL] = tel or card.get("tel", "")

        # 住所も詳細優先 (詳細は都道府県付き、一覧は市区町村以降)
        address = address or card.get("address", "")
        pref, addr_rest = self._split_pref(address)
        item[Schema.PREF] = pref
        item[Schema.ADDR] = addr_rest

        item[Schema.HOLIDAY] = holiday
        item["最寄駅"] = station

        # --- サイト内エリア表記 ("ミナミ/宗右衛門町/ホストクラブ") --------
        area_parts = [p.strip() for p in card.get("area", "").split("/") if p.strip()]
        item["エリア"] = area_parts[0] if area_parts else ""
        item["エリア詳細"] = area_parts[1] if len(area_parts) > 1 else ""
        item[Schema.CAT_SITE] = area_parts[-1] if len(area_parts) > 2 else ""
        item["一覧見出し"] = card.get("heading", "")

        # --- 営業部 (ul.open のアイコンクラス) ---------------------------
        parts = []
        for li in info.select("ul.open li"):
            for cls in li.get("class", []):
                if cls in _PART_LABELS and _PART_LABELS[cls] not in parts:
                    parts.append(_PART_LABELS[cls])
        item["営業部"] = "/".join(parts)

        # --- SNS / 公式サイト --------------------------------------------
        item.update(self._parse_links(soup))

        # --- 系列グループ -------------------------------------------------
        item.update(self._parse_group(soup, detail_url))

        return item

    def _parse_name(self, info: bs4.element.Tag, soup: bs4.BeautifulSoup) -> tuple:
        """サイドバーの div.shop-name から店名と読み仮名 (span.eng) を分離する。"""
        name_el = info.select_one("div.shop-name")
        if name_el is None:
            name_el = soup.select_one(".shop-nav-wrap .shop-name")
        if name_el is None:
            return "", ""

        kana_el = name_el.select_one("span.eng")
        kana = kana_el.get_text(strip=True) if kana_el else ""
        if kana_el:
            kana_el.extract()
        name = name_el.get_text(" ", strip=True)
        return name, kana

    def _parse_spec(self, info: bs4.element.Tag) -> tuple:
        """ul.spec の各 li を内容で判別して TEL / 住所 / 最寄駅 / 定休日 を返す。

        li の並び順に依存すると掲載項目が欠けた店舗でズレるため、
        クラス名と本文パターンで判定する。
        """
        tel = address = station = holiday = ""

        for li in info.select("ul.spec li"):
            classes = li.get("class", [])
            text = li.get_text(" ", strip=True)
            if not text:
                continue

            if "holiday" in classes:
                # "定休日 月曜日" の見出し span を除いた残りが値
                label = li.select_one("span")
                holiday = text
                if label:
                    holiday = text.replace(label.get_text(strip=True), "", 1).strip()
                continue

            m = _TEL_PATTERN.search(text)
            if m:
                tel = m.group(1).strip()
                continue

            if _PREF_PATTERN.match(text) or re.search(r"[市区郡町村]", text):
                if not address:
                    address = text
                continue

            if "駅" in text or "線" in text:
                station = text

        return tel, address, station, holiday

    def _split_pref(self, address: str) -> tuple:
        """住所文字列を都道府県と市区町村以降に分割する (判定できなければ PREF は空)。"""
        address = (address or "").strip()
        if not address:
            return "", ""
        m = _PREF_PATTERN.match(address)
        if m:
            return m.group(1), address[m.end():].strip()
        return "", address

    def _parse_links(self, soup: bs4.BeautifulSoup) -> dict:
        """SNS アイコンリンクと公式サイトリンクを取得する。"""
        result = {
            Schema.X: "",
            Schema.INSTA: "",
            Schema.FB: "",
            "YouTubeチャンネル": "",
            Schema.HP: "",
        }

        selectors = {
            Schema.X: "a.icon-twitter[href]",
            Schema.INSTA: "a.icon-instagram[href]",
            Schema.FB: "a.icon-facebook2[href], a.icon-facebook[href]",
            "YouTubeチャンネル": "a.icon-youtube[href]",
        }
        for key, selector in selectors.items():
            el = soup.select_one(selector)
            if el:
                href = el.get("href", "").strip()
                if href and not href.startswith("#"):
                    result[key] = href

        site = soup.select_one("div.site-link a[href]")
        if site:
            href = site.get("href", "").strip()
            if href and not href.startswith("#"):
                result[Schema.HP] = href

        return result

    def _parse_group(self, soup: bs4.BeautifulSoup, detail_url: str) -> dict:
        """系列グループ (aside.side-group) の名称・ID・系列店舗名を取得する。

        サイト側でグループ名の表示 (div.group-name の span) が空のため、
        取得できなければ空文字にする (値を捏造しない)。
        """
        result = {"系列グループ名": "", "系列グループID": "", "系列店舗": ""}

        group = soup.select_one("aside.side-group")
        if group is None:
            return result

        name_el = group.select_one(".group-name")
        if name_el:
            result["系列グループ名"] = name_el.get_text(" ", strip=True)

        link = group.select_one('a[href*="/group/detail/"]')
        if link:
            m = re.search(r"/group/detail/(\d+)", link.get("href", ""))
            if m:
                result["系列グループID"] = m.group(1)

        # 系列店舗名 (自店は除く)
        self_id_match = _SHOP_HREF_PATTERN.search(detail_url)
        self_id = self_id_match.group(1) if self_id_match else ""
        siblings = []
        for a in group.select('ul.group-list a[href*="/shop/"]'):
            m = _SHOP_HREF_PATTERN.search(urljoin(detail_url, a.get("href", "")))
            if not m or m.group(1) == self_id:
                continue
            img = a.select_one("img[alt]")
            label = (img.get("alt", "").strip() if img else "") or a.get_text(" ", strip=True)
            if label and label not in siblings:
                siblings.append(label)
        result["系列店舗"] = " / ".join(siblings)

        return result


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = HostKingScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://hos-kin.com/hostclub")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
