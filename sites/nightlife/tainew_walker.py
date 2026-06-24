"""
体入キャストウォーカー (tainew-walker.jp) — キャバクラ/ガールズバー等 体験入店求人スクレイパー

取得対象:
    - 関東 / 関西 / 東海 / 九州 各エリアの掲載店舗（推定 約216件）
    - 店舗名 / 名称_カナ / 都道府県 / 住所 / TEL / 業種(サイト定義) / 営業時間 / 定休日 / HP / LINE 等
    - サイト固有: 給与 / 体験時給 / コンセプト(衣装) / こだわり条件 / アクセス / 採用担当 / 店舗番号 / エリア

取得フロー:
    1. robots.txt から地域別 sitemap_*.xml を取得し、/{region}/prefecture/{pref} の一覧URLを収集
    2. 各 prefecture 一覧ページを ?page=N でページ送りし、店舗カード(.part-shop-list-item)を抽出
    3. 有料掲載店(詳細リンクあり)は /{region}/shop/{id} 詳細ページから全項目を取得
    4. 無料掲載店(詳細リンクなし)は一覧カードの情報(店名/住所/エリア/業種)のみ取得
    5. 詳細URL もしくは 店名+住所 で重複排除し、1件ずつ即 yield

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/tainew_walker.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id tainew_walker
"""

from __future__ import annotations

import re
import sys
import warnings
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup, Tag
from bs4 import XMLParsedAsHTMLWarning

# sitemap.xml を html.parser で読む際の警告を抑制（loc 抽出のみで十分なため）
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

from src.const.schema import Schema
from src.framework.static import StaticCrawler

# prefecture スラッグ → 正式都道府県名（住所に都道府県が含まれない政令市表記対策のため
# 一覧URLのスラッグから確実に都道府県を決定する）
_PREF_SLUG: dict[str, str] = {
    "hokkaido": "北海道",
    "aomori": "青森県", "iwate": "岩手県", "miyagi": "宮城県",
    "akita": "秋田県", "yamagata": "山形県", "fukushima": "福島県",
    "ibaraki": "茨城県", "tochigi": "栃木県", "gunma": "群馬県",
    "saitama": "埼玉県", "chiba": "千葉県", "tokyo": "東京都", "kanagawa": "神奈川県",
    "niigata": "新潟県", "toyama": "富山県", "ishikawa": "石川県", "fukui": "福井県",
    "yamanashi": "山梨県", "nagano": "長野県",
    "gifu": "岐阜県", "shizuoka": "静岡県", "aichi": "愛知県", "nagoya": "愛知県", "mie": "三重県",
    "shiga": "滋賀県", "kyoto": "京都府", "osaka": "大阪府", "hyogo": "兵庫県",
    "nara": "奈良県", "wakayama": "和歌山県",
    "tottori": "鳥取県", "shimane": "島根県", "okayama": "岡山県",
    "hiroshima": "広島県", "yamaguchi": "山口県",
    "tokushima": "徳島県", "kagawa": "香川県", "ehime": "愛媛県", "kochi": "高知県",
    "fukuoka": "福岡県", "saga": "佐賀県", "nagasaki": "長崎県", "kumamoto": "熊本県",
    "oita": "大分県", "miyazaki": "宮崎県", "kagoshima": "鹿児島県", "okinawa": "沖縄県",
}

_PREF_PATTERN = re.compile(
    r"^(北海道|東京都|京都府|大阪府|"
    r"青森県|岩手県|宮城県|秋田県|山形県|福島県|"
    r"茨城県|栃木県|群馬県|埼玉県|千葉県|神奈川県|"
    r"新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|"
    r"滋賀県|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|"
    r"徳島県|香川県|愛媛県|高知県|"
    r"福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)
_TEL_PATTERN = re.compile(r"0\d{1,4}-?\d{1,4}-?\d{3,4}")
_SHOP_PATH_RE = re.compile(r"^/[a-z]+/shop/\d+/?$")
_MAP_NOTE_RE = re.compile(r"\s*MAP\s*で表示\s*$")

# 詳細ページの定義リスト見出し → 抽出キー
_TITLE_CLASSES = (
    (".shop-show-body-condition-inner", "-title", "-body"),
    (".shop-show-body-introduction-inner", "-title", "-body"),
)


class TainewWalkerScraper(StaticCrawler):
    """体入キャストウォーカー スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS: list[str] = [
        "給与",
        "体験時給",
        "コンセプト(衣装)",
        "こだわり条件",
        "アクセス",
        "採用担当",
        "店舗番号",
        "エリア",
    ]

    # ------------------------------------------------------------------ #
    # メインフロー
    # ------------------------------------------------------------------ #

    def parse(self, url: str) -> Generator[dict, None, None]:
        sitemaps = self._sitemap_urls(url)
        self.logger.info("対象 sitemap: %d件", len(sitemaps))

        seen: set[str] = set()
        self.total_items = 0

        for sitemap_url in sitemaps:
            for pref_url in self._prefecture_urls(sitemap_url, url):
                yield from self._crawl_prefecture(pref_url, seen)

        self.logger.info("取得完了: %d件", len(seen))

    def _crawl_prefecture(
        self, pref_url: str, seen: set[str]
    ) -> Generator[dict, None, None]:
        page = 1
        while True:
            page_url = pref_url if page == 1 else f"{pref_url}?page={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                break

            if page == 1:
                self.total_items += self._extract_result_count(soup)

            items = soup.select(".part-shop-list-item")
            if not items:
                break

            for item in items:
                try:
                    record = self._build_record(item, pref_url)
                except Exception as e:
                    self.logger.warning("店舗解析失敗: %s (%s)", page_url, e)
                    continue
                if not record:
                    continue
                key = record.get(Schema.URL) or (
                    f"{record.get(Schema.NAME, '')}|{record.get(Schema.ADDR, '')}"
                )
                if key in seen:
                    continue
                seen.add(key)
                self.logger.info(
                    "取得: %s (%s)",
                    record.get(Schema.NAME) or "?",
                    record.get(Schema.PREF) or "",
                )
                yield record

            # 次ページが無ければ終了
            if not soup.select_one(f'a[href*="page={page + 1}"]'):
                break
            page += 1

    # ------------------------------------------------------------------ #
    # sitemap / 一覧URL 収集（全て引数 url を起点に派生）
    # ------------------------------------------------------------------ #

    def _sitemap_urls(self, root_url: str) -> list[str]:
        """robots.txt の Sitemap 行から地域別 sitemap を収集。失敗時は既知の地域にフォールバック。"""
        robots_url = urljoin(root_url, "/robots.txt")
        sitemaps: list[str] = []
        try:
            resp = self.session.get(robots_url, timeout=self.TIMEOUT)
            resp.raise_for_status()
            for line in resp.text.splitlines():
                line = line.strip()
                if line.lower().startswith("sitemap:"):
                    sm = line.split(":", 1)[1].strip()
                    if sm and "blog" not in sm.lower():
                        sitemaps.append(urljoin(root_url, sm))
        except Exception as e:
            self.logger.warning("robots.txt 取得失敗: %s (%s)", robots_url, e)

        if not sitemaps:
            sitemaps = [
                urljoin(root_url, f"/sitemap_{region}.xml")
                for region in ("kanto", "kansai", "tokai", "kyushu")
            ]
        return list(dict.fromkeys(sitemaps))

    def _prefecture_urls(self, sitemap_url: str, root_url: str) -> list[str]:
        try:
            resp = self.session.get(sitemap_url, timeout=self.TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            self.logger.warning("sitemap 取得失敗: %s (%s)", sitemap_url, e)
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        urls: list[str] = []
        seen: set[str] = set()
        for loc in soup.find_all("loc"):
            href = (loc.get_text() or "").strip()
            if not href:
                continue
            base = urljoin(root_url, href.split("?")[0])
            if "/prefecture/" in urlparse(base).path and base not in seen:
                seen.add(base)
                urls.append(base)
        return urls

    @staticmethod
    def _extract_result_count(soup: BeautifulSoup) -> int:
        el = soup.select_one(".search-result-title")
        if not el:
            return 0
        m = re.search(r"([\d,]+)\s*件", el.get_text())
        return int(m.group(1).replace(",", "")) if m else 0

    # ------------------------------------------------------------------ #
    # 1 店舗レコードの構築（一覧 + 詳細マージ）
    # ------------------------------------------------------------------ #

    def _build_record(self, item: Tag, pref_url: str) -> dict | None:
        pref = self._pref_from_url(pref_url)
        list_data = self._extract_list_item(item, pref)
        if not list_data.get(Schema.NAME):
            return None

        detail_url = self._detail_url(item, pref_url)
        if detail_url:
            try:
                detail = self._scrape_detail(detail_url, pref)
            except Exception as e:
                self.logger.warning("詳細取得失敗: %s (%s)", detail_url, e)
                detail = {Schema.URL: detail_url}
            # 詳細を優先しつつ、空欄は一覧の値で補完
            merged = dict(list_data)
            for k, v in detail.items():
                if v:
                    merged[k] = v
            return merged

        return list_data

    def _detail_url(self, item: Tag, pref_url: str) -> str:
        a = item.select_one("a.btn-detail[href]") or item.select_one(
            'a[href*="/shop/"]'
        )
        if not a:
            return ""
        href = a.get("href", "").strip()
        if not href:
            return ""
        if not _SHOP_PATH_RE.match(urlparse(urljoin(pref_url, href)).path):
            return ""
        return urljoin(pref_url, href)

    def _extract_list_item(self, item: Tag, pref: str) -> dict:
        name_el = item.select_one(".part-shop-list-item-name")
        kana_el = item.select_one(".part-shop-list-item-kana")
        body = self._list_body_map(item)

        addr = self._clean(body.get("住所", ""))
        return {
            Schema.URL: "",
            Schema.NAME: self._clean(name_el.get_text()) if name_el else "",
            Schema.NAME_KANA: self._clean(kana_el.get_text()) if kana_el else "",
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.CAT_SITE: self._clean(body.get("業種", "")),
            Schema.TIME: self._clean(body.get("時間", "")),
            "給与": self._clean(body.get("給与", "")),
            "こだわり条件": self._list_feature(item),
            "エリア": self._clean(body.get("エリア", "")),
        }

    def _list_body_map(self, item: Tag) -> dict[str, str]:
        result: dict[str, str] = {}
        for inner in item.select(".shop-list-item-body-inner"):
            t = inner.select_one(".shop-list-item-inner-title")
            b = inner.select_one(".shop-list-item-inner-body")
            if not t or not b:
                continue
            label = self._clean(t.get_text())
            if label and label not in result:
                result[label] = self._clean(b.get_text(" "))
        return result

    def _list_feature(self, item: Tag) -> str:
        feat = item.select_one(".shop-list-item-body-wrap-feature")
        if not feat:
            return ""
        for inner in feat.select(".shop-list-item-body-inner"):
            t = inner.select_one(".shop-list-item-inner-title")
            b = inner.select_one(".shop-list-item-inner-body")
            if t and b and self._clean(t.get_text()) == "こだわり条件":
                return self._clean(b.get_text(" "))
        return ""

    # ------------------------------------------------------------------ #
    # 詳細ページ
    # ------------------------------------------------------------------ #

    def _scrape_detail(self, url: str, pref: str) -> dict:
        soup = self.get_soup(url)
        if soup is None:
            return {Schema.URL: url}

        info = self._detail_info_map(soup)
        name, kana = self._detail_name_kana(soup, info)
        addr = _MAP_NOTE_RE.sub("", info.get("住所", "")).strip()
        sho_tel, saiyo_tel = self._detail_tels(soup, info)
        sns = self._detail_sns(soup)

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.NAME_KANA: kana,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: sho_tel or saiyo_tel,
            Schema.CAT_SITE: self._clean(info.get("業種", "")),
            Schema.TIME: self._clean(info.get("営業時間", "")),
            Schema.HOLIDAY: self._clean(info.get("定休日", "")),
            Schema.HP: self._detail_hp(soup),
            Schema.LINE: sns["line"],
            Schema.INSTA: sns["insta"],
            Schema.X: sns["x"],
            Schema.FB: sns["fb"],
            Schema.TIKTOK: sns["tiktok"],
            "給与": self._clean(info.get("給与", "")),
            "体験時給": self._clean(info.get("体験時給", "")),
            "コンセプト(衣装)": self._clean(info.get("コンセプト(衣装)", "")),
            "こだわり条件": self._clean(info.get("こだわり条件", "")),
            "アクセス": self._clean(info.get("アクセス", "")),
            "採用担当": saiyo_tel,
            "店舗番号": sho_tel,
        }

    def _detail_info_map(self, soup: BeautifulSoup) -> dict[str, str]:
        info: dict[str, str] = {}
        for base, t_suffix, b_suffix in _TITLE_CLASSES:
            for inner in soup.select(base):
                t = inner.select_one(base + t_suffix)
                b = inner.select_one(base + b_suffix)
                if not t or not b:
                    continue
                label = self._clean(t.get_text())
                if label and label not in info:
                    info[label] = self._clean(b.get_text(" "))
        return info

    def _detail_name_kana(
        self, soup: BeautifulSoup, info: dict[str, str]
    ) -> tuple[str, str]:
        nm = soup.select_one(".shop-show-body-condition-inner-body-name")
        kn = soup.select_one(".shop-show-body-condition-inner-body-kana")
        name = self._clean(nm.get_text()) if nm else ""
        kana = self._clean(kn.get_text()) if kn else ""
        if not name:
            # "店舗名" の値（"店名 カナ"）から復元
            shop = info.get("店舗名", "")
            if shop:
                name = self._clean(shop)
        return name, kana

    def _detail_tels(
        self, soup: BeautifulSoup, info: dict[str, str]
    ) -> tuple[str, str]:
        """(店舗番号, 採用担当) を返す。"""
        shop_tel = self._first_tel(info.get("店舗番号", ""))
        saiyo_tel = self._first_tel(info.get("採用担当", ""))
        if shop_tel or saiyo_tel:
            return shop_tel, saiyo_tel

        # フォールバック: TEL モーダル内の tel: リンク
        tel_links = [
            a.get("href", "").replace("tel:", "")
            for a in soup.select(".part-shop-tel-modal a[href^='tel:']")
        ]
        tel_links = [t for t in tel_links if t]
        if len(tel_links) >= 2:
            return tel_links[1], tel_links[0]
        if tel_links:
            return tel_links[0], ""
        return "", ""

    @staticmethod
    def _first_tel(text: str) -> str:
        if not text:
            return ""
        m = _TEL_PATTERN.search(text)
        return m.group(0) if m else ""

    def _detail_hp(self, soup: BeautifulSoup) -> str:
        for inner in soup.select(".shop-show-body-introduction-inner"):
            t = inner.select_one(".shop-show-body-introduction-inner-title")
            if t and self._clean(t.get_text()) == "店舗HP":
                a = inner.select_one("a[href]")
                if a and a.get("href", "").startswith("http"):
                    return a["href"].strip()
        return ""

    def _detail_sns(self, soup: BeautifulSoup) -> dict[str, str]:
        sns = {"insta": "", "x": "", "fb": "", "line": "", "tiktok": ""}
        for a in soup.select("a[href]"):
            href = a.get("href", "").strip()
            if not href.startswith("http"):
                continue
            low = href.lower()
            if "instagram.com" in low and not sns["insta"]:
                sns["insta"] = href
            elif (
                ("x.com" in low or "twitter.com" in low)
                and "intent" not in low
                and "share" not in low
                and not sns["x"]
            ):
                sns["x"] = href
            elif "facebook.com" in low and not sns["fb"]:
                sns["fb"] = href
            elif ("line.me" in low or "lin.ee" in low) and not sns["line"]:
                sns["line"] = href
            elif "tiktok.com" in low and not sns["tiktok"]:
                sns["tiktok"] = href
        return sns

    # ------------------------------------------------------------------ #
    # ユーティリティ
    # ------------------------------------------------------------------ #

    def _pref_from_url(self, pref_url: str) -> str:
        slug = pref_url.rstrip("/").rsplit("/", 1)[-1].split("?")[0].lower()
        return _PREF_SLUG.get(slug, "")

    def _clean(self, value: str | None) -> str:
        if not value:
            return ""
        return re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = TainewWalkerScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://tainew-walker.jp/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
