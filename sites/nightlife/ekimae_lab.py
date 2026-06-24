"""
エキラボ (ekimae-lab.com) — 郡山駅前ナイトビジネス店舗ポータル スクレイパー

取得対象:
    - 全ジャンル/エリア (キャバクラ・クラブ / ガールズバー・コンカフェ /
      スナック・ラウンジ / セクキャバ / ホストクラブ / バー・その他 / 各エリア) の
      掲載店舗 (推定 約54件)
    - 店舗名 / 名称カナ / 都道府県 / 住所 / TEL / 営業時間 / 定休日 /
      サイト定義ジャンル / SNS
    - サイト固有 (EXTRA): 予算

取得フロー:
    1. 引数 url を起点に店舗一覧ページ (/shop/list/1) を取得し、ジャンル/エリア別の
       一覧URL (/shop/list/N) をナビゲーションから動的に収集する (カテゴリIDをハードコードしない)
    2. 各一覧をページ送り (?p=N) しながら店舗詳細リンク (/shop/{slug}/) を抽出
    3. 重複を除外しつつ、詳細を1件取得するごとに即 yield する (早期 yield)

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/ekimae_lab.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id ekimae_lab
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from bs4 import BeautifulSoup

from src.const.schema import Schema
from src.framework.static import StaticCrawler

_PREF_PATTERN = re.compile(
    r"(北海道|東京都|京都府|大阪府|"
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

# 店舗詳細パス: /shop/{slug}/ (list / recruit / immediate 等の機能ページは除外)
_SHOP_PATH_RE = re.compile(r"^/shop/(?!list|recruit|immediate\b)[a-z0-9_-]+/?$")
# 一覧パス: /shop/list/{id}
_LIST_PATH_RE = re.compile(r"^/shop/list/\d+/?$")

# 一覧ページの最大ページ数 (無限ループ防止のセーフティ)
_MAX_PAGES = 30

# 詳細テーブルから抽出するラベル → EXTRA カラム名 (Schema に該当しない構造化情報)
_EXTRA_LABELS = {
    "予算": "予算",
}


class EkimaeLabScraper(StaticCrawler):
    """エキラボ (ekimae-lab.com) スクレイパー"""

    DELAY = 1.5
    EXTRA_COLUMNS: list[str] = list(_EXTRA_LABELS.values())

    # ------------------------------------------------------------------ #
    # メインフロー (引数 url を唯一のルートとして使用)
    # ------------------------------------------------------------------ #

    def parse(self, url: str) -> Generator[dict, None, None]:
        list_urls = self._collect_list_urls(url)
        self.logger.info("対象一覧URL: %d件", len(list_urls))

        seen_shops: set[str] = set()
        for list_url in list_urls:
            for shop_url in self._iter_shop_urls(list_url):
                if shop_url in seen_shops:
                    continue
                seen_shops.add(shop_url)
                try:
                    record = self._scrape_detail(shop_url)
                except Exception as e:  # 個別店舗の失敗は握りつぶして続行
                    self.logger.warning("詳細取得失敗: %s (%s)", shop_url, e)
                    continue
                if record:
                    self.total_items = len(seen_shops)
                    self.logger.info(
                        "取得: %s (%s)",
                        record.get(Schema.NAME) or "?",
                        record.get(Schema.CAT_SITE) or "",
                    )
                    yield record

    # ------------------------------------------------------------------ #
    # 一覧URLの収集 (ジャンル/エリアのナビから動的取得)
    # ------------------------------------------------------------------ #

    def _collect_list_urls(self, root_url: str) -> list[str]:
        """url 起点の /shop/list/1 を取得し、ジャンル/エリア別一覧URLを動的収集。"""
        seed = urljoin(root_url, "/shop/list/1")
        urls: list[str] = [seed]
        seen: set[str] = {self._strip_page(seed)}

        soup = self.get_soup(seed)
        if soup is not None:
            for a in soup.find_all("a", href=True):
                abs_url = urljoin(root_url, a["href"])
                path = re.sub(r"^https?://[^/]+", "", abs_url).split("?")[0]
                if _LIST_PATH_RE.match(path):
                    key = self._strip_page(abs_url)
                    if key not in seen:
                        seen.add(key)
                        urls.append(key)
        return urls

    def _iter_shop_urls(self, list_url: str) -> Generator[str, None, None]:
        """一覧を ?p=N でページ送りしながら店舗詳細URLを順次 yield。"""
        seen_in_list: set[str] = set()
        for page in range(1, _MAX_PAGES + 1):
            page_url = list_url if page == 1 else f"{list_url}?p={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                break

            new_found = False
            for a in soup.find_all("a", href=True):
                href = a["href"]
                path = re.sub(r"^https?://[^/]+", "", urljoin(list_url, href)).split("?")[0]
                if not _SHOP_PATH_RE.match(path):
                    continue
                shop_url = urljoin(list_url, path)
                if shop_url in seen_in_list:
                    continue
                seen_in_list.add(shop_url)
                new_found = True
                yield shop_url

            # 新規店舗が無ければ末尾ページとみなして終了
            if not new_found:
                break

    # ------------------------------------------------------------------ #
    # 詳細ページ
    # ------------------------------------------------------------------ #

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        name, kana = self._name_kana(soup)
        if not name:
            return None

        info = self._info_map(soup)
        addr = self._clean(info.get("住所", ""))
        pref = self._pref(addr)
        if pref and addr.startswith(pref):
            addr = addr[len(pref):].strip()
        sns = self._detect_sns(soup)

        record = {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.NAME_KANA: kana,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: self._first_tel(info.get("電話番号", "")),
            Schema.CAT_SITE: self._genre(soup),
            Schema.TIME: self._clean(info.get("営業時間", "")),
            Schema.HOLIDAY: self._clean(info.get("店休日", "")),
            Schema.LINE: sns["line"],
            Schema.INSTA: sns["insta"],
            Schema.X: sns["x"],
            Schema.FB: sns["fb"],
            Schema.TIKTOK: sns["tiktok"],
        }
        for label, col in _EXTRA_LABELS.items():
            record[col] = self._clean(info.get(label, ""))
        return record

    def _name_kana(self, soup: BeautifulSoup) -> tuple[str, str]:
        """店舗名テーブルの <p class="name"><span>名称</span>カナ</p> から名称とカナを分離。"""
        p = soup.select_one("table.tableBox p.name")
        if p:
            span = p.find("span")
            name = self._clean(span.get_text()) if span else ""
            kana = self._clean(p.get_text().replace(name, "", 1)) if name else self._clean(p.get_text())
            if name:
                return name, kana
        # フォールバック: h1 "NAME(カナ) - ジャンル"
        h1 = soup.find("h1")
        if h1:
            text = self._clean(h1.get_text(" "))
            text = re.split(r"\s*-\s*", text)[0]
            m = re.match(r"(.+?)\s*[（(]\s*(.+?)\s*[)）]", text)
            if m:
                return self._clean(m.group(1)), self._clean(m.group(2))
            return text, ""
        return "", ""

    def _genre(self, soup: BeautifulSoup) -> str:
        """h1 "NAME(カナ) - ジャンル" の末尾ジャンルをサイト定義業種として採用。"""
        h1 = soup.find("h1")
        if not h1:
            return ""
        text = self._clean(h1.get_text(" "))
        parts = re.split(r"\s*-\s*", text)
        return self._clean(parts[-1]) if len(parts) > 1 else ""

    def _info_map(self, soup: BeautifulSoup) -> dict[str, str]:
        """店舗情報テーブル (table.tableBox) の th/td ペアをラベル辞書化。

        料金テーブル等の他 tableBox は th が無いか既知ラベルに一致しないため自然に除外される。
        """
        info: dict[str, str] = {}
        for tr in soup.select("table.tableBox tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not th or not td:
                continue
            label = self._clean(th.get_text())
            if label and label not in info:
                info[label] = self._clean(td.get_text(" "))
        return info

    def _detect_sns(self, soup: BeautifulSoup) -> dict[str, str]:
        """店舗名テーブル内 SNS アイコン (ul.listSnsIcon) のアンカーから SNS を判定。"""
        sns = {"insta": "", "x": "", "fb": "", "line": "", "tiktok": ""}
        for a in soup.select("table.tableBox ul.listSnsIcon a[href]"):
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

    @staticmethod
    def _strip_page(u: str) -> str:
        return u.split("?")[0]

    @staticmethod
    def _pref(address: str) -> str:
        m = _PREF_PATTERN.search(address)
        return m.group(1) if m else ""

    @staticmethod
    def _first_tel(text: str) -> str:
        if not text:
            return ""
        m = _TEL_PATTERN.search(text)
        return m.group(0) if m else ""

    @staticmethod
    def _clean(value: str | None) -> str:
        if not value:
            return ""
        return re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = EkimaeLabScraper()
    # 🔒 この URL は sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://ekimae-lab.com/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
