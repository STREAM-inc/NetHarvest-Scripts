"""
スナカラ (旧・スナックdeカラオケnavi) — 全国スナック/カラオケ店舗スクレイパー

取得対象:
    - 全国のスナック・ラウンジ・カラオケ店舗情報
      (店舗名 / 名称カナ / 都道府県 / 郵便番号 / 住所 / TEL /
       サイト定義業種 / SNS(LINE/Instagram/X/Facebook/TikTok) / HP /
       定休日 / 営業時間 / 最寄り駅 / アクセス / 通常料金 / 備考 /
       店舗タグ / ブログURL)

取得フロー (Pattern B: 一覧→詳細、詳細URLを一定数集めたら即 yield):
    list.php (エリア索引)
      → list.php?pref={都道府県}  (47都道府県)
        → list.php?aid={エリアID}  (中エリア / 店舗)
          → list.php?bid={エリアID} (小エリア / 店舗)
    上記エリアツリーを BFS で巡回し shop.php?sno=N を重複除去しつつ収集する。
    全件の収集完了は待たず、詳細URLが一定数 (SHOP_URL_BATCH_SIZE) たまった
    時点でそのバッチの詳細ページを取得し 1件ずつ yield する (ストリーミング)。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/snakaranavi.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id snakaranavi
"""

from __future__ import annotations

import re
import sys
import time
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin, urlparse, parse_qs

from bs4 import BeautifulSoup

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler


class SnakaranaviCrawler(StaticCrawler):
    """スナカラ クローラー — 全国スナック/カラオケ店舗情報を取得"""

    DELAY = 1.0

    # Schema に対応カラムが無い項目は EXTRA_COLUMNS として出力する。
    EXTRA_COLUMNS = ["最寄り駅", "アクセス", "通常料金", "備考", "店舗タグ", "ブログURL"]

    # スナカラは「スナック情報メディア」であり、掲載店はサイト上すべて
    # スナックとして分類されているため、サイト定義業種は固定値とする。
    SITE_GENRE = "スナック"

    BASE_URL = "https://www.snakaranavi.net"
    LIST_URL = f"{BASE_URL}/list.php"

    # エリアツリー巡回時の安全上限 (無限ループ防止)
    MAX_AREA_PAGES = 5000

    # 詳細URLをこの件数ぶん集めるごとに、収集完了を待たず即座に
    # そのバッチの詳細取得→yield を行う (ストリーミング処理)。
    SHOP_URL_BATCH_SIZE = 100

    # 推定レコード数 (進捗ログ用の概算値。実数は巡回完了まで不明)
    ESTIMATED_TOTAL = 10000

    # 所在地テキスト先頭の都道府県を切り出す正規表現
    _PREF_RE = re.compile(
        r"(北海道|東京都|大阪府|京都府|"
        r"(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|新潟|"
        r"富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|兵庫|奈良|和歌山|"
        r"鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|"
        r"大分|宮崎|鹿児島|沖縄)県)"
    )
    _POST_RE = re.compile(r"〒?\s*(\d{3}-\d{4})")
    # エリア索引リンク (都道府県 / 中エリア / 小エリア)
    _AREA_LINK_RE = re.compile(r"list\.php\?(?:pref|aid|bid)=")
    _SNO_RE = re.compile(r"shop\.php\?sno=(\d+)")
    # 最寄り駅テキスト先頭の「最寄駅」ラベルを除去する
    _STATION_LABEL_RE = re.compile(r"^最寄(?:り)?駅\s*")
    # サイト自身のアカウント・共有ボタン・アプリ/地図等 (店舗のSNSではないもの)
    _SNS_EXCLUDE_RE = re.compile(
        r"snakara_navi|snakaranavi\.net|karanavi\.net|karaoke\.or\.jp|"
        r"play\.google\.com|apps\.apple\.com|maps\.google|google\.[^/]+/maps|"
        r"twitter\.com/share|/sharer|social-plugins\.line\.me|/lineit/|/intent/",
        re.I,
    )

    # ------------------------------------------------------------------
    # メインフロー
    # ------------------------------------------------------------------

    def parse(self, url: str) -> Generator[dict, None, None]:
        # 実数は巡回完了まで不明なため、進捗ログ用に推定レコード数を用いる。
        self.total_items = self.ESTIMATED_TOTAL
        self.logger.info("推定レコード数: %d", self.total_items)

        index = 0
        saved = 0
        failed = 0
        # エリア巡回と詳細取得をストリーミング処理する。詳細URLが
        # SHOP_URL_BATCH_SIZE 件たまるごとにバッチが yield されるので、
        # 全件の収集完了を待たずにそのつど詳細取得→yield する。
        for batch in self._iter_shop_url_batches(url):
            for shop_url in batch:
                index += 1
                try:
                    record = self._scrape_detail(shop_url)
                except Exception as e:  # 個別アイテムのエラーはログして継続
                    failed += 1
                    self.logger.warning(
                        "詳細取得失敗: %d/~%d 取得済み%d 失敗%d URL=%s (%s)",
                        index, self.total_items, saved, failed, shop_url, e,
                    )
                    continue

                if record:
                    saved += 1
                    self.logger.info(
                        "詳細取得OK: %d/~%d 取得済み%d 失敗%d 店舗=%s",
                        index, self.total_items, saved, failed,
                        record.get(Schema.NAME) or shop_url,
                    )
                    yield record
                else:
                    failed += 1
                    self.logger.warning(
                        "詳細取得スキップ: %d/~%d 取得済み%d 失敗%d URL=%s",
                        index, self.total_items, saved, failed, shop_url,
                    )

        self.logger.info(
            "詳細取得完了: 候補%d 取得済み%d 失敗/スキップ%d",
            index, saved, failed,
        )

    # ------------------------------------------------------------------
    # 店舗URL収集 (エリアツリーを BFS で巡回)
    # ------------------------------------------------------------------

    def _iter_shop_url_batches(
        self, seed_url: str | None = None
    ) -> Generator[list[str], None, None]:
        """エリアツリーを BFS で巡回しつつ、詳細URLを SHOP_URL_BATCH_SIZE 件
        たまるごとにバッチ (list[str]) として yield するジェネレータ。

        全件の収集完了を待たずにストリーミングで詳細URLを払い出すため、
        呼び出し側はバッチ単位で即座に詳細取得を開始できる。最後に残った
        端数バッチも巡回終了時に yield する。
        """
        # 開始URLが個別店舗ページの場合はそれだけを対象にする
        seed = seed_url or self.LIST_URL
        if "shop.php" in seed:
            m = self._SNO_RE.search(seed)
            if m:
                yield [f"{self.BASE_URL}/shop.php?sno={m.group(1)}"]
            return

        total_snos = 0
        seen_snos: set[str] = set()
        seen_area: set[str] = set()
        stack: list[str] = [self.LIST_URL]
        batch: list[str] = []

        while stack and len(seen_area) < self.MAX_AREA_PAGES:
            area_url = stack.pop()
            norm = self._norm_area_url(area_url)
            if norm in seen_area:
                continue
            seen_area.add(norm)

            soup = self.get_soup(area_url)
            if soup is None:
                continue

            added = 0
            for href in self._iter_hrefs(soup, area_url):
                # 店舗リンクを収集
                m = self._SNO_RE.search(href)
                if m:
                    sno = m.group(1)
                    if sno not in seen_snos:
                        seen_snos.add(sno)
                        total_snos += 1
                        added += 1
                        batch.append(f"{self.BASE_URL}/shop.php?sno={sno}")
                        # 一定数たまったら即座にバッチを払い出す
                        if len(batch) >= self.SHOP_URL_BATCH_SIZE:
                            self.logger.info(
                                "詳細URLバッチ払い出し: %d件 (店舗累計%d)",
                                len(batch), total_snos,
                            )
                            yield batch
                            batch = []
                    continue
                # エリアリンク (都道府県/中/小) を巡回キューへ
                if self._AREA_LINK_RE.search(href):
                    nxt = self._norm_area_url(href)
                    if nxt not in seen_area:
                        stack.append(href)

            self.logger.info(
                "エリア巡回: 訪問%d 店舗累計%d (+%d) URL=%s",
                len(seen_area), total_snos, added, area_url,
            )
            time.sleep(self.DELAY)

        # 端数 (バッチサイズ未満の残り) を最後に払い出す
        if batch:
            self.logger.info(
                "詳細URLバッチ払い出し(端数): %d件 (店舗累計%d)",
                len(batch), total_snos,
            )
            yield batch

    def _iter_hrefs(self, soup: BeautifulSoup, base_url: str) -> Generator[str, None, None]:
        for a in soup.find_all("a", href=True):
            yield urljoin(base_url, a["href"])

    def _norm_area_url(self, url: str) -> str:
        """エリアページURLを (pref|aid|bid)=値 で正規化 (#fragment や順序の揺れを吸収)"""
        full = urljoin(self.BASE_URL + "/", url)
        parsed = urlparse(full)
        qs = parse_qs(parsed.query)
        for key in ("bid", "aid", "pref"):
            if key in qs and qs[key] and qs[key][0]:
                return f"{key}={qs[key][0]}"
        return parsed.path.rstrip("/") or "/"

    # ------------------------------------------------------------------
    # 詳細ページ解析
    # ------------------------------------------------------------------

    def _scrape_detail(self, detail_url: str) -> dict | None:
        soup = self.get_soup(detail_url)
        if soup is None:
            return None

        record: dict = {Schema.URL: detail_url}

        # 店舗名 (h1)
        h1 = soup.find("h1")
        name = self._c(h1.get_text(" ", strip=True) if h1 else "")
        if not name:
            self.logger.warning("店舗名なし: %s", detail_url)
            return None
        record[Schema.NAME] = name

        # 名称カナ (#shop_name 内の .snamekana — 任意)
        kana_el = soup.select_one(".snamekana")
        if kana_el:
            kana = self._c(kana_el.get_text(" ", strip=True))
            if kana:
                record[Schema.NAME_KANA] = kana

        # サイト定義業種 (掲載店はすべてスナックとして分類されている)
        record[Schema.CAT_SITE] = self.SITE_GENRE

        # 基本情報テーブル (th -> td)
        info = self._table_dict(soup)

        # 電話番号
        tel = info.get("電話番号", "")
        if tel:
            record[Schema.TEL] = tel

        # 所在地 (〒郵便番号 + 都道府県 + 住所)
        addr_raw = info.get("所在地", "")
        if addr_raw:
            pm = self._POST_RE.search(addr_raw)
            if pm:
                record[Schema.POST_CODE] = pm.group(1)
                addr_raw = addr_raw[pm.end():].strip()
            pref, body = self._split_pref(addr_raw)
            if pref:
                record[Schema.PREF] = pref
            record[Schema.ADDR] = body or addr_raw

        # 営業時間 / 定休日
        if info.get("営業時間"):
            record[Schema.TIME] = info["営業時間"]
        if info.get("定休日"):
            record[Schema.HOLIDAY] = info["定休日"]

        # EXTRA: アクセス / 通常料金 / 備考 (テーブルの短いラベル)
        if info.get("アクセス"):
            record["アクセス"] = info["アクセス"]
        if info.get("通常料金"):
            record["通常料金"] = info["通常料金"]
        if info.get("備考"):
            record["備考"] = info["備考"]

        # EXTRA: 最寄り駅 (#shop_station — 「最寄駅」ラベルを除去)
        station_el = soup.select_one("#shop_station")
        if station_el:
            station = self._STATION_LABEL_RE.sub(
                "", self._c(station_el.get_text(" ", strip=True))
            ).strip()
            if station:
                record["最寄り駅"] = station

        # EXTRA: 店舗タグ (.shop_tag 内の #タグ を空白区切りで連結)
        tag_el = soup.select_one(".shop_tag")
        if tag_el:
            tags = " ".join(
                self._c(s.get_text(" ", strip=True))
                for s in tag_el.find_all("span")
                if s.get_text(strip=True)
            )
            if tags:
                record["店舗タグ"] = tags

        # SNS / HP / ブログURL (店舗固有の外部リンクのみ抽出)
        self._extract_links(soup, record)

        return record

    def _extract_links(self, soup: BeautifulSoup, record: dict) -> None:
        """店舗コンテンツ内の外部リンクを SNS / HP / ブログ に振り分ける。

        サイト自身のアカウント (snakara_navi) や共有ボタン、アプリ/地図リンクは
        除外し、店舗が登録した公式リンクのみを取得する。
        """
        content = soup.select_one(".in_contents") or soup
        for a in content.find_all("a", href=True):
            href = (a.get("href") or "").strip()
            if not href or href.startswith("#") or href.lower().startswith("javascript"):
                continue
            full = urljoin(self.BASE_URL + "/", href)
            if self._SNS_EXCLUDE_RE.search(full):
                continue

            low = full.lower()
            if "instagram.com" in low:
                record.setdefault(Schema.INSTA, full)
            elif "twitter.com" in low or "//x.com" in low or "://x.com" in low:
                record.setdefault(Schema.X, full)
            elif "facebook.com" in low:
                record.setdefault(Schema.FB, full)
            elif "tiktok.com" in low:
                record.setdefault(Schema.TIKTOK, full)
            elif "line.me" in low or "lin.ee" in low:
                record.setdefault(Schema.LINE, full)
            elif "ameblo.jp" in low or "blog" in low:
                record.setdefault("ブログURL", full)
            elif low.startswith("http"):
                # SNS/ブログ以外の店舗外部リンクは公式HPとみなす
                record.setdefault(Schema.HP, full)

    def _table_dict(self, soup: BeautifulSoup) -> dict[str, str]:
        result: dict[str, str] = {}
        for table in soup.find_all("table"):
            for tr in table.find_all("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if th and td:
                    key = self._c(th.get_text(" ", strip=True))
                    if key and key not in result:
                        result[key] = self._c(td.get_text(" ", strip=True))
        return result

    # ------------------------------------------------------------------
    # ユーティリティ
    # ------------------------------------------------------------------

    def _split_pref(self, address: str) -> tuple[str, str]:
        if not address:
            return "", ""
        m = self._PREF_RE.match(address)
        if not m:
            return "", address
        return m.group(1), address[m.end():].strip()

    def _c(self, value: str | None) -> str:
        if not value:
            return ""
        return re.sub(r"\s+", " ", str(value)).strip()


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    scraper = SnakaranaviCrawler()
    scraper.execute("https://www.snakaranavi.net/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
