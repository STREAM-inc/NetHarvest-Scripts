# -*- coding: utf-8 -*-
"""
LIFULL HOME'S 注文住宅(ライフルホームズ) — 不動産会社[不動産屋]の検索

対象サイト: https://www.homes.co.jp/realtor/

取得対象:
    - 全国の不動産会社(店舗)の会社情報
      (会社名・カナ・所在地・TEL・FAX・営業時間・定休日・HP・免許番号・
       交通・所属団体名・保証協会・取引物件・特徴)

取得フロー (全件 = 都道府県ごとに探索):
    ルート(/realtor/) を起点に
      → 都道府県ページ /realtor/{pref}/ から市区町村一覧 (…-city/list/) を抽出
      → 各市区町村の店舗一覧を ?page=N でページ送り
      → 各店舗詳細 /realtor/mid-XXXX/ を開いて table.table-def を解析
      → 1 件取得するごとに即 yield (mid-ID で全国グローバル重複排除)

実行方法:
    # ローカルテスト
    python scripts/sites/realestate/lifull_home_s.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id lifull_home_s

備考:
    homes.co.jp は短時間に多数アクセスすると WAF が HTTP 202(空ボディ/チャレンジ)
    を返すことがある。get_soup() で 202/空応答をリトライしつつ、最終的に取得不能なら
    None を返して当該ページをスキップする (CONTINUE_ON_ERROR)。
"""

from __future__ import annotations

import re
import sys
import time
from pathlib import Path
from typing import Generator, List, Optional
from urllib.parse import urljoin

import bs4
import requests

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

# 47 都道府県 (homes.co.jp の romaji スラッグ, 表示名)。
# ルートページにはこの他に地方ブロック (kanto, kinki 等) のリンクも含まれるため、
# 確実な 47 件のみを明示する (全件 = 都道府県ごとに探索)。
PREFS = [
    ("hokkaido", "北海道"), ("aomori", "青森県"), ("iwate", "岩手県"),
    ("miyagi", "宮城県"), ("akita", "秋田県"), ("yamagata", "山形県"),
    ("fukushima", "福島県"), ("ibaraki", "茨城県"), ("tochigi", "栃木県"),
    ("gunma", "群馬県"), ("saitama", "埼玉県"), ("chiba", "千葉県"),
    ("tokyo", "東京都"), ("kanagawa", "神奈川県"), ("niigata", "新潟県"),
    ("toyama", "富山県"), ("ishikawa", "石川県"), ("fukui", "福井県"),
    ("yamanashi", "山梨県"), ("nagano", "長野県"), ("gifu", "岐阜県"),
    ("shizuoka", "静岡県"), ("aichi", "愛知県"), ("mie", "三重県"),
    ("shiga", "滋賀県"), ("kyoto", "京都府"), ("osaka", "大阪府"),
    ("hyogo", "兵庫県"), ("nara", "奈良県"), ("wakayama", "和歌山県"),
    ("tottori", "鳥取県"), ("shimane", "島根県"), ("okayama", "岡山県"),
    ("hiroshima", "広島県"), ("yamaguchi", "山口県"), ("tokushima", "徳島県"),
    ("kagawa", "香川県"), ("ehime", "愛媛県"), ("kochi", "高知県"),
    ("fukuoka", "福岡県"), ("saga", "佐賀県"), ("nagasaki", "長崎県"),
    ("kumamoto", "熊本県"), ("oita", "大分県"), ("miyazaki", "宮崎県"),
    ("kagoshima", "鹿児島県"), ("okinawa", "沖縄県"),
]

# 店舗詳細(ルート)URL: /realtor/mid-XXXX/ のみ。/bukken/ /coupon/ /map/ 等の下層は除外。
_RE_DETAIL = re.compile(r"/realtor/(mid-[A-Za-z0-9_]+)/?$")
_RE_POST = re.compile(r"〒?\s*(\d{3}-?\d{4})")


class LifullHomeSScraper(StaticCrawler):
    """LIFULL HOME'S 不動産会社検索 スクレイパー"""

    DELAY = 1.5
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
    )

    EXTRA_COLUMNS = [
        "交通",
        "FAX",
        "免許番号",
        "所属団体名",
        "保証協会",
        "取引物件（賃貸）",
        "取引物件（売買）",
        "特徴",
    ]

    # ------------------------------------------------------------------ #
    # 通信 (WAF 202 対策つき)
    # ------------------------------------------------------------------ #
    def _setup(self):
        """標準セッションに加え、より現行に近いブラウザヘッダを付与する。"""
        super()._setup()
        self.session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        })

    def get_soup(self, url: str) -> Optional[bs4.BeautifulSoup]:
        """homes.co.jp は WAF で 202(空/チャレンジ)を返すことがあるためリトライする。

        StaticCrawler.get_soup は 202 を成功扱い(raise_for_status 非該当)にしてしまうため、
        ここで明示的に 202 / 空ボディを検知し、待機して数回リトライ。最終的に取得不能なら
        None を返してスキップ (CONTINUE_ON_ERROR)。
        """
        self.logger.info("取得中: %s", url)
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=self.TIMEOUT)
            except requests.exceptions.RequestException as e:
                self.logger.warning("通信エラー (スキップ): %s — %s", url, e)
                return None
            if resp.status_code == 202 or not resp.text.strip():
                # WAF チャレンジ。少し待って再試行。
                self.logger.warning("WAF 応答 (HTTP %s) — リトライ %d/3: %s",
                                    resp.status_code, attempt + 1, url)
                time.sleep(2.0 * (attempt + 1))
                continue
            if resp.status_code >= 400:
                self.logger.warning("HTTP %s (スキップ): %s", resp.status_code, url)
                return None
            ctype = resp.headers.get("Content-Type", "")
            if "charset=" not in ctype.lower():
                resp.encoding = resp.apparent_encoding
            return bs4.BeautifulSoup(resp.text, "html.parser")
        self.logger.warning("WAF により取得不能 (スキップ): %s", url)
        return None

    # ------------------------------------------------------------------ #
    # 一覧 → 詳細
    # ------------------------------------------------------------------ #
    def parse(self, url: str) -> Generator[dict, None, None]:
        # 引数 url を唯一のルート(SSOT)として使う。末尾スラッシュを保証。
        root = url if url.endswith("/") else url + "/"
        seen: set[str] = set()

        for slug, pref_ja in PREFS:
            pref_url = urljoin(root, f"{slug}/")
            pref_soup = self.get_soup(pref_url)
            if pref_soup is None:
                continue

            # この都道府県の市区町村一覧 (…-city/list/) を抽出して重複排除。
            city_urls: List[str] = []
            seen_city: set[str] = set()
            city_pat = re.compile(rf"/realtor/{re.escape(slug)}/[A-Za-z0-9_]+-city/list/?$")
            for a in pref_soup.select("a[href]"):
                href = urljoin(root, a["href"].split("?")[0].split("#")[0])
                if city_pat.search(href) and href not in seen_city:
                    seen_city.add(href)
                    city_urls.append(href)
            self.logger.info("=== %s (%s): %d 市区町村 ===", pref_ja, slug, len(city_urls))

            for city_url in city_urls:
                yield from self._crawl_city(city_url, pref_ja, seen)

    def _crawl_city(self, city_url: str, pref_ja: str,
                    seen: set) -> Generator[dict, None, None]:
        """1 市区町村の店舗一覧を ?page=N で巡回し、各詳細を取得即 yield。"""
        page = 1
        prev_roots: Optional[List[str]] = None
        while True:
            page_url = city_url if page == 1 else f"{city_url}?page={page}"
            soup = self.get_soup(page_url)
            if soup is None:
                break

            roots: List[str] = []
            seen_root: set[str] = set()
            for a in soup.select("a[href]"):
                href = a["href"].split("#")[0].split("?")[0]
                m = _RE_DETAIL.search(href)
                if not m:
                    continue
                full = urljoin(city_url, href)
                if full not in seen_root:
                    seen_root.add(full)
                    roots.append(full)

            if not roots:
                break
            # ページ番号がクランプされ同じページが返ってきたら終了。
            if roots == prev_roots:
                break
            prev_roots = roots

            for detail_url in roots:
                m = _RE_DETAIL.search(detail_url)
                mid = m.group(1)
                if mid in seen:
                    continue
                seen.add(mid)
                item = self._scrape_detail(detail_url, pref_ja)
                if item:
                    yield item

            page += 1

    def _scrape_detail(self, url: str, pref_ja: str) -> Optional[dict]:
        soup = self.get_soup(url)
        if soup is None:
            return None

        # table.table-def の th→td を辞書化。
        data: dict[str, bs4.Tag] = {}
        for tbl in soup.select("table.table-def"):
            for tr in tbl.select("tr"):
                th = tr.find("th")
                td = tr.find("td")
                if th and td:
                    data.setdefault(th.get_text(strip=True), td)

        def text_of(key: str) -> str:
            td = data.get(key)
            return self._clean(td.get_text(" ", strip=True)) if td else ""

        # 会社名 + カナ (ruby/rt)
        name, name_kana = "", ""
        name_p = soup.select_one("p.realtorName")
        if name_p:
            rt = name_p.find("rt")
            if rt:
                name_kana = self._clean(rt.get_text(" ", strip=True))
                rt.extract()  # rt を除去すると残りが基底テキスト
            name = self._clean(name_p.get_text(" ", strip=True))
        if not name:
            return None  # 会社名が取れないレコードは破棄

        # HP リンク
        hp = ""
        hp_a = soup.select_one("p.forOfficialSiteLink a[href]")
        if hp_a:
            hp = hp_a["href"].strip()

        # 所在地 → 郵便番号 / 住所 ("地図を見る" 等のリンク文言を除去)
        addr_raw = ""
        addr_td = data.get("所在地")
        if addr_td:
            td = bs4.BeautifulSoup(str(addr_td), "html.parser")
            for a in td.select("a"):
                a.extract()
            addr_raw = self._clean(td.get_text(" ", strip=True))
        post_code, addr = "", addr_raw
        mp = _RE_POST.search(addr_raw)
        if mp:
            post_code = mp.group(1)
            addr = self._clean(addr_raw[mp.end():])

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.NAME_KANA: name_kana,
            Schema.PREF: pref_ja,
            Schema.POST_CODE: post_code,
            Schema.ADDR: addr,
            Schema.TEL: text_of("TEL"),
            Schema.TIME: text_of("営業時間"),
            Schema.HOLIDAY: text_of("定休日"),
            Schema.HP: hp,
            "交通": text_of("交通"),
            "FAX": text_of("FAX"),
            "免許番号": text_of("免許番号"),
            "所属団体名": text_of("所属団体名"),
            "保証協会": text_of("保証協会"),
            "取引物件（賃貸）": text_of("取引物件（賃貸）"),
            "取引物件（売買）": text_of("取引物件（売買）"),
            "特徴": text_of("特徴"),
        }

    @staticmethod
    def _clean(s: Optional[str]) -> str:
        if not s:
            return ""
        s = s.replace("　", " ")
        return re.sub(r"\s+", " ", s).strip()


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = LifullHomeSScraper()
    # 🔒 sites.yml に登録する url と完全一致させること (SSOT = sites.yml)。
    scraper.execute("https://www.homes.co.jp/realtor/")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
