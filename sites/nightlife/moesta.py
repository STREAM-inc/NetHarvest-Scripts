"""
MOESTA+（もえすた） (moe-sta.com) — 全国コンカフェ／メイドカフェ／コンセプトスナック等 店舗情報スクレイパー

取得対象:
    全国のコンカフェ・メイドカフェ・メンズコンカフェ・コンセプトスナック・
    アニソンバー・シーシャコンカフェ・リフレ等、サイト掲載の全ジャンル・全エリアの店舗。
    取得項目:
      店舗名 / サイト内業種(掲載ジャンル) / 都道府県 / 住所 / 電話番号 /
      営業時間 / 定休日 / 平均予算 / HP(公式サイト) / スマホサイト / LINE / X(Twitter) /
      掲載ページURL / 取得日時

取得フロー:
    /sitemap-shops.xml から全店舗詳細ページ (/shops/{id}) を列挙 (約3700件)
        → 各詳細ページを取得し、ラベル(div.text-gray-500)→次兄弟div(値) のペアから項目値を抽出
        → 都道府県は BreadcrumbList(JSON-LD) から取得 (住所に都道府県が無い店舗があるため)
        → X(Twitter) は本文中の x.com/twitter.com リンクから取得 (専用ラベルが無いため)
        → 1 店舗パースするごとに即 yield (途中中断しても無駄通信が起きない)

備考:
    - サイトは Next.js 製だが詳細ページは SSR 済みのため Static で取得可能。
    - sitemap-shops.xml は全ジャンル・全エリアの店舗を dedup 済みで含むため、
      エリア別一覧ページ(/cafe/shops/{area})のページ送りを個別に巡回する必要はない。
    - 電話番号欄はあるが値が無い店舗があり、その場合は空欄のまま出力する。
    - 紹介文(「備考」ラベルの長文)は著作権配慮のため取得しない。項目値のみ保存する。
    - 利用規約(https://moe-sta.com/kiyaku): スクレイピングを明示的に禁止する条項は無し
      (データの転売転用は禁止)。robots.txt は ClaudeBot に /shops/ を Allow。

実行方法:
    # ローカルテスト
    python scripts/sites/nightlife/moesta.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id moesta
"""

from __future__ import annotations

import json
import re
import sys
import warnings
from pathlib import Path
from typing import Generator
from urllib.parse import urljoin

from bs4 import XMLParsedAsHTMLWarning

# sitemap.xml を html.parser で読む際の警告を抑制 (取得自体は問題なく動作する)
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler


class MoestaCrawler(StaticCrawler):
    """MOESTA+ クローラー — 全国のコンカフェ／メイドカフェ／スナック等 店舗情報を取得"""

    DELAY = 1.0
    EXTRA_COLUMNS = ["平均予算", "スマホサイト"]

    # 店舗詳細 URL 判定 (/shops/{数字})
    _SHOP_RE = re.compile(r"/shops/\d+/?$")

    # 都道府県一覧 (BreadcrumbList からの抽出 / 住所先頭の判定に使用)
    _PREFS = (
        "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県", "茨城県",
        "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県", "新潟県", "富山県",
        "石川県", "福井県", "山梨県", "長野県", "岐阜県", "静岡県", "愛知県", "三重県",
        "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県", "鳥取県", "島根県",
        "岡山県", "広島県", "山口県", "徳島県", "香川県", "愛媛県", "高知県", "福岡県",
        "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
    )
    _PREF_SET = set(_PREFS)

    def parse(self, url: str) -> Generator[dict, None, None]:
        # url (= sites.yml の正規 URL) を唯一のルートとして店舗サイトマップを派生
        sitemap_url = urljoin(url, "/sitemap-shops.xml")
        sm = self.get_soup(sitemap_url)
        if sm is None:
            self.logger.warning("店舗サイトマップを取得できませんでした: %s", sitemap_url)
            return

        shop_urls: list[str] = []
        seen: set[str] = set()
        for loc in sm.find_all("loc"):
            href = (loc.get_text(strip=True) or "")
            if not href or not self._SHOP_RE.search(href):
                continue
            full = urljoin(url, href)
            if full not in seen:
                seen.add(full)
                shop_urls.append(full)

        self.total_items = len(shop_urls)
        self.logger.info("対象店舗数: %d", len(shop_urls))

        for shop_url in shop_urls:
            try:
                item = self._scrape_shop(shop_url)
            except Exception as e:  # noqa: BLE001 — 1店舗の失敗で全体を止めない
                self.logger.warning("店舗の処理に失敗 (スキップ): %s — %s", shop_url, e)
                continue
            if item:
                yield item

    def _scrape_shop(self, shop_url: str) -> dict | None:
        soup = self.get_soup(shop_url)
        if soup is None:
            return None

        # 店舗名: H1 を採用 (「店舗名」ラベルの値には「優良」等のバッジが混ざるため)
        h1 = soup.select_one("h1")
        name = h1.get_text(strip=True) if h1 else ""
        if not name:
            return None

        item: dict = {
            Schema.NAME: name,
            Schema.URL: shop_url,
        }

        # --- ラベル→値 (text) 系 ---
        genre = self._field_text(soup, "ジャンル")
        if genre:
            item[Schema.CAT_SITE] = genre

        tel = self._field_text(soup, "電話番号")
        tel = re.sub(r"^tel:\s*", "", tel, flags=re.IGNORECASE).strip()
        if tel:
            item[Schema.TEL] = tel

        biz_hours = self._field_text(soup, "営業時間")
        if biz_hours:
            item[Schema.TIME] = biz_hours

        holiday = self._field_text(soup, "定休日")
        if holiday:
            item[Schema.HOLIDAY] = holiday

        budget = self._field_text(soup, "平均予算（お一人様）")
        if budget:
            item["平均予算"] = budget

        # --- 住所 + 都道府県 ---
        addr = self._field_text(soup, "住所")
        pref = self._breadcrumb_pref(soup)
        if pref:
            item[Schema.PREF] = pref
        if addr:
            # 住所先頭に都道府県が含まれる場合は除去し、市区町村以降を住所に
            m = re.match(r"^(" + "|".join(map(re.escape, self._PREFS)) + r")", addr)
            if m:
                if not pref:
                    item[Schema.PREF] = m.group(1)
                item[Schema.ADDR] = addr[m.end():].strip()
            else:
                item[Schema.ADDR] = addr

        # --- URL 系 (href 優先、http/https のみ採用) ---
        hp = self._field_url(soup, "ホームページ")
        if hp:
            item[Schema.HP] = hp

        smart = self._field_url(soup, "スマホサイト")
        if smart:
            item["スマホサイト"] = smart

        line = self._field_url(soup, "LINE")
        if line:
            item[Schema.LINE] = line

        # --- X(Twitter): 専用ラベルが無いため本文中のリンクから取得 ---
        x_url = self._find_x(soup)
        if x_url:
            item[Schema.X] = x_url

        return item

    # ------------------------------------------------------------------
    # 値抽出ヘルパー
    # ------------------------------------------------------------------
    @staticmethod
    def _value_div(soup, label: str):
        """ラベル(div.text-gray-500)に一致する要素の次兄弟(値の div)を返す。"""
        for lbl in soup.find_all("div", class_="text-gray-500"):
            if lbl.get_text(strip=True) == label:
                return lbl.find_next_sibling()
        return None

    def _field_text(self, soup, label: str) -> str:
        ns = self._value_div(soup, label)
        return ns.get_text(" ", strip=True) if ns else ""

    def _field_url(self, soup, label: str) -> str:
        """URL 項目を取得。値 div 内の a[href] が http(s) ならそれを、
        無ければテキストが http(s) で始まる場合のみ採用 (placeholder は除外)。"""
        ns = self._value_div(soup, label)
        if ns is None:
            return ""
        a = ns.find("a", href=True)
        if a and a["href"].startswith("http"):
            return a["href"].strip()
        text = ns.get_text(" ", strip=True)
        if text.startswith("http"):
            return text
        return ""

    def _breadcrumb_pref(self, soup) -> str:
        """BreadcrumbList(JSON-LD) から都道府県名を取得する。"""
        for s in soup.find_all("script", type="application/ld+json"):
            raw = s.get_text()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except (ValueError, TypeError):
                continue
            candidates = data if isinstance(data, list) else [data]
            for d in candidates:
                if isinstance(d, dict) and d.get("@type") == "BreadcrumbList":
                    for it in d.get("itemListElement", []):
                        if isinstance(it, dict) and it.get("name") in self._PREF_SET:
                            return it["name"]
        return ""

    @staticmethod
    def _find_x(soup) -> str:
        """本文中の X(Twitter) プロフィールリンクを取得する。"""
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "x.com/" in href or "twitter.com/" in href:
                return href.strip()
        return ""


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = MoestaCrawler()
    scraper.execute("https://moe-sta.com/")
