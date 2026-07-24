# -*- coding: utf-8 -*-
"""
全宅管理（全国賃貸不動産管理業協会） — 会員店紹介

取得対象:
    - 全宅管理の会員店（賃貸不動産管理業者）の店舗概要

取得フロー:
    area_search（都道府県一覧）
      → /shop/town_search/{pref}/（市区町村ごとの会員店リスト）
        → /shop/info/{town}?page=N（会員店リスト・ページネーション）
          → /shop/detail/{id}.html（店舗概要）を取得して即 yield

実行方法:
    # ローカルテスト
    python scripts/sites/realestate/chinkan_jp.py

    # Prefect Flow 経由
    docker compose exec worker python /app/bin/run_flow.py --site-id chinkan_jp
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.framework.static import StaticCrawler
from src.const.schema import Schema

_PREF_PATTERN = re.compile(
    r"^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|"
    r"埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|"
    r"岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|"
    r"鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|"
    r"佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)"
)

_DETAIL_ID = re.compile(r"/shop/detail/(\w+)\.html")
_MAX_PAGES = 100  # 1 会員店リストあたりの安全上限


def _norm(text: str) -> str:
    """全角スペースを半角に寄せて余分な空白を畳む。"""
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.replace("　", " ")).strip()


class ChinkanJpScraper(StaticCrawler):
    """全宅管理 会員店紹介 スクレイパー（一覧→詳細 / 取得即 yield）"""

    DELAY = 1.5
    EXTRA_COLUMNS = [
        "FAX",
        "宅建免許番号",
        "交通",
    ]

    def parse(self, url: str):
        seen: set[str] = set()

        soup = self.get_soup(url)
        if soup is None:
            return

        # 都道府県ページ（/shop/town_search/{pref}/）を列挙
        pref_urls = []
        for a in soup.select('a[href*="/shop/town_search/"]'):
            pref_url = urljoin(url, a["href"])
            if pref_url not in pref_urls:
                pref_urls.append(pref_url)

        for pref_url in pref_urls:
            psoup = self.get_soup(pref_url)
            if psoup is None:
                continue

            # 市区町村の会員店リストページ（/shop/info/{town}）を列挙
            info_urls = []
            for a in psoup.select('a[href*="/shop/info/"]'):
                info_url = urljoin(url, a["href"].split("?")[0])
                if info_url not in info_urls:
                    info_urls.append(info_url)

            for info_url in info_urls:
                yield from self._crawl_info(info_url, url, seen)

    def _crawl_info(self, info_url: str, root_url: str, seen: set):
        """会員店リストページをページ送りしながら詳細を取得即 yield する。"""
        for page in range(1, _MAX_PAGES + 1):
            lsoup = self.get_soup(f"{info_url}?page={page}")
            if lsoup is None:
                return

            detail_urls = []
            for a in lsoup.select('a[href*="/shop/detail/"]'):
                detail_urls.append(urljoin(root_url, a["href"]))

            # このページに詳細リンクが無ければ末尾に到達
            if not detail_urls:
                return

            new_found = False
            for detail_url in detail_urls:
                m = _DETAIL_ID.search(detail_url)
                shop_id = m.group(1) if m else detail_url
                if shop_id in seen:
                    continue
                seen.add(shop_id)
                new_found = True

                try:
                    item = self._scrape_detail(detail_url)
                except Exception as e:  # noqa: BLE001
                    self.logger.warning("詳細取得失敗 %s: %s", detail_url, e)
                    continue
                if item:
                    yield item

            # 新規リンクが 1 件も無ければ実質末尾（重複ページ返却）とみなす
            if not new_found:
                return

    def _scrape_detail(self, url: str) -> dict | None:
        soup = self.get_soup(url)
        if soup is None:
            return None

        name_el = soup.select_one("h2.main_company_name") or soup.select_one("h3.company_name")
        name = _norm(name_el.get_text()) if name_el else ""
        if not name:
            return None

        # 店舗概要（ラベル→値）を辞書化
        fields: dict[str, str] = {}
        for w in soup.select("div.outline_wrapper"):
            title = w.select_one(".outline_title")
            desc = w.select_one(".outline_description")
            if not title:
                continue
            label = _norm(title.get_text())
            value = _norm(desc.get_text(" ")) if desc else ""
            fields[label] = value

        addr = fields.get("事務所所在地", "")
        pref = ""
        m = _PREF_PATTERN.match(addr)
        if m:
            pref = m.group(1)
            addr = addr[m.end():].strip()

        # 宅建免許番号: 値の先頭にラベルが重複して入るため除去
        license_no = fields.get("宅建免許番号", "")
        license_no = re.sub(r"^宅建免許番号\s*", "", license_no).strip()

        return {
            Schema.URL: url,
            Schema.NAME: name,
            Schema.PREF: pref,
            Schema.ADDR: addr,
            Schema.TEL: fields.get("TEL", ""),
            Schema.EMAIL: fields.get("MAIL", ""),
            Schema.REP_NM: fields.get("代表者名", ""),
            Schema.HP: fields.get("ホームページ", ""),
            Schema.TIME: fields.get("営業時間", ""),
            Schema.HOLIDAY: fields.get("定休日", ""),
            "FAX": fields.get("FAX", ""),
            "宅建免許番号": license_no,
            "交通": fields.get("交通", ""),
        }


if __name__ == "__main__":
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    scraper = ChinkanJpScraper()
    # 🔒 sites.yml に登録する url と完全一致させること (SSOT = sites.yml)
    scraper.execute("https://chinkan.jp/shop/area_search")

    print(f"\n出力ファイル: {scraper.output_filepath}")
    print(f"取得件数: {scraper.item_count}")
